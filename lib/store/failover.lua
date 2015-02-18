-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local logger = require('../logger')
local JSON = require('json')
local http = require('http')
local timer = require('timer')
local table = require('table')
local lmmdb = require("../lmmdb")
Env = lmmdb.Env
DB = lmmdb.DB
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor


return function(Store)
	function Store:add_self_to_cluster(member,ip,port,cb)
		local body = JSON.stringify(member)
		local options = {
			host = ip,
			port = port,
			method = 'post',
			path = "/store/servers/" .. self.id,
			headers = {
				["Content-Type"] = "application/json",
				["Content-Length"] = #body}
		}

		local req = http.request(options, function (res)
			local chunks = {}
			res:on('data', function (chunk)
				chunks[#chunks + 1] = chunk
				end)
			res:on('end',function()
				if (res.status_code > 199) and (res.status_code < 300) then
					logger:info("node was added to cluster")
					cb()
				else
					logger:error('got a bad response',table.concat(chunks))
					cb("unable to sync with remote server")
				end
			end)
		end)
		req:on('error',cb)
		req:on('end',function() end)
		logger:info("adding this node to the cluster",body,self.id)
		req:done(body)
	end



	function Store:cancel_sync(ip,port)
		local sync = self.connections[ip .. ":" .. port]
		if sync.timer then
			timer.clearTimer(sync.timer)
		end
		if sync.connection then
			sync.connection:destroy()
		end
		self.connections[ip .. ":" .. port] = nil
	end

	function Store:begin_sync(ip,port,cb)
		local key = ip .. ":" .. port
		if self.connections[key] and self.connections[key].connection then
			logger:info("already syncing with remote",ip,port)
			if cb then
				cb()
			end
		else
			if self.connections[key] and  self.connections[key].timer then
				timer.clearTimer(self.connections[key].timer)
			end
			self.connections[key] = {}
			local options = {
				host = ip,
				port = port,
				method = 'get',
				path = "/store/sync/" .. tostring(self.version)
			}

			local req = http.request(options, function (res)
				self.connections[key].connection = res
				local broadcast = false
				local syncd = false
				local storage = {
					storage = {},
					storage_idx = {}
				}
				local txn,err = Env.txn_begin(self.env,nil,0)
				if err then
					logger:error("unable to begin txn to clear log")
					
				else
					local dropped = false
					local need_refresh = false
					local new_version = 0

					res:on('data', function (chunk)
						local event = JSON.parse(chunk)
						if event == true then
							logger:info("commiting all the data")
							local member,err = self:fetch("servers",self.id)
							err = Txn.commit(txn)
							syncd = true

							if err then
								if cb then
									cb(err)
									cb = nil
								end
							else
								self:add_self_to_cluster(member,ip,port,function(err)
									if need_refresh and not err then
										-- at this point, all systems need to be rebuilt
										-- on this node
										self:emit("refresh")
									end
									if cb then
										cb(err)
										cb = nil
									end
								end)
							end
							logger:info("updating version to",new_version)
							self.version = new_version
						elseif syncd then
							if event.data.last_updated > self.version then
									self.version = event.data.last_updated
								end
							if event.action == "store" then
								event = event.data
								logger:info("sync (store)",event.bucket,event.id)
								self:_store(event.bucket,event.id,event,true,true)
							elseif event.action == "delete" then
								event = event.data
								logger:info("sync (delete)",event.bucket,event.id)
								self:_delete(event.bucket,event.id,true,true)
							end
						else
							if not dropped then
								need_refresh = true
								dropped = true
								-- the first thing to happen in this transaction is we
								-- need to drop everything because we are performing a
								-- complete sync of a remote database
								local objects,err = DB.open(txn,"objects",0)
								local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
								DB.drop(txn,objects,0)
								DB.drop(txn,buckets,0)
							end
							-- we don't want to broadcast these changes, they aren't
							-- available yet.
							self:_store(event.bucket,event.id,event,true,false,txn)
							if event.last_updated > new_version then
								new_version = event.last_updated
							end
						end
					end)
				end
			end)
			req:on('error',function(err) 
				logger:warning("unable to sync with store",err)
				if cb then
					cb(err.message)
				end
				self.connections[key].timer = timer.setTimeout(5000,function() self:begin_sync(ip,port,cb) end)
				self.connections[key].connection = nil
			end)
			req:on('end',function()
				self.connections[key].timer = timer.setTimeout(5000,function() self:begin_sync(ip,port,cb) end)
				self.connections[key].connection = nil
			end)
			req:done()
		end
	end

	function Store:sync(version)
		local ops = {}
		local synced = false
		local sync = Emitter:new()
		logger:info("syncing",version)
		version = tonumber(version)
		timer.setTimeout(0, function ()
	
			local txn,err = Env.txn_begin(self.env,nil,0)
			if err then
				logger:error("unable to begin txn to clear log")
				return
			end

			logger:info("sync transaction begun",version)

			local logs,err = DB.open(txn,"logs",DB.MDB_DUPSORT)
			if err then
				logger:info("unable to open 'logs' DB",err)
				return nil,err
			end
			local cursor,err = Cursor.open(txn,logs)
			if err then
				logger:info("unable to create cursor",err)
				return nil,err
			end

			logger:info("log cursor open")

			local key,op = Cursor.get(cursor,version,Cursor.MDB_SET_KEY,"unsigned long*")
			
			logger:info("comparing last known logs",self.version,key,version)

			if not key then
				key = self.version
			else
				key = key[0]
			end			
			if (key and not (key == version)) or (not key and not(self.version == version) ) then
				logger:info("performing full sync")
				local objects,err = DB.open(txn,"objects",0)
				if err then
					logger:info("unable to open 'objects' DB",err)
					return nil,err
				end

				local obj_cursor,err = Cursor.open(txn,objects)
				if err then
					logger:info("unable to create cursor",err)
					return nil,err
				end
				local id,json,err = Cursor.get(obj_cursor,version,Cursor.MDB_FIRST)
				while json do
					sync:emit("event",json)
					id,json,err = Cursor.get(obj_cursor,id,Cursor.MDB_NEXT)
				end	
				Cursor.close(obj_cursor)
			else
				logger:info("performing partial sync")
				while key do
					if op then
						logger:info("syncing",op)
						sync:emit("event",op)
					end
					key,op,err = Cursor.get(cursor,nil,Cursor.MDB_NEXT)
				end
			end
			logger:info('sync is complete')
			sync:emit("event","true")
			Cursor.close(cursor)
			Txn.abort(txn)
			synced = true
			for _,op in pairs(ops) do
				sync:emit("event",op)
			end
			ops = nil
		end)

		self:on("sync",function(op)
			if synced then
				sync:emit("event",op)
			else
				ops[#ops + 1] = op
			end
		end)
		return sync
	end
end
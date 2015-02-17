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

	function Store:slave_of(ip,port,cb)
		logger:info("slave of",ip,port)
		if self.connection then
			timer.clearTimer(self.connection)
		end
		self.is_master = false
		self.master.ip = ip
		self.master.port = port
		
		-- we need to start syncing up with the master
		self:start_sync(cb)
	end

	function Store:promote_to_master(cb)
		logger:info("i am new new master for the store")
		self.is_master = true
		if self.connection then
			-- this may need to wait for all data from the old master
			timer.clearTimer(self.connection)
		end
		cb()
	end

	function Store:add_self_to_cluster(cb)
		local member,err = self:fetch("servers",self.id)
		local body = JSON.stringify(member)
		local options = {
			host = self.master.ip,
			port = self.master.port,
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

	function Store:start_sync(cb)
		local options = {
			host = self.master.ip,
			port = self.master.port,
			method = 'get',
			path = "/store/sync/" .. tostring(self.version)
		}

		self.connection = nil
		local req = http.request(options, function (res)
			local broadcast = false
			local syncd = false
			local storage = {
				storage = {},
				storage_idx = {}
			}
			local txn,err = Env.txn_begin(self.env,nil,0)
			if err then
				logger:error("unable to begin txn to clear log")
				if cb then
					cb("unable to begin sync txn")
				end
			else
				local dropped = false
				local need_refresh = false
				res:on('data', function (chunk)
					local event = JSON.parse(chunk)
					if event == true then
						logger:info("commiting all the data")
						err = Txn.commit(txn)
						syncd = true

						if err then
							if cb then
								cb(err)
								cb = nil
							end
						else
							self:add_self_to_cluster(function(err)
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
					elseif syncd then
						if event.action == "store" then
							logger:info("got store update (store)",event.bucket,event.id)
							event = event.data
							self:_store(event.bucket,event.id,event,true,true)
						elseif event.action == "delete" then
							logger:info("got store update (delete)",event.bucket,event.id)
							event = event.data
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
					end
				end)
			end
		end)
		req:on('error',function(err) 
			logger:warning("unable to sync with store",err)
			if cb then
				cb(err.message)
			end
			self.connection = timer.setTimeout(5000,function() self:start_sync(cb) end)
		end)
		req:on('end',function()
			self.connection = timer.setTimeout(5000,function() self:start_sync(cb) end)
		end)
		req:done()
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
		logger:info("comparing last known logs",key,version)
		if (key and not (key[0] == version)) or not key then
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
				logger:info("syncing",id)
				sync:emit("event",json)
				id,json,err = Cursor.get(obj_cursor,id,Cursor.MDB_NEXT)
			end	
			Cursor.close(obj_cursor)
		else
			logger:info("performing partial sync")
			while key do
				logger:info("syncing",key)
				sync:emit("event",op)
				key,op,err = Cursor.get(cursor,key,Cursor.MDB_NEXT)
			end
		end
		logger:info('full sync is complete')
		sync:emit("event","true")
		Cursor.close(cursor)
		Txn.abort(txn)
		local synced = true
	end)

	self:on(sync,function(op)
		if synced then
			logger:info("tailing",op)
			sync:emit("event",op)
		else
			ops[#ops + 1] = op
		end
	end)
	return sync
end
end
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

local logger = require('../logger')
local JSON = require('json')
local http = require('http')
local timer = require('timer')
local table = require('table')

return function(Store)

	function Store:slave_of(ip,port,cb)
		logger:info("slave of",ip,port)
		if self.connection then
			timer.clearTimer(self.connection)
		end
		self.is_master = false
		self.master.ip = ip
		self.master.port = port
		
		-- we need to start syncing up with the slave
		self:sync(cb)
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

	function Store:add_self_to_cluster(member,cb)
		local copy = self:prepare_json(member)
		copy.systems = nil
		local body = JSON.stringify(copy)
		local options = {
			host = self.master.ip,
			port = self.master.port,
			method = 'post',
			path = "/store/servers/" .. copy.id,
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
					cb()
				else
					logger:error('got a bad response',table.concat(chunks))
					cb("unable to sync with remote server")
				end
			end)
		end)
		req:on('error',cb)
		req:on('end',function() end)
		req:done(body)
	end

	function Store:sync(cb)
		local options = {
			host = self.master.ip,
			port = self.master.port,
			method = 'get',
			path = "/store/sync/1"
		}

		self.connection = nil
		local req = http.request(options, function (res)
			local broadcast = false
			local storage = {
				storage = {},
				storage_idx = {}
			}
			res:on('data', function (chunk)
				logger:info(chunk)
				local event = JSON.parse(chunk)
				logger:info("got store event",event)
				if event.kind == "delete" then
					self:_delete(storage,event.bucket,event.id,event.object.last_updated,true,broadcast)
				elseif event.kind == "store" then
					self:_store(storage,event.bucket,event.id,event.object,event.object.last_updated,true,broadcast)
				elseif event.kind == "sync'd" then
					broadcast = true
					-- we store off our self so that we can be added back in.
					local member = self:fetch("servers",self.id)
					-- self:_store(storage,"servers",self.id,member,0,true,false)
					
					self.storage = storage
					-- this could take a long time to finish
					-- TODO i need a better solution that also deleted data that
					-- is not needed
					for _idx,b_id in pairs({'stores','systems','servers'}) do
						local bucket = storage.storage[b_id]
						if bucket then
							for id,object in pairs(bucket) do
								self:emit(b_id,"store",id,object)
								self:emit("sync",b_id,"store",id,object)
							end
						end
					end
					for b_id,bucket in pairs(storage.storage) do
						if (b_id == "stores") or (b_id == "systems") or (b_id == "servers") then
							logger:debug("skipping",b_id)
						else
							if bucket then
								for id,object in pairs(bucket) do
									local copy = self:prepare_json(object)
									self:emit(b_id,"store",id,copy)
									self:emit("sync",b_id,"store",id,copy)
								end
							end
						end
					end
					logger:info("store is now in sync")
					self:add_self_to_cluster(member,cb)
				else
					logger:info("unknown event received:",event)
				end
				end)
			end)
		req:on('error',function(err) 
			logger:warning("unable to sync with store",err)
			self.connection = timer.setTimeout(5000,self.sync,self,cb)
		end)
		req:on('end',function() 
			self.connection = timer.setTimeout(5000,self.sync,self,cb)
		end)
		req:done()
	end

	function Store:to_json(version)
		local streamer = Emitter:new()
		local bounce = function(b_id,kind,id,object)
			streamer:emit('event',{bucket = b_id,kind = kind,id = id,object = object})
		end
		self:on("sync",bounce)
		streamer.close = function()
			self.removeListener("sync",bounce)
		end
		timer.setTimeout(0,function()
			-- this can probably block the entire server for quite some time.
			-- maybe cause the master to be flagged as down?
			-- TODO this should be split up across multiple async callbacks.
			-- maybe as an idle callback in luv?
			for b_id,bucket in pairs(self.storage.storage) do
				for id,object in pairs(bucket) do

					streamer:emit('event',{bucket = b_id,kind = 'store',id = id,object = self:prepare_json(object)})
				end
			end
			streamer:emit('event',{kind = 'sync\'d'})
		end)
		return streamer
	end
end
-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   27 Jan 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local logger = require('./logger')
local JSON = require('json')
local http = require('http')
local Timer = require('timer')
local fs = require('fs')
local hrtime = require('uv').Process.hrtime

local Store = Emitter:extend()

function Store:initialize(id,version,ip,port)
	self.id = id
	self.version = version
	self.storage = {
		storage = {},
		storage_idx = {}
	}
	self.ip = ip
	self.port = port
	self.is_master = true
	self.master = {}
end

function Store:open(cb)
	local alive = 
		{["$script"] = 
[[
return function(data,cb)
	local member,err = store:fetch_idx("servers",data[1])
	if (store.ip == member.ip) and (store.port == member.port) then
		store:slave_of(member.ip,member.port,cb)
	else
		store:promote_to_master(cb)
	end
end
]]}

	local default = {
		alive = "update_master",
		["type"] = "choose_one"}
	self:_store(self.storage,"store","update_master",alive,0,false,true)
	self:_store(self.storage,"systems","store",default,0,false,true)

	local replicated = 
		{["$script"] = fs.readFileSync('./lib/plan/topologies/replicated.lua')}
	local round_robin = 
		{["$script"] = fs.readFileSync('./lib/plan/topologies/round_robin.lua')}
	local choose_one = 
		{["$script"] = fs.readFileSync('./lib/plan/topologies/choose_one.lua')}

	assert(self:_store(self.storage,"topologies","replicated",replicated,0,false,true))
	assert(self:_store(self.storage,"topologies","round_robin",round_robin,0,false,true))
	assert(self:_store(self.storage,"topologies","choose_one",choose_one,0,false,true))
	logger:info('loaded store from disk')
	cb()
end

function Store:slave_of(ip,port,cb)
	if self.is_master then
		logger:info("slave of",ip,port)
		self.is_master = false
		self.master.ip = ip
		self.master.port = port
		-- i need to connect up to the new master
		-- ask it for who it thinks is the master
		-- maybe connect there
		-- and issue a sync to pull down the data.
		-- and then add myself into the cluster
		self:sync(cb)
	elseif cb then
		cb("already a member of a cluster, or joining is in progress")
	end
end

function Store:promote_to_master(cb)
	logger:info("i am new new master for the store")
	self.is_master = true
	cb()
end

function Store:fetch(b_id,id)
	local bucket = self.storage.storage[b_id]
	if bucket then
		if id then
			local object = bucket[id]

			if not(object == nil) then
				return object
			else
				return nil,"not found"
			end
		else
			return bucket
		end
	else
		return nil,"not found"
	end
end

function Store:fetch_idx(b_id,idx)
	local bucket = self.storage.storage_idx[b_id]
	if bucket then
		if idx then
			local object = bucket[idx]
			if not(object == nil) then
				return object
			else
				return nil,"not found"
			end
		else
			return bucket
		end
	else
		return nil,"not found"
	end
end

function Store:store(b_id,id,data,last_known)
	if self.is_master then
		return self:_store(self.storage,b_id,id,data,last_known,false,true)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:delete(b_id,id,last_known)
	if self.is_master then
		return self:_delete(self.storage,b_id,id,last_known,false,true)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:_store(store,b_id,id,data,last_known,sync,broadcast)
	local bucket = store.storage[b_id]
	local bucket_idx = store.storage_idx[b_id]
	if bucket == nil then
		bucket = {}
		bucket_idx = {}
		store.storage[b_id] = bucket
		store.storage_idx[b_id] = bucket_idx
	end
	local object = bucket[id]
	if sync == false then
		logger:info("sync",sync,sync == false)
		if not(object == nil) then
			
			if not (object.last_updated == last_known) then
				return object,"try again"
			end
			data.last_updated = math.floor(hrtime() * 100)
			data.created_at = object.created_at
			data.idx = object.idx
		else
			data.created_at = math.floor(hrtime() * 100)
			data.last_updated = data.created_at
			data.idx = #bucket_idx + 1
		end
	end
	data.id = id
	local compiled,err = self:compile(data,b_id,id)
	if err then
		return nil,err
	else
		bucket[id] = compiled
		bucket_idx[compiled.idx] = compiled
		logger:info(bucket_idx,sync)
		if broadcast then
			logger:info("broadcasting",data)
			self:emit(b_id,"store",id,data)
			self:emit("all",b_id,"store",id,data)
		end
		return compiled
	end
end

function Store:_delete(store,b_id,id,last_known,sync,broadcast)
	local bucket = store.storage[b_id]
	local bucket_idx = store.storage_idx[b_id]
	if not(bucket == nil) then
		local object = bucket[id]
		if not(object == nil) then
			if (sync == false) and not(object.last_updated == last_known) then
				return object,"try again"
			end
			bucket[id] = nil
			-- just a little expensive. and will cause data elements
			-- to be redistributed for anything other then replicated
			table.remove(bucket_idx,object.idx)
			if #bucket_idx == 0 then
				store.storage[b_id] = nil
				store.storage_idx[b_id] = nil
			end

			-- we need to re-adjust the idx member in all elements
			-- this could get extremely expensive. TODO
			-- maybe this should be a linked list? insertions would always
			-- be at the end, but removals would only need to jump to the
			-- correct element. and we know the correct already.
			for i = object.idx, #bucket_idx do
				local data = bucket_idx[i]
				data.idx = data.idx - 1
			end
			if broadcast then
				self:emit(b_id,"delete",id)
				self:emit("all",b_id,"delete",id,object)
			end
		end
	end
end

function Store:compile(data,bucket,id)
	local script = data["$script"]
	if script then
		local fn,err = loadstring(script, '@store/bucket:' .. bucket .. '/script:' .. id)
		if err then
			return nil,err
		end
		setfenv(fn, 
			{__filename = id
			,__dirname = bucket
			,store = self
			,logger = logger})
		data.script = fn
	end
	return data
end

function Store:add_self_to_cluster(member,cb)
	logger:info("got",member,self.id)
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
	
	logger:info('syncing upto cluster',body)
	req:done(body)
end

function Store:sync(cb)
	local options = {
		host = self.master.ip,
		port = self.master.port,
		method = 'get',
		path = "/store/sync/1"
	}

	local req = http.request(options, function (res)
		local broadcast = false
		local storage = {
			storage = {},
			storage_idx = {}
		}
		res:on('data', function (chunk)
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
				self:_store(storage,"servers",self.id,member,0,true,false)
				
				self.storage = storage
				-- this could take a long time to finish
				-- TODO i need a better solution that also deleted data that
				-- is not needed
				for _idx,b_id in pairs({'stores','systems','servers'}) do
					local bucket = storage.storage[b_id]
					if bucket then
						for id,object in pairs(bucket) do
							self:emit(b_id,"store",id,object)
							self:emit("all",b_id,"store",id,object)
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
	req:done()
end

function Store:to_json(version)
	local streamer = Emitter:new()
	local bounce = function(b_id,kind,id,object)
		streamer:emit('event',{bucket = b_id,kind = kind,id = id,object = object})
	end
	self:on('all',bounce)
	streamer.close = function()
		self.removeListener('all',bounce)
	end
	Timer.setTimeout(0,function()
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

function Store:prepare_json(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in pairs(orig) do
        		if not (type(orig_value) == 'function') then
	            copy[orig_key] = orig_value
	           end
        end
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end

return Store
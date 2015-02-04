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
local table = require('table')
local hrtime = require('uv').Process.hrtime

local Store = Emitter:extend()
require('./store/failover')(Store)
require('./store/storage')(Store)

function Store:initialize(path,id,version,ip,port,api)
	self.api = api
	self.id = id
	self.version = version
	self.storage = {
		storage = {},
		storage_idx = {}
	}
	self.ip = ip
	self.port = port
	self.is_master = true
	self.connection = nil
	self.master = {}
	self.db_path = path
end

function Store:upto_date()
	return self.is_master or not self.connection
end

function Store:fetch(b_id,id)
	local bucket = self.storage.storage[b_id]
	if bucket then
		if id then
			local object = bucket[id]

			if not(object == nil) then
				if self:upto_date() then
					return object
				else
					return object,"old data"
				end
			else
				return nil,"not found"
			end
		else
			if self:upto_date() then
				return bucket
			else
				return bucket,"old data"
			end
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
				if self:upto_date() then
					return object
				else
					return object,"old data"
				end
			else
				return nil,"not found"
			end
		else
			if self:upto_date() then
				return bucket
			else
				return bucket,"old data"
			end
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
	data.bucket = b_id
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
	local err = self:compile(data,b_id,id)
	if err then
		return nil,err
	else
		local updated = true
		-- i need to check if there were any changes, so that I know
		-- if i need to broadcast those changes
		bucket[id] = data
		bucket_idx[data.idx] = data

		if broadcast and updated then
			self:emit(b_id,"store",id,data)
			self:emit("sync",b_id,"store",id,data)
		end
		return data
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
				self:emit("sync",b_id,"delete",id,object)
			end
		end
	end
end

function Store:compile(data,bucket,id)
	local script = data["$script"]
	local env =
			{__filename = id
			,__dirname = bucket
			,pairs = pairs
			,pcall = pcall
			,xpcall = xpcall
			,table = table
			,store = self
			,logger = logger
			,JSON = JSON
			,error_code = self.api.error_code
			,["require"] = function() end}
	local fn,err = self:build(data,script,env,bucket,id)
	if err then
		logger:error("script failed to compile",err)
		return err
	elseif fn then
		data.script = fn()
	end
end

function Store:build(data,script,env,bucket,id)
	if script then
		local fn,err = loadstring(script, '@store/bucket:' .. bucket .. '/script:' .. id)
		if err then
			return nil,err
		end
		setfenv(fn,env)
		return fn
	end
end

function Store:prepare_json(orig)
	local copy
	if type(orig) == 'table' then
		copy = {}
		for orig_key, orig_value in pairs(orig) do
			if type(orig_value) == 'table' then
				local obj = {}
				for key, value in pairs(orig_value) do
					if not (type(value) == 'function') then
						obj[key] = value
					end
				end
				copy[orig_key] = obj
			elseif not (type(orig_value) == 'function') then
				copy[orig_key] = orig_value
			end
		end
	else -- number, string, boolean, etc
		copy = orig
	end
	return copy
end

return Store
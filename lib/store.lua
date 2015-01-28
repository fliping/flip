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
local hrtime = require('uv').Process.hrtime

local Store = Emitter:extend()

function Store:initialize(id)
	self.version = id
	self.storage = {}
	self.storage_idx = {}
	self.is_master = true
	self.master = {}
end

function Store:open(cb)
	cb()
end

function Store:slave_of(ip,port,cb)
	if self.is_master then
		self.is_master = false
		self.master.ip = ip
		self.master.port = port
		-- i need to connect up to the new master
		-- ask it for who it thinks is the master
		-- maybe connect there
		-- and issue a sync to pull down the data.
		self:sync(ip,port,cb)
	else
		cb("already a member of a cluster, or joining is in progress")
	end
end

function Store:promote_to_master(cb)
	cb()
end

function Store:fetch(b_id,id)
	local bucket = self.storage[b_id]
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
	local bucket = self.storage_idx[b_id]
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
		if self.is_master then
			local bucket = self.storage[b_id]
			local bucket_idx = self.storage_idx[b_id]
			if bucket == nil then
				bucket = {}
				bucket_idx = {}
				self.storage[b_id] = bucket
				self.storage_idx[b_id] = bucket_idx
			end
			local object = bucket[id]
			if not(object == nil) then
				if not (object.last_updated == last_known) then
					return object,"try again"
				end
				data.last_updated = math.floor(hrtime() * 100000)
				data.created_at = object.created_at
				data.idx = object.idx
			else
				data.created_at = math.floor(hrtime() * 100000)
				data.last_updated = data.created_at
				data.idx = #bucket_idx + 1
			end
			data.id = id
			bucket[id] = data
			bucket_idx[data.idx] = data
			self:emit(b_id,"store",id,data)
			self:emit("all",b_id,"store",id,data)
			return bucket[id]
		else
			return
		end
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:delete(b_id,id,last_known)
	if self.is_master then
		local bucket = self.storage[b_id]
		local bucket_idx = self.storage_idx[b_id]
		if not(bucket == nil) then
			local object = bucket[id]
			if not(object == nil) then
				if not (object.last_updated == last_known) then
					return object,"try again"
				end
				bucket[id] = nil
				-- just a little expensive. and will cause data elements
				-- to be redistributed for anything other then replicated
				table.remove(bucket_idx,object.idx)
				if #bucket_idx == 0 then
					self.storage[b_id] = nil
					self.storage_idx[b_id] = nil
				end

				-- we need to re-adjust the idx member in all elements
				-- this could get extremely expensive. TODO
				for i = object.idx, #bucket_idx do
					local data = bucket_idx[i]
					data.idx = data.idx - 1
				end

				self:emit(b_id,"delete",id)
				self:emit("all",b_id,"delete",id,object)
			end
		end
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:sync(ip,port,cb)
	cb()
end

return Store
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
local timer = require('timer')
local table = require('table')
local hrtime = require('uv').Process.hrtime
local lmmdb = require("./lmmdb")
Env = lmmdb.Env
DB = lmmdb.DB
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor

local Store = Emitter:extend()
require('./store/failover')(Store)
require('./store/storage')(Store)

function Store:initialize(path,id,version,ip,port,api)
	self.api = api
	self.id = id
	self.scripts = {}
	self.ip = ip
	self.port = port
	self.is_master = true
	self.connections = {}
	self.master = {}
	self.master_replication = {}
	self.db_path = path
end

function Store:index(b_id,idx)
	local txn,err = Env.txn_begin(self.env,nil,Txn.MDB_RDONLY)
	if err then
		return nil,err
	end
	local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
	if err then
		logger:info("unable to open 'buckets' DB",err)
		return nil,err
	end
	local cursor,err = Cursor.open(txn,buckets)
	if err then
		logger:info("unable to create cursor",err)
		return nil,err
	end

	local key,check = Cursor.get(cursor,b_id,Cursor.MDB_SET_KEY)
	local ret
	local count = 1
	if idx then
		while key == b_id do
			if count == idx then
				ret = check
				break
			end
			count = count + 1
			key,check,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
		end
	else
		ret = {}
		while key == b_id do
			ret[#ret + 1] = check
			key,check,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
		end
	end

	Cursor.close(cursor)
	Txn.abort(txn)
	return ret
end

function Store:fetch(b_id,id,cb)
	-- this should be a read only transaction
	local txn,err = Env.txn_begin(self.env,nil,Txn.MDB_RDONLY)
	if err then
		return nil,err
	end

	local objects,err = DB.open(txn,"objects",0)
	if err then
		logger:info("unable to open 'objects' DB",err)
		Txn.abort(txn)
		return nil,err
	end


	if id then
		local json,err = Txn.get(txn,objects, b_id .. ":" .. id)
		Txn.abort(txn)
		if err then
			return nil,err
		else
			json = JSON.parse(json)
			json.script = self.scripts[b_id .. ":" .. id]
			if json["$script"] and not json.script then
				json.script = self:compile(json,b_id,id)
				self.scripts[b_id .. ":" .. id] = json.script
			end
			return json
		end
	else
		local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
		if err then
			logger:info("unable to open 'buckets' DB",err)
			return nil,err
		end
		local cursor,err = Cursor.open(txn,buckets)
		if err then
			logger:info("unable to create cursor",err)
			return nil,err
		end

		local key,id = Cursor.get(cursor,b_id,Cursor.MDB_SET_KEY)
		local acc
		if cb then
			while key == b_id do
				json,err = Txn.get(txn,objects, b_id .. ":" .. id)
				json = JSON.parse(json)
				json.script = self.scripts[b_id .. ":" .. id]
				if json["$script"] and not json.script then
					json.script = self:compile(json,b_id,id)
					self.scripts[b_id .. ":" .. id] = json.script
				end
				cb(key,json)
				key,id,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
			end
		else
			acc = {}
			while key == b_id do
				local json,err = Txn.get(txn,objects, b_id .. ":" .. id)
				json = JSON.parse(json)
				json.script = self.scripts[b_id .. ":" .. id]
				if json["$script"] and not json.script then
					json.script = self:compile(json,b_id,id)
					self.scripts[b_id .. ":" .. id] = json.script
				end
				acc[#acc + 1] = json
				key,id,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
			end
		end

		Cursor.close(cursor)
		Txn.abort(txn)
		return acc
	end

end

function Store:store(b_id,id,data,cb)
	if self.is_master then
		return self:_store(b_id,id,data,false,true,nil,cb)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:delete(b_id,id,cb)
	if self.is_master then
		return self:_delete(b_id,id,false,true,nil,cb)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:start(parent)
	local txn,err = Env.txn_begin(self.env,parent,0)
	if err then
		return nil,nil,nil,nil,err
	end

	local objects,err = DB.open(txn,"objects",0)
	if err then
		Txn.abort(txn)
		return nil,nil,nil,nil,err
	end

	local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
	if err then
		Txn.abort(txn)
		return nil,nil,nil,nil,err
	end

	local logs,err = DB.open(txn,"logs",DB.MDB_INTEGERKEY)
	if err then
		Txn.abort(txn)
		return nil,nil,nil,nil,err
	end

	return txn,objects,buckets,logs
end

function Store:_store(b_id,id,data,sync,broadcast,parent,cb)
	local key = b_id .. ":" .. id

	local txn,objects,buckets,logs,err = self:start(parent)
	if err then
		logger:warning("unable to store data",err)
		return nil,err
	end

	if sync then
		Txn.put(txn,buckets,b_id,id,Txn.MDB_NODUPDATA)
	else
		local json,err = Txn.get(txn,objects,key)
		if err then
			err = Txn.put(txn,buckets,b_id,id,Txn.MDB_NODUPDATA)
			if err then
				logger:error("unable to add id to 'buckets' DB",err)
				return nil,err
			end
			data.created_at = math.floor(hrtime() * 100)
			data.last_updated = data.created_at
		else
			-- there has got to be a better way to do this.
			local obj = JSON.parse(json)
			-- we carry over the created_at
			data.created_at = obj.created_at
			data.last_updated = math.floor(hrtime() * 100)
		end
		data.bucket = b_id
		data.id = id
	end


	local encoded = JSON.stringify(data)
	local err = Txn.put(txn,objects,key,encoded,0)
	if err then
		logger:error("unable to add value to 'objects' DB",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local op
	local op_timestamp
	if not sync then
		op = JSON.stringify({action = "store",data = data})
		 op_timestamp = hrtime() * 100000
		self.version = op_timestamp
		local err = Txn.put(txn,logs,self.version,op,0)

		if err then
			logger:error("unable to add to commit log",key,err)
			Txn.abort(txn)
			return nil,err
		end
	end

	-- commit all changes
	err = Txn.commit(txn)
	
	if err then
		logger:error("unable to commit transaction",err)
		return nil,err
	end
	
	logger:info("stored",key)

	-- compile any scripts and store them off.
	fn = self:compile(data,b_id,id)
	self.scripts[key] = fn

	-- send any updates off
	local updated = true
	if broadcast and updated then
		self:emit(b_id,"store",id,data)
	end
	
	if not sync then
		self:replicate(op,op_timestamp,cb,#self.master_replication + 1)
	end
end

function Store:_delete(b_id,id,sync,broadcast,parent,cb)
	
	local txn,objects,buckets,logs,err = self:start(parent)
	if err then
		logger:warning("unable to delete data",err)
		return nil,err
	end

	local json,err = Txn.get(txn,objects,key)
	-- there has got to be a better way to do this.
	if not json then
		return nil,err
	end
	local obj = JSON.parse(json)

	local err = Txn.del(txn,objects,key)
	if err then
		logger:error("unable to delete object",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local err = Txn.del(txn,buckets,b_id,id)
	if err then
		logger:error("unable to delete object key",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local op
	local op_timestamp
	if not sync then
		op = JSON.stringify({action = "delete",data = {bucket = b_id,id = id}})
		 op_timestamp = hrtime() * 100000
		self.version = op_timestamp
		local err = Txn.put(txn,logs,self.version,op,0)

		if err then
			logger:error("unable to add to commit log",key,err)
			Txn.abort(txn)
			return nil,err
		end
	end

	-- commit all changes
	err = Txn.commit(txn)
	
	if err then
		return nil,err
	end

	self.scripts[key] = nil

	if broadcast then
		self:emit(b_id,"delete",id)
	end
	
	if not sync then
		self:replicate(op,op_timestamp,cb,#self.master_replication + 1)
	end
end

function  Store:replicate(operation,op_timestamp,cb,total)
	self:emit('sync',operation)
	local complete = function(completed)
		if cb then
			cb(completed,nil)
		end
		if completed == 1 then
			local txn,err = Env.txn_begin(self.env,nil,0)
			if err then
				logger:error("unable to begin txn to clear log")
				return
			end
			local logs,err = DB.open(txn,"logs",DB.MDB_INTEGERKEY)
			if err then
				Txn.abort(txn)
				logger:error("unable to open logs DB for cleaning")
				return
			end
			Txn.del(txn,logs,op_timestamp)
			err = Txn.commit(txn)
			if err then
				logger:error("unable to open clean logs DB")
			end
		end
	end

	local current = 1
	
	-- this needs to be implemented eventually

	-- for _idx,connection in pairs(self.master_replication) do
	-- 	connection:send(op,function()
	-- 		current = current + 1
	-- 		cb(current/total,nil)
	-- 	end)
	-- end
	-- complete(current/total)
	complete(1)
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
			,require = function() end} -- this needs to be fixed.
	local fn,err = self:build(data,script,env,bucket,id)
	if err then
		logger:error("script failed to compile",err)
		return nil,err
	elseif fn then
		return fn()
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

return Store
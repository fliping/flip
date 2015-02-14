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
	self.connection = nil
	self.master = {}
	self.db_path = path
end

function Store:upto_date()
	return self.is_master or not self.connection
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
				cb(key,json)
				key,id,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
			end
			Cursor.close(cursor)
			Txn.abort(txn)
		else
			acc = {}
			while key == b_id do
				local json,err = Txn.get(txn,objects, b_id .. ":" .. id)
				json = JSON.parse(json)
				json.script = self.scripts[b_id .. ":" .. id]
				acc[#acc + 1] = json
				key,id,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
			end
		end

		Cursor.close(cursor)
		Txn.abort(txn)
		return acc
	end

end

function Store:store(b_id,id,data,last_known)
	if self.is_master then
		return self:_store(b_id,id,data,last_known,false,true)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:delete(b_id,id,last_known)
	if self.is_master then
		return self:_delete(b_id,id,last_known,false,true)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:start()
	local txn,err = Env.txn_begin(self.env,nil,0)
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

function Store:_store(b_id,id,data,last_known,sync,broadcast)
	local key = b_id .. ":" .. id

	local txn,objects,buckets,logs,err = self:start()
	if err then
		logger:warning("unable to store data",err)
		return nil,err
	end

	if not sync then
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

	local op = JSON.stringify({action = "store",data = data})
	self.version = self.version + 1
	local err = Txn.put(txn,logs,self.version,op,0)

	if err then
		logger:error("unable to add to commit log",key,err)
		Txn.abort(txn)
		return nil,err
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
		self:emit("sync",b_id,"store",id,data)
	end
end

function Store:_delete(b_id,id,last_known,sync,broadcast)
	
	local txn,objects,buckets,logs,err = self:start()
	if err then
		logger:warning("unable to store data",err)
		return nil,err
	end

	local json,err = Txn.get(txn,objects,key)
	-- there has got to be a better way to do this.
	local obj = JSON.parse(json)
	if not obj then
		return
	end

	local err = Txn.delete(txn,objects,key)
	if err then
		logger:error("unable to delete object",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local err = Txn.delete(txn,buckets,b_id,id)
	if err then
		logger:error("unable to delete objcet key",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local op = JSON.stringify({action = "delete",data = {bucket = b_id,id = id}})
	self.version = self.version + 1
	local err = Txn.put(txn,logs,self.version,op,0)

	if err then
		logger:error("unable to add to commit log",key,err)
		Txn.abort(txn)
		return nil,err
	end

	-- commit all changes
	err = Txn.commit(txn)
	
	if err then
		return nil,err
	end

	self.scripts[key] = nil

	if broadcast then
		self:emit(b_id,"delete",id)
		self:emit("sync",b_id,"delete",id,object)
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
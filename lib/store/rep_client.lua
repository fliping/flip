-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   18 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local logger = require('../logger')
local Packet = require('../packet')
local JSON = require('json')
local net = require('net')
local lmmdb = require("../lmmdb")
local store = require("../store")
Env = lmmdb.Env
DB = lmmdb.DB
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor

function write(client)
	local buffer = {}
	local sync = false
	return function(data,check)
		if not sync and check == true then
			buffer[#buffer + 1 ] = data
			if check then
				sync = true
				-- we need to go through everything and send it off
				for _idx,data in pairs(buffer) do
					local a,b,c,d = Packet:pack(data:len())
					client:write(a .. b .. c .. d .. data)
				end
			end
		else
			local a,b,c,d = Packet:pack(data:len())
			client:write(a .. b .. c .. d .. data)
		end
	end
end

function close(client)
	return function()

	end
end

function wrap(client)
	return {connection = connection
	,write = write(client)
	,close = close(client)}
end

function parser(buffer)
	
	local operations = {}
	while #buffer > 4 do
		local length = Packet:integerify(buffer:sub(1,4))
		if length + 4 <= buffer:len() then
			local operation = buffer:sub(5,4 + length)
			buffer = buffer:sub(5 + length)
			length = Packet:integerify(buffer:sub(1,4))
			operations[#operations + 1] = operation
		else
			break
		end
	end
	return buffer,operations
end

function push_sync(operation,client)
	local version = operation
	local txn,err = Env.txn_begin(client.env,nil,Txn.MDB_RDONLY)
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
	
	logger:info("comparing last known logs",client.version,key,version)

	if not key then
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
			client.write(json)
			id,json,err = Cursor.get(obj_cursor,id,Cursor.MDB_NEXT)
		end	
		Cursor.close(obj_cursor)
		client.write("")
	else
		logger:info("performing partial sync")
		client.write("")
		while key do
			if op then
				logger:info("syncing",op)
				client.write(op)
			end
			key,op,err = Cursor.get(cursor,nil,Cursor.MDB_NEXT)
		end
	end
	logger:info('sync is complete')
	Cursor.close(cursor)
	Txn.abort(txn)
end

function push_find_common(operation,client)
	logger:info("trying to find a common point",operation)
	push_sync(operation,client)
	-- something like this...
	client.write("",true)
	return push_flush_logs
end

function push_port(operation,client)
	logger:info("starting a connection back",client.remote_ip,tonumber(operation))
	store:begin_sync(client.remote_ip,tonumber(operation))
	return push_find_common
end

function push_ip(operation,client)
	client.remote_ip = operation
	return push_port
end

function push_identify(operation,client,connections)
	logger:info("push connected",operation)
	if connections[operation] then
		logger:warning("client reconnected",operation)
		connections[operation].close()
	end
	connections[operation] = client
	client.id = operation
	return push_ip
end

function push_flush_logs(operation,client)
	logger:info("client has commited",operation)
	return push_flush_logs
end

function push_init(connections,client,id,env)
	logger:info("push connected",connections,client,id)
	client = wrap(client)
	state = push_identify
	local buffer = ""
	local chunk
	client.local_id = id
	client.env = env

	client.write(id)
	chunk = coroutine.yield()
	while chunk do
		buffer = buffer .. chunk
		buffer,operations = parser(buffer)
		for _,operation in pairs(operations) do
			state = state(operation,client,connections)
		end
		chunk = coroutine.yield()
	end
end




function pull_replicate(operation,client)
	logger:info("got a replicate",operation)
	operation = JSON.parse(operation)
	local txn = Env.txn_begin(env,nil,0)
	local replication,err = DB.open(txn,"replication",0)
	if err then
		Txn.abort(txn)
		logger:warning("unable to store replicated data",err)
		return replicate
	end
	local event = operation.data

	err = Txn.put(txn,replication,client.id,event.last_updated,0)
	logger:info("pulled",event.id,err)
	if operation.action == "store" then
		store:_store(event.bucket,event.id,event,true,true,txn)
	elseif operation.action == "delete" then
		store:_delete(event.bucket,event.id,true,true,txn)
	end
	local err = Txn.commit(txn)
	if err then
		logger:warning("unable to store replicated data",err)
		-- we probably should close the connection at this point
		-- and close this coroutine
	end
	return pull_replicate
end

function pull_sync(operation,client)
	if operation == "" then
		logger:info("now all data needs to be refreshed on this node")
		err = Txn.put(client.txn,client.replication,client.remote_id,client.last_updated,0)
		if err then
			logger:error("unable to sync up with remote",err)
			-- i probably should close the connection here
		end
		err = Txn.commit(client.txn)
		client.txn = nil
		if err then
			logger:error("unable to sync up with remote",err)
			-- i probably should close the connection here
		end
		if client.cb then
			client.cb()
		end
		return pull_replicate
	else
		local event = JSON.parse(operation)
		if event.last_updated > client.last_updated then
			client.last_updated = event.last_updated
		end
		store:_store(event.bucket,event.id,event,true,false,client.txn)
		return pull_sync
	end
end

function pull_identify(operation,client)
	logger:info("storing remote id",operation)
	client.remote_id = operation
	local replication,err = DB.open(client.txn,"replication",0)
	if err then
		Txn.abort(client.txn)
		logger:warning("unable to open replication DB",err)
		-- now i need to close the connection
	end
	client.replication = replication
	local last_updated,err = Txn.get(client.txn,replication,operation)
	if last_updated then
		client.write(last_updated)
	else
		client.write("0")
	end
	return pull_sync
end

function pull_init(env,id,ip,port,client,cb)
	logger:info("we are connected!",env,id,client,cb)
	client = wrap(client)
	local txn,err = Env.txn_begin(env,nil,0)
	client.txn = txn
	client.cb = cb
	client.last_updated = 0
	logger:info("going to send",id,ip,port)
	client.write(id)
	client.write(ip)
	client.write(tostring(port))
	local state = pull_identify
	local buffer = ""

	chunk = coroutine.yield()
	while chunk do
		buffer = buffer .. chunk
		buffer,operations = parser(buffer)
		for _,operation in pairs(operations) do
			state = state(operation,client)
		end
		chunk = coroutine.yield()
	end
end

return {pull = pull_init,push = push_init}
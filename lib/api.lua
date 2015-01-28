-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   20 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local hrtime = require('uv').Process.hrtime
local logger = require('./logger')
local JSON = require('json')
local Lever = require('lever')
local utils = require('utils')
local table = require('table')

local Readable = Lever.Stream.Readable
local Start = Readable:extend()

function Start:initialize()
  Readable.initialize(self,{objectMode = true})
end

function Start:_read() end

local Api = Emitter:extend()

function Api:initialize(flip,port,ip)
	self.flip = flip
	self.lever = Lever:new(port,ip)
	self.status = Start:new()

	-- express routes
	self.lever:get('/cluster'
		,function(req,res) self:node_status(req,res) end)

	-- piped routes

	-- subscribe routes
	self.status
		:pipe(self.lever.json())
		:pipe(self.lever:get('/cluster/stream'))

	-- kv-store
	self.lever:get('/store/sync/?version'
		,function(req,res) self:sync(req,res) end)
	self.lever:get('/store/?bucket'
		,function(req,res) self:fetch(req,res) end)
	self.lever:get('/store/?bucket/?id'
		,function(req,res) self:fetch(req,res) end)
	self.lever:post('/store/?bucket/?id'
		,function(req,res) self:post(req,res) end)
	self.lever:delete('/store/?bucket/?id'
		,function(req,res) self:delete(req,res) end)

	-- kv-store actions
	self.lever:post('/cluster/join/?ip/?port'
		,function(req,res) self:join_cluster(req,res) end)

end

function Api:node_status(req,res)
	local data = {}
	for id,node in pairs(self.flip.servers) do
		
		data[#data + 1] = 
			{id = node.id
			,state = node.state
			,systems = node.systems
			,opts = node.opts}
	end
	res:writeHead(200,{})
	local time = hrtime()
	self.status:push({time = time})
  res:finish(JSON.stringify({time = time,data = data}))
end


function Api:system_status(req,res)
	local data = {}
	for id,node in pairs(self.flip.systems) do
		
		data[#data + 1] = 
			{id = node.id
			,state = node.state
			,opts = node.opts}
	end
	res:writeHead(200,{})
  res:finish(JSON.stringify(data))
end


function Api:fetch(req,res)
	logger:info("fetch",req.env.bucket,req.env.id)
	local object,err = self.flip.store:fetch(req.env.bucket,req.env.id)
	if err then
		local code = self:error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err}))
	else
		res:writeHead(200,{})
		res:finish(JSON.stringify(object))
	end
end

function Api:post(req,res)
	logger:info("post",req.env.bucket,req.env.id)
	local chunks = {}
	
	req:on('data',function(chunk)
		chunks[#chunks + 1] = chunk
	end)
	
	req:on('end',function()

		local data = JSON.parse(table.concat(chunks))
		local last_known = req.headers["last-known-update"] or data.last_updated
		
		local object,err = self.flip.store:store(req.env.bucket,req.env.id,data,last_known)
		if err then
			local code = self:error_code(err)
			res:writeHead(code,{})
			res:finish(JSON.stringify({error = err,updated = object}))
		else
			res:writeHead(201,{})
			res:finish(JSON.stringify(object))
		end
	end)
end

function Api:delete(req,res)
	local last_known = 0 + req.headers["last-known-update"]
	logger:info("delete",req.env.bucket,req.env.id,last_known)
	
	local updated,err = self.flip.store:delete(req.env.bucket,req.env.id,last_known)
	if err then
		local code = self:error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err,updated = updated}))
	else
		res:writeHead(204,{})
		res:finish()
	end
end


function Api:join_cluster(req,res)
	self.flip.store:slave_of(req.env.ip,req.env.port,function(err)
		if err then
			local code = self:error_code(err)
			res:writeHead(code,{})
			res:finish(JSON.stringify({error = err}))
		else
			res:writeHead(200,{})
			res:finish(JSON.stringify({status = "joined"}))
		end
	end)
end

function Api:sync(req,res)
	local sync,err = self.flip.store:to_json(req.env.version)
	if err then
		local code = self:error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err}))
	else
		res:writeHead(200,{})
		sync:on('event',function(object)
			res:write(JSON.stringify(object))
		end)
	end
end

function Api:error_code(err)
	if (err == "not found") then
		return 404
	elseif (err == "try again") then
		return 400
	else
		return 500
	end
end

return Api
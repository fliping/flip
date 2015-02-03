-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   2 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

-- kv-store
lever:get('/store/sync/?version'
	,function(req,res) sync(req,res) end)
lever:get('/store/?bucket'
	,function(req,res) fetch(req,res) end)
lever:get('/store/?bucket/?id'
	,function(req,res) fetch(req,res) end)
lever:post('/store/?bucket/?id'
	,function(req,res) post(req,res) end)
lever:delete('/store/?bucket/?id'
	,function(req,res) delete(req,res) end)

-- kv-store actions
lever:post('/cluster/join/?ip/?port'
	,function(req,res) join_cluster(req,res) end)



function fetch(req,res)
	logger:info("fetch",req.env.bucket,req.env.id)
	local object,err = store:fetch(req.env.bucket,req.env.id)
	if err then
		local code = error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err}))
	else
		res:writeHead(200,{})
		res:finish(JSON.stringify(object))
	end
end

function post(req,res)
	logger:info("post",req.env.bucket,req.env.id)
	local chunks = {}
	
	req:on('data',function(chunk)
		chunks[#chunks + 1] = chunk
	end)
	
	req:on('end',function()

		local data = JSON.parse(table.concat(chunks))
		local last_known = req.headers["last-known-update"] or data.last_updated
		
		local object,err = store:store(req.env.bucket,req.env.id,data,last_known)
		if err then
			local code = error_code(err)
			res:writeHead(code,{})
			res:finish(JSON.stringify({error = err,updated = object}))
		else
			res:writeHead(201,{})
			res:finish(JSON.stringify(object))
		end
	end)
end

function delete(req,res)
	local last_known = 0 + req.headers["last-known-update"]
	logger:info("delete",req.env.bucket,req.env.id,last_known)
	
	local updated,err = store:delete(req.env.bucket,req.env.id,last_known)
	if err then
		local code = error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err,updated = updated}))
	else
		res:writeHead(204,{})
		res:finish()
	end
end


function join_cluster(req,res)
	store:slave_of(req.env.ip,req.env.port,function(err)
		if err then
			local code = error_code(err)
			res:writeHead(code,{})
			res:finish(JSON.stringify({error = err}))
		else
			res:writeHead(200,{})
			res:finish(JSON.stringify({status = "joined"}))
		end
	end)
end

function sync(req,res)
	local sync,err = store:to_json(req.env.version)
	if err then
		local code = error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err}))
	else
		res:writeHead(200,{})
		sync:on('event',function(object)
			res:write(JSON.stringify(object))
		end)
	end
end

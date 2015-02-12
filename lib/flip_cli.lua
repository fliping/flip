#!/usr/local/bin/luvit
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
local logger = require('./logger')
local os = require('os')
local JSON = require('json')
local http = require('http')

function main()
	logger:debug("entering cli mode")
	local system = table.remove(process.argv,1)
	local command = table.remove(process.argv,1)
	if system == 'help' then
		command = 'help'
		system = nil
	end
	logger:debug("making request",system,command)
	request('get','store','systems',system,nil,{},function(result,err)
		if err == "not found" then
			logger:error("command was not found",system,command)
			process.exit(1)
		elseif err then
			logger:error(err)
			process.exit(1)
		elseif (system == nil) and (command == "help") then
			for _idx,system in pairs(result) do
				if system.description then
					process.stdout:write(system.description)
				end
			end
		elseif command == "help" then
			if result.description then
				process.stdout:write(result.description)
			end
			if result.help then
				process.stdout:write(result.help)
			end
		elseif result and result.cli and result.cli[command] then
			logger:debug("running script",result.cli[command])
			request('get','store',system,result.cli[command],nil,{},function(result,err)
				if err then
					logger:error("command was not found",system,command)
					process.exit(1)
				elseif result and result['$script'] then
					logger:debug("running script",result)
					local fn,err = loadstring(result['$script'], '@store/system:' .. system .. '/command:' .. command)
					if err then
						logger:error("script failed to compile",err)
					else
						local batton = {}

						-- set the env
						setfenv(fn,
							{__filename = id
							,__dirname = bucket
							,process = process
							,pairs = pairs
							,pcall = pcall
							,loadstring = loadstring
							,logger = logger
							,require = local_require(batton)
							,store = bind_store(batton)})

						local co = coroutine.create(function()
							-- and now we run the script!
							local err = function(err) logger:error(err) end
							local success,fn = xpcall(fn,err)
							if success then
								local success,fn = xpcall(function() fn(unpack(process.argv)) end,err)
							end
						end)
						coroutine.resume(co)
						check_baton(batton,co)
						
					end
				else
					logger:error("command was not found",system,command)
					process.exit(1)
				end
			end)
		else
			logger:error("command was not found",system,command)
			process.exit(1)		
		end
	end)
		
end

function check_baton(batton,co)
	if batton.call then
		batton.call(function(...)
			batton.call = nil
			batton.respose = {...}
			if coroutine.resume(co) then
				batton.respose = nil
				check_baton(batton,co)
			end
		end)
	end
end

function bind_store(batton)
	return
		{fetch = function(_,bucket,id) 
			batton.call = function(cb) request('get','store',bucket,id,nil,{},cb) end
			coroutine.yield()
			return unpack(batton.respose)
		end
		,delete = function(_,bucket,id,last_known) 
			batton.call = function(cb) request('delete','store',bucket,id,nil,{["last-known-update"]=last_known},cb) end
			coroutine.yield()
			return unpack(batton.respose)
		end
		,store = function(_,bucket,id,data,last_known) 
			logger:debug("making request",bucket,id,data)
			batton.call = function(cb) request('post','store',bucket,id,data,{["last-known-update"]=last_known},cb) end
			coroutine.yield()
			return unpack(batton.respose)
		end}
end


function request(method,prefix,bucket,id,data,headers,cb)
	logger:debug("request",method,prefix,bucket,id,data,cb)
	local path
	if id then
		path = "/" .. prefix .. "/" .. bucket .. "/" .. id
	else
		path = "/" .. prefix .. "/" .. bucket
	end
	local options =
			{host = "127.0.0.1" -- these need to be pulled from the config file...
			,port = 8081
			,method = method
			,path = path
			,headers = headers}
	if data then
		data = JSON.stringify(data)
		options.headers["content-length"] = #data
		options.headers["content-type"] = "application/json"
	end
	local req = http.request(options, function (res)
		local chunks = {}
		res:on('data', function (chunk)
			chunks[#chunks + 1] = chunk
			end)
		res:on('end',function()
			if (res.status_code > 199) and (res.status_code < 300) then
				if #chunks > 0 then
					local result = JSON.parse(table.concat(chunks))
					cb(result)
				else
					cb()
				end
			elseif res.status_code == 404 then
				if #chunks > 0 then
					logger:error("bad response",table.concat(chunks))
				end
				
				cb(nil,"not found")
			else
				if #chunks > 0 then
					cb(nil,res.status_code ..": "..table.concat(chunks))
				else
					cb(nil,res.status_code)
				end
			end
		end)
	end)
	req:on('error',cb)
	req:done(data)
end

function local_require(batton)
	return function(object)
		local sucess,ret = pcall(require,object)
		if sucess then
			logger:debug("required a local thing",ret)
			return ret
		else
			logger:warning('I need to require a remote script')
			batton.call = function(cb) cb() end
			coroutine.yield()
			return unpack(batton.respose)
		end
	end
end

main()
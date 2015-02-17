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

return function(req,res)
	logger:info("post",req.env.bucket,req.env.id)
	local chunks = {}
	
	req:on('data',function(chunk)
		chunks[#chunks + 1] = chunk
	end)
	
	req:on('end',function()

		local success,data = xpcall(function() return JSON.parse(table.concat(chunks)) end,function(err)
			res:writeHead(400,{})
			res:finish('{"error":"bad json"}')
		end)

		if success then
			
			local object,err = store:store(req.env.bucket,req.env.id,data)
			if object then
				-- we can't stringify a function
				object.script = nil
			end
			if err then
				local code = error_code(err)
				res:writeHead(code,{})
				res:finish(JSON.stringify({error = err,updated = object}))
			else
				res:writeHead(201,{})
				res:finish(JSON.stringify(object))
			end
		end
	end)
end
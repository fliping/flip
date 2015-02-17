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
	local sync,err = store:sync(req.env.version)
	if err then
		local code = error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err}))
	else
		res:writeHead(200,{})
		sync:on('event',function(object)
			res:write(object)
		end)
	end
end

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
	local last_known = 0 + req.headers["last-known-update"]
	logger:info("delete",req.env.bucket,req.env.id,last_known)
	
	local updated,err = store:delete(req.env.bucket,req.env.id,last_known)
	if err then
		local code = error_code(err)
		res:writeHead(code,{})
		res:finish(JSON.stringify({error = err,updated = store:prepare_json(updated)}))
	else
		res:writeHead(204,{})
		res:finish()
	end
end
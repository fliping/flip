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

-- this will eventually be run by the cli when trying to add
-- something to the store

local fs = require('fs')

return function (dir,name)
	if not name then 
		name = 'new_system'
	end
	if not dir then
		dir = './' .. name
	end

	fs.mkdir()
	
end
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

return function(data,cb)
	local member,err = store:fetch_idx("servers",data[1])
	if (store.ip == member.ip) and (store.port == member.port) then
		store:slave_of(member.ip,member.port,cb)
	else
		store:promote_to_master(cb)
	end
end
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

-- when a new master is chosen this script is called.
return function(members,cb)
	local member = members[1]
	if (store.id == member.id) then
		store:promote_to_master(cb)
	else
		store:slave_of(member.http_ip,member.http_port,cb)
	end
end
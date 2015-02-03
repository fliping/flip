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
	if (store.id == member.id) then
		store:promote_to_master(cb)
	else
		store:slave_of(member.http_ip,member.http_port,cb)
	end
end
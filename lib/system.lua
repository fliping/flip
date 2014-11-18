-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------


return 
	{shard = require('./system/sharded')
	,replicate = require('./system/replicated')}


local Emitter = require('core').Emitter
local logger = require('../logger')

local System = Emitter:extend()

function System:initialize(config,node_id)
	self.config = config
	self.members = {}
	self.id = node_id
end

function System:add_member(member)
	self.members[#self.members + 1] = member


end

function System:enable()

end
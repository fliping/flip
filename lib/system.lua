-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   18 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Plan = require('./plan/plan')


local Emitter = require('core').Emitter
local logger = require('./logger')

local System = Emitter:extend()
function System:initialize(store,flip)
	self.flip = flip
	self.store = store
	self.plans = {}
	self.enabled = false
end

function System:disable(cb)
	if self.enabled then
		self.enabled = false
		count = 0
		for _idx,plan in pairs(self.plans) do
			count = count + 1
			plan:disable(function()
				count = count - 1
				if count == 0 then
					if cb then
						cb()
					end
				end
			end)
		end
		if count == 0 then
			if cb then
				cb()
			end
		end
	else
		logger:warning('requested to disable system, but already disabled')
	end
end

function System:check_system(kind,id,system_config) 
	if kind == "store" then
		local plan = self.plans[id]
		if plan then
			plan:update(system_config)
			logger:info("updated plan:",id)
		else
			self.plans[id] = Plan:new(system_config,id,self.flip,self.store)
			logger:info("created plan:",id)
		end
	elseif kind == "delete" then
		local plan = self.plans[id]
		if plan then
			self.plans[id] = nil
			plan:disable(function() 
				logger:info("removed plan:",id)
			end)
		end
	end
end

function System:regen(systems)
	if systems then
		logger:info("begining regeneration of",systems)
		for _idx,system in pairs(systems) do
			local plan = self.plans[system]
			if plan then
				plan:next_plan()
			end
		end
	end
end

function System:enable()
	if not self.enabled then
		self.enabled = true
		local systems,err = self.store:fetch("systems")
		
		if err then
			logger:info("no systems present in cluster",err)
			systems = {}
		end

		
		for sys_id,system_config in pairs(systems) do
			local plan = Plan:new(system_config,sys_id,self.flip,self.store)
			self.plans[sys_id] = plan

			-- this should only be enabled if I am a member of the system
			plan:enable()
		end

		self.store:on("systems",function(kind,id,system_config) self:check_system(kind,id,system_config) end)

		-- when this process shutsdown, we want to remove all data
		-- that it is responsible for, but only if requested
		if true then
			local me = self
			local stop = function() me:disable(function() process.exit(0) end) end
			process:on('SIGINT',stop)
			process:on('SIGQUIT',stop)
			process:on('SIGTERM',stop)
		end

		-- we don't do this, this happens when this server gets added into the store
		-- -- we enable all the plans to start the ball rolling
		-- for _idx,plan in pairs(self.plans) do
		-- 	plan:enable()
		-- end
	else
		logger:warning('requested to enable system, but already enabled')
	end
end

return System
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

local Emitter = require('core').Emitter
local JSON = require('json')
local spawn = require('childprocess').spawn
local timer = require('timer')
local logger = require('../logger')

local topologies = require('./topologies/topologies')

local Plan = Emitter:extend()

function Plan:initialize(system,id,type)
	local Topology = topologies[type]
	if not Topology then
		logger:fatal("unknown data distribution type",type)
		process:exit(1)
	end

	-- In future topologies, I might need to prepare the data before I 
	-- have the topology divide it between then nodes. right now I do
	-- nothing
	self.topology = Topology:new()

	self.system = system
	self.id = id
	self.plan = {}
	self.members = {}

	-- we expose the ability to remove all data on start
	-- it defaults to removing the data
	self.mature = system.remove_on_start
	if self.mature == nil then
		self.mature = false
	end
end

function Plan:add_member(member)
	self.members[#self.members + 1] =  member
	-- all members start out alive
	self.alive[#self.members] = true
end

function Plan:enable()
	local plan = self
	-- when members change their state, we need to have a new plan built
	for id,member in members do
		member:on('state_change',function(_member,state)
			if state == 'alive' then
				self.alive[id] = true
			elseif state == 'down' then
				self.alive[id] = false
			else
				return
			end

			-- one of the members changed state
			-- create a new generation of this plan
			plan:next_plan()
		end)
	end
end

function Plan:next_plan()

	-- we ask the topology what data points are needed
	local add,remove = self.topology(self.system.data
																	,self.id,self.alive)

	local new_plan = {add = add,remove = remove}

	-- a plan was made and is still waiting to be run, we have a new one
	-- so lets clear it out
	if self.plan_activation_timer then
		logger:debug("clearing timer")

		timer.clearTimer(self.plan_activation_timer)
	end

	if self.queue then
		-- we are currently running scripts, lets wait for them to be done
		self.queue = new_plan
	else

		-- we delay how long it takes for a new plan to be put in place
		-- so that if any members change at the exact smae time, the 
		-- changes get pulled into a single plan update
		logger:debug("setting plan",new_plan)
		local me = self
		self.plan_activation_timer = timer.setTimeout(Timeout,function(a_plan)
			self.plan_activation_timer = nil
			me:compute(a_plan)
		end,new_plan)
	end
end

function Plan:compute(new_plan)
	
	
	-- we order the array to make the comparison easier
	local new_add = new_plan.add
	table.sort(new_add)

	local add = {}
	local remove = {}
	local index = 1
	local lidx = 1
	logger:debug("start",idx,new_plan)

	for idx=1, #new_add do
		
		
		logger:debug("compare",self.plan[index],new_add[idx])
		if (new_add[idx] and not self.plan[index]) or (self.plan[index] > new_add[idx]) then
			-- we need to add data points that are members of new_add and 
			-- not members of self.plan
			logger:debug("adding",new_add[idx])
			add[#add +1] = new_add[idx]
		elseif (self.plan[idx] and not new_add[index]) or (self.plan[index] < new_add[idx]) then
			-- we need to remove data points that are members of self.plan and 
			-- not members of new_add
			logger:debug("removing",new_add[idx])
			remove[#remove +1] = self.plan[index]
			index = index + 1
			idx = idx - 1
		else
			-- everything else gets ignored.
			logger:debug("skipping",new_add[idx])
			idx = idx + 1
			index = index + 1
		end
		lidx = idx
	end
	lidx = lidx + 1

	-- everything leftover else gets removed
	for index=index,#self.plan do
		logger:debug("batch removing",self.plan[index])
		remove[#remove +1] = self.plan[index]
	end

	-- everything leftover else gets added
	for idx=lidx,#new_add do
		logger:debug("batch adding",new_add[idx])
		add[#add +1] = new_add[idx]
	end

	-- on a startup, there could possibly be data that needs to be 
	-- removed
	if not self.mature then
		logger:info("not mature yet, removing",new_plan.remove)
		for _idx,value in pairs(new_plan.remove) do
			remove[#remove +1] = value
		end
	end

	-- if there were changes
	if (#add > 0) or (#remove > 0) then
		self.queue = {add = add,remove = remove}
		self:run()
	else
		logger:info("no change in the plan",self.plan)
	end
end

function Plan:shutdown()
	self:_run('down',self.plan,function()
		process.exit(1)
	end)
end

function Plan:run()

	local newest_plan = {}

	-- we want to add in all data points that haven't been flagged
	-- to be removed
	for _idx,current in pairs(self.plan) do
		local skip = false
		for _idx,value in pairs(self.queue.remove) do
			if value == current then
				skip = true
				break
			end
		end
		if not skip then
			newest_plan[#newest_plan + 1] = current
		end
	end


	-- add in all new data points
	for _idx,value in pairs(self.queue.add) do
		newest_plan[#newest_plan + 1] = value
	end

	self.plan = newest_plan
	
	local queue = self.queue
	self.queue = true

	-- run the alive scripts
	self:_run("alive",queue.add,function()
		-- run the down scripts
		self:_run("down",queue.remove,function()
			self.mature = true
			
			if self.queue == true then
				-- if not, lets end
				self.queue = nil
				logger:info("current plan",self.plan)
			else
				-- if there is another thing queued up to run,
				-- lets run it
				logger:info("running next set of jobs",self.queue)
				local queue = self.queue
				self.queue = nil
				self:compute(queue)
			end
		end)
	end)

end

function Plan:_run(state,data,cb)
	local sys = self.system
	local count = #data
	for idx,value in pairs(data) do
		local child = spawn(sys[state],{value,JSON.stringify(sys.config)})
		
		child.stdout:on('data', function(chunk)
			logger:debug("got",chunk)
		end)

		child:on('exit', function(code,other)
			count = count - 1
			if not (code == 0 ) then
				logger:error("script failed",sys[state],{value,JSON.stringify(sys.config)})
			else
				logger:info("script worked",sys[state],{value,JSON.stringify(sys.config)})
			end
			if count == 0 and cb then
				cb()
			end
		end)

	end
	if count == 0 then
		process.nextTick(cb)
	end
end

return Plan
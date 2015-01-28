-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Sept 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local dgram = require('dgram')
local timer = require('timer')
local table = require('table')
local string = require('string')
local json = require('json')
local math = require('math')
local hrtime = require('uv').Process.hrtime
local logger = require('./lib/logger')
local Member = require('./lib/member')
local System = require('./lib/system')
local Packet = require('./lib/packet')
local Api = require('./lib/api')
local Store = require('./lib/store')

local Flip = Emitter:extend()

function Flip:initialize(config)
	self.config = config
	self.members = {}
	-- self.iservers = {}
	self.alive = {}
	self.packet = Packet:new()
	self.api = Api:new(self,config.api.port,config.api.ip)

	----
	-- this needs to be reworked.
	----
	
	-- for idx,opts in pairs(config.sorted_servers) do
		
	-- 	-- everything starts out alive
	-- 	self.alive[idx] = true

	-- 	member = Member:new(opts,config)
	-- 	self.system:add_member(member)
	-- 	self:add_member(member)
	-- 	member:on('state_change',function(...) self:track(idx,...) end)
	-- 	if member.id == config.id then
	-- 		self.id = idx
	-- 	end
	-- end
	-- create a unique id for this store. bascially a UUID
	self.store = Store:new({node = config.id,time = hrtime(),random = math.random(100000)})
end

function Flip:start()
	-- we create a system so that it is setup by the time that servers
	-- are added in, it starts working and creating plans
	self.system = System:new(self.store,self.config.id)
	self.system:enable()

	-- we also want all servers to be setup correctly by the time we
	-- set this server to be active
	self.store:on("servers",function(kind,id,data) self:process_server_update(kind,id,data) end)

	self.store:open(function(err)
		if not err then

			-- if there are not servers, then this is a new cluter.
			-- lets set it up
			local members,err = self.store:fetch("servers",nil)
			if err == "not found" then
				me = 
					{ip = self.config.gossip.ip
					,port = self.config.gossip.port}
				local object,err = self.store:store("servers",self.config.id,me)
				if err then
					logger:error("unable to create cluster: ",err)
					process.exit(1)	
				end
			elseif err then
				logger:error("unable to access store: ",err)
				process.exit(1)
			end

			-- we set ourself to be alive. This probably should be a quorum
			-- decision TODO
			local member = self:find_member(self.config.id)
			if member == nil then
					logger:info("unable to find this server",self.config.id,err,member)
					process.exit(1)
			else
				member:update_state('alive')
			
				-- we start responding to udp queries
				local socket = dgram.createSocket('udp4')
				socket:bind(member.port,member.ip)
				socket:on('message',function(...) self:handle_message(...) end)
				self.dgram = socket

				-- we start probing other members
				self.gossip_timer = timer.setTimeout(self.config.gossip_interval, self.gossip_time, self)
			end
		else
			logger:error("unable to start the store: ",err)
			process.exit(1)
		end
	end)
end

function Flip:process_server_update(kind,id,data)
	logger:info("processing",kind,id,data)
	if kind == "store" then
		local member = self.members[id]
		if member then
			member:update(data)
		else
			member = Member:new(id,data,self.config)
			self.members[id] = member
			member:on('state_change',function(...) self:track(id,...) end)
			member:enable()
		end
		self.system:update_member(member)
	elseif kind == "delete" then
		local member = self.members[id]
		self.members[id] = nil
		if member then
			self.system:remove_member(member)
			member:destroy()
		end
	end
end

function Flip:find_member(key)
	local server
	if type(key) == "number" then
		server = self.store:fetch_idx("servers",key)
	else
		server =  self.store:fetch("servers",key)
	end

	if server then
		return self.members[server.id]
	end
end

function Flip:get_idx()
	local object,err = self.store:fetch("servers",self.config.id)
	if err then
		logger:info(self.store,self.config.id)
		logger:warning("unable to find my idx",err)
		process.exit(1)
	end
	return object.idx
end

function Flip:get_gossip_members()
	local members = {}
	for _idx,member in pairs(self.members) do
		if member:needs_ping() then
			members[#members + 1] = member
		end
	end
	-- important ones should be at the front of the list
	table.sort(members,Flip.sort_members)
	return members
end

function Flip:sort_members(member,member2)
	-- i need to check this
	logger:debug("checking",member,member2)
	return (math.random() == 1)
end

function Flip:handle_message(msg, rinfo)
	logger:debug('message received',msg,rinfo)
	local key,id,seq,nodes = self.packet:parse(msg)
	if key == self.config.key then
		local down = {}
		self:ping(seq,id,nodes)
	else
		logger:warning('wrong key in packet',rinfo,msg)
	end
end



function Flip:gossip_time()
	collectgarbage()
	local members = self:get_gossip_members()
	self:ping_members(members)
end

function Flip:ping_members(members)
	if not members then
		logger:debug('no more members')
		timer.setTimeout(self.config.gossip_interval,self.gossip_time,self)
		return
	end
	local member = table.remove(members,1)
	local count = 0
	local idx = self:get_idx()
	while member do
		if member:needs_ping() then
			local packet = self.packet:build(self.config.key,idx,member:next_seq(),self.alive)
			logger:debug('sending ping',member.id)
			self:send_packet(packet,member)
			member:start_alive_check()
			count = count + 1 
		end
		if count < self.config.ping_per_interval then
			member = table.remove(members,1)
		else
			break
		end
	end

	logger:debug("done with round")
	-- if we still have some members left over, we need to ping them
	-- on the next timeout. Otherwise we start gossiping all over again
	if not (#members == 0) then
		timer.setTimeout(self.config.gossip_interval,self.ping_members,self,members)
	else
		timer.setTimeout(self.config.gossip_interval,self.gossip_time,self)
	end

end

function Flip:send_packet(packet,member)
	logger:debug('sending',packet)
	self.dgram:send(packet, member.port, member.ip, function(err)
		if err then
			logger:error('udp send errored',err)
		end
	end)
end

function Flip:ping(seq,id,nodes)
	local member = self:find_member(id)

	if member then
		member:alive(seq)
		if member:needs_ping() then
			local idx = self:get_idx()
			local packet = self.packet:build(self.config.key,idx,member:next_seq(),self.alive)
			logger:debug('sending ping (ack)',id)
			self:send_packet(packet,member)
			for node,alive in pairs(nodes) do
				if not alive then
					self:probe(id,node)
				end
			end
		end
	else
		logger:warning('unknown member',id)
	end
end

function Flip:track(id,member,new_state)
	if self.api then
		self.api.status:push(
			{id = member.id
			,state = new_state
			,opts = member.opts
			,systems = member.systems
			,time = hrtime()})
	end

	local server,err = self.store:fetch("servers",id)
	if err then
		logger:info("member doesn't exist anymore")
		process.exit(1)
	end
	-- this is used to build the packets
	if new_state == 'alive' then
		self.alive[server.idx] = true
	elseif (new_state == 'down') or (new_state == 'probably_down') then
		self.alive[server.idx] = false
	end
end

function Flip:probe(from,...)
	for idx,who in pairs({...}) do
		if not (self.config.id == who) then
			local down_member = self:find_member(who)

			if down_member then
				down_member:probe(from)
			end
		end
	end
end

return Flip
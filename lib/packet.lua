-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   19 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------


-- this really needs to be rewritten

local ffi = require('ffi')
local math = require('math')
local bit = require('bit')
local logger = require('./logger')

local Packet = {}

ffi.cdef([[
void* malloc (size_t size);
void free (void* ptr);
]])

function Packet:build(secret,id,alive_servers)
	if not (#alive_servers < 512) then
		logger:fatal("too many servers")
		process.exit(1)
	end
	local bytes = math.ceil(#alive_servers/8)
	local header_size = 32 + 8
	local len = bytes + header_size
	logger:info("allocating",len)
	local p = ffi.gc(ffi.C.malloc(len), ffi.C.free)
	local p = ffi.cast("unsigned char *",p)
	ffi.fill(p,len)
	ffi.copy(p,secret,math.min(secret:len(),32))
	-- ffi.copy(p+math.min(secret:len(),32),id,4)
	for idx,is_alive in pairs(alive_servers) do
		local index = header_size + math.floor((idx-1) / 8)
		if is_alive then
			p[index] = bit.bor(p[index], bit.lshift(1,(idx-1) % 8))
		end
		logger:info(is_alive,index,idx,((idx-1) % 8) + 1,p[index])
	end

	return ffi.string(p,len)
end

function Packet:parse(packet)
	local size = packet:len()
	local nodes = {}
	local header_size = 32 + 8
	local id = -1

	local p = ffi.gc(ffi.C.malloc(size), ffi.C.free)
	local p = ffi.cast("unsigned char *",p)
	

	logger:info(packet)
	for idx=header_size+1,size do
		logger:info("byte",idx)
		local byte = packet:byte(idx)
		for i=0,7 do
			logger:info("bit",i,byte,bit.lshift(1,i))
			nodes[#nodes + 1] = not (bit.band(byte,bit.lshift(1,i)) == 0)
		end
	end
	return packet:sub(1,32),id,nodes
end

return Packet
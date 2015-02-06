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
local logger = require('../logger')
local JSON = require('json')
local fs = require('fs')

return function(Store)

	function Store:load_from_disk(cb)
		local files,err = fs.readdir(self.db_path,function(err,files)
			if err then
				cb(err)
			else
				logger:debug("loading",files)
				for _idx,file in pairs(files) do
					local path = self.db_path .. "/" .. file
					if file:sub(-1) == "~" then
						fs.unlink(path,function() end)
					else
						local json,err = fs.readFileSync(path)
						if err then
							logger:error("it errored",err)
							process.exit(1)
						else
							local object = JSON.parse(json)
							self:_store(self.storage,object.bucket,object.id,object,nil,false,true)
						end
					end
				end
				cb()
			end
		end)
	end

	function Store:open(cb)
		self:load_from_disk(function(err)
			if err then
				logger:info("bootstrapping store")
				self:start_async_io()

				
				-- we use the dev load command to load all the default
				-- systems into flip
				local load = require('../system-dev/cli/load')
				env = getfenv(load)

				-- we don't need any logs. the load command will always work
				env.logger = 
					{info = function() end
					,warning = function() end
					,error = function() end
					,debug = function() end}
				env.store = self
				setfenv(load,env)

				fs.mkdir(self.db_path,"0700",function() end)

				load(__dirname .. '/../system-topology','topology')
				load(__dirname .. '/../system-store','store')
				load(__dirname .. '/../system-dev','dev')

				logger:info('loaded bootstrapped store')
			else
				logger:info('store was loaded from disk')
				self:start_async_io()
			end
			cb()
		end)
	end

	function Store:start_async_io()
		self:on("sync",function(b_id,action,id,data)
			local path = self.db_path .. "/" .. b_id .. ":" .. id .. ".json"
			if action == "store" then
				local json = self:prepare_json(data)
				json = JSON.stringify(json)
				fs.writeFile(path.. "~",json,function(err)
					if not err then
						fs.rename(path .. '~', path,function() end)
					end
				end)
			elseif action == "delete" then
				fs.unlink(path,function() end)
			end
		end)
	end
end
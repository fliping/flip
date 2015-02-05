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
				logger:info(err,files)
				fs.mkdir(self.db_path,"0700",function()
					cb(err)
				end)
			else
				logger:info("loading",files)
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

				
				local sync = 
					{["$script"] = fs.readFileSync('./lib/system-store/endpoints/store/sync/?version/get-sync.lua')}
				local fetch = 
					{["$script"] = fs.readFileSync('./lib/system-store/endpoints/store/?bucket/?id/get-fetch.lua')}
				local post = 
					{["$script"] = fs.readFileSync('./lib/system-store/endpoints/store/?bucket/?id/post-post.lua')}
				local delete = 
					{["$script"] = fs.readFileSync('./lib/system-store/endpoints/store/?bucket/?id/delete-delete.lua')}
				local join_cluster = 
					{["$script"] = fs.readFileSync('./lib/system-store/endpoints/cluster/join/?ip/?port/post-join_cluster.lua')}

				local cli_join = 
					{["$script"] = fs.readFileSync('./lib/system-store/cli/join.lua')}
				local cli_leave = 
					{["$script"] = fs.readFileSync('./lib/system-store/cli/leave.lua')}
				local cli_store = 
					{["$script"] = fs.readFileSync('./lib/system-store/cli/store.lua')}
				local cli_fetch = 
					{["$script"] = fs.readFileSync('./lib/system-store/cli/fetch.lua')}
				local cli_delete = 
					{["$script"] = fs.readFileSync('./lib/system-store/cli/delete.lua')}

				local alive = 
					{["$script"] = fs.readFileSync('./lib/system-store/alive.lua')}
				local init = 
					{["$script"] = fs.readFileSync('./lib/system-store/init.lua')}
				local stop = 
					{["$script"] = fs.readFileSync('./lib/system-store/stop.lua')}

				local default = 
					{init = "init"
					,stop = "stop"
					,alive = "update_master"
					,["type"] = "choose_master"
					,endpoints =
						{get = 
							{["/store/sync/?version"] = "get-sync"
							,["/store/?bucket"] = "get-fetch"
							,["/store/?bucket/?id"] = "get-fetch"}
						,post = 
							{["/store/?bucket/?id"] = "post-post"
							,["/cluster/join/?ip/?port"] = "post-join_cluster"}
						,delete = 
							{["/store/?bucket/?id"] = "delete-delete"}}
					,cli =
						{join = "join"
						,leave = "leave"
						,store = "store"
						,fetch = "fetch"
						,delete = "delete"}}

				self:_store(self.storage,"store","update_master",alive,0,false,true)
				self:_store(self.storage,"store","get-sync",sync,0,false,true)
				self:_store(self.storage,"store","get-fetch",fetch,0,false,true)
				self:_store(self.storage,"store","post-post",post,0,false,true)
				self:_store(self.storage,"store","delete-delete",delete,0,false,true)
				self:_store(self.storage,"store","post-join_cluster",join_cluster,0,false,true)

				self:_store(self.storage,"store","join",cli_join,0,false,true)
				self:_store(self.storage,"store","leave",cli_leave,0,false,true)
				self:_store(self.storage,"store","store",cli_store,0,false,true)
				self:_store(self.storage,"store","fetch",cli_fetch,0,false,true)
				self:_store(self.storage,"store","delete",cli_delete,0,false,true)

				self:_store(self.storage,"store","init",init,0,false,true)
				self:_store(self.storage,"store","stop",stop,0,false,true)
				
				self:_store(self.storage,"systems","store",default,0,false,true)

				local replicated = 
					{["$script"] = fs.readFileSync('./lib/system-topology/replicated.lua')}
				local round_robin = 
					{["$script"] = fs.readFileSync('./lib/system-topology/round_robin.lua')}
				local choose_master = 
					{["$script"] = fs.readFileSync('./lib/system-topology/choose_master.lua')}
				local nothing = 
					{["$script"] = fs.readFileSync('./lib/system-topology/nothing.lua')}

				assert(self:_store(self.storage,"topology","replicated",replicated,0,false,true))
				assert(self:_store(self.storage,"topology","round_robin",round_robin,0,false,true))
				assert(self:_store(self.storage,"topology","choose_master",choose_master,0,false,true))
				assert(self:_store(self.storage,"topology","nothing",nothing,0,false,true))
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
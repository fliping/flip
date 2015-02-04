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
					local json,err = fs.readFileSync(self.db_path .. "/" .. file)
					if err then
						logger:error("it errored",err)
						process.exit(1)
					else
						local object = JSON.parse(json)
						self:_store(self.storage,object.bucket,object.id,object,nil,false,true)
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

				local alive = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/alive.lua')}
				local sync = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/sync.lua')}
				local fetch = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/fetch.lua')}
				local post = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/post.lua')}
				local delete = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/delete.lua')}
				local join_cluster = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/join_cluster.lua')}

				local cli_join = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/cli_join.lua')}
				local cli_leave = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/cli_leave.lua')}
				local cli_store = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/cli_store.lua')}
				local cli_fetch = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/cli_fetch.lua')}
				local cli_delete = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/cli_delete.lua')}

				local init = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/init.lua')}
				local stop = 
					{["$script"] = fs.readFileSync('./lib/store-scripts/stop.lua')}

				local default = 
					{init = "init"
					,stop = "stop"
					,alive = "update_master"
					,["type"] = "choose_master"
					,endpoints =
						{get = 
							{["/store/sync/?version"] = "sync"
							,["/store/?bucket"] = "fetch"
							,["/store/?bucket/?id"] = "fetch"}
						,post = 
							{["/store/?bucket/?id"] = "post"
							,["/cluster/join/?ip/?port"] = "join_cluster"}
						,delete = 
							{["/store/?bucket/?id"] = "delete"}}
					,cli =
						{join = "cli_join"
						,leave = "cli_leave"
						,store = "cli_store"
						,fetch = "cli_fetch"
						,delete = "cli_delete"}}

				self:_store(self.storage,"store","update_master",alive,0,false,true)
				self:_store(self.storage,"store","sync",sync,0,false,true)
				self:_store(self.storage,"store","fetch",fetch,0,false,true)
				self:_store(self.storage,"store","post",post,0,false,true)
				self:_store(self.storage,"store","delete",delete,0,false,true)
				self:_store(self.storage,"store","join_cluster",join_cluster,0,false,true)

				self:_store(self.storage,"store","cli_join",cli_join,0,false,true)
				self:_store(self.storage,"store","cli_leave",cli_leave,0,false,true)
				self:_store(self.storage,"store","cli_store",cli_store,0,false,true)
				self:_store(self.storage,"store","cli_fetch",cli_fetch,0,false,true)
				self:_store(self.storage,"store","cli_delete",cli_delete,0,false,true)

				self:_store(self.storage,"store","init",init,0,false,true)
				self:_store(self.storage,"store","stop",stop,0,false,true)
				
				self:_store(self.storage,"systems","store",default,0,false,true)

				local replicated = 
					{["$script"] = fs.readFileSync('./lib/plan/topologies/replicated.lua')}
				local round_robin = 
					{["$script"] = fs.readFileSync('./lib/plan/topologies/round_robin.lua')}
				local choose_master = 
					{["$script"] = fs.readFileSync('./lib/plan/topologies/choose_master.lua')}

				assert(self:_store(self.storage,"topologies","replicated",replicated,0,false,true))
				assert(self:_store(self.storage,"topologies","round_robin",round_robin,0,false,true))
				assert(self:_store(self.storage,"topologies","choose_master",choose_master,0,false,true))
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
				fs.writeFile(path,json,function() end)
			elseif action == "delete" then
				fs.unlink(path,function() end)
			end
		end)
	end
end
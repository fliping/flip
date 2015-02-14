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
local lmmdb = require('../lmmdb')
Env = lmmdb.Env
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor
DB = lmmdb.DB

return function(Store)

	function Store:load_from_disk(cb)
		fs.mkdir(self.db_path, "0755", function (is_new)
			if is_new and not (is_new.code == "EEXIST") then
				cb(false,is_new)
			else
				local env,err = Env.create()
				Env.set_maxdbs(env,3)
				if err then 
					logger:fatal('unable to create store',err)
					process.exit(1)
				end
				err = Env.open(env,self.db_path,0,0755)
				if err then 
					logger:fatal('unable to open store',err)
					process.exit(1)
				end
				self.env = env
				local txn = Env.txn_begin(env,nil,0)
				logger:info("going to open","objects",DB.MDB_CREATE)
				DB.open(txn,"objects",DB.MDB_CREATE)
				logger:info("going to open","logs",DB.MDB_CREATE)
				local logs,err1 = DB.open(txn,"logs",DB.MDB_CREATE + DB.MDB_INTEGERKEY)
				logger:info("logs db",logs,err1)
				local cursor,err = Cursor.open(txn,logs)
				local key,_op,err = Cursor.get(cursor,nil,Cursor.MDB_LAST,"unsigned long")
				logger:info("last log",key,_op,err)
				self.version = key
				if not self.version then
					self.version = 0
				end
				logger:info("going to open","buckets",DB.MDB_CREATE)
				DB.open(txn,"buckets",DB.MDB_DUPSORT + DB.MDB_CREATE)
				logger:info(Txn.commit(txn))
				cb(not (is_new == nil),err)
			end
		end)
	end

	function Store:open(cb)
		self:load_from_disk(function(need_bootstrap,err)
			logger:info(need_bootstrap,err)
			if need_bootstrap then
				logger:info("bootstrapping store")

				
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

				load(__dirname .. '/../system-topology','topology')
				load(__dirname .. '/../system-store','store')
				load(__dirname .. '/../system-dev','dev')

				logger:info('loaded bootstrapped store')
			elseif err then
				logger:fatal("unable to open disk store",err)
				process.exit(1)
			else
				logger:info('store was loaded from disk')
			end
			cb()
		end)
	end
end
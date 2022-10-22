/*************************************************************************
Copyright (C) 2022 The xfdb Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
***************************************************************************/

#ifndef __xfdb_engine_h__
#define __xfdb_engine_h__

#include <map>
#include "xfdb/strutil.h"
#include "types.h"
#include "notify_msg.h"
#include "file_util.h"
#include "block_pool.h"
#include <mutex>

namespace xfdb 
{

class Engine : public std::enable_shared_from_this<Engine>
{	
public:
	explicit Engine(const GlobalConfig& conf) : m_conf(conf)
	{}
	virtual ~Engine()
	{}
	
	inline const GlobalConfig& GetConfig() const
	{
		return m_conf;
	}
	
	BlockPool& GetBlockPool()
	{
		return m_pool;
	}
public:	
	virtual Status Start() = 0;

	Status OpenDB(const DBConfig& conf, const std::string& db_path, DBPtr& db);
	void CloseDB();
	
	virtual Status RemoveDB(const std::string& db_path)
	{
		return ERR_INVALID_MODE;
	}

protected:
	virtual DBImplPtr NewDB(const DBConfig& conf, const std::string& db_path) = 0;
	bool QueryDB(const std::string& db_path, DBImplPtr& dbptr);

protected:
	const GlobalConfig m_conf;
	BlockPool m_pool;
	
	mutable std::mutex m_db_mutex;
	std::map<std::string, DBImplWptr> m_dbs;	//key: db path
	
private:	
	Engine(const Engine&) = delete;
	Engine& operator=(const Engine&) = delete;
};


}  

#endif


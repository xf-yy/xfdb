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

#include "dbtypes.h"
#include "engine.h"
#include "logger.h"
#include "hash.h"
#include "file.h"
#include "readonly_db.h"
#include "notify_file.h"
#include "bucket.h"
#include "process.h"
#include "xfdb/db.h"
#include "readonly_engine.h"
#include "writable_engine.h"

namespace xfdb 
{

class EngineWrapper
{
public:
	~EngineWrapper()
	{
		if(m_engine)
		{
			m_engine->Stop();
		}
	}	

	EnginePtr GetEngine()
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		return m_engine;
	}

	Status Start(const GlobalConfig& conf)
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		if(m_engine)
		{
			return ERR_STARTED;
		}

		EnginePtr engine = NewEngine(conf);
		Status s = engine->Start();
		if(s == OK)
		{
			m_engine = engine;
		}
		return s;
	}

private:
	EnginePtr NewEngine(const GlobalConfig& conf)
	{
		if(conf.mode == MODE_READONLY)
		{
			return NewReadOnlyEngine(conf);
		}
		return NewWritableEngine(conf);
	}
	
private:
	std::mutex m_mutex;
	EnginePtr m_engine;
};
static EngineWrapper s_engine_wrapper;

const char* XfdbVersion()
{
	return XFDB_VERSION_DESC;
}

Status XfdbStart(const GlobalConfig& gconf)
{
	return s_engine_wrapper.Start(gconf);
}

EnginePtr Engine::GetEngine()
{
	return s_engine_wrapper.GetEngine();
}

Status Engine::Init()
{
	if(!m_conf.Check()) 
	{
		return ERR_INVALID_CONFIG;
	}

    //初始化随机数
    srand(time(nullptr));

	//如果多个进程同时打开日志呢？写锁
	//xfutil::LogConf log_conf;
	//conf.log_file_path;
	//if(Logger::Init(log_conf) != 0) 
	//{
		//return ERROR;
	//}
	uint64_t cache_num = m_conf.write_cache_size / LARGE_BLOCK_SIZE;
	if(m_conf.write_cache_size % LARGE_BLOCK_SIZE != 0)
	{
		++cache_num;
	}
	m_large_pool.Init(LARGE_BLOCK_SIZE, cache_num);
	m_small_pool.Init(4096, 16384);

	return OK;
}

void Engine::Uninit()
{
	//LogWarn("xfdb stopped");
	//Logger::Close();

}

Status Engine::OpenDB(const DBConfig& conf, const std::string& db_path, DBPtr& db)
{
	DBImplPtr dbptr;
	if(!QueryDB(db_path, dbptr))
	{
		dbptr = NewDB(conf, db_path);
		Status s = dbptr->Open();
		if(s != OK)
		{
			return s;
		}
		UpdateDB(db_path, dbptr);
	}
	
	db = std::shared_ptr<DB>(new DB(dbptr));
	return OK;
}

void Engine::CloseAllDB()
{
	std::map<std::string, DBImplWptr> dbs;

	m_db_mutex.lock();
	dbs.swap(m_dbs);
	m_db_mutex.unlock();

	for(auto it = dbs.begin(); it != dbs.end(); ++it)
	{
		DBImplPtr dbptr = it->second.lock();
		if(dbptr)
		{
			dbptr->Flush();
		}
	}
}

bool Engine::UpdateDB(const std::string& db_path, DBImplPtr& dbptr)
{
	DBImplWptr dbwptr(dbptr);

	m_db_mutex.lock();
	auto ret = m_dbs.insert(std::make_pair(db_path, dbwptr));
	if(!ret.second)
	{
		DBImplPtr prev_dbptr = ret.first->second.lock();
		if(prev_dbptr)
		{
			dbptr = prev_dbptr;
		}
		else
		{
			ret.first->second = dbwptr;
		}
	}
	m_db_mutex.unlock();

	return ret.second;
}

bool Engine::QueryDB(const std::string& db_path, DBImplPtr& dbptr)
{	
	bool found = false;

	m_db_mutex.lock();
	
	auto it = m_dbs.find(db_path);
	if(it != m_dbs.end())
	{
		dbptr = it->second.lock();
		if(dbptr)
		{
			found = true;
		}
		else
		{
			m_dbs.erase(it);
		}
	}
	m_db_mutex.unlock();

	return found;
}

}  



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

#include "db_types.h"
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

static EngineWrapper s_engine_wrapper;

const char* GetVersionString()
{
	return XFDB_VERSION_DESC;
}
uint32_t GetVersion()
{
    return (XFDB_MAJOR_VERSION<<16) | (XFDB_MINOR_VERSION<<8) | XFDB_PATCH_VERSION;
}

Status Start(const GlobalConfig& gconf)
{
	return s_engine_wrapper.Start(gconf);
}

EngineWrapper::~EngineWrapper()
{
    if(m_engine)
    {
        m_engine->Stop();
    }
}	

Status EngineWrapper::Start(const GlobalConfig& conf)
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

EnginePtr EngineWrapper::NewEngine(const GlobalConfig& conf)
{
    if(conf.mode == MODE_READONLY)
    {
        return NewReadOnlyEngine(conf);
    }
    return NewWritableEngine(conf);
}

EnginePtr& Engine::GetEngine()
{
	return s_engine_wrapper.GetEngine();
}

Status Engine::Start()
{
	if(!m_conf.Check()) 
	{
		return ERR_INVALID_CONFIG;
	}

    if(!m_conf.log_file_path.empty())
    {
        LogConfig log_conf;
        log_conf.file_path = m_conf.log_file_path;

        if(!Logger::Start(log_conf)) 
        {
            return ERR_FILE_OPEN;
        }
    }

    //初始化随机数
    srand(time(nullptr));

	uint64_t cache_num = (m_conf.write_cache_size+LARGE_BLOCK_SIZE-1) / LARGE_BLOCK_SIZE;
	m_large_block_pool.Init(LARGE_BLOCK_SIZE, cache_num);
	m_small_block_pool.Init(SMALL_BLOCK_SIZE, cache_num);

	Status s = Start_();
    m_started = (s == OK);
    return s;
}

void Engine::Stop()
{
    if(!m_started)
	{
		return;
	}

	//尝试关闭所有的db
	CloseAllDB();

    Stop_();

    if(!m_conf.log_file_path.empty())
    {
        Logger::Close(); 
    }
    m_started = false;
}

Status Engine::OpenDB(const DBConfig& conf, const std::string& db_path, DBPtr& db)
{
	DBImplPtr dbptr;
	if(!QueryDB(db_path, dbptr))
	{
		DBImplPtr new_dbptr = NewDB(conf, db_path);
		Status s = new_dbptr->Open();
		if(s != OK)
		{
			return s;
		}
		if(InsertDB(db_path, new_dbptr, dbptr))
        {
            dbptr = new_dbptr;
        }
        else
        {
            LogWarn("duplicate open db: %s", db_path.c_str());
        }
	}
	
	db = std::shared_ptr<DB>(new DB(dbptr));
	return OK;
}

void Engine::CloseDB(DBImplPtr& db)
{
	db->Flush();
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

bool Engine::InsertDB(const std::string& db_path, DBImplPtr& dbptr, DBImplPtr& old_dbptr)
{
	DBImplWptr dbwptr(dbptr);

	m_db_mutex.lock();
	auto ret = m_dbs.insert(std::make_pair(db_path, dbwptr));
    if(!ret.second)
    {
        old_dbptr = ret.first->second.lock();
        //对于无效的db，则重置
        if(!old_dbptr)
        {
            ret.first->second = dbptr;
            ret.second = true;
        }
    }
	m_db_mutex.unlock();

	return ret.second;
}

bool Engine::QueryDB(const std::string& db_path, DBImplPtr& dbptr) const
{	
	dbptr.reset();

	m_db_mutex.lock();
	
	auto it = m_dbs.find(db_path);
	if(it != m_dbs.end())
	{
		dbptr = it->second.lock();
	}

	m_db_mutex.unlock();

	return (bool)dbptr;
}

}  



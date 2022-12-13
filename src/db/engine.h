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
#include "db_types.h"
#include "notify_msg.h"
#include "file_util.h"
#include "block_pool.h"
#include <mutex>
#include "lru_cache.h"

namespace xfdb 
{

class Engine : public std::enable_shared_from_this<Engine>
{	
public:
	explicit Engine(const GlobalConfig& conf) 
		: m_conf(conf), 
		m_index_cache(conf.index_cache_size), 
		m_data_cache(conf.data_cache_size),
		m_bloom_filter_cache(conf.bloom_filter_cache_size)
	{
		m_started = false;
	}
	virtual ~Engine()
	{
		
	}
	
	static EnginePtr GetEngine();

	inline const GlobalConfig& GetConfig() const
	{
		return m_conf;
	}
	
	inline BlockPool& GetLargePool()
	{
		return m_large_pool;
	}
	inline BlockPool& GetSmallPool()
	{
		return m_small_pool;
	}	
	inline LruCache<std::string, std::string>& GetIndexCache()
	{
		return m_index_cache;
	}	
	inline LruCache<std::string, std::string>& GetDataCache()
	{
		return m_data_cache;
	}	
	inline LruCache<std::string, std::string>& GetBloomFilterCache()
	{
		return m_bloom_filter_cache;
	}	
public:	
	virtual Status Start() = 0;
	virtual void Stop() = 0;

	Status OpenDB(const DBConfig& conf, const std::string& db_path, DBPtr& db);
	void CloseAllDB();
	
	virtual Status RemoveDB(const std::string& db_path)
	{
		return ERR_INVALID_MODE;
	}

protected:
	Status Init();
	void Uninit();

	virtual DBImplPtr NewDB(const DBConfig& conf, const std::string& db_path) = 0;
	bool QueryDB(const std::string& db_path, DBImplPtr& dbptr);
	bool UpdateDB(const std::string& db_path, DBImplPtr& dbptr);

protected:
	const GlobalConfig m_conf;

	volatile bool m_started;

	BlockPool m_large_pool;
	BlockPool m_small_pool;

	//key:, value:
	LruCache<std::string, std::string> m_index_cache;
	LruCache<std::string, std::string> m_data_cache;
	LruCache<std::string, std::string> m_bloom_filter_cache;

	mutable std::mutex m_db_mutex;
	std::map<std::string, DBImplWptr> m_dbs;	//key: db path
	
private:	
	Engine(const Engine&) = delete;
	Engine& operator=(const Engine&) = delete;
};


}  

#endif


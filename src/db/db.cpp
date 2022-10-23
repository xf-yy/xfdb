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

#include <atomic>
#include <mutex>
#include "xfdb/db.h"
#include "types.h"
#include "logger.h"
#include "readonly_engine.h"
#include "writable_engine.h"
#include "process.h"

namespace xfdb
{

static std::mutex s_engine_mutex;
static EnginePtr s_engine;

const char* XfdbVersion()
{
	return XFDB_VERSION_DESC;
}

static inline EnginePtr GetEngine()
{
	std::lock_guard<std::mutex> lock(s_engine_mutex);
	return s_engine;
}

static inline EnginePtr NewEngine(const GlobalConfig& conf)
{
	if(conf.mode == MODE_READONLY)
	{
		return NewReadOnlyEngine(conf);
	}
	return NewWritableEngine(conf);
}

Status XfdbStart(const GlobalConfig& gconf)
{
	std::lock_guard<std::mutex> lock(s_engine_mutex);
	if(s_engine)
	{
		return ERR_INITIALIZED;
	}
	EnginePtr engine = NewEngine(gconf);
	Status s = engine->Start();
	if(s != OK)
	{
		return s;
	}
	s_engine.swap(engine);
	return OK;
}

DB::DB(DBImplPtr& db) : m_db(db)
{
	assert(db);
}

DB::~DB()
{
	m_db->Flush();
}

Status DB::Open(const DBConfig& dbconf, const std::string& db_path, DBPtr& db)
{
	if(db_path.empty() || db_path.back() == '/')
	{
		return ERR_PATH_INVALID;
	}
	EnginePtr engine = GetEngine();
	if(!engine)
	{
		return ERR_UNINITIALIZED;
	}
	return engine->OpenDB(dbconf, db_path, db);
}

Status DB::Remove(const std::string& db_path)
{
	if(db_path.empty() || db_path.back() == '/')
	{
		return ERR_PATH_INVALID;
	}
	EnginePtr engine = GetEngine();
	if(!engine)
	{
		return ERR_UNINITIALIZED;
	}
	return engine->RemoveDB(db_path);
}

Status DB::CreateBucket(const std::string& bucket_name)
{
	assert(m_db);
	return m_db->CreateBucket(bucket_name);
}

Status DB::DeleteBucket(const std::string& bucket_name)
{
	assert(m_db);
	return m_db->DeleteBucket(bucket_name);
}

bool DB::ExistBucket(const std::string& bucket_name) const
{
	assert(m_db);
	return m_db->ExistBucket(bucket_name);
}

void DB::ListBucket(std::vector<std::string>& bucket_names) const
{
	assert(m_db);
	m_db->ListBucket(bucket_names);
}

Status DB::GetBucketStat(const std::string& bucket_name, BucketStat& stat) const
{
	assert(m_db);
	return m_db->GetBucketStat(bucket_name, stat);
}

Status DB::Get(const std::string& bucket_name, const StrView& key, String& value) const
{
	assert(m_db);
	return m_db->Get(bucket_name, key, value);
}

Status DB::Set(const std::string& bucket_name, const StrView& key, const StrView& value)
{
	assert(m_db);
	return m_db->Set(bucket_name, key, value);
}

Status DB::Delete(const std::string& bucket_name, const StrView& key)
{
	assert(m_db);
	return m_db->Delete(bucket_name, key);
}
#if 0
Status DB::Write(const ObjectBatch& wb)
{
	assert(m_db);
	return m_db->Write(wb);
}
#endif

//将所有内存表（不限大小）刷盘(异步)，形成segment文件
Status DB::Flush()					
{
	assert(m_db);
	return m_db->Flush();
}

Status DB::Flush(const std::string& bucket_name)
{
	assert(m_db);
	return m_db->Flush(bucket_name);
}

Status DB::Merge()
{
	assert(m_db);
	return m_db->Merge();
}

Status DB::Merge(const std::string& bucket_name)	
{
	assert(m_db);
	return m_db->Merge(bucket_name);
}


}   



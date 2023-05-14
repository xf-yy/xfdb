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
#include "xfdb/iterator.h"
#include "db_types.h"
#include "logger.h"
#include "process.h"
#include "db_impl.h"
#include "engine.h"

namespace xfdb
{

bool GlobalConfig::Check() const
{
    switch(mode)
    {
    case MODE_READONLY:
        if(auto_reload_db)
        {
            if(notify_dir.empty())
            {
                return false;
            }
        }
        break;
    case MODE_WRITEONLY:
    case MODE_READWRITE:
        break;
    default: return false; 
        break;
    }

    //if(!log_file_path.empty() && log_file_path.back() == '/')
    //{
    //	return false;
    //}
    if(!notify_dir.empty() && notify_dir.back() == '/')
    {
        return false;
    }
    return true;
}

bool BucketConfig::Check() const
{
    if(max_level_num > MAX_LEVEL_ID)
    {
        return false;
    }
    return true;
}

bool DBConfig::Check() const
{
    return true;
}

DB::DB(DBImplPtr& db) : m_db(db)
{
	assert(db);
}

DB::~DB()
{
	EnginePtr engine = Engine::GetEngine();
	if(engine)
	{
        engine->CloseDB(m_db);
	}
}

Status DB::Open(const DBConfig& dbconf, const std::string& db_path, DBPtr& db)
{
	if(db_path.empty() || db_path.back() == '/')
	{
		return ERR_PATH_INVALID;
	}
	EnginePtr engine = Engine::GetEngine();
	if(!engine)
	{
		return ERR_STOPPED;
	}
	return engine->OpenDB(dbconf, db_path, db);
}

Status DB::Remove(const std::string& db_path)
{
	if(db_path.empty() || db_path.back() == '/')
	{
		return ERR_PATH_INVALID;
	}
	EnginePtr engine = Engine::GetEngine();
	if(!engine)
	{
		return ERR_STOPPED;
	}
	return engine->RemoveDB(db_path);
}

const std::string& DB::GetPath()
{
    assert(m_db);
    return m_db->GetPath();
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

bool DB::BucketExist(const std::string& bucket_name) const
{
	assert(m_db);
	return m_db->BucketExist(bucket_name);
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

Status DB::Get(const std::string& bucket_name, const StrView& key, std::string& value) const
{
	assert(m_db);
	return m_db->Get(bucket_name, key, value);
}

Status DB::NewIterator(const std::string& bucket_name, IteratorPtr& iter)
{
	assert(m_db);
	IteratorImplPtr iterptr;
    Status s = m_db->NewIterator(bucket_name, iterptr);
    if(s == OK)
    {
        iter = std::shared_ptr<Iterator>(new Iterator(iterptr));
    }
    return s;
}

Status DB::Set(const std::string& bucket_name, const StrView& key, const StrView& value)
{
	assert(m_db);
	return m_db->Set(bucket_name, key, value);
}

Status DB::Append(const std::string& bucket_name, const StrView& key, const StrView& value)
{
	assert(m_db);
	return m_db->Append(bucket_name, key, value);
}

Status DB::Delete(const std::string& bucket_name, const StrView& key)
{
	assert(m_db);
	return m_db->Delete(bucket_name, key);
}

Status DB::Write(const ObjectBatch& ob)
{
	assert(m_db);
	return m_db->Write(ob);
}

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

Status DB::Backup(const std::string& backup_dir)
{
	assert(m_db);
	return m_db->Backup(backup_dir);
}

}   



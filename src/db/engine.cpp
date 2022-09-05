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

#include "types.h"
#include "engine.h"
#include "logger.h"
#include "hash.h"
#include "file.h"
#include "readonly_db.h"
#include "notify_file.h"
#include "bucket.h"
#include "process.h"
#include "xfdb/db.h"

namespace xfdb 
{

Status Engine::OpenDB(const DBConfig& conf, const std::string& db_path, DBPtr& db)
{
	if(!conf.Check())
	{
		return ERR_INVALID_CONFIG;
	}	
	m_db_rwlock.ReadLock();
	bool found = (m_dbs.find(db_path) != m_dbs.end());
	m_db_rwlock.ReadUnlock();
	if(found)
	{
		return ERR_DB_OPENED;
	}
	
	DBImplPtr dbptr = NewDB(conf, db_path);
	Status s = dbptr->Open();
	if(s != OK)
	{
		return s;
	}

	m_db_rwlock.WriteLock();
	auto ret = m_dbs.insert(std::make_pair(dbptr->GetPath(), dbptr));
	m_db_rwlock.WriteUnlock();
	if(!ret.second)
	{
		return ERR_DB_OPENED;
	}
	
	db = std::shared_ptr<DB>(new DB(shared_from_this(), dbptr.get()));
	return OK;
}

void Engine::CloseDB(DBImpl* db)
{
	assert(db != nullptr);
	if(db == nullptr)
	{
		return;
	}
	db->BeforeClose();
	
	//如果是最后一个实例，将在这里释放
	DBImplPtr dbptr;
	QueryDB(db->GetPath(), dbptr, true);
}

void Engine::CloseAllDB()
{
    WriteLockGuard guard(m_db_rwlock);
	for(auto it = m_dbs.begin(); it != m_dbs.end(); ++it)
	{
		it->second->BeforeClose();
	}
	m_dbs.clear();
}

bool Engine::QueryDB(const std::string& db_path, DBImplPtr& dbptr, bool erased)
{	
	ReadLockGuard guard(m_db_rwlock);
	
	auto it = m_dbs.find(db_path);
	if(it != m_dbs.end())
	{
		dbptr = it->second;
		if(erased)
		{
			m_dbs.erase(it);
		}
		return true;
	}
	return false;
}

}  



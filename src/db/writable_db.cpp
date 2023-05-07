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
#include "db_types.h"
#include "writable_db.h"
#include "writeonly_bucket.h"
#include "bucketmeta_file.h"
#include "dbmeta_file.h"
#include "bucket_set.h"
#include "file.h"
#include "directory.h"
#include "object_reader_snapshot.h"
#include "notify_file.h"
#include "readwrite_bucket.h"
#include "logger.h"

namespace xfdb
{


/////////////////////////////////////////////////////////////////////
WritableDB::WritableDB(const DBConfig& conf, const std::string& db_path) 
	: DBImpl(conf, db_path)
{
	m_bucket_changed_cnt = 0;
}

WritableDB::~WritableDB()
{
}

BucketPtr WritableDB::NewBucket(const BucketInfo& bucket_info)
{
	EnginePtr engine = Engine::GetEngine();

	if(engine->GetConfig().mode & MODE_READONLY)
	{
		return NewReadWriteBucket((WritableEngine*)engine.get(), shared_from_this(), bucket_info);
	}
	return NewWriteOnlyBucket((WritableEngine*)engine.get(), shared_from_this(), bucket_info);
}

Status WritableDB::Remove(const std::string& db_path)
{
	//获取写锁
	{
		LockFile lockfile;
		Status s = lockfile.Open(db_path, LF_TRY_WRITE);
		if(s != OK) 
		{
			return s;
		}
		//获取bucket文件列表，依次删除bucket及数据
		std::vector<FileName> file_names;
		ListDBMetaFile(db_path.c_str(), file_names);
		for(size_t i = 0; i < file_names.size(); ++i)
		{
			const FileName& fn = file_names[i];
			
			DBMeta dm;
			s = DBMetaFile::Read(db_path.c_str(), fn.str, dm);
			if(s != OK)
			{
				return s;
			}
			char bucket_path[MAX_PATH_LEN];
			for(const auto bi : dm.deleted_buckets)
			{
				MakeBucketPath(db_path.c_str(), bi.name.c_str(), bi.id, bucket_path);
				s = WriteOnlyBucket::Remove(bucket_path);
				if(s != OK)
				{
					return s;
				}
			}
			//最后一个将存活的所有bucket删除掉
			if(i == file_names.size()-1)
			{
				for(const auto bi : dm.alive_buckets)
				{
					MakeBucketPath(db_path.c_str(), bi.name.c_str(), bi.id, bucket_path);
					s = WriteOnlyBucket::Remove(bucket_path);
					if(s != OK)
					{
						return s;
					}
				}
			}
		}
	}

	Directory::Remove(db_path.c_str());
	return OK;
}


Status WritableDB::CreateIfMissing()
{
	//判断锁文件是否存在
	if(LockFile::Exist(m_path))
	{
		return OK;
	}
	EnginePtr engine = Engine::GetEngine();
	if(!engine->GetConfig().create_db_if_missing) 
	{
		return ERR_DB_NOT_EXIST;
	}

	if(!Directory::Create(m_path.c_str()))
	{
		return ERR_PATH_CREATE;
	}
	return LockFile::Create(m_path) ? OK : ERR_PATH_CREATE;
}

Status WritableDB::Open()
{	
	if(!m_conf.Check())
	{
		return ERR_INVALID_CONFIG;
	}	
	Status s = CreateIfMissing();
	if(s != OK) 
	{
		return s;
	}
	//打开锁文件
	s = m_lock_file.Open(m_path, LF_TRY_WRITE);
	if(s != OK) 
	{
		return s;
	}
	RemoveTempFile(m_path.c_str());
	
	//读取最新的bucket list
	std::vector<FileName> file_infos;
	s = ListDBMetaFile(m_path.c_str(), file_infos);
	if(s != OK) 
	{
		return s;
	}
	if(file_infos.empty()) 
	{
		return OK;
	}
	for(size_t i = 0; i < file_infos.size()-1; ++i)
	{
		m_tobe_delete_dbmeta_files.push_back(file_infos[i]);
	}

	DBMeta dm;
	s = DBMetaFile::Read(m_path.c_str(), file_infos.back().str, dm);
	if(s != OK)
	{
		return s;
	}
	
	return OpenBucket(file_infos.back().str, dm);
}

////////////////////////////////////////////////////////////////////////////

Status WritableDB::Clean()
{
	Status s1 = CleanDBMeta();
	Status s2 = CleanBucket();
	return (s1 == ERR_NOMORE_DATA && s2 == ERR_NOMORE_DATA) ? ERR_NOMORE_DATA : OK;
}

Status WritableDB::CleanDBMeta()
{
	//尝试清除bucket list，保证deleted的bucket已被清理
	//保证一次只有一个线程在执行clean操作
	FileName clean_filename;
	for(;;)
	{		
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(m_tobe_delete_dbmeta_files.empty()) 
			{
				return ERR_NOMORE_DATA;
			}
			clean_filename = m_tobe_delete_dbmeta_files.front();
		}
		
		Status s = DBMetaFile::Remove(m_path.c_str(), clean_filename.str);
		if(s != OK)
		{
			return s;
		}
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(!m_tobe_delete_dbmeta_files.empty()) 
			{
				m_tobe_delete_dbmeta_files.pop_front();
			}
		}
	}

	return OK;	
}

Status WritableDB::CleanBucket()
{
	//FIXME: 这里尝试清除所有的bucket
	m_bucket_rwlock.ReadLock();
	BucketSetPtr bs_ptr = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	if(bs_ptr)
	{
		bs_ptr->Clean();
	}

	return OK;
}

Status WritableDB::WriteDBMeta()
{
	DBMeta dm;
	fileid_t dbmeta_fileid;

	{
		std::lock_guard<std::mutex> lock(m_mutex);
		
		//注意：保证只被1个线程调用
		if(m_bucket_changed_cnt == 0)
		{
			return ERR_NOMORE_DATA;	//没有可写的数据
		}
		assert(m_next_dbmeta_fileid < MAX_FILE_ID);

		m_bucket_changed_cnt = 0;
		dbmeta_fileid = m_next_dbmeta_fileid++;
		WriteDBMeta(dm);
	}	
	
	//写bucket文件
	char dbmeta_filename[MAX_FILENAME_LEN];
	MakeDBMetaFileName(dbmeta_fileid, dbmeta_filename);
	
	Status s = DBMetaFile::Write(m_path.c_str(), dbmeta_filename, dm);
	if(s != OK)
	{
        LogWarn("write dbmeta of %s failed, status: %u", m_path.c_str(), s)
		return s;
	}
	
	NotifyData nd(NOTIFY_UPDATE_DB_META, m_path, dbmeta_fileid);

	EnginePtr engine = Engine::GetEngine();
	((WritableEngine*)engine.get())->WriteNotifyFile(nd);

	if(dbmeta_fileid != MIN_FILE_ID)
	{
		FileName name;
		MakeDBMetaFileName(dbmeta_fileid-1, name.str);

		{
		std::lock_guard<std::mutex> lock(m_mutex);
		m_tobe_delete_dbmeta_files.push_back(name);
		}
		((WritableEngine*)engine.get())->NotifyClean(shared_from_this());
	}
	return OK;
}

Status WritableDB::CreateBucket(const std::string& bucket_name)
{
	BucketPtr bptr;
	return CreateBucket(bucket_name, bptr);
}

Status WritableDB::CreateBucket(const std::string& bucket_name, BucketPtr& bptr)
{
	if(!CheckBucketName(bucket_name))
	{
		return ERR_BUCKET_NAME;
	}

	{
		std::lock_guard<std::mutex> lock(m_mutex);
		
		m_bucket_rwlock.ReadLock();
		BucketSetPtr bs_ptr = m_bucket_set;
		m_bucket_rwlock.ReadUnlock();

		std::map<std::string, BucketPtr> new_buckets;
		if(bs_ptr)
		{
			const auto& buckets = bs_ptr->Buckets();
			auto it = buckets.find(bucket_name);
			if(it != buckets.end())
			{
				return ERR_BUCKET_EXIST;
			}
			new_buckets = buckets;
		}
		
		BucketInfo bi(bucket_name, m_next_bucket_id++);
		Status s = OpenBucket(bi, bptr);
		if(s != OK)
		{
			return s;
		}
		
		new_buckets[bucket_name] = bptr;
		BucketSetPtr new_bucket_set = NewBucketSet(new_buckets);

		m_bucket_rwlock.WriteLock();
		m_bucket_set.swap(new_bucket_set);
		m_bucket_rwlock.WriteUnlock();

		++m_bucket_changed_cnt;
	}
	EnginePtr engine = Engine::GetEngine();
	((WritableEngine*)engine.get())->NotifyWriteDBMeta(shared_from_this());
	return OK;
}

Status WritableDB::CreateBucketIfMissing(const std::string& bucket_name, BucketPtr& bptr)
{
	if(GetBucket(bucket_name, bptr))
	{
		return OK;
	}
	EnginePtr engine = Engine::GetEngine();
	if(!m_conf.create_bucket_if_missing)
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	return CreateBucket(bucket_name, bptr);
}

Status WritableDB::DeleteBucket(const std::string& bucket_name)
{
	BucketSetPtr new_bs_ptr;

	{
		std::lock_guard<std::mutex> lock(m_mutex);

		m_bucket_rwlock.ReadLock();
		BucketSetPtr bs_ptr = m_bucket_set;
		m_bucket_rwlock.ReadUnlock();

		if(!bs_ptr)
		{
			return ERR_BUCKET_NOT_EXIST;
		}
		auto buckets = bs_ptr->Buckets();
		auto it = buckets.find(bucket_name);
		if(it == buckets.end())
		{
			return ERR_BUCKET_NOT_EXIST;
		}
		//清除内存表
		WriteOnlyBucket* bucket = (WriteOnlyBucket*)it->second.get();
		bucket->Clear();
		buckets.erase(it);
		
		new_bs_ptr = NewBucketSet(buckets);

		m_bucket_rwlock.WriteLock();
		m_bucket_set.swap(new_bs_ptr);
		m_bucket_rwlock.WriteUnlock();
		
		m_deleting_buckets[bucket->GetInfo().id] = bucket_name;

		++m_bucket_changed_cnt;
	}
	EnginePtr engine = Engine::GetEngine();
	((WritableEngine*)engine.get())->NotifyWriteDBMeta(shared_from_this());
	return OK;
}

Status WritableDB::Write(const std::string& bucket_name, const Object* object)
{	
	BucketPtr bptr;
	Status s = CreateBucketIfMissing(bucket_name, bptr);
	if(s != OK)
	{
		return s;
	}
	
	WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
	return bucket->Write(object);
}

Status WritableDB::Set(const std::string& bucket_name, const StrView& key, const StrView& value)
{
	assert(key.size > 0 && key.size <= MAX_KEY_SIZE);
	assert(value.size <= MAX_VALUE_SIZE);
	if(key.size == 0 || key.size > MAX_KEY_SIZE || value.size > MAX_VALUE_SIZE)
	{
		return ERR_OBJECT_TOO_LARGE;
	}

	Object obj = {SetType, key, value};
	return Write(bucket_name, &obj);
}

Status WritableDB::Append(const std::string& bucket_name, const StrView& key, const StrView& value)
{
	assert(key.size > 0 && key.size <= MAX_KEY_SIZE);
	assert(value.size <= MAX_VALUE_SIZE);
	if(key.size == 0 || key.size > MAX_KEY_SIZE || value.size > MAX_VALUE_SIZE)
	{
		return ERR_OBJECT_TOO_LARGE;
	}

	Object obj = {AppendType, key, value};
	return Write(bucket_name, &obj);
}

Status WritableDB::Delete(const std::string& bucket_name, const StrView& key)
{
	assert(key.size > 0 && key.size <= MAX_KEY_SIZE);
	if(key.size == 0 || key.size > MAX_KEY_SIZE)
	{
		return ERR_OBJECT_TOO_LARGE;
	}

	Object obj = {DeleteType, key};
	return Write(bucket_name, &obj);
}

Status WritableDB::Write(const ObjectBatch& ob)
{
	for(auto it = ob.m_data.begin(); it != ob.m_data.end(); ++it)
	{
		BucketPtr bptr;
		Status s = CreateBucketIfMissing(it->first, bptr);
		if(s == OK)
		{
			WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
			bucket->Write(it->second);
		}
	}
	return OK;
}

Status WritableDB::Get(const std::string& bucket_name, const StrView& key, std::string& value) const
{	
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
	return bucket->Get(key, value);
}

Status WritableDB::NewIterator(const std::string& bucket_name, IteratorImplPtr& iter)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
	return bucket->NewIterator(iter);
}

Status WritableDB::TryFlush()
{
	m_bucket_rwlock.ReadLock();
	BucketSetPtr bucket_set = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_set)
	{
		//FIXME: 查找m_tobe_flush_buckets
		bucket_set->TryFlush();
	}
	return OK;
}

Status WritableDB::TryFlush(const std::string& bucket_name)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}

	return bptr->TryFlush();
}

Status WritableDB::Flush()
{
	m_bucket_rwlock.ReadLock();
	BucketSetPtr bucket_set = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_set)
	{
		//FIXME: 查找m_tobe_flush_buckets
		bucket_set->Flush();
	}
	return OK;
}

Status WritableDB::Flush(const std::string& bucket_name)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}

	return bptr->Flush();
}

Status WritableDB::Merge()
{
	m_bucket_rwlock.ReadLock();
	BucketSetPtr bucket_set = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_set)
	{
		bucket_set->Merge();
	}
	return OK;
}

Status WritableDB::Merge(const std::string& bucket_name)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	return bptr->Merge();
}

void WritableDB::WriteDBMeta(DBMeta& dm)
{
	//已经获取到锁
	
	m_bucket_rwlock.ReadLock();
	BucketSetPtr bs_ptr = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	if(bs_ptr)
	{
		const auto& buckets = bs_ptr->Buckets();
		
		dm.alive_buckets.reserve(buckets.size());
		for(auto it = buckets.begin(); it != buckets.end(); ++it)
		{
			dm.alive_buckets.push_back(it->second->GetInfo());
		}
	}
	for(auto it = m_deleting_buckets.begin(); it != m_deleting_buckets.end(); ++it)
	{
		BucketInfo binfo(it->second, it->first);
		dm.deleted_buckets.push_back(binfo);
	}
	m_deleting_buckets.clear();

	dm.next_bucketid = m_next_bucket_id;
}



}   



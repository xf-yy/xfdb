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
#include "types.h"
#include "write_db.h"
#include "writeonly_bucket.h"
#include "bucket_meta_file.h"
#include "db_info_file.h"
#include "bucket_snapshot.h"
#include "file.h"
#include "directory.h"
#include "table_reader_snapshot.h"
#include "notify_file.h"
#include "readwrite_bucket.h"
#include "logger.h"

namespace xfdb
{


/////////////////////////////////////////////////////////////////////
WritableDB::WritableDB(WritableEngine* engine, const DBConfig& conf, const std::string& db_path) 
	: DBImpl(engine, conf, db_path)
{
	m_bucket_changed_cnt = 0;
}

WritableDB::~WritableDB()
{
}

BucketPtr WritableDB::NewBucket(const BucketInfo& bucket_info)
{
	if(m_engine->GetConfig().mode & MODE_READONLY)
	{
		return NewReadWriteBucket((WritableEngine*)m_engine, shared_from_this(), bucket_info);
	}
	return NewWriteOnlyBucket((WritableEngine*)m_engine, shared_from_this(), bucket_info);
}

Status WritableDB::Remove(const std::string& db_path)
{
	//获取写锁
	{
		LockFile lockfile;
		Status s = lockfile.Open(db_path, LOCK_TRY_WRITE);
		if(s != OK) 
		{
			return s;
		}
		//获取bucket文件列表，依次删除bucket及数据
		std::vector<FileName> file_names;
		ListDbInfoFile(db_path.c_str(), file_names);
		for(size_t i = 0; i < file_names.size(); ++i)
		{
			const FileName& fn = file_names[i];
			
			DbInfoData bd;
			s = DbInfoFile::Read(db_path.c_str(), fn.str, bd);
			if(s != OK)
			{
				return s;
			}
			char bucket_path[MAX_PATH_LEN];
			for(const auto bi : bd.deleted_buckets)
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
				for(const auto bi : bd.alive_buckets)
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
	if(!m_engine->GetConfig().create_db_if_missing) 
	{
		return ERR_DB_NOT_EXIST;
	}
	//TODO: 使用全局锁?
	if(!Directory::Create(m_path.c_str()))
	{
		return ERR_PATH_CREATE;
	}
	return LockFile::Create(m_path) ? OK : ERR_PATH_CREATE;
}

Status WritableDB::Open()
{	
	Status s = CreateIfMissing();
	if(s != OK) 
	{
		return s;
	}
	//打开锁文件
	s = m_lock_file.Open(m_path, LOCK_TRY_WRITE);
	if(s != OK) 
	{
		return s;
	}
	RemoveTempFile(m_path.c_str());
	
	//读取最新的bucket list
	std::vector<FileName> file_infos;
	s = ListDbInfoFile(m_path.c_str(), file_infos);
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
		m_delete_dbinfo_files.push_back(file_infos[i]);
	}

	DbInfoData bld;
	s = DbInfoFile::Read(m_path.c_str(), file_infos.back().str, bld);
	if(s != OK)
	{
		return s;
	}
	
	s = OpenBucket(file_infos.back().str, bld);
	if(s != OK) 
	{
		return s;
	}
	for(auto bi : bld.alive_buckets)
	{
		m_clean_buckets.insert(bi.name);
	}
	
	return OK;
}

void WritableDB::BeforeClose()
{
	Flush();
}

////////////////////////////////////////////////////////////////////////////

Status WritableDB::Clean()
{
	Status s1 = CleanDbInfo();
	Status s2 = CleanBucket();
	return (s1 == ERR_NOMORE_DATA && s2 == ERR_NOMORE_DATA) ? ERR_NOMORE_DATA : OK;
}

Status WritableDB::CleanDbInfo()
{
	//尝试清除bucket list，保证deleted的bucket已被清理
	//保证一次只有一个线程在执行clean操作
	FileName clean_filename;
	for(;;)
	{		
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(m_delete_dbinfo_files.empty()) 
			{
				return ERR_NOMORE_DATA;
			}
			clean_filename = m_delete_dbinfo_files.front();
		}
		
		Status s = DbInfoFile::Remove(m_path.c_str(), clean_filename.str);
		if(s != OK)
		{
			return s;
		}
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(!m_delete_dbinfo_files.empty()) 
			{
				m_delete_dbinfo_files.pop_front();
			}
		}
	}

	return OK;	
}

Status WritableDB::CleanBucket(const std::string& bucket_name)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}

	WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
	return bucket->Clean();
}

Status WritableDB::CleanBucket()
{
	for(auto it = m_clean_buckets.begin(); it != m_clean_buckets.end(); )
	{
		if(CleanBucket(*it) == OK)
		{
			++it;
		}
		else
		{
			m_clean_buckets.erase(it++);
		}
	}
	return m_clean_buckets.empty() ? ERR_NOMORE_DATA : OK;
}

Status WritableDB::WriteDbInfo()
{
	DbInfoData bd;
	fileid_t dbinfo_fileid;

	{
		std::lock_guard<std::mutex> lock(m_mutex);
		
		//注意：保证只被1个线程调用
		if(m_bucket_changed_cnt == 0)
		{
			return ERR_NOMORE_DATA;	//没有可写的数据
		}
		assert(m_next_dbinfo_fileid < MAX_FILEID);

		m_bucket_changed_cnt = 0;
		dbinfo_fileid = m_next_dbinfo_fileid++;
		FillDbInfoData(bd);
	}	
	
	//写bucket文件
	char dbinfo_filename[MAX_FILENAME_LEN];
	MakeDbInfoFileName(dbinfo_fileid, dbinfo_filename);
	
	Status s = DbInfoFile::Write(m_path.c_str(), dbinfo_filename, bd);
	if(s != OK)
	{
		//LogWarn(msg_fmt, ...);
		return s;
	}
	
	NotifyData nd(NOTIFY_UPDATE_DB_INFO, m_path, dbinfo_fileid);
	((WritableEngine*)m_engine)->WriteNotifyFile(nd);

	//TODO: 将旧的dbinfo写入delete队列
	if(dbinfo_fileid != MIN_FILEID)
	{
		FileName name;
		MakeDbInfoFileName(dbinfo_fileid-1, name.str);

		{
		std::lock_guard<std::mutex> lock(m_mutex);
		m_delete_dbinfo_files.push_back(name);
		}
		((WritableEngine*)m_engine)->NotifyClean(shared_from_this());
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
	//TODO:校验name字符"_-数字字母"
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		
		m_bucket_rwlock.ReadLock();
		BucketSnapshotPtr bs_ptr = m_bucket_snapshot;
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
		BucketSnapshotPtr new_bucket_snapshot = NewBucketSnapshot(new_buckets);

		m_bucket_rwlock.WriteLock();
		m_bucket_snapshot.swap(new_bucket_snapshot);
		m_bucket_rwlock.WriteUnlock();
		
		++m_bucket_changed_cnt;
	}
	
	((WritableEngine*)m_engine)->NotifyWriteDbInfo(shared_from_this());
	return OK;
}

Status WritableDB::CreateBucketIfMissing(const std::string& bucket_name, BucketPtr& bptr)
{
	if(GetBucket(bucket_name, bptr))
	{
		return OK;
	}
	if(!m_engine->GetConfig().create_bucket_if_missing)
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	return CreateBucket(bucket_name, bptr);
}

Status WritableDB::DeleteBucket(const std::string& bucket_name)
{
	BucketSnapshotPtr new_bs_ptr;

	{
		std::lock_guard<std::mutex> lock(m_mutex);

		m_bucket_rwlock.ReadLock();
		BucketSnapshotPtr bs_ptr = m_bucket_snapshot;
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
		
		new_bs_ptr = NewBucketSnapshot(buckets);

		m_bucket_rwlock.WriteLock();
		m_bucket_snapshot.swap(new_bs_ptr);
		m_bucket_rwlock.WriteUnlock();
		
		m_deleting_buckets[bucket->GetInfo().id] = bucket_name;
		++m_bucket_changed_cnt;
	}
	((WritableEngine*)m_engine)->NotifyWriteDbInfo(shared_from_this());
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


Status WritableDB::Get(const std::string& bucket_name, const StrView& key, String& value) const
{	
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
	return bucket->Get(key, value);
}

Status WritableDB::TryFlush()
{
	m_bucket_rwlock.ReadLock();
	BucketSnapshotPtr bucket_snapshot = m_bucket_snapshot;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_snapshot)
	{
		bucket_snapshot->TryFlush();
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
	BucketSnapshotPtr bucket_snapshot = m_bucket_snapshot;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_snapshot)
	{
		bucket_snapshot->Flush();
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
	BucketSnapshotPtr bucket_snapshot = m_bucket_snapshot;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_snapshot)
	{
		bucket_snapshot->Merge();
	}
	return OK;
}

Status WritableDB::Merge(const std::string& bucket_name)
{
	//TODO:是否要先判断是否满足full merge条件
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	bptr->Merge();
	return OK;
}

Status WritableDB::FullMerge(const std::string& bucket_name)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}

	WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
	return bucket->FullMerge();
}

Status WritableDB::PartMerge(const std::string& bucket_name)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}

	WriteOnlyBucket* bucket = (WriteOnlyBucket*)bptr.get();
	return bucket->PartMerge();
}

void WritableDB::FillDbInfoData(DbInfoData& bd)
{
	m_bucket_rwlock.ReadLock();
	BucketSnapshotPtr bs_ptr = m_bucket_snapshot;
	m_bucket_rwlock.ReadUnlock();

	if(bs_ptr)
	{
		const auto& buckets = bs_ptr->Buckets();
		
		bd.alive_buckets.reserve(buckets.size());
		for(auto it = buckets.begin(); it != buckets.end(); ++it)
		{
			bd.alive_buckets.push_back(it->second->GetInfo());
		}
	}
	for(auto it = m_deleting_buckets.begin(); it != m_deleting_buckets.end(); ++it)
	{
		BucketInfo binfo(it->second, it->first);
		bd.deleted_buckets.push_back(binfo);
	}
	m_deleting_buckets.clear();
	//for(auto it = m_deleted_buckets.begin(); it != m_deleted_buckets.end(); ++it)
	//{
	//	BucketInfo binfo(it->second, it->first);
	//	bd.deleted_buckets.push_back(binfo);
	//}
	bd.next_bucketid = m_next_bucket_id;
}



}   



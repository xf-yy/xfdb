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

#include "db_impl.h"
#include "db_types.h"
#include "bucket.h"
#include "db_metafile.h"
#include "bucket_set.h"
#include "logger.h"
#include "file_util.h"
#include "directory.h"
#include "lock_file.h"


namespace xfdb 
{

DBImpl::DBImpl(const DBConfig& conf, const std::string& db_path)
	: m_conf(conf), m_path(db_path)
{
	m_next_bucket_id = MIN_BUCKET_ID;
	m_next_dbmeta_fileid = MIN_FILE_ID;
}

DBImpl::~DBImpl()
{
}

bool DBImpl::ExistBucket(const std::string& bucket_name) const
{
	BucketPtr bptr;
	return GetBucket(bucket_name, bptr);
}

void DBImpl::ListBucket(std::vector<std::string>& bucket_names) const
{
	m_bucket_rwlock.ReadLock();
	BucketSetPtr bucket_set = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_set)
	{
		bucket_set->List(bucket_names);
	}
}

//如果name为nullptr，则获取的是default桶
bool DBImpl::GetBucket(const std::string& bucket_name, BucketPtr& ptr) const
{
	m_bucket_rwlock.ReadLock();
	BucketSetPtr bucket_set = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	if(!bucket_set)
	{
		return false;
	}

	const auto& buckets = bucket_set->Buckets();
	auto it = buckets.find(bucket_name);
	if(it == buckets.end())
	{
		return false;
	}
	ptr = it->second;
	return true;
}

//完成
Status DBImpl::GetBucketStat(const std::string& bucket_name, BucketStat& stat) const
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	bptr->GetStat(stat);
	return OK;
}

Status DBImpl::OpenBucket(const BucketInfo& bi, BucketPtr& bptr)
{
	bptr = NewBucket(bi);
	Status s = bptr->Open();
	if(s != OK)
	{
		LogWarn("failed to open bucket: %s, status: %u, errno: %u", bi.name.c_str(), s, LastError);
	}
	return s;
}

void DBImpl::OpenBucket(const DBMeta& dm, const BucketSet* last_bucket_set, std::map<std::string, BucketPtr>& buckets)
{	
	assert(last_bucket_set != nullptr);
	const auto& last_buckets = last_bucket_set->Buckets();
	
	BucketPtr bptr;
	for(const auto& bi : dm.alive_buckets)
	{
		auto it = last_buckets.find(bi.name);
		if(it != last_buckets.end())
		{
			buckets[bi.name] = it->second;
		}
		else if(OpenBucket(bi, bptr) == OK)
		{
			buckets[bi.name] = bptr;
		}
	}
}

void DBImpl::OpenBucket(const DBMeta& dm, std::map<std::string, BucketPtr>& buckets)
{	
	BucketPtr bptr;
	for(const auto& bi : dm.alive_buckets)
	{
		if(OpenBucket(bi, bptr) == OK)
		{
			buckets[bi.name] = bptr;
		}
	}
}

Status DBImpl::OpenBucket()
{
	//读取最新的bucket list
	std::vector<FileName> file_names;
	Status s = ListDBMetaFile(m_path.c_str(), file_names);
	if(s != OK)
	{
		return s;
	}
	if(file_names.empty()) 
	{
		return OK;
	}
	return OpenBucket(file_names.back().str);
}

Status DBImpl::OpenBucket(const char* dbmeta_filename)
{
	assert(dbmeta_filename != nullptr);
	DBMeta dm;
	Status s = DBMetaFile::Read(m_path.c_str(), dbmeta_filename, dm);
	if(s != OK)
	{
		return s;
	}
	return OpenBucket(dbmeta_filename, dm);
}

Status DBImpl::OpenBucket(const char* dbmeta_filename, const DBMeta& dm)
{	
	fileid_t fileid = strtoull(dbmeta_filename, nullptr, 10);
	
	std::lock_guard<std::mutex> lock(m_mutex);
	if(fileid < m_next_dbmeta_fileid)
	{
		return ERROR;
	}

	m_bucket_rwlock.ReadLock();
	BucketSetPtr bucket_set = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

	std::map<std::string, BucketPtr> buckets;
	if(bucket_set)
	{
		OpenBucket(dm, bucket_set.get(), buckets);
	}
	else
	{
		OpenBucket(dm, buckets);
	}
	BucketSetPtr new_bucket_set = NewBucketSet(buckets);

	m_bucket_rwlock.WriteLock();
	m_bucket_set.swap(new_bucket_set);
	m_next_dbmeta_fileid = fileid+1;
	m_next_bucket_id = dm.next_bucketid;
	m_bucket_rwlock.WriteUnlock();
	
	return OK;
}

Status DBImpl::BackupDBMeta(BucketSetPtr& bucket_set, const std::string& backup_db_dir)
{
	DBMeta dm;
	fileid_t dbmeta_fileid;

	{
		std::lock_guard<std::mutex> lock(m_mutex);

		dbmeta_fileid = m_next_dbmeta_fileid > MIN_FILE_ID ? (m_next_dbmeta_fileid-1) : m_next_dbmeta_fileid;

        const auto& buckets = bucket_set->Buckets();
        
        dm.alive_buckets.reserve(buckets.size());
        for(auto it = buckets.begin(); it != buckets.end(); ++it)
        {
            dm.alive_buckets.push_back(it->second->GetInfo());
        }

        dm.next_bucketid = m_next_bucket_id;
	}	
	
	//写bucket文件
	char dbmeta_filename[MAX_FILENAME_LEN];
	MakeDBMetaFileName(dbmeta_fileid, dbmeta_filename);
	
	Status s = DBMetaFile::Write(backup_db_dir.c_str(), dbmeta_filename, dm);
	if(s != OK)
	{
        LogWarn("write dbmeta of %s/%s failed, status: %u", backup_db_dir.c_str(), dbmeta_filename, s);
	}

	return s;
}

Status DBImpl::Backup(const std::string& backup_dir)
{
    if(!xfutil::Directory::Exist(backup_dir.c_str()))
    {
        return ERR_PATH_NOT_EXIST;
    }
    //
    std::string backup_db_dir = backup_dir;
    backup_db_dir.append("/");
    backup_db_dir.append(Path::GetFileName(m_path.c_str()));

    if(xfutil::Directory::Exist(backup_db_dir.c_str()))
    {
        return ERR_PATH_EXIST;
    }

    if(!xfutil::Directory::Create(backup_db_dir.c_str()))
    {
        return ERR_PATH_CREATE;
    }

	m_bucket_rwlock.ReadLock();
	BucketSetPtr bucket_set = m_bucket_set;
	m_bucket_rwlock.ReadUnlock();

    if(!bucket_set)
    {
        return OK;
    }

    //对每个bucket进行backup，然后再写dbmeta
    const auto& buckets = bucket_set->Buckets();
    for(auto it = buckets.begin(); it != buckets.end(); ++it)
    {
        Status s = it->second->Backup(backup_db_dir);
        if(s != OK)
        {
            return s;
        }
    }
    Status s = BackupDBMeta(bucket_set, backup_db_dir);
    if(s != OK)
    {
        return s;
    }

    if(!LockFile::Create(backup_db_dir))
    {
        return ERR_PATH_CREATE;
    }
    return OK;
}

}  



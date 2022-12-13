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
#include "db_infofile.h"
#include "bucket_list.h"
#include "logger.h"
#include "file_util.h"


namespace xfdb 
{

DBImpl::DBImpl(const DBConfig& conf, const std::string& db_path)
	: m_conf(conf), m_path(db_path)
{
	m_next_bucket_id = MIN_BUCKET_ID;
	m_next_dbinfo_fileid = MIN_FILE_ID;
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
	BucketListPtr bucket_list = m_bucket_list;
	m_bucket_rwlock.ReadUnlock();

	if(bucket_list)
	{
		bucket_list->ListBucket(bucket_names);
	}
}

//如果name为nullptr，则获取的是default桶
bool DBImpl::GetBucket(const std::string& bucket_name, BucketPtr& ptr) const
{
	m_bucket_rwlock.ReadLock();
	BucketListPtr bucket_list = m_bucket_list;
	m_bucket_rwlock.ReadUnlock();

	if(!bucket_list)
	{
		return false;
	}

	const auto& buckets = bucket_list->Buckets();
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

void DBImpl::OpenBucket(const DBInfoData& bld, const BucketList* last_bucket_list, std::map<std::string, BucketPtr>& buckets)
{	
	assert(last_bucket_list != nullptr);
	const auto& last_buckets = last_bucket_list->Buckets();
	
	BucketPtr bptr;
	for(const auto& bi : bld.alive_buckets)
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

void DBImpl::OpenBucket(const DBInfoData& bld, std::map<std::string, BucketPtr>& buckets)
{	
	BucketPtr bptr;
	for(const auto& bi : bld.alive_buckets)
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
	Status s = ListDBInfoFile(m_path.c_str(), file_names);
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

Status DBImpl::OpenBucket(const char* dbinfo_filename)
{
	assert(dbinfo_filename != nullptr);
	DBInfoData bld;
	Status s = DBInfoFile::Read(m_path.c_str(), dbinfo_filename, bld);
	if(s != OK)
	{
		return s;
	}
	return OpenBucket(dbinfo_filename, bld);
}

Status DBImpl::OpenBucket(const char* dbinfo_filename, const DBInfoData& bld)
{	
	fileid_t fileid = strtoull(dbinfo_filename, nullptr, 10);
	
	std::lock_guard<std::mutex> lock(m_mutex);
	if(fileid < m_next_dbinfo_fileid)
	{
		return ERROR;
	}

	m_bucket_rwlock.ReadLock();
	BucketListPtr bucket_list = m_bucket_list;
	m_bucket_rwlock.ReadUnlock();

	std::map<std::string, BucketPtr> buckets;
	if(bucket_list)
	{
		OpenBucket(bld, bucket_list.get(), buckets);
	}
	else
	{
		OpenBucket(bld, buckets);
	}
	BucketListPtr new_bucket_list = NewBucketList(buckets);

	m_bucket_rwlock.WriteLock();
	m_bucket_list.swap(new_bucket_list);
	m_next_dbinfo_fileid = fileid+1;
	m_next_bucket_id = bld.next_bucketid;
	m_bucket_rwlock.WriteUnlock();
	
	return OK;
}

}  



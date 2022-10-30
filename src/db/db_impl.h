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

#ifndef __xfdb_db_impl_h__
#define __xfdb_db_impl_h__

#include <vector>
#include <map>
#include <mutex>
#include "xfdb/util_types.h"
#include "types.h"
#include "xfdb/strutil.h"
#include "rwlock.h"
#include "db_infofile.h"

namespace xfdb 
{

class DBImpl : public std::enable_shared_from_this<DBImpl>
{
public:
	DBImpl(const DBConfig& conf, const std::string& db_path);
	virtual ~DBImpl();

public:	
	virtual Status Open() = 0;
	virtual Status CreateBucket(const std::string& bucket_name)
	{
		return ERR_INVALID_MODE;
	}
	virtual Status DeleteBucket(const std::string& bucket_name)
	{
		return ERR_INVALID_MODE;
	}
	virtual bool ExistBucket(const std::string& bucket_name) const;	
	virtual Status GetBucketStat(const std::string& bucket_name, BucketStat& stat) const;
	virtual void ListBucket(std::vector<std::string>& bucket_names) const;

public:	
	virtual Status Get(const std::string& bucket_name, const StrView& key, String& value) const
	{
		return ERR_INVALID_MODE;
	}

	virtual Status Set(const std::string& bucket_name, const StrView& key, const StrView& value)
	{
		return ERR_INVALID_MODE;
	}
	virtual Status Delete(const std::string& bucket_name, const StrView& key)
	{
		return ERR_INVALID_MODE;
	}
	//virtual Status Write(const ObjectBatch& rb);
	//Append(...)

	//将所有内存表（不限大小）刷盘(异步)，形成segment文件
	virtual Status Flush(const std::string& bucket_name)
	{
		return ERR_INVALID_MODE;
	}
	virtual Status Flush()
	{
		return ERR_INVALID_MODE;
	}

	//将所有的segment表合并成最大segment(异步)
	virtual Status Merge(const std::string& bucket_name)
	{
		return ERR_INVALID_MODE;
	}
	virtual Status Merge()
	{
		return ERR_INVALID_MODE;
	}

public:
	inline const std::string& GetPath() const
	{
		return m_path;
	}
	inline const DBConfig& GetConfig() const
	{
		return m_conf;
	}

protected:	
	Status OpenBucket();
	Status OpenBucket(const char* dbinfo_filename);
	Status OpenBucket(const char* dbinfo_filename, const DBInfoData& bld);

	void OpenBucket(const DBInfoData& bld, const BucketSnapshot* last_bucket_snapshot, std::map<std::string, BucketPtr>& buckets);
	void OpenBucket(const DBInfoData& bld, std::map<std::string, BucketPtr>& buckets);

	Status OpenBucket(const BucketInfo& bi, BucketPtr& bptr);
	bool GetBucket(const std::string& bucket_name, BucketPtr& ptr) const;

	virtual BucketPtr NewBucket(const BucketInfo& bucket_info) = 0;

protected:
	const DBConfig m_conf;
	const std::string m_path;				//路径
	
	std::mutex m_mutex;						//互斥锁
	bucketid_t m_next_bucket_id;
	fileid_t m_next_dbinfo_fileid;
	
	mutable ReadWriteLock m_bucket_rwlock;	//bucket读写锁
	BucketSnapshotPtr m_bucket_snapshot;	//bucket集
	
private:
	DBImpl(const DBImpl&) = delete;
  	DBImpl& operator=(const DBImpl&) = delete;
};

}  

#endif


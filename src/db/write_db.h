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

#ifndef __xfdb_write_db_h__
#define __xfdb_write_db_h__

#include <set>
#include <map>
#include <vector>
#include <deque>
#include "types.h"
#include "rwlock.h"
#include "lock_file.h"
#include "db_impl.h"
#include "write_engine.h"
#include "db_info_file.h"

namespace xfdb 
{

class WritableDB : public DBImpl
{
public:
	WritableDB(WritableEngine* engine, const DBConfig& conf, const std::string& db_path);
	~WritableDB();
	
public:		
	Status Open() override;
	
	//删除db数据，删除前请先close db
	static Status Remove(const std::string& db_path);
	
	//bucket api
	Status CreateBucket(const std::string& bucket_name) override;	
	Status DeleteBucket(const std::string& bucket_name) override;
	
	//object api
	Status Get(const std::string& bucket_name, const StrView& key, String& value) const override;
	Status Set(const std::string& bucket_name, const StrView& key, const StrView& value) override;
	Status Delete(const std::string& bucket_name, const StrView& key) override;
	//Status Write(const ObjectBatch& rb) override;
		
	Status TryFlush();
	Status TryFlush(const std::string& bucket_name);
	Status Flush() override;		//将所有内存表（不限大小）刷盘(异步)，形成file-table文件
	Status Flush(const std::string& bucket_name) override;

	Status Merge() override;		//将所有的block表合并成最大block(异步)
	Status Merge(const std::string& bucket_name) override;
	
protected:
	Status FullMerge(const std::string& bucket_name);		//同步merge
	Status PartMerge(const std::string& bucket_name);		//同步merge
	
	BucketPtr NewBucket(const BucketInfo& bucket_info) override;
	void BeforeClose() override;

private:
	Status CreateIfMissing();
	Status CreateBucket(const std::string& bucket_name, BucketPtr& bptr);
	Status CreateBucketIfMissing(const std::string& bucket_name, BucketPtr& bptr);
	
	Status Write(const std::string& bucket_name, const Object* object);
		
	Status Clean();
	Status CleanBucket();
	Status CleanBucket(const std::string& bucket_name);

	Status CleanDbInfo();
	Status WriteDbInfo();
	void FillDbInfoData(DbInfoData& bd);

private:
	//engine
	friend class WritableEngine;	

	//锁文件
	LockFile m_lock_file;

	//待删除的dbinfo文件,如果有删除的bucket，则删除bucket后才能删除dbinfo文件
	std::deque<FileName> m_delete_dbinfo_files; 
	
	//已删除但还未写入dbinfo中的bucket
	std::map<bucketid_t, std::string> m_deleting_buckets;
	//bucket list未更新的bucket改变次数
	int m_bucket_changed_cnt;
	
	//待flush的bucket
	std::set<std::string> m_flush_buckets;
	
	//待清理的bucket
	std::set<std::string> m_clean_buckets;
	
private:
	WritableDB(const WritableDB&) = delete;
	WritableDB& operator=(const WritableDB&) = delete;
	
};


}  

#endif


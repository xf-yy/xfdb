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

#ifndef __xfdb_writable_db_h__
#define __xfdb_writable_db_h__

#include <set>
#include <map>
#include <vector>
#include <deque>
#include "dbtypes.h"
#include "rwlock.h"
#include "lock_file.h"
#include "db_impl.h"
#include "writable_engine.h"
#include "db_infofile.h"

namespace xfdb 
{

class WritableDB : public DBImpl
{
public:
	WritableDB(const DBConfig& conf, const std::string& db_path);
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
    IteratorImplPtr NewIterator(const std::string& bucket_name) override;

	Status Set(const std::string& bucket_name, const StrView& key, const StrView& value) override;
	Status Delete(const std::string& bucket_name, const StrView& key) override;
	Status Write(const ObjectBatch& ob) override;
		
	Status TryFlush();
	Status TryFlush(const std::string& bucket_name);
	Status Flush() override;		//将所有内存表（不限大小）刷盘(异步)，形成file-table文件
	Status Flush(const std::string& bucket_name) override;

	Status Merge() override;		//将所有的block表合并成最大block(异步)
	Status Merge(const std::string& bucket_name) override;
	
protected:	
	BucketPtr NewBucket(const BucketInfo& bucket_info) override;

private:
	Status CreateIfMissing();
	Status CreateBucket(const std::string& bucket_name, BucketPtr& bptr);
	Status CreateBucketIfMissing(const std::string& bucket_name, BucketPtr& bptr);
	
	Status Write(const std::string& bucket_name, const Object* object);
		
	Status Clean();
	Status CleanBucket();

	Status CleanDBInfo();
	Status WriteDBInfo();
	void WriteDBInfoData(DBInfoData& bd);

private:
	//锁文件
	LockFile m_lock_file;

	//待删除的dbinfo文件,如果有删除的bucket，则删除bucket后才能删除dbinfo文件
	std::deque<FileName> m_tobe_delete_dbinfo_files; 
	
	//已删除但还未写入dbinfo中的bucket
	std::map<bucketid_t, std::string> m_deleting_buckets;
	//bucket list未更新的bucket改变次数
	int m_bucket_changed_cnt;
	
private:
	//engine
	friend class WritableEngine;	
	WritableDB(const WritableDB&) = delete;
	WritableDB& operator=(const WritableDB&) = delete;
	
};


}  

#endif


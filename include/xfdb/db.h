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

#ifndef __xfdb_db_h__
#define __xfdb_db_h__

#include <vector>
#include "xfdb/types.h"
#include "xfdb/strutil.h"
#include "xfdb/batch.h"

namespace xfdb 
{

//db类
class DB
{
public:
	~DB();
		
public:	
	/**打开db, db路径不能以'/'结尾*/
	static Status Open(const DBConfig& dbconf, const std::string& db_path, DBPtr& db);

	/**删除db目录，db路径不能以'/'结尾*/
	static Status Remove(const std::string& db_path);

public:	
	//创建bucket
	Status CreateBucket(const std::string& bucket_name);	

	//删除bucket
	Status DeleteBucket(const std::string& bucket_name);

	//判断bucket是否存在
	bool ExistBucket(const std::string& bucket_name) const;

	//获取bucket统计
	Status GetBucketStat(const std::string& bucket_name, BucketStat& stat) const;

	//列举所有的bucket
	void ListBucket(std::vector<std::string>& bucket_names) const;

public:
	//获取指定bucket中的记录
	Status Get(const std::string& bucket_name, const xfutil::StrView& key, xfutil::String& value) const;

    //获取迭代器
    IteratorPtr NewIterator(const std::string& bucket_name);

	//设置指定bucket中的记录
	Status Set(const std::string& bucket_name, const xfutil::StrView& key, const xfutil::StrView& value);
	//Append

	//删除指定bucket中的记录
	Status Delete(const std::string& bucket_name, const xfutil::StrView& key);

	//批量写
	Status Write(const ObjectBatch& ob);

	//将所有内存表（不限大小）刷盘(异步操作)
	Status Flush(const std::string& bucket_name);	
	Status Flush();							

	//将所有的segment表合并成最大segment(异步操作)
	Status Merge(const std::string& bucket_name);
	Status Merge();						

private:
	DB(DBImplPtr& db);

private:
	DBImplPtr m_db;
	
private:
	friend class Engine;
	DB(const DB&) = delete;
  	DB& operator=(const DB&) = delete;
};


}  

#endif


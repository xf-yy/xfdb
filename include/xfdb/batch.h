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

#ifndef __xfdb_batch_h__
#define __xfdb_batch_h__

#include <vector>
#include <map>
#include "xfdb/types.h"
#include "xfdb/strutil.h"

namespace xfdb 
{

class ObjectBatch
{
public:
	ObjectBatch();
	~ObjectBatch();
		
public:
	//设置指定bucket中的记录
	Status Set(const std::string& bucket_name, const xfutil::StrView& key, const xfutil::StrView& value);
	Status Append(const std::string& bucket_name, const xfutil::StrView& key, const xfutil::StrView& value);


	//删除指定bucket中的记录
	Status Delete(const std::string& bucket_name, const xfutil::StrView& key);

	//清理
	void Clear();
	
private:
    Status GetWriter(const std::string& bucket_name, WriteOnlyObjectWriterPtr& writer);    

private:
	//key:bucket_name
	std::map<std::string, WriteOnlyObjectWriterPtr> m_data;
	
private:
    friend class WritableDB;
	ObjectBatch(const ObjectBatch&) = delete;
  	ObjectBatch& operator=(const ObjectBatch&) = delete;
	
};


}  

#endif


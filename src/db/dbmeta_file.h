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

#ifndef __xfdb_dbmeta_file_h__
#define __xfdb_dbmeta_file_h__

#include <vector>
#include "xfdb/strutil.h"
#include "buffer.h"
#include "db_types.h"

namespace xfdb 
{

struct DBMeta
{
	std::vector<BucketInfo> alive_buckets;
	std::vector<BucketInfo> deleted_buckets;//只有name和id值有效
	uint32_t next_bucketid;
};

class DBMetaFile
{
public:	
	//按升序排列
	static Status Read(const char* db_path, const char* file_name, DBMeta& dm);
	static Status Write(const char* db_path, const char* file_name, DBMeta& dm);
	
	static Status Remove(const char* db_path, const char* file_name);
	
private:
	static Status Parse(const byte_t* data, uint32_t size, DBMeta& dm);
	static Status Serialize(const DBMeta& dm, String& str);
	static uint32_t EstimateSize(const DBMeta& dm);
};


}  

#endif


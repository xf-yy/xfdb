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
#include "directory.h"
#include "path.h"
#include "coding.h"
#include "file_util.h"
#include "dbmeta_file.h"
#include "writeonly_bucket.h"

using namespace xfutil;

namespace xfdb
{

//属性ID
enum
{
	MID_NEXT_BUCKET_ID = MID_START,
	MID_BUCKET_ID,
	MID_BUCKET_NAME,
	MID_CREATE_TIME,
};

Status DBMetaFile::Read(const char* db_path, const char* file_name, DBMeta& dm)
{
	char full_path[MAX_PATH_LEN];
	Path::Combine(full_path, sizeof(full_path), db_path, file_name);

	String str;
	Status s = ReadFile(full_path, str);
	if(s != OK)
	{
		return s;
	}
	return Parse((byte_t*)str.Data(), str.Size(), dm);
}

Status DBMetaFile::Write(const char* db_path, const char* file_name, DBMeta& dm)
{
	String str;
	Status s = Serialize(dm, str);
	if(s != OK)
	{
		return s;
	}

	return WriteFile(db_path, file_name, str.Data(), str.Size());
}

static const bool ParseBucketInfo(const byte_t*& data, const byte_t* data_end, BucketInfo& info)
{
	StrView name;
	for(;;)
	{
		uint32_t id = DecodeV32(data, data_end);
		switch(id)
		{
		case MID_BUCKET_ID:
			info.id = DecodeV32(data, data_end);
			break;
		case MID_BUCKET_NAME:
			name = DecodeString(data, data_end);
			info.name.assign(name.data, name.size);
			break;
		case MID_CREATE_TIME:
			info.create_time = DecodeV64(data, data_end);
			break;
		case MID_END:
			return true;
			break;
		default:
			return false;
			break;
		}
	}
	return false;
}

static const bool ParseBucketMeta(const byte_t*& data, const byte_t* data_end, DBMeta& dm)
{
	for(;;)
	{
		uint32_t id = DecodeV32(data, data_end);
		switch(id)
		{
		case MID_NEXT_BUCKET_ID:
			dm.next_bucketid = DecodeV32(data, data_end);
			break;
		case MID_END:
			return true;
			break;
		default:
			return false;
			break;
		}
	}
	return false;
}

static const bool ParseBucketData(const byte_t*& data, const byte_t* data_end, DBMeta& dm)
{
	uint32_t bucket_cnt = DecodeV32(data, data_end);
	dm.alive_buckets.resize(bucket_cnt);
	for(uint32_t i = 0; i < bucket_cnt; ++i)
	{
		ParseBucketInfo(data, data_end, dm.alive_buckets[i]);
	}
	
	bucket_cnt = DecodeV32(data, data_end);
	dm.deleted_buckets.resize(bucket_cnt);
	for(uint32_t i = 0; i < bucket_cnt; ++i)
	{
		ParseBucketInfo(data, data_end, dm.deleted_buckets[i]);
	}
	return ParseBucketMeta(data, data_end, dm);
}

Status DBMetaFile::Parse(const byte_t* data, uint32_t size, DBMeta& dm)
{
	const byte_t* data_end = data + size;

	FileHeader header;
	if(!ParseDBMetaFileHeader(data, size, header))
	{
		return ERR_FILE_FORMAT;
	}
	return ParseBucketData(data, data_end, dm) ? OK : ERR_FILE_FORMAT;
}

Status DBMetaFile::Serialize(const DBMeta& dm, String& str)
{
	uint32_t esize = EstimateSize(dm);
	if(!str.Reserve(esize))
	{
		return ERR_MEMORY_NOT_ENOUGH;
	}
	
	byte_t* ptr = WriteDBMetaFileHeader((byte_t*)str.Data());
	assert(ptr - (byte_t*)str.Data() == FILE_HEAD_SIZE);
		
	uint32_t bucket_cnt = dm.alive_buckets.size();
	ptr = EncodeV32(ptr, bucket_cnt);
	
	for(uint32_t i = 0; i < bucket_cnt; ++i)
	{
		const BucketInfo& info = dm.alive_buckets[i];
		ptr = EncodeV32(ptr, MID_BUCKET_ID, info.id);
		ptr = EncodeString(ptr, MID_BUCKET_NAME, info.name.data(), info.name.size());
		ptr = EncodeV64(ptr, MID_CREATE_TIME, info.create_time);
		ptr = EncodeV32(ptr, MID_END);
	}
	bucket_cnt = dm.deleted_buckets.size();
	ptr = EncodeV32(ptr, bucket_cnt);
	for(uint32_t i = 0; i < bucket_cnt; ++i)
	{
		const BucketInfo& info = dm.deleted_buckets[i];
		ptr = EncodeV32(ptr, MID_BUCKET_ID, info.id);
		ptr = EncodeString(ptr, MID_BUCKET_NAME, info.name.data(), info.name.size());
		ptr = EncodeV32(ptr, MID_END);
	}
	
	ptr = EncodeV32(ptr, MID_NEXT_BUCKET_ID, dm.next_bucketid);
	ptr = EncodeV32(ptr, MID_END);

	ptr = Encode32(ptr, 0);//FIXME: crc填0
	
	assert(ptr - (byte_t*)str.Data() <= esize);
	str.Resize(ptr - (byte_t*)str.Data());
	return OK;
}

static constexpr uint32_t EstimateBucketInfoSize()
{
	return MAX_BUCKET_NAME_LEN+(MAX_V32_SIZE + MAX_V64_SIZE)*4/*属性数*/;
}

uint32_t DBMetaFile::EstimateSize(const DBMeta& dm)
{
	//重新计算
	return    FILE_HEAD_SIZE 
			+ dm.alive_buckets.size() *  EstimateBucketInfoSize() 
			+ dm.deleted_buckets.size() * EstimateBucketInfoSize()
			+ (MAX_V32_SIZE + MAX_V64_SIZE)*2
			+ sizeof(uint32_t)/*crc*/;
			
}

Status DBMetaFile::Remove(const char* db_path, const char* file_name)
{
	DBMeta dm;
	Status s = DBMetaFile::Read(db_path, file_name, dm);
	if(s != OK)
	{
		assert(s != ERR_FILE_READ);
		return s;
	}

	char path[MAX_PATH_LEN];
	for(const auto& bi : dm.deleted_buckets)
	{
		MakeBucketPath(db_path, bi.name.c_str(), bi.id, path);
		s = WriteOnlyBucket::Remove(path);
		if(s != OK)
		{
			assert(s != ERR_FILE_READ);
			return s;
		}
	}
	MakeDBMetaFilePath(db_path, file_name, path);
	return File::Remove(path) ? OK : ERR_PATH_DELETE;
}

}   


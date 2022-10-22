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

#ifndef __xfdb_types_h__
#define __xfdb_types_h__

#include <string>
#include <atomic>
#include <memory>
#include "xfdb/util_types.h"
#include "xfdb/strutil.h"

namespace xfdb 
{
#if __cplusplus < 201103L
#error "only support c++ 11 or later, use -std=c++11 option for compile"
#endif

//错误码
enum Status
{
	OK = 0,
	ERROR,
	
	ERR_PATH_NOT_EXIST = 10,
	ERR_PATH_EXIST,
	ERR_PATH_CREATE,
	ERR_PATH_DELETE,
	ERR_PATH_INVALID,

	ERR_FILE_OPEN = 20,
	ERR_FILE_READ,
	ERR_FILE_WRITE,
	ERR_FILE_LOCK,
	ERR_FILE_FORMAT,

	ERR_MEMORY_NOT_ENOUGH  = 30,
	ERR_BUFFER_FULL,
	ERR_NOMORE_DATA,
	ERR_RES_EXHAUST,

	ERR_INITIALIZED = 40,
	ERR_UNINITIALIZED,
	ERR_INVALID_MODE,
	ERR_INVALID_CONFIG,
	ERR_IN_PROCESSING,
	
	//db
	ERR_DB_OPENED = 50,
	ERR_DB_CLOSED,
	ERR_DB_EXIST,
	ERR_DB_NOT_EXIST,

	//bucket
	ERR_BUCKET_EXIST = 60,
	ERR_BUCKET_NOT_EXIST,
	ERR_BUCKET_DELETED ,
	ERR_BUCKET_EMPTY,
	ERR_BUCKET_NAME,

	//object
	ERR_OBJECT_NOT_EXIST = 70,
	ERR_OBJECT_TOO_LARGE,

	
};

enum Mode : uint16_t
{
	MODE_READONLY = 0x01,
	MODE_WRITEONLY = 0x02,
	MODE_READWRITE = (MODE_READONLY | MODE_WRITEONLY),
};


//系统配置
struct GlobalConfig
{
	//common
	Mode mode = MODE_WRITEONLY;
	
	//std::string log_file_path;		//日志文件路径，如果为空则不输出日志
	
	std::string notify_dir;				//通知文件目录，不能以'/'结尾
	uint16_t notify_file_ttl_s = 30;	//通知文件生存周期，单位秒

	//ReadConfig
	bool auto_reload_db = true;
	uint16_t reload_db_thread_num = 4;
	
	//WriteConfig
	bool create_db_if_missing = true;
	bool create_bucket_if_missing = true;
	
	uint16_t write_segment_thread_num = 8;
	uint16_t write_metadata_thread_num = 4;
	
	//uint8_t force_merge_deleted_percent = 1;	//百分比
	uint16_t part_merge_thread_num = 4;
	uint16_t full_merge_thread_num = 2;
	uint16_t merge_factor = 10;					//合并因子
	uint64_t max_merge_size = GB(32);			//segment超过此值时不参与merge
	
	//uint64_t total_memtable_size = GB(2);		//总大小，超过时，阻塞写
	uint32_t max_memtable_size = MB(64);		//1~1024
	uint32_t max_memtable_objects = 50*10000;	//1000~100*10000
	uint16_t flush_interval_s = 30;				//1~600
	
	uint16_t clean_interval_s = 30;				//检测clean时间间隔，单位秒
	
public:
	bool Check() const
	{
		switch(mode)
		{
		case MODE_READONLY:
		case MODE_WRITEONLY:
		case MODE_READWRITE:
			break;
		default: return false; 
			break;
		}

		//if(!log_file_path.empty() && log_file_path.back() == '/')
		//{
		//	return false;
		//}
		if(!notify_dir.empty() && notify_dir.back() == '/')
		{
			return false;
		}
		if(auto_reload_db)
		{
			if(notify_dir.empty())
			{
				return false;
			}
		}
		return true;
	}
};

//bucket配置
struct BucketConfig
{
	//CompressionType compress_type = COMPRESSION_NONE;//只用在超过filter_size的块中;//暂不支持
	//uint8_t bloom_bitnum; //0表示不启用，默认10; 暂不支持

public:
	bool Check() const
	{
		return true;
	}
};

//db配置
struct DBConfig
{
	//std::string wal_path; 				//暂不支持
	//BucketConfig default_bucket_conf;

public:
	bool Check() const
	{
		return true;
	}
	//void SetBucketConfig(const std::string& bucket_name, const BucketConfig& bucket_conf);
	//BucketConfig* GetBucketConfig(const std::string& bucket_name);

private:
	//std::map<std::string, BucketConfig> bucket_confs;

};


struct ObjectStatItem
{
	uint64_t count;
	uint64_t key_size;
	uint64_t value_size;

	inline void Add(const ObjectStatItem& stat)
	{
		count += stat.count;
		key_size += stat.key_size;
		value_size += stat.value_size;
	}
	inline void Add(uint64_t key_size_, uint64_t value_size_)
	{
		++count;
		key_size += key_size_;
		value_size += value_size_;
	}
};

struct ObjectStat
{
	ObjectStatItem set_stat;		//set的统计
	ObjectStatItem delete_stat; 	//删除的次数

	inline uint64_t Count() const
	{
		return set_stat.count + delete_stat.count;
	}
	inline void Add(const ObjectStat& stat)
	{
		set_stat.Add(stat.set_stat);
		delete_stat.Add(stat.delete_stat);
	}
};

struct TableStat
{
	uint64_t count;			//内存文件大小
	uint64_t size;

	inline void Add(uint64_t size_)
	{
		++count;
		size += size_;
	}

	inline void Add(const TableStat& stat)
	{
		count += stat.count;
		size += stat.size;
	}
};

struct BucketStat
{
	ObjectStat object_stat;
	TableStat memwriter_stat;		//内存文件大小
	TableStat segment_stat;			//segment文件大小
};

#ifdef DEBUG
union test
{
	BucketStat stat;
	int a;
};
#endif

class DB;
typedef std::shared_ptr<DB> DBPtr;

class DBImpl;
typedef std::shared_ptr<DBImpl> DBImplPtr;
typedef std::weak_ptr<DBImpl> DBImplWptr;

}

#endif



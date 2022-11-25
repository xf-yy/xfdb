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

#ifndef __xfdb_file_util_h__
#define __xfdb_file_util_h__

#include "dbtypes.h"
#include "path.h"
#include "buffer.h"

namespace xfutil
{
	class File;
}
namespace xfdb 
{

#define DB_INFO_FILE_VERSION		1
#define BUCKET_META_FILE_VERSION	1
#define INDEX_FILE_VERSION			1
#define DATA_FILE_VERSION			1
#define NOTIFY_FILE_VERSION			1

#define DB_INFO_FILE_MAGIC			"INFO"
#define BUCKET_META_FILE_MAGIC 		"META"
#define INDEX_FILE_MAGIC			"IDXF"
#define DATA_FILE_MAGIC				"DATA"
#define NOTIFY_FILE_MAGIC			"MSGF"

#define DB_INFO_FILE_EXT			".info"
#define BUCKET_META_FILE_EXT 		".meta"
#define INDEX_FILE_EXT				".idx"
#define DATA_FILE_EXT				".data"
#define NOTIFY_FILE_EXT				".msg"

//有效属性ID从2开始
enum
{
	MID_INVALID = 0,
	MID_END,
	MID_START,
};

#define FILE_MAGIC_SIZE				4
#define FILE_HEADER_VERSION_OFF		5
#define FILE_VERSION_OFF			6
#define FILE_CREATE_TIME_OFF		8
#define FILE_HEAD_SIZE				32

struct FileHeader
{
	char magic[FILE_MAGIC_SIZE];	//magic
	uint16_t version;				//版本号
	second_t create_time_s;			//创建时间，秒数
};

static inline void MakeLockFilePath(const char* db_path, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/LOCK", db_path);
}

bool CheckBucketName(const std::string& bucket_name);
static inline void MakeBucketPath(const char* db_path, const char* bucket_name, bucketid_t bucket_id, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%s.%u", db_path, bucket_name, bucket_id);
}

static inline void MakeDBInfoFileName(fileid_t seqid, char name[MAX_FILENAME_LEN])
{
	snprintf(name, MAX_FILENAME_LEN, "%lu" DB_INFO_FILE_EXT, seqid);
}
static inline void MakeDBInfoFilePath(const char* db_path, const char* filename, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%s", db_path, filename);
}
static inline void MakeBucketMetaFileName(fileid_t seqid, char name[MAX_FILENAME_LEN])
{
	snprintf(name, MAX_FILENAME_LEN, "%lu" BUCKET_META_FILE_EXT, seqid);
}
static inline void MakeBucketMetaFilePath(const char* bucket_path, fileid_t seqid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%lu" BUCKET_META_FILE_EXT, bucket_path, seqid);
}
static inline void MakeBucketMetaFilePath(const char* bucket_path, const char* filename, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%s", bucket_path, filename);
}
static inline void MakeIndexFilePath(const char* bucket_path, fileid_t segmentid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%lx" INDEX_FILE_EXT, bucket_path, segmentid);
}
static inline void MakeTmpIndexFilePath(const char* bucket_path, fileid_t segmentid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/~%lx" INDEX_FILE_EXT, bucket_path, segmentid);
}
static inline void MakeDataFilePath(const char* bucket_path, fileid_t segmentid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%lx" DATA_FILE_EXT, bucket_path, segmentid);
}
static inline void MakeTmpDataFilePath(const char* bucket_path, fileid_t segmentid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/~%lx" DATA_FILE_EXT, bucket_path, segmentid);
}

static inline void MakeNotifyFileName(tid_t pid, fileid_t seqid, char name[MAX_FILENAME_LEN])
{
	snprintf(name, MAX_FILENAME_LEN, "%u-%lu" NOTIFY_FILE_EXT, pid, seqid);
}
static inline void MakeNotifyFilePath(const char* notify_path, tid_t pid, fileid_t seqid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%u-%lu" NOTIFY_FILE_EXT, notify_path, pid, seqid);
}

Status ListFile(const char* path, const char* pattern, std::vector<FileName>& names, bool sort = false);	
static inline Status ListDBInfoFile(const char* path, std::vector<FileName>& names)
{
	return ListFile(path, "*" DB_INFO_FILE_EXT, names, true);
}
static inline Status ListBucketMetaFile(const char* path, std::vector<FileName>& names)
{
	return ListFile(path, "*" BUCKET_META_FILE_EXT, names, true);
}

static inline Status ListNotifyFile(const char* path, std::vector<FileName>& names)
{
	return ListFile(path, "*" NOTIFY_FILE_EXT, names);
}

byte_t* WriteHeader(byte_t* data, const char magic[FILE_MAGIC_SIZE], uint16_t version);
bool ParseHeader(const byte_t* &data, size_t size, const char expect_magic[FILE_MAGIC_SIZE], uint16_t max_version, FileHeader& header);

static inline byte_t* WriteDBInfoFileHeader(byte_t* buf)
{
	return WriteHeader(buf, DB_INFO_FILE_MAGIC, DB_INFO_FILE_VERSION);
}
static inline bool ParseDBInfoFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, DB_INFO_FILE_MAGIC, DB_INFO_FILE_VERSION, header);
}

static inline byte_t* WriteBucketMetaFileHeader(byte_t* buf)
{
	return WriteHeader(buf, BUCKET_META_FILE_MAGIC, BUCKET_META_FILE_VERSION);
}
static inline bool ParseBucketMetaFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, BUCKET_META_FILE_MAGIC, BUCKET_META_FILE_VERSION, header);
}

static inline byte_t* WriteIndexFileHeader(byte_t* buf)
{
	return WriteHeader(buf, INDEX_FILE_MAGIC, INDEX_FILE_VERSION);
}
static inline bool ParseIndexFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, INDEX_FILE_MAGIC, INDEX_FILE_VERSION, header);
}

static inline byte_t* WriteDataFileHeader(byte_t* buf)
{
	return WriteHeader(buf, DATA_FILE_MAGIC, DATA_FILE_VERSION);
}
static inline bool ParseDataFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, DATA_FILE_MAGIC, DATA_FILE_VERSION, header);
}
static inline byte_t* WriteNotifyFileHeader(byte_t* buf)
{
	return WriteHeader(buf, NOTIFY_FILE_MAGIC, NOTIFY_FILE_VERSION);
}
static inline bool ParseNotifyFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, NOTIFY_FILE_MAGIC, NOTIFY_FILE_VERSION, header);
}

Status ReadFile(const char* file_path, String& str);
Status ReadFile(const File& file, String& str);
Status ReadFile(const File& file, uint64_t offset, int64_t size, String& str);

Status WriteFile(const char* file_path, void* data, int64_t size);
//先写临时文件，再重命名为正式文件
Status WriteFile(const char* dir_path, const char* filename, void* data, int64_t size);

//删除~开头的临时文件，并返回数量
int RemoveTempFile(const char* dir_path);


}  

#endif


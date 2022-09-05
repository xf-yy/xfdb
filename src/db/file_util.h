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

#include "types.h"
#include "path.h"
#include "buffer.h"

namespace xfutil
{
	class File;
}
namespace xfdb 
{

#define BUCKET_LIST_FILE_VERSION	1
#define SEGMENT_LIST_FILE_VERSION	1
#define INDEX_FILE_VERSION			1
#define DATA_FILE_VERSION			1
#define NOTIFY_FILE_VERSION			1

#define BUCKET_LIST_FILE_MAGIC		"BKTL"
#define SEGMENT_LIST_FILE_MAGIC 	"SEGL"
#define INDEX_FILE_MAGIC			"INDX"
#define DATA_FILE_MAGIC				"DATA"
#define NOTIFY_FILE_MAGIC			"NTFY"

#define BUCKET_LIST_FILE_EXT		".bkt"
#define SEGMENT_LIST_FILE_EXT 		".seg"
#define INDEX_FILE_EXT				".idx"
#define DATA_FILE_EXT				".dat"
#define NOTIFY_FILE_EXT				".ntf"

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

static inline void MakeBucketListFileName(fileid_t seqid, char name[MAX_FILENAME_LEN])
{
	snprintf(name, MAX_FILENAME_LEN, "%lu" BUCKET_LIST_FILE_EXT, seqid);
}
static inline void MakeBucketListFilePath(const char* db_path, const char* filename, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%s", db_path, filename);
}
static inline void MakeSegmentListFileName(fileid_t seqid, char name[MAX_FILENAME_LEN])
{
	snprintf(name, MAX_FILENAME_LEN, "%lu" SEGMENT_LIST_FILE_EXT, seqid);
}
static inline void MakeSegmentListFilePath(const char* bucket_path, fileid_t seqid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%lu" SEGMENT_LIST_FILE_EXT, bucket_path, seqid);
}
static inline void MakeSegmentListFilePath(const char* bucket_path, const char* filename, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%s", bucket_path, filename);
}
static inline void MakeIndexFileName(fileid_t segmentid, char name[MAX_FILENAME_LEN])
{
	snprintf(name, MAX_FILENAME_LEN, "%lx" INDEX_FILE_EXT, segmentid);
}
static inline void MakeIndexFilePath(const char* bucket_path, fileid_t segmentid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%lx" INDEX_FILE_EXT, bucket_path, segmentid);
}
static inline void MakeDataFileName(fileid_t segmentid, char name[MAX_FILENAME_LEN])
{
	snprintf(name, MAX_FILENAME_LEN, "%lx" DATA_FILE_EXT, segmentid);
}
static inline void MakeDataFilePath(const char* bucket_path, fileid_t segmentid, char path[MAX_PATH_LEN])
{
	snprintf(path, MAX_PATH_LEN, "%s/%lx" DATA_FILE_EXT, bucket_path, segmentid);
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
static inline Status ListBucketListFile(const char* path, std::vector<FileName>& names)
{
	return ListFile(path, "*" BUCKET_LIST_FILE_EXT, names, true);
}
static inline Status ListSegmentListFile(const char* path, std::vector<FileName>& names)
{
	return ListFile(path, "*" SEGMENT_LIST_FILE_EXT, names, true);
}

static inline Status ListNotifyFile(const char* path, std::vector<FileName>& names)
{
	return ListFile(path, "*" NOTIFY_FILE_EXT, names);
}

byte_t* FillHeader(byte_t* data, const char magic[FILE_MAGIC_SIZE], uint16_t version);
bool ParseHeader(const byte_t* &data, size_t size, const char expect_magic[FILE_MAGIC_SIZE], uint16_t max_version, FileHeader& header);

static inline byte_t* FillBucketListFileHeader(byte_t* buf)
{
	return FillHeader(buf, BUCKET_LIST_FILE_MAGIC, BUCKET_LIST_FILE_VERSION);
}
static inline bool ParseBucketListFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, BUCKET_LIST_FILE_MAGIC, BUCKET_LIST_FILE_VERSION, header);
}

static inline byte_t* FillSegmentListFileHeader(byte_t* buf)
{
	return FillHeader(buf, SEGMENT_LIST_FILE_MAGIC, SEGMENT_LIST_FILE_VERSION);
}
static inline bool ParseSegmentListFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, SEGMENT_LIST_FILE_MAGIC, SEGMENT_LIST_FILE_VERSION, header);
}

static inline byte_t* FillIndexFileHeader(byte_t* buf)
{
	return FillHeader(buf, INDEX_FILE_MAGIC, INDEX_FILE_VERSION);
}
static inline bool ParseIndexFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, INDEX_FILE_MAGIC, INDEX_FILE_VERSION, header);
}

static inline byte_t* FillDataFileHeader(byte_t* buf)
{
	return FillHeader(buf, DATA_FILE_MAGIC, DATA_FILE_VERSION);
}
static inline bool ParseDataFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, DATA_FILE_MAGIC, DATA_FILE_VERSION, header);
}
static inline byte_t* FillNotifyFileHeader(byte_t* buf)
{
	return FillHeader(buf, NOTIFY_FILE_MAGIC, NOTIFY_FILE_VERSION);
}
static inline bool ParseNotifyFileHeader(const byte_t* &data, size_t size, FileHeader& header)
{
	return ParseHeader(data, size, NOTIFY_FILE_MAGIC, NOTIFY_FILE_VERSION, header);
}

Status ReadFile(const char* file_path, K4Buffer& buf);
Status ReadFile(const File& file, K4Buffer& buf);
Status ReadFile(const File& file, uint64_t offset, int64_t size, K4Buffer& buf);

Status WriteFile(const char* file_path, void* data, int64_t size);
//先写临时文件，再重命名为正式文件
Status WriteFile(const char* dir_path, const char* filename, void* data, int64_t size);

//删除~开头的临时文件，并返回数量
int RemoveTempFile(const char* dir_path);

}  

#endif


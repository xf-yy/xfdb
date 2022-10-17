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

#ifndef __xfdb_db_type_h__
#define __xfdb_db_type_h__

#include <vector>
#include <ctime>
#include <set>
#include <map>
#include "xfdb/util_types.h"
#include "xfdb/db_types.h"
#include "xfdb/strutil.h"
#include "hash.h"


using namespace xfutil;

namespace xfdb 
{

typedef uint32_t bucketid_t;
typedef uint64_t fileid_t;
typedef uint64_t objectid_t;

//segment fileid: 高60bit是segmentid，低4bit是level
#define MAX_LEVEL_ID				(7)
#define LEVEL_BITNUM				(4)
#define LEVEL_MASK					((1<<LEVEL_BITNUM) - 1)
#define LEVEL_ID(id)				((id) & LEVEL_MASK)
static_assert(MAX_LEVEL_ID > 2 && MAX_LEVEL_ID < 16, "invalid MAX_LEVEL_ID");
static_assert(LEVEL_MASK == 15, "invalid LEVEL_MASK");

#define MAX_FULLMERGE_NUM			(15)						//LEVEL达最大值时才开始计数，非严格意义上的合并最大数
#define FULLMERGE_BITNUM			(4)
#define FULLMERGE_MASK				((1<<FULLMERGE_BITNUM) - 1)
#define FULLMERGE_COUNT(id)			(((id) >> LEVEL_BITNUM) & FULLMERGE_MASK)
static_assert(FULLMERGE_MASK == 15, "invalid FULLMERGE_MASK");

#define SEGMENT_ID_SHIFT			(FULLMERGE_BITNUM + LEVEL_BITNUM)
#define SEGMENT_ID(id)				((id) >> SEGMENT_ID_SHIFT)
#define MAX_SEGMENT_ID				((0x1ULL << (64-SEGMENT_ID_SHIFT)) - 1)
#define SEGMENT_FILEID(id, level)			(((id) << SEGMENT_ID_SHIFT) | level)
#define SEGMENT_FILEID2(id, cnt, level)		(((id) << SEGMENT_ID_SHIFT) | (cnt<<FULLMERGE_BITNUM) | level)

#define INVALID_FILEID				(0)
#define MIN_FILEID					(INVALID_FILEID + 1)
#define MAX_FILEID					(fileid_t(-1) - 1)
static_assert(MIN_FILEID > 0, "invalid MIN_FILEID");

#define INVALID_OBJECTID			(0)
#define MIN_OBJECTID				(INVALID_OBJECTID + 1)
#define MAX_OBJECTID				(objectid_t(-1) - 1)		//64bit
static_assert(MIN_OBJECTID > 0, "invalid MIN_OBJECTID");

#define INVALID_BUCKETID			(0)
#define MIN_BUCKETID				(INVALID_BUCKETID + 1)
#define MAX_BUCKETID				(bucketid_t(-1) - 1)		//32bit
static_assert(MIN_BUCKETID > 0, "invalid MIN_BUCKETID");

#define MAX_KEY_SIZE				(16*1024)
#define MAX_VALUE_SIZE				(64*1024)
static_assert(MAX_KEY_SIZE <= 64*1024, "invalid MAX_KEY_SIZE");				//不能超16bit大小
static_assert(MAX_VALUE_SIZE <= 64*1024, "invalid MAX_VALUE_SIZE");			//待支持kv分离时，可支持大value

#define EXTRA_OBJECT_SIZE			(256)		//object相关属性估值
#define MAX_OBJECT_SIZE				(MAX_KEY_SIZE + MAX_VALUE_SIZE + EXTRA_OBJECT_SIZE)
#define MAX_UNCOMPRESS_BLOCK_SIZE	(32*1024)	//未启用压缩时的块大小
#define MAX_COMPRESS_BLOCK_SIZE		(3*MAX_COMPRESS_BLOCK_SIZE)	//启用压缩时的块大小
#define MAX_BUFFER_SIZE				(MAX_COMPRESS_BLOCK_SIZE + MAX_OBJECT_SIZE)

#define MEM_BLOCK_SIZE				(256*1024)

#ifndef MAX_FILENAME_LEN
#define MAX_FILENAME_LEN			(64)	//包括结束符'\0'
#endif

#ifndef MIN_BUCKET_NAME_LEN
#define MIN_BUCKET_NAME_LEN			(3)		//不包含结束符'\0'
#endif

#ifndef MAX_BUCKET_NAME_LEN
#define MAX_BUCKET_NAME_LEN			(64)	//包含结束符'\0'
#endif

//NOTE:值必须小于32
enum ObjectType : uint8_t
{
	DeleteType = 0, 	//直到最终的level后才会消除
						//如果value长度不为0，则是计数值，
						//value是剩余消除key的计数，为0时该delete记录也消除
	SetType = 1,
	//AppendType,
};	

struct Object
{
	ObjectType type;
	//uint8_t flag;
	StrView key;
	StrView value;
	objectid_t id;
	
	Object(ObjectType type_ = SetType)
	{
		type = type_;
	}
	Object(ObjectType type_, StrView key_) : key(key_)
	{
		type = type_;
	}
	Object(ObjectType type_, StrView key_, StrView value_) : key(key_), value(value_)
	{
		type = type_;
	}	
};

struct ObjectCmp
{
	bool operator()(const Object* r1, const Object* r2) const
	{
		//FIXME: 先按key升序，再按id降序
		int ret = r1->key.Compare(r2->key);
		return (ret == 0) ? (r2->id < r1->id) : (ret < 0);
	}
};

struct FileName
{
	char str[MAX_FILENAME_LEN];
	
	bool operator<(const FileName& dst) const
	{
		uint64_t v1 = strtoll(this->str, nullptr, 10);
		uint64_t v2 = strtoll(dst.str, nullptr, 10);
		return (v1 < v2);
	}
};

struct BucketInfo
{	
	std::string name;				//名称
	bucketid_t id;					//id
	second_t create_time;			//

	BucketInfo()
	{
		id = 0;
		create_time = 0;
	}
	BucketInfo(const std::string& name_, bucketid_t id_, uint64_t ctime = time(nullptr)) : name(name_)
	{
		id = id_;
		create_time = ctime;
	}

	void Set(const std::string& name_, bucketid_t id_)
	{
		name = name_;
		id = id_;
		create_time = time(nullptr);
	}
};

struct StrViewHash
{
	size_t operator()(const StrView& str) const
	{
		return Hash32((byte_t*)str.data, str.size);
	}
};

#if 0
struct StrViewEqual
{
	bool operator()(const StrView& str1, const StrView& str2) const
	{
		return (str1.size == str2.size && memcmp(str1.data, str2.data, str1.size) == 0);
	}
};
#endif

struct SegmentL0Index
{
	StrView start_key;			//仅写入时有效
	uint64_t L0offset;
	uint32_t L0compress_size;
	uint32_t L0origin_size;
	uint32_t L0index_size;
};

struct SegmentL1Index
{
	StrView start_key;
	uint64_t L1offset;			//仅读取时有效
	uint32_t bloom_size;
	uint32_t L1compress_size;
	uint32_t L1origin_size;
	uint32_t L1index_size;
};

struct SegmentMeta
{
	std::vector<fileid_t> deleted_segment_fileids;
	ObjectStat object_stat;
};

struct SegmentIndexInfo
{
	fileid_t segment_fileid;	//segment fileid
	uint64_t data_filesize;		//data文件大小
	uint64_t index_filesize;	//index文件大小
	uint32_t L2index_meta_size;	//L2层索引+meta大小，包含2*4Byte
};

#define MAX_OBJECT_NUM_OF_GROUP		(8)
#define MAX_OBJECT_NUM_OF_BLOCK		(MAX_OBJECT_NUM_OF_GROUP*MAX_OBJECT_NUM_OF_GROUP*MAX_OBJECT_NUM_OF_GROUP)

struct LnGroupIndex
{
	StrView start_key;
	uint32_t group_size;
	uint32_t index_size;

	LnGroupIndex()
	{
		group_size = 0;
		index_size = 0;
	}
};

struct L0GroupIndex
{
	StrView start_key;
	uint32_t group_size;

	L0GroupIndex()
	{
		group_size = 0;
	}
};

class ReadOnlyEngine;
typedef std::shared_ptr<ReadOnlyEngine> ReadOnlyEnginePtr;
#define NewReadOnlyEngine 	std::make_shared<ReadOnlyEngine>

class WritableEngine;
typedef std::shared_ptr<WritableEngine> WritableEnginePtr;
#define NewWritableEngine 	std::make_shared<WritableEngine>

class WritableDB;
typedef std::shared_ptr<WritableDB> WritableDBPtr;
#define NewWritableDB 	std::make_shared<WritableDB>

class ReadOnlyDB;
typedef std::shared_ptr<ReadOnlyDB> ReadOnlyDBPtr;
#define NewReadOnlyDB 	std::make_shared<ReadOnlyDB>

class Bucket;
typedef std::shared_ptr<Bucket> BucketPtr;

class BucketSnapshot;
typedef std::shared_ptr<BucketSnapshot> BucketSnapshotPtr;
#define NewBucketSnapshot 	std::make_shared<BucketSnapshot>

class WriteOnlyBucket;
typedef std::shared_ptr<WriteOnlyBucket> WriteOnlyBucketPtr;
#define NewWriteOnlyBucket 	std::make_shared<WriteOnlyBucket>

class ReadWriteBucket;
typedef std::shared_ptr<ReadWriteBucket> ReadWriteBucketPtr;
#define NewReadWriteBucket 	std::make_shared<ReadWriteBucket>

class ReadOnlyBucket;
typedef std::shared_ptr<ReadOnlyBucket> ReadOnlyBucketPtr;
#define NewReadOnlyBucket 	std::make_shared<ReadOnlyBucket>

class TableReader;
typedef std::shared_ptr<TableReader> TableReaderPtr;

class Iterator;
typedef std::shared_ptr<Iterator> IteratorPtr;

class TableReaderSnapshot;
typedef std::shared_ptr<TableReaderSnapshot> TableReaderSnapshotPtr;
#define NewTableReaderSnapshot 	std::make_shared<TableReaderSnapshot>

class TableWriter;
typedef std::shared_ptr<TableWriter> TableWriterPtr;

class ReadWriteMemWriter;
typedef std::shared_ptr<ReadWriteMemWriter> ReadWriteMemWriterPtr;
#define NewReadWriteMemWriter 	std::make_shared<ReadWriteMemWriter>

class WriteOnlyMemWriter;
typedef std::shared_ptr<WriteOnlyMemWriter> WriteOnlyMemWriterPtr;
#define NewWriteOnlyMemWriter 	std::make_shared<WriteOnlyMemWriter>

class TableWriterSnapshot;
typedef std::shared_ptr<TableWriterSnapshot> TableWriterSnapshotPtr;
#define NewTableWriterSnapshot 	std::make_shared<TableWriterSnapshot>

class IndexWriter;
typedef std::shared_ptr<IndexWriter> IndexWriterPtr;
#define NewIndexWriter 	std::make_shared<IndexWriter>

class DataReader;
typedef std::shared_ptr<DataReader> DataReaderPtr;
#define NewDataReader 	std::make_shared<DataReader>

class DataWriter;
typedef std::shared_ptr<DataWriter> DataWriterPtr;
#define NewDataWriter 	std::make_shared<DataWriter>

class BucketMetaFile;
typedef std::shared_ptr<BucketMetaFile> BucketMetaFilePtr;
#define NewBucketMetaFile 	std::make_shared<BucketMetaFile>

class SegmentReader;
typedef std::shared_ptr<SegmentReader> SegmentReaderPtr;
#define NewSegmentReader 	std::make_shared<SegmentReader>

class SegmentReaderIterator;
typedef std::shared_ptr<SegmentReaderIterator> SegmentReaderIteratorPtr;
#define NewSegmentReaderIterator 	std::make_shared<SegmentReaderIterator>

class SegmentWriter;
typedef std::shared_ptr<SegmentWriter> SegmentWriterPtr;
#define NewSegmentWriter 	std::make_shared<SegmentWriter>

class IteratorSet;
typedef std::shared_ptr<IteratorSet> IteratorSetPtr;
#define NewIteratorSet 	std::make_shared<IteratorSet>

class WriteOnlyMemWriterIterator;
typedef std::shared_ptr<WriteOnlyMemWriterIterator> WriteOnlyMemWriterIteratorPtr;
#define NewWriteOnlyMemWriterIterator 	std::make_shared<WriteOnlyMemWriterIterator>

class ReadWriteMemWriterIterator;
typedef std::shared_ptr<ReadWriteMemWriterIterator> ReadWriteMemWriterIteratorPtr;
#define NewReadWriteMemWriterIterator 	std::make_shared<ReadWriteMemWriterIterator>

struct BucketReaderSnapshot
{
	BucketMetaFilePtr meta_file;
	TableReaderSnapshotPtr readers;
};

struct MergingSegmentInfo
{
	fileid_t new_segment_fileid;
	SegmentReaderPtr new_segment_reader;
	std::set<fileid_t> merging_segment_fileids;
	BucketReaderSnapshot reader_snapshot;

	fileid_t NewSegmentFileID() const;
	void GetMergingReaders(std::map<fileid_t, TableReaderPtr>& segment_readers) const;

};



}

#endif //__xfdb_db_type_h__



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

#ifndef __xfdb_data_file_h__
#define __xfdb_data_file_h__

#include <map>
#include <deque>
#include "db_types.h"
#include "xfdb/strutil.h"
#include "buffer.h"
#include "file.h"
#include "iterator_impl.h"
#include "path.h"

namespace xfdb 
{

class DataReader
{
public:
	DataReader();
	~DataReader();
	
public:	
	Status Open(const char* bucket_path, fileid_t fileid);
	Status Search(const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, std::string& value) const;

private:
	File m_file;
	std::string m_path;
	BlockPool& m_large_block_pool;

private:
	friend class SegmentReaderIterator;	
	DataReader(const DataReader&) = delete;
	DataReader& operator=(const DataReader&) = delete;
};


class DataWriter
{
public:
	DataWriter(const BucketConfig& bucket_conf, BlockPool& pool, IndexWriter& index_writer);
	~DataWriter();
	
public:	
	Status Create(const char* bucket_path, fileid_t fileid);
	Status Write(IteratorImpl& iter);
	Status Finish();
	inline uint64_t FileSize()
	{
		return m_file.Size();
	}
		
	static Status Remove(const char* bucket_path, fileid_t fileid);
	
private:
	Status WriteGroup(IteratorImpl& iter, L0GroupIndex& gi);
	Status WriteL2Group(IteratorImpl& iter, LnGroupIndex& ci);
	Status WriteBlock(IteratorImpl& iter, uint32_t& index_size);
	Status WriteGroupIndex(const L0GroupIndex* group_indexs, int index_cnt);
	Status WriteL2GroupIndex(const LnGroupIndex* group_indexs, int index_cnt);
	
	StrView ClonePrevKey(const StrView& str);

private:
	const BucketConfig& m_bucket_conf;
	IndexWriter& m_index_writer;
	BlockPool& m_large_block_pool;

	char m_bucket_path[MAX_PATH_LEN];
	fileid_t m_segment_fileid;
	
	File m_file;
	uint64_t m_offset;

	byte_t* m_block_start;
	byte_t* m_block_end;
	byte_t* m_block_ptr;
	
	WriteBuffer m_key_buf;
	String m_prev_key;
	
	std::deque<uint32_t> m_key_hashs;

	ObjectStat m_stat;
	
private:
	friend class SegmentWriter;

	DataWriter(const DataWriter&) = delete;
	DataWriter& operator=(const DataWriter&) = delete;
};


}  

#endif


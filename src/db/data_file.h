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
#include "types.h"
#include "xfdb/strutil.h"
#include "buffer.h"
#include "file.h"
#include "iterator.h"

using namespace xfutil;

namespace xfdb 
{

class DataReader
{
public:
	DataReader();
	~DataReader();
	
public:	
	Status Open(const char* bucket_path, fileid_t fileid);
	Status Search(const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, String& value) const;

private:
	Status SearchBlock(const byte_t* block, uint32_t block_size, const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, String& value) const;
	Status SearchChunk(const byte_t* chunk, uint32_t chunk_size, const ChunkIndex& chunk_index, const StrView& key, ObjectType& type, String& value) const;
	Status SearchGroup(const byte_t* group, uint32_t chunk_size, const GroupIndex& group_index, const StrView& key, ObjectType& type, String& value) const;

	StrView ClonePrevKey(const StrView& str);

private:
	File m_file;
	String m_prev_key;
	
private:
	DataReader(const DataReader&) = delete;
	DataReader& operator=(const DataReader&) = delete;
};


class DataWriter
{
public:
	DataWriter(const DBConfig& db_conf, BlockPool& pool, IndexWriter& index_writer);
	~DataWriter();
	
public:	
	Status Create(const char* bucket_path, fileid_t fileid);
	Status Write(Iterator& iter);
	Status Finish();
	inline uint64_t FileSize()
	{
		return m_file.Size();
	}
		
	static Status Remove(const char* bucket_path, fileid_t fileid);
	
private:
	Status FillGroup(Iterator& iter, GroupIndex& gi);
	Status FillChunk(Iterator& iter, ChunkIndex& ci);
	Status FillBlock(Iterator& iter, uint32_t& index_size);
	Status FillGroupIndex(const GroupIndex* group_indexs, int index_cnt);
	Status FillChunkIndex(const ChunkIndex* chunk_indexs, int index_cnt);
	
	StrView ClonePrevKey(const StrView& str);
	StrView CloneKey(const StrView& str);

private:
	const DBConfig& m_db_conf;
	IndexWriter& m_index_writer;
	BlockPool& m_pool;
	
	File m_file;
	uint64_t m_offset;

	byte_t* m_block_start;
	byte_t* m_block_ptr;
	
	WriteBuffer m_key_buf;
	String m_prev_key;
	
	ObjectStat m_stat;
	
private:
	DataWriter(const DataWriter&) = delete;
	DataWriter& operator=(const DataWriter&) = delete;
};


}  

#endif


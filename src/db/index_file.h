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

#ifndef __xfdb_index_file_h__
#define __xfdb_index_file_h__

#include <vector>
#include <map>
#include <list>
#include "types.h"
#include "file.h"
#include "buffer.h"
#include "xfdb/strutil.h"
#include "path.h"

using namespace xfutil;
namespace xfdb 
{

StrView MakeKey(StrView& prev_key, uint32_t shared_keysize, StrView& nonshared_key, String& prev_str1, String& prev_str2);
void GetMaxShortKey(const StrView& key, String& str); 
void GetMinShortKey(const StrView& key, String& str);
void GetMidShortKey(const StrView& key1, const StrView& key2, String& str);

class IndexReader
{
public:
	IndexReader(BlockPool& pool);
	~IndexReader();
	
public:	
	Status Open(const char* bucket_path, const SegmentFileIndex& info);

	Status Search(const StrView& key, SegmentL0Index& idx) const;
	
	inline const SegmentMeta& GetMeta() const
	{
		return m_meta;
	}
	inline uint64_t FileSize()
	{
		return m_file.Size();
	}
			
private:
	const SegmentL1Index* Search(const StrView& key) const;

	Status SearchBlock(const byte_t* block, uint32_t block_size, const SegmentL1Index* L1Index, const StrView& key, SegmentL0Index& L0_index) const;
	Status SearchChunk(const byte_t* chunk, uint32_t group_size, const LnGroupIndex& chunk_index, const StrView& key, SegmentL0Index& L0_index) const;
	Status SearchGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, const StrView& key, SegmentL0Index& L0_index) const;

	Status ParseL2Index(const byte_t* data, uint32_t L2index_meta_size);
	Status ParseMeta(const byte_t* data, uint32_t metasize);

	StrView CloneKey(const StrView& str);
	
	bool ParseObjectStat(const byte_t* &data, const byte_t* data_end);
	bool ParseDeletedSegment(const byte_t* &data, const byte_t* data_end);
	bool ParseOtherMeta(const byte_t* &data, const byte_t* data_end);
	bool ParseKeyIndex(const byte_t* &data, const byte_t* data_end);
	bool ParseKeyIndex(const byte_t* &data, const byte_t* data_end, uint64_t& last_offset, SegmentL1Index& L1Index);

private:
	File m_file;
	BlockPool& m_pool;
	WriteBuffer m_buf;
	std::vector<SegmentL1Index> m_L1indexs;
	SegmentMeta m_meta;
	DBConfig m_conf;
	
private:
	IndexReader(const IndexReader&) = delete;
	IndexReader& operator=(const IndexReader&) = delete;
};

class IndexWriter
{
public:
	IndexWriter(const DBConfig& db_conf, BlockPool& pool);
	~IndexWriter();
	
public:	
	Status Create(const char* bucket_path, fileid_t fileid);
	Status Write(const SegmentL0Index& L0_index);
	Status Finish(const SegmentMeta& meta);
	inline uint64_t FileSize()
	{
		return m_file.Size();
	}
	inline uint32_t L2IndexMetaSize()
	{
		return (m_file.Size() - m_L2offset_start);
	}
	static Status Remove(const char* bucket_path, fileid_t fileid);

private:
	Status FillGroup(uint32_t& L0_idx, L0GroupIndex& gi);
	Status FillGroupIndex(const L0GroupIndex* group_indexs, int index_cnt);
	Status FillChunk(uint32_t& L0_idx, LnGroupIndex& ci);
	Status FillChunkIndex(const LnGroupIndex* chunk_indexs, int index_cnt);
	Status FillBlock(uint32_t& index_size);
	Status WriteBlock();
	Status WriteL2Index(uint32_t& L2index_size);
	Status WriteMeta(uint32_t L2index_size, const SegmentMeta& meta);
	void FillMeta(const SegmentMeta& meta);
	void FillObjectStat(ObjectType type, const ObjectStatItem& stat);
	
	Status WriteL2IndexMeta(const SegmentMeta& meta);
	
	StrView CloneKey(const StrView& str, WriteBuffer& buf);

private:
	const DBConfig& m_db_conf;
	BlockPool& m_pool;

	char m_bucket_path[MAX_PATH_LEN];
	fileid_t m_segment_fileid;
	File m_file;
	uint64_t m_offset;
	uint64_t m_L1offset_start;
	uint64_t m_L2offset_start;
	
	std::list<SegmentL1Index> m_L1indexs;
	WriteBuffer m_L1key_buf;
	
	SegmentL0Index* m_L0indexs;
	uint32_t m_L0index_cnt;
	uint32_t m_writing_size;
	String m_prev_key;

	WriteBuffer m_L0key_buf;

	byte_t* m_block_start;	//256KB-buffer
	byte_t* m_block_end;
	byte_t* m_block_ptr;
	
private:
	IndexWriter(const IndexWriter&) = delete;
	IndexWriter& operator=(const IndexWriter&) = delete;
};



}  

#endif


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
#include "db_types.h"
#include "file.h"
#include "buffer.h"
#include "xfdb/strutil.h"
#include "path.h"

namespace xfdb 
{

class IndexReader
{
public:
	IndexReader();
	~IndexReader();
	
public:	
	Status Open(const char* bucket_path, const SegmentStat& info);
	bool Read(const SegmentL1Index* L1Index, std::string& bf_data, std::string& index_data) const;

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
	ssize_t Find(const StrView& key) const;
	const SegmentL1Index* Search(const StrView& key) const;

	Status ParseL2Index(const byte_t* data, uint32_t L2index_meta_size);
	Status ParseMeta(const byte_t* data, uint32_t metasize);
	
	bool ParseObjectStat(const byte_t* &data, const byte_t* data_end);
	bool ParseKeyIndex(const byte_t* &data, const byte_t* data_end);
	bool ParseKeyIndex(const byte_t* &data, const byte_t* data_end, uint64_t& last_offset, SegmentL1Index& L1Index);

	bool CheckBloomFilter(const SegmentL1Index* L1Index, const StrView& key) const;

private:
	BlockPool& m_large_block_pool;

	File m_file;
	std::string m_path;
	
	WriteBuffer m_buf;
	std::vector<SegmentL1Index> m_L1indexs;
	SegmentMeta m_meta;
	
private:
	friend class SegmentReaderIterator;
	friend class IndexBlockReader;
	
	IndexReader(const IndexReader&) = delete;
	IndexReader& operator=(const IndexReader&) = delete;
};

class IndexWriter
{
public:
	IndexWriter(const BucketConfig& bucket_conf, BlockPool& pool);
	~IndexWriter();
	
public:	
	Status Create(const char* bucket_path, fileid_t fileid);
	Status Write(const SegmentL0Index& L0_index, std::deque<uint32_t>& key_hashcodes);
	Status Finish(std::deque<uint32_t>& key_hashcodes, const SegmentMeta& meta);
    
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
	Status WriteGroup(uint32_t& L0_idx, L0GroupIndex& gi);
	Status WriteGroupIndex(const L0GroupIndex* group_indexs, int index_cnt);
	Status WriteL2Group(uint32_t& L0_idx, LnGroupIndex& ci);
	Status WriteL2GroupIndex(const LnGroupIndex* group_indexs, int index_cnt);
	Status WriteBlock(uint32_t& index_size);
	Status WriteBlock(std::deque<uint32_t>& key_hashcodes);
	Status WriteL2Index(uint32_t& L2index_size);
	Status WriteMeta(uint32_t L2index_size, const SegmentMeta& meta);
	void WriteMeta(const SegmentMeta& meta);
	void WriteObjectStat(ObjectType type, const TypeObjectStat& stat);
	
	Status WriteL2IndexMeta(const SegmentMeta& meta);

private:
	const BucketConfig& m_bucket_conf;

	BlockPool& m_large_block_pool;

	char m_bucket_path[MAX_PATH_LEN];
	fileid_t m_segment_fileid;
	File m_file;
	uint64_t m_offset;
	uint64_t m_L1offset_start;
	uint64_t m_L2offset_start;
	
	std::list<SegmentL1Index> m_L1indexs;
	WriteBuffer m_L1key_buf;
	
	std::vector<SegmentL0Index> m_L0indexs;
	uint32_t m_writing_size;
	//String m_prev_key;

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


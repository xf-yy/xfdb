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

#include <vector>
#include <map>
#include <list>
#include "index_block.h"
#include "block_pool.h"
#include "file_util.h"
#include "coding.h"
#include "engine.h"

namespace xfdb 
{

IndexBlockReader::IndexBlockReader()
{
}
IndexBlockReader::~IndexBlockReader()
{
}

Status IndexBlockReader::Read(const File& file, const std::string& file_path, const SegmentL1Index& L1Index)
{
	//TODO: 读取bloom cache

	//读取L1块 cache
	auto& cache = Engine::GetEngine()->GetIndexCache();

	std::string cache_key = file_path;
	cache_key.append((char*)&L1Index.L1offset, sizeof(L1Index.L1offset));

	std::string data;
	if(!cache.Get(cache_key, data) || data.size() < L1Index.L1compress_size)
	{
		data.resize(L1Index.L1compress_size);
		int64_t r_size = file.Read(L1Index.L1offset, (void*)data.data(), L1Index.L1compress_size);
		if((uint64_t)r_size != L1Index.L1compress_size)
		{
			return ERR_FILE_READ;
		}
		cache.Add(cache_key, data, data.size());
	}
	
	assert(!data.empty());
	m_data = data;
	m_L1Index = L1Index;
	return OK;
}

Status IndexBlockReader::Search(const StrView& key, SegmentL0Index& L0_index)
{
	//TODO: 判断是否需要解压
	//TODO: 将bloom和data放入cache中
	//TODO: 判断是否有bloom，有则判断bloom是否命中

	const byte_t* block = (byte_t*)m_data.data() + m_L1Index.bloom_size;
	uint32_t block_size = m_L1Index.L1compress_size - m_L1Index.bloom_size;

	return SearchBlock(block, block_size, &m_L1Index, key, L0_index);
}

Status IndexBlockReader::SearchBlock(const byte_t* block, uint32_t block_size, const SegmentL1Index* L1Index, const StrView& key, SegmentL0Index& L0_index) const
{
	assert(block_size != 0);
	const byte_t* block_end = block + block_size;
	const byte_t* index_ptr = block_end - L1Index->L1index_size - sizeof(uint32_t)/*crc32*/;
	const byte_t* group_ptr = block;
	
	//找到第1个大于key的group
	LnGroupIndex lngroup_index;

	String prev_str1, prev_str2;
	StrView prev_key = L1Index->start_key;

	const byte_t* index_end = block_end - sizeof(uint32_t)/*crc32*/;
	while(index_ptr < index_end)
	{
		//解 group key
		uint32_t shared_keysize = DecodeV32(index_ptr, index_end);
		StrView nonshared_key = DecodeString(index_ptr, index_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);

		int ret = key.Compare(curr_key);
		if(ret < 0)
		{
			break;
		}
		
		lngroup_index.start_key = curr_key;
		lngroup_index.group_size = DecodeV32(index_ptr, index_end);
		lngroup_index.index_size = DecodeV32(index_ptr, index_end);

		if(ret == 0)
		{
			return SearchL2Group(group_ptr, lngroup_index.group_size, lngroup_index, key, L0_index);
		}
		
		group_ptr += lngroup_index.group_size;

		prev_key = curr_key;
	}
	
	return SearchL2Group(group_ptr-lngroup_index.group_size, lngroup_index.group_size, lngroup_index, key, L0_index);
}

Status IndexBlockReader::SearchL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, const StrView& key, SegmentL0Index& L0_index) const
{
	assert(group_size != 0);
	const byte_t* group_end = group_start + group_size;
	const byte_t* index_ptr = group_end - lngroup_index.index_size;
	const byte_t* group_ptr = group_start;

	String prev_str1, prev_str2;
	L0GroupIndex group_index;
	StrView prev_key = lngroup_index.start_key;
	
	while(index_ptr < group_end)
	{
		//解group key
		uint32_t shared_keysize = DecodeV32(index_ptr, group_end);
		StrView nonshared_key = DecodeString(index_ptr, group_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);

		int ret = key.Compare(curr_key);		
		if(ret < 0)
		{
			break;
		}
		//解group_size
		group_index.start_key = curr_key;
		group_index.group_size = DecodeV32(index_ptr, group_end);
		
		if(ret == 0)
		{
			return SearchGroup(group_ptr, group_index.group_size, group_index, key, L0_index);
		}
		group_ptr += group_index.group_size;

		prev_key = curr_key;
	}
	return SearchGroup(group_ptr-group_index.group_size, group_index.group_size, group_index, key, L0_index);
}

Status IndexBlockReader::SearchGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, const StrView& key, SegmentL0Index& L0_index) const
{
	assert(group_size != 0);
	const byte_t* group_end = group + group_size;

	String prev_str1, prev_str2;
	StrView prev_key = group_index.start_key;
	uint64_t prev_offset = 0;
	
	const byte_t* data_ptr = group;

	while(data_ptr < group_end)
	{
		//解key
		uint32_t shared_keysize = DecodeV32(data_ptr, group_end);
		StrView nonshared_key = DecodeString(data_ptr, group_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);

		int ret = key.Compare(curr_key);
		if(ret < 0)
		{
			break;
		}
		
		L0_index.L0offset = DecodeV64(data_ptr, group_end);
		L0_index.L0offset += prev_offset;
		
		//L0_index.start_key = curr_key;
		L0_index.L0compress_size = DecodeV32(data_ptr, group_end);
		uint32_t diff_size = DecodeV32(data_ptr, group_end);
		L0_index.L0origin_size = L0_index.L0compress_size + diff_size;
		L0_index.L0index_size = DecodeV32(data_ptr, group_end);

		if(ret == 0)
		{
			return OK;
		}
		
		prev_key = curr_key;
		prev_offset = L0_index.L0offset;
		
	}
	return OK;
}

Status IndexBlockReader::ParseBlock(const byte_t* block, uint32_t block_size, const SegmentL1Index* L1Index, IndexBlockReaderIteratorPtr& iter_ptr) const
{
	assert(block_size != 0);
	const byte_t* block_end = block + block_size;
	const byte_t* index_ptr = block_end - L1Index->L1index_size - sizeof(uint32_t)/*crc32*/;
	const byte_t* group_ptr = block;
	
	//找到第1个大于key的group
	LnGroupIndex lngroup_index;

	String prev_str1, prev_str2;
	StrView prev_key = L1Index->start_key;

	const byte_t* index_end = block_end - sizeof(uint32_t)/*crc32*/;
	while(index_ptr < index_end)
	{
		//解 group key
		uint32_t shared_keysize = DecodeV32(index_ptr, index_end);
		StrView nonshared_key = DecodeString(index_ptr, index_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);
		
		lngroup_index.start_key = curr_key;
		lngroup_index.group_size = DecodeV32(index_ptr, index_end);
		lngroup_index.index_size = DecodeV32(index_ptr, index_end);

		ParseL2Group(group_ptr, lngroup_index.group_size, lngroup_index, iter_ptr);
		
		group_ptr += lngroup_index.group_size;

		prev_key = curr_key;
	}	
	return OK;
}

Status IndexBlockReader::ParseL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, IndexBlockReaderIteratorPtr& iter_ptr) const
{
	assert(group_size != 0);
	const byte_t* group_end = group_start + group_size;
	const byte_t* index_ptr = group_end - lngroup_index.index_size;
	const byte_t* group_ptr = group_start;

	String prev_str1, prev_str2;
	L0GroupIndex group_index;
	StrView prev_key = lngroup_index.start_key;
	
	while(index_ptr < group_end)
	{
		//解group key
		uint32_t shared_keysize = DecodeV32(index_ptr, group_end);
		StrView nonshared_key = DecodeString(index_ptr, group_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);

		//解group_size
		group_index.start_key = curr_key;
		group_index.group_size = DecodeV32(index_ptr, group_end);
		
		ParseGroup(group_ptr, group_index.group_size, group_index, iter_ptr);
		
		group_ptr += group_index.group_size;

		prev_key = curr_key;
	}
	return OK;
}

Status IndexBlockReader::ParseGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, IndexBlockReaderIteratorPtr& iter_ptr) const
{
	assert(group_size != 0);
	const byte_t* group_end = group + group_size;

	String prev_str1, prev_str2;
	StrView prev_key = group_index.start_key;
	uint64_t prev_offset = 0;
	SegmentL0Index L0_index;

	const byte_t* data_ptr = group;

	while(data_ptr < group_end)
	{
		//解key
		uint32_t shared_keysize = DecodeV32(data_ptr, group_end);
		StrView nonshared_key = DecodeString(data_ptr, group_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);
		L0_index.start_key = curr_key;
		
		L0_index.L0offset = DecodeV64(data_ptr, group_end);
		L0_index.L0offset += prev_offset;
		
		//L0_index.start_key = curr_key;
		L0_index.L0compress_size = DecodeV32(data_ptr, group_end);
		uint32_t diff_size = DecodeV32(data_ptr, group_end);
		L0_index.L0origin_size = L0_index.L0compress_size + diff_size;
		L0_index.L0index_size = DecodeV32(data_ptr, group_end);

		iter_ptr->Add(L0_index);
		
		prev_key = curr_key;
		prev_offset = L0_index.L0offset;
		
	}
	return OK;
}

IndexBlockReaderIterator::IndexBlockReaderIterator() : m_buf(Engine::GetEngine()->GetLargePool())
{
	m_L0indexs.reserve(MAX_OBJECT_NUM_OF_BLOCK);
}

IndexBlockReaderIteratorPtr IndexBlockReader::NewIterator()
{
	IndexBlockReaderIteratorPtr iter_ptr = NewIndexBlockReaderIterator();

	const byte_t* block = (byte_t*)m_data.data() + m_L1Index.bloom_size;
	uint32_t block_size = m_L1Index.L1compress_size - m_L1Index.bloom_size;

	ParseBlock(block, block_size, &m_L1Index, iter_ptr);

	iter_ptr->First();
	return iter_ptr;
}


}  



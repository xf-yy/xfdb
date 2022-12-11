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

#include <algorithm>
#include <vector>
#include <map>
#include <list>
#include "index_block.h"
#include "block_pool.h"
#include "file_util.h"
#include "coding.h"
#include "engine.h"
#include "bloom_filter.h"
#include "index_file.h"

namespace xfdb 
{

IndexBlockReader::IndexBlockReader(const IndexReader& index_reader) : m_index_reader(index_reader)
{
}

IndexBlockReader::~IndexBlockReader()
{
}

Status IndexBlockReader::Read(const SegmentL1Index& L1Index)
{
	//读取L1块 cache
	std::string cache_key = m_index_reader.m_path;
	uint64_t data_offset = L1Index.L1offset + L1Index.bloom_filter_size;
	cache_key.append((char*)&data_offset, sizeof(data_offset));

	auto& cache = Engine::GetEngine()->GetIndexCache();
	std::string data;
	if(!cache.Get(cache_key, data) || data.size() != L1Index.L1origin_size-L1Index.bloom_filter_size)
	{
		std::string bf_data;
		if(!m_index_reader.Read(&L1Index, bf_data, data))
		{
			return ERR_FILE_READ;
		}
	}
	
	assert(!data.empty());
	m_data = data;
	m_L1Index_start_key = L1Index.start_key;
	m_L1Index_size = L1Index.L1index_size;

	return OK;
}

Status IndexBlockReader::Search(const StrView& key, SegmentL0Index& L0_index)
{
	return SearchBlock(key, L0_index);
}

Status IndexBlockReader::SearchBlock(const StrView& key, SegmentL0Index& L0_index) const
{
	assert(!m_data.empty());
	const byte_t* block_end = (byte_t*)m_data.data() + m_data.size();
	const byte_t* index_ptr = block_end - m_L1Index_size - sizeof(uint32_t)/*crc32*/;
	const byte_t* group_ptr = (byte_t*)m_data.data();
	
	//找到第1个大于key的group
	LnGroupIndex lngroup_index;

	String prev_str1, prev_str2;
	StrView prev_key = m_L1Index_start_key;

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

Status IndexBlockReader::ParseBlock(IndexBlockReaderIteratorPtr& iter_ptr) const
{
	assert(!m_data.empty());
	const byte_t* block_end = (byte_t*)m_data.data() + m_data.size();
	const byte_t* index_ptr = block_end - m_L1Index_size - sizeof(uint32_t)/*crc32*/;
	const byte_t* group_ptr = (byte_t*)m_data.data();
	
	//找到第1个大于key的group
	LnGroupIndex lngroup_index;

	String prev_str1, prev_str2;
	StrView prev_key = m_L1Index_start_key;

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

IndexBlockReaderIteratorPtr IndexBlockReader::NewIterator()
{
	IndexBlockReaderIteratorPtr iter_ptr = NewIndexBlockReaderIterator();

	ParseBlock(iter_ptr);

	iter_ptr->First();
	return iter_ptr;
}

IndexBlockReaderIterator::IndexBlockReaderIterator() : m_buf(Engine::GetEngine()->GetSmallPool())
{
	m_L0indexs.reserve(MAX_OBJECT_NUM_OF_BLOCK);
    m_idx = 0;
}

static bool UpperCmp(const StrView& key, const SegmentL0Index& index)
{
	return key < index.start_key;
}
void IndexBlockReaderIterator::Seek(const StrView& key)
{
	assert(key.Compare(m_L0indexs[0].start_key) >= 0);

	size_t pos = std::upper_bound(m_L0indexs.begin(), m_L0indexs.end(), key, UpperCmp) - m_L0indexs.begin();
	assert(pos > 0 && pos <= m_L0indexs.size());

    m_idx = pos - 1;

}

}  



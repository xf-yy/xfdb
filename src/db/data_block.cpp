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
#include "types.h"
#include "data_block.h"
#include "xfdb/strutil.h"
#include "file.h"
#include "block_pool.h"
#include "key_util.h"
#include "coding.h"

namespace xfdb 
{

DataBlockReader::DataBlockReader(BlockPool& pool) : m_pool(pool)
{
	m_data = nullptr;
}
DataBlockReader::~DataBlockReader()
{
	if(m_data != nullptr)
	{
		m_pool.Free(m_data);
	}
}

Status DataBlockReader::Read(const File& file, const SegmentL0Index& L0_index)
{
	//TODO: 读cache

	//从文件中读取数据块
	//printf("L0_index.L0compress_size:%u\n", L0_index.L0compress_size);
	m_data = m_pool.Alloc();
	if(m_data == nullptr)
	{
		return ERR_MEMORY_NOT_ENOUGH;
	}
	
	//TODO: 是否要解压
	
	int64_t r_size = file.Read(L0_index.L0offset, m_data, L0_index.L0compress_size);
	if((uint64_t)r_size != L0_index.L0compress_size)
	{
		return ERR_FILE_READ;
	}
	m_L0Index = L0_index;
	return OK;
}

Status DataBlockReader::SearchGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, const StrView& key, ObjectType& type, String& value) const
{
	assert(group_size != 0);
	const byte_t* group_end = group + group_size;
	const byte_t* data_ptr = group;

	String prev_str1, prev_str2;
	StrView prev_key = group_index.start_key;
	
	while(data_ptr < group_end)
	{
		uint32_t shared_keysize = DecodeV32(data_ptr, group_end);
		StrView nonshared_key = DecodeString(data_ptr, group_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);
		
		byte_t curr_type = *data_ptr++;
		StrView curr_value = DecodeString(data_ptr, group_end);
		
		int ret = key.Compare(curr_key);
		if(ret == 0)
		{
			type = (ObjectType)(curr_type & 0x0F);
			value.Assign(curr_value.data, curr_value.size);
			return OK;
		}
		else if(ret < 0)
		{
			break;
		}
		prev_key = curr_key;
	}
	return ERR_OBJECT_NOT_EXIST;
}

Status DataBlockReader::SearchL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, const StrView& key, ObjectType& type, String& value) const
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
		
		group_index.start_key = curr_key;
		group_index.group_size = DecodeV32(index_ptr, group_end);
		
		if(ret == 0)
		{
			return SearchGroup(group_ptr, group_index.group_size, group_index, key, type, value);
		}
		group_ptr += group_index.group_size;
		
		prev_key = curr_key;
	}

	return SearchGroup(group_ptr-group_index.group_size, group_index.group_size, group_index, key, type, value);;
}

Status DataBlockReader::SearchBlock(const byte_t* block, uint32_t block_size, const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, String& value) const
{
	assert(block_size != 0);
	const byte_t* block_end = block + block_size;
	const byte_t* index_ptr = block_end - L0_index.L0index_size - sizeof(uint32_t)/*crc32*/;
	const byte_t* group_ptr = block;
	
	//找到第1个大于key的group
	String prev_str1, prev_str2;
	LnGroupIndex lngroup_index;
	StrView prev_key;

	const byte_t* index_end = block_end - sizeof(uint32_t)/*crc32*/;
	while(index_ptr < index_end)
	{
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

		//前后key比较	
		if(ret == 0)
		{
			return SearchL2Group(group_ptr, lngroup_index.group_size, lngroup_index, key, type, value);
		}
		group_ptr += lngroup_index.group_size;
		prev_key = curr_key;
	}
	return SearchL2Group(group_ptr-lngroup_index.group_size, lngroup_index.group_size, lngroup_index, key, type, value);;
}

Status DataBlockReader::Search(const StrView& key, ObjectType& type, String& value)
{
	//TODO: 将bloom和data放入cache中
	//TODO: 判断是否有bloom，有则判断bloom是否命中

	return SearchBlock(m_data, m_L0Index.L0compress_size, m_L0Index, key, type, value);
}

Status DataBlockReader::ParseGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, DataBlockReaderIteratorPtr& iter_ptr) const
{
	assert(group_size != 0);
	const byte_t* group_end = group + group_size;
	const byte_t* data_ptr = group;

	String prev_str1, prev_str2;
	StrView prev_key = group_index.start_key;
	Object obj;
	obj.id = 0;//FIXME:使用segmentid

	while(data_ptr < group_end)
	{
		uint32_t shared_keysize = DecodeV32(data_ptr, group_end);
		StrView nonshared_key = DecodeString(data_ptr, group_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);
		obj.key = curr_key;

		byte_t curr_type = *data_ptr++;
		obj.type = (ObjectType)(curr_type & 0x0F);

		obj.value = DecodeString(data_ptr, group_end);

		iter_ptr->Add(obj);
		
		prev_key = curr_key;
	}
	return OK;
}

Status DataBlockReader::ParseL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, DataBlockReaderIteratorPtr& iter_ptr) const
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
		
		group_index.start_key = curr_key;
		group_index.group_size = DecodeV32(index_ptr, group_end);
		
		ParseGroup(group_ptr, group_index.group_size, group_index, iter_ptr);
		
		group_ptr += group_index.group_size;
		
		prev_key = curr_key;
	}

	return OK;
}

Status DataBlockReader::ParseBlock(const byte_t* block, uint32_t block_size, const SegmentL0Index& L0_index, DataBlockReaderIteratorPtr& iter_ptr) const
{
	assert(block_size != 0);
	const byte_t* block_end = block + block_size;
	const byte_t* index_ptr = block_end - L0_index.L0index_size - sizeof(uint32_t)/*crc32*/;
	const byte_t* group_ptr = block;
	
	//找到第1个大于key的group
	String prev_str1, prev_str2;
	LnGroupIndex lngroup_index;
	StrView prev_key;

	const byte_t* index_end = block_end - sizeof(uint32_t)/*crc32*/;
	while(index_ptr < index_end)
	{
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

DataBlockReaderIteratorPtr DataBlockReader::NewIterator()
{
	DataBlockReaderIteratorPtr iter_ptr = NewDataBlockReaderIterator(*this, m_pool);

	ParseBlock(m_data, m_L0Index.L0compress_size, m_L0Index, iter_ptr);
	iter_ptr->First();
	return iter_ptr;
}



}  

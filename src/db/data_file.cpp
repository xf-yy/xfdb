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

#include "coding.h"
#include "path.h"
#include "block_pool.h"
#include "data_file.h"
#include "index_file.h"
#include "file_util.h"

using namespace xfutil;

namespace xfdb 
{

DataReader::DataReader()
{
}
DataReader::~DataReader()
{
}

Status DataReader::Open(const char* bucket_path, fileid_t fileid)
{
	char data_path[MAX_PATH_LEN];
	MakeDataFilePath(bucket_path, fileid, data_path);
	
	if(!m_file.Open(data_path, OF_READONLY))
	{
		return ERR_FILE_READ;
	}
	return OK;
}

StrView DataReader::ClonePrevKey(const StrView& str)
{
	m_prev_key.Assign(str.data, str.size);
	return StrView(m_prev_key.Data(), m_prev_key.Size());
}

Status DataReader::SearchGroup(const byte_t* group, uint32_t chunk_size, const GroupIndex& group_index, const StrView& key, ObjectType& type, String& value) const
{
	const byte_t* group_end = group + chunk_size;
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

Status DataReader::SearchChunk(const byte_t* chunk, uint32_t chunk_size, const ChunkIndex& chunk_index, const StrView& key, ObjectType& type, String& value) const
{
	const byte_t* chunk_end = chunk + chunk_size;
	const byte_t* index_ptr = chunk_end - chunk_index.index_size;
	const byte_t* group_ptr = chunk;
	
	String prev_str1, prev_str2;
	GroupIndex group_index;
	StrView prev_key = chunk_index.start_key;
	
	while(index_ptr < chunk_end)
	{
		//解group key
		uint32_t shared_keysize = DecodeV32(index_ptr, chunk_end);
		StrView nonshared_key = DecodeString(index_ptr, chunk_end);
		StrView curr_key = MakeKey(prev_key, shared_keysize, nonshared_key, prev_str1, prev_str2);
		int ret = key.Compare(curr_key);
		if(ret < 0)
		{
			break;
		}
		
		group_index.start_key = curr_key;
		group_index.group_size = DecodeV32(index_ptr, chunk_end);
		
		if(ret == 0)
		{
			return SearchGroup(group_ptr, group_index.group_size, group_index, key, type, value);
		}
		group_ptr += group_index.group_size;
		
		prev_key = curr_key;
	}

	return SearchGroup(group_ptr-group_index.group_size, group_index.group_size, group_index, key, type, value);;
}

Status DataReader::SearchBlock(const byte_t* block, uint32_t block_size, const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, String& value) const
{
	const byte_t* block_end = block + block_size;
	const byte_t* index_ptr = block_end - L0_index.L0index_size;
	const byte_t* chunk_ptr = block;
	
	//找到第1个大于key的chunk
	String prev_str1, prev_str2;
	ChunkIndex chunk_index;
	StrView prev_key;

	const byte_t* index_end = block_end - sizeof(uint32_t); //crc长度
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
		
		chunk_index.start_key = curr_key;
		chunk_index.chunk_size = DecodeV32(index_ptr, index_end);
		chunk_index.index_size = DecodeV32(index_ptr, index_end);

		//前后key比较	
		if(ret == 0)
		{
			return SearchChunk(chunk_ptr, chunk_index.chunk_size, chunk_index, key, type, value);
		}
		chunk_ptr += chunk_index.chunk_size;
		prev_key = curr_key;
	}
	return SearchChunk(chunk_ptr-chunk_index.chunk_size, chunk_index.chunk_size, chunk_index, key, type, value);;
}

Status DataReader::Search(const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, String& value) const
{
	//TODO: 读cache
	
	//从文件中读取数据块
	K4Buffer buf;
	Status s = ReadFile(m_file, L0_index.L0offset, (int64_t)(uint64_t)L0_index.L0compress_size, buf);
	if(s != OK)
	{
		return s;
	}
	//TODO: 是否要解压
	//TODO: 存cache
	
	return SearchBlock((byte_t*)buf.Data(), buf.Size(), L0_index, key, type, value);
}

///////////////////////////////////////////////////////////////////////////////


DataWriter::DataWriter(const DBConfig& db_conf, BlockPool& pool, IndexWriter& index_writer)
	: m_db_conf(db_conf), m_index_writer(index_writer), m_pool(pool), m_key_buf(pool)
{	
	m_offset = 0;
	m_block_start = m_pool.Alloc();
	m_block_ptr = m_block_start;
}

DataWriter::~DataWriter()
{
	m_pool.Free(m_block_start);
}

Status DataWriter::Create(const char* bucket_path, fileid_t seqid)
{
	char file_path[MAX_PATH_LEN];
	MakeDataFilePath(bucket_path, seqid, file_path);
	
	bool ret = m_file.Open(file_path, OF_CREATE|OF_WRITEONLY);
	if(!ret)
	{
		return ERR_FILE_WRITE;
	}
	
	m_block_ptr = FillDataFileHeader(m_block_start);
	m_offset = m_block_ptr - m_block_start;
	
	//写header
	if(m_file.Write(m_block_start, m_offset) != (int64_t)m_offset)
	{
		return ERR_FILE_WRITE;
	}
	
	return OK;
}

StrView DataWriter::CloneKey(const StrView& str)
{
	byte_t *p = m_key_buf.Write((byte_t*)str.data, str.size);
	return StrView((char*)p, str.size);
}

StrView DataWriter::ClonePrevKey(const StrView& str)
{
	m_prev_key.Assign(str.data, str.size);
	return StrView(m_prev_key.Data(), m_prev_key.Size());
}

Status DataWriter::FillGroup(Iterator& iter, GroupIndex& gi)
{
	if(!iter.Valid())
	{
		gi.group_size = 0;
		return ERR_NOMORE_DATA;
	}
	
	Status s = OK;
	byte_t* group_start = m_block_ptr;
	byte_t* block_end = m_block_start + m_pool.BlockSize();

	gi.start_key = CloneKey(iter.Key());
	StrView prev_key = gi.start_key;

	for(int i = 0; i < MAX_GROUP_OBJECT_NUM && iter.Valid(); ++i)
	{
		const StrView key = iter.Key();
		const StrView value = iter.Value();
		
		//当剩余空间不足时，结束掉该block
		uint32_t need_size = key.size + value.size + EXTRA_OBJECT_SIZE;
		if(block_end - m_block_ptr <= need_size)
		{
			s = ERR_BUFFER_FULL;
			break;
		}
		
		uint32_t shared_keysize = prev_key.PrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);

		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);

		*m_block_ptr++ = iter.Type();

		m_block_ptr = EncodeString(m_block_ptr, value.data, value.size);

		prev_key = ClonePrevKey(key);
		iter.Next();
		
		//当block超过一定大小时，结束该block
		if(m_block_ptr - m_block_start > MAX_COMPRESS_BLOCK_SIZE)
		{
			s = ERR_BUFFER_FULL;
			break;
		}
	}
	gi.group_size = m_block_ptr - group_start;
	
	return s;
}


Status DataWriter::FillGroupIndex(const GroupIndex* group_indexs, int index_cnt)
{
	StrView prev_key = group_indexs[0].start_key;
	for(int i = 0; i < index_cnt; ++i)
	{
		StrView key = group_indexs[i].start_key;
		
		uint32_t shared_keysize = prev_key.PrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);
		
		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);

		m_block_ptr = EncodeV32(m_block_ptr, group_indexs[i].group_size);

		prev_key = key;
	}
	return OK;
}

Status DataWriter::FillChunk(Iterator& iter, ChunkIndex& ci)
{
	if(!iter.Valid())
	{
		ci.chunk_size = 0;
		ci.index_size = 0;
		return ERR_NOMORE_DATA;
	}

	byte_t* chunk_start = m_block_ptr;
	
	ci.start_key = CloneKey(iter.Key());

	Status s = ERR_BUFFER_FULL;
	
	GroupIndex gis[MAX_CHUNK_GROUP_NUM];
	int gi_idx = 0;
	for(; gi_idx < MAX_CHUNK_GROUP_NUM && iter.Valid(); ++gi_idx)
	{
		s = FillGroup(iter, gis[gi_idx]);
		if(s != OK)
		{
			if(gis[gi_idx].group_size != 0)
			{
				++gi_idx;
			}
			break;
		}
	}
	byte_t* chunk_index_start = m_block_ptr;
	
	FillGroupIndex(gis, gi_idx);

	ci.chunk_size = m_block_ptr - chunk_start;
	ci.index_size = m_block_ptr - chunk_index_start;
	return s;
}

Status DataWriter::FillChunkIndex(const ChunkIndex* chunk_indexs, int index_cnt)
{
	StrView prev_key;
	for(int i = 0; i < index_cnt; ++i)
	{
		StrView key = chunk_indexs[i].start_key;
		
		uint32_t shared_keysize = prev_key.PrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);
		
		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);

		m_block_ptr = EncodeV32(m_block_ptr, chunk_indexs[i].chunk_size);
		m_block_ptr = EncodeV32(m_block_ptr, chunk_indexs[i].index_size);

		prev_key = key;
	}
	m_block_ptr = Encode32(m_block_ptr, 0);	//FIXME: crc填0
	return OK;
}

Status DataWriter::FillBlock(Iterator& iter, uint32_t& index_size)
{
	assert(iter.Valid());
	
	Status s = ERR_BUFFER_FULL;
	
	ChunkIndex cis[MAX_BLOCK_CHUNK_NUM];
	int ci_idx = 0;
	for(; ci_idx < MAX_BLOCK_CHUNK_NUM && iter.Valid(); ++ci_idx)
	{
		s = FillChunk(iter, cis[ci_idx]);
		if(s != OK)
		{
			if(cis[ci_idx].chunk_size != 0)
			{
				++ci_idx;
			}
			break;
		}
	}
	byte_t* index_start = m_block_ptr;
	
	FillChunkIndex(cis, ci_idx);

	index_size = m_block_ptr - index_start;

	return s;
}

Status DataWriter::Write(Iterator& iter)
{
	uint32_t index_size;
	while(iter.Valid())
	{
		m_block_ptr = m_block_start;
		m_key_buf.Clear();
		
		SegmentL0Index L0_index;
		L0_index.start_key = CloneKey(iter.Key());
		L0_index.L0offset = m_offset;
		
		FillBlock(iter, index_size);

		uint32_t block_size = m_block_ptr - m_block_start;
		//TODO:压缩
		
		L0_index.L0compress_size = block_size;
		L0_index.L0origin_size = block_size;
		L0_index.L0index_size = index_size;
		
		//写block
		if(m_file.Write(m_block_start, block_size) != block_size)
		{
			return ERR_FILE_WRITE;
		}
		m_offset += block_size;
		
		m_index_writer.Write(L0_index);
	}

	return OK;	
}

Status DataWriter::Finish()
{
	return m_file.Sync() ? OK : ERR_FILE_WRITE;
}

Status DataWriter::Remove(const char* bucket_path, fileid_t fileid)
{
	char data_path[MAX_PATH_LEN];
	MakeDataFilePath(bucket_path, fileid, data_path);

	return File::Remove(data_path) ? OK : ERR_PATH_DELETE;
}


}  



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
#include "key_util.h"
#include "data_block.h"
#include "engine.h"

using namespace xfutil;

namespace xfdb 
{

DataReader::DataReader() : m_large_pool(Engine::GetEngine()->GetLargePool())
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
	m_path = data_path;
	return OK;
}


Status DataReader::Search(const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, std::string& value) const
{
	DataBlockReader block(m_file, m_path);
	Status s = block.Read(L0_index);
	if(s != OK)
	{
		return s;
	}

	return block.Search(key, type, value);
}

///////////////////////////////////////////////////////////////////////////////


DataWriter::DataWriter(const BucketConfig& bucket_conf, BlockPool& pool, IndexWriter& index_writer)
	: m_bucket_conf(bucket_conf), m_index_writer(index_writer), m_large_pool(pool), m_key_buf(pool)
{	
	m_offset = 0;
	m_block_start = m_large_pool.Alloc();
	m_block_end = m_block_start + m_large_pool.BlockSize();
	m_block_ptr = m_block_start;

    m_prev_key.Reserve(1024);
}

DataWriter::~DataWriter()
{
	m_file.Close();
	
	char file_path[MAX_PATH_LEN], tmp_file_path[MAX_PATH_LEN];
	MakeTmpDataFilePath(m_bucket_path, m_segment_fileid, tmp_file_path);
	MakeDataFilePath(m_bucket_path, m_segment_fileid, file_path);

	File::Rename(tmp_file_path, file_path);

	m_large_pool.Free(m_block_start);
}

Status DataWriter::Create(const char* bucket_path, fileid_t fileid)
{
	StrCpy(m_bucket_path, sizeof(m_bucket_path), bucket_path);
	m_segment_fileid = fileid;

	char file_path[MAX_PATH_LEN];
	MakeTmpDataFilePath(bucket_path, fileid, file_path);
	
	if(!m_file.Open(file_path, OF_CREATE|OF_WRITEONLY))
	{
		return ERR_FILE_WRITE;
	}
	
	m_block_ptr = WriteDataFileHeader(m_block_start);
	m_offset = m_block_ptr - m_block_start;
	
	//写header
	if(m_file.Write(m_block_start, m_offset) != (int64_t)m_offset)
	{
		return ERR_FILE_WRITE;
	}
	
	return OK;
}

StrView DataWriter::ClonePrevKey(const StrView& str)
{
	m_prev_key.Assign(str.data, str.size);
	return StrView(m_prev_key.Data(), m_prev_key.Size());
}

Status DataWriter::WriteGroup(IteratorImpl& iter, L0GroupIndex& gi)
{
	if(!iter.Valid())
	{
		return ERR_NOMORE_DATA;
	}

	gi.start_key = CloneKey(m_key_buf, iter.object().key);
	StrView prev_key = gi.start_key;
	
	Status s = OK;
	byte_t* group_start = m_block_ptr;

	for(int i = 0; i < MAX_OBJECT_NUM_OF_GROUP && iter.Valid(); ++i)
	{
        const Object& obj = iter.object();
		const StrView& key = obj.key;
		const StrView& value = obj.value;
		
		//当剩余空间不足时，结束掉该block
		uint32_t need_size = key.size + value.size + EXTRA_OBJECT_SIZE;
		if(m_block_end - m_block_ptr <= need_size)
		{
			s = ERR_BUFFER_FULL;
			break;
		}
		if(m_bucket_conf.bloom_filter_bitnum != 0)
		{
			uint32_t hc = Hash32((byte_t*)key.data, key.size);
			m_key_hashs.push_back(hc);
		}
		uint32_t shared_keysize = prev_key.GetPrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);

		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);

		*m_block_ptr++ = obj.type;

		m_block_ptr = EncodeString(m_block_ptr, value.data, value.size);

		//key可能是临时的key，需要clone下
		prev_key = ClonePrevKey(key);
		iter.Next();
		
		//当block超过一定大小时，结束该block
		if(m_block_ptr - m_block_start > MAX_UNCOMPRESS_BLOCK_SIZE)
		{
			s = ERR_BUFFER_FULL;
			break;
		}
	}
	gi.group_size = m_block_ptr - group_start;
	
	return s;
}

Status DataWriter::WriteGroupIndex(const L0GroupIndex* group_indexs, int index_cnt)
{
	StrView prev_key = group_indexs[0].start_key;
	for(int i = 0; i < index_cnt; ++i)
	{
		StrView key = group_indexs[i].start_key;
		
		uint32_t shared_keysize = prev_key.GetPrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);
		
		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);

		m_block_ptr = EncodeV32(m_block_ptr, group_indexs[i].group_size);

		prev_key = key;
	}
	return OK;
}

Status DataWriter::WriteL2Group(IteratorImpl& iter, LnGroupIndex& ci)
{
	if(!iter.Valid())
	{
		return ERR_NOMORE_DATA;
	}
	ci.start_key = CloneKey(m_key_buf, iter.object().key);
	
	Status s = ERR_BUFFER_FULL;
	byte_t* group_start = m_block_ptr;
	
	L0GroupIndex gis[MAX_OBJECT_NUM_OF_GROUP];
	int gi_cnt = 0;
	for(; gi_cnt < MAX_OBJECT_NUM_OF_GROUP && iter.Valid(); ++gi_cnt)
	{
		s = WriteGroup(iter, gis[gi_cnt]);
		if(s != OK)
		{
			if(gis[gi_cnt].group_size != 0)
			{
				++gi_cnt;
			}
			break;
		}
	}
	byte_t* group_index_start = m_block_ptr;
	
	WriteGroupIndex(gis, gi_cnt);

	ci.group_size = m_block_ptr - group_start;
	ci.index_size = m_block_ptr - group_index_start;
	return s;
}

Status DataWriter::WriteL2GroupIndex(const LnGroupIndex* group_indexs, int index_cnt)
{
	StrView prev_key;
	for(int i = 0; i < index_cnt; ++i)
	{
		StrView key = group_indexs[i].start_key;
		
		uint32_t shared_keysize = prev_key.GetPrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);
		
		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);

		m_block_ptr = EncodeV32(m_block_ptr, group_indexs[i].group_size);
		m_block_ptr = EncodeV32(m_block_ptr, group_indexs[i].index_size);

		prev_key = key;
	}
	return OK;
}

Status DataWriter::WriteBlock(IteratorImpl& iter, uint32_t& index_size)
{
	assert(iter.Valid());
	
	Status s = ERR_BUFFER_FULL;
	
	LnGroupIndex cis[MAX_OBJECT_NUM_OF_GROUP];
	int ci_cnt = 0;
	for(; ci_cnt < MAX_OBJECT_NUM_OF_GROUP && iter.Valid(); ++ci_cnt)
	{
		s = WriteL2Group(iter, cis[ci_cnt]);
		if(s != OK)
		{
			if(cis[ci_cnt].group_size != 0)
			{
				++ci_cnt;
			}
			break;
		}
	}
	byte_t* index_start = m_block_ptr;
	
	WriteL2GroupIndex(cis, ci_cnt);

	index_size = m_block_ptr - index_start;
	
	m_block_ptr = Encode32(m_block_ptr, 0);	//FIXME: crc填0

	return s;
}

Status DataWriter::Write(IteratorImpl& iter)
{
	while(iter.Valid())
	{
		m_block_ptr = m_block_start;
		m_key_buf.Clear();
		
		SegmentL0Index L0_index;
		L0_index.start_key = CloneKey(m_key_buf, iter.object().key);	//iter.Key可能是临时key
		L0_index.L0offset = m_offset;
		
		uint32_t index_size;
		WriteBlock(iter, index_size);

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
		m_index_writer.Write(L0_index, m_key_hashs);
		
		m_offset += block_size;
	}

	return OK;	
}

Status DataWriter::Finish()
{
	if(!m_file.Sync())
	{
		return ERR_FILE_WRITE;
	}
	return OK;
}

Status DataWriter::Remove(const char* bucket_path, fileid_t fileid)
{
	char data_path[MAX_PATH_LEN];
	MakeDataFilePath(bucket_path, fileid, data_path);

	return File::Remove(data_path) ? OK : ERR_PATH_DELETE;
}


}  



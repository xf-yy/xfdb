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
#include "path.h"
#include "coding.h"
#include "block_pool.h"
#include "index_file.h"
#include "file_util.h"

using namespace xfutil;

namespace xfdb 
{

enum
{
	MID_UPMOST_KEY = MID_START,
	MID_MAX_OBJECTID,
	//MID_BLOOM_BITNUM,
	//MID_COMPRESS_TYPE,
};

#if 0
static bool LowerCmp(const SegmentL1Index index, const StrView key)
{
	return index.start_key.Compare(key) < 0;
}
#endif
static bool UpperCmp(const StrView& key, const SegmentL1Index& index)
{
	return key.Compare(index.start_key) < 0;
}

StrView MakeKey(StrView& prev_key, uint32_t shared_keysize, StrView& nonshared_key, String& prev_str1, String& prev_str2)
{
	if(prev_key.data == prev_str1.Data())
	{
		prev_str2.Assign(prev_key.data, shared_keysize);
		prev_str2.Append(nonshared_key.data, nonshared_key.size);
		return StrView(prev_str2.Data(), prev_str2.Size());
	}
	prev_str1.Assign(prev_key.data, shared_keysize);
	prev_str1.Append(nonshared_key.data, nonshared_key.size);
	return StrView(prev_str1.Data(), prev_str1.Size());
}

IndexReader::IndexReader(BlockPool& pool) : m_pool(pool), m_buf(pool)
{
}

IndexReader::~IndexReader()
{
}

Status IndexReader::Open(const char* bucket_path, const SegmentFileIndex& info)
{
	char index_path[MAX_PATH_LEN];
	MakeIndexFilePath(bucket_path, info.segment_fileid, index_path);

	if(!m_file.Open(index_path, OF_READONLY))
	{
		return ERR_FILE_READ;
	}
	String str;	
	uint64_t offset = info.index_filesize - info.L2index_meta_size;
	Status s = ReadFile(m_file, offset, info.L2index_meta_size, str);
	if(s != OK)
	{
		return s;
	}
	const byte_t* ptr = (byte_t*)str.Data() + info.L2index_meta_size - sizeof(uint32_t)*2;
	uint32_t L2index_size = Decode32(ptr);
	uint32_t meta_size = Decode32(ptr);
	
	s = ParseMeta((byte_t*)str.Data()+L2index_size, meta_size);
	if(s != OK)
	{
		return s;
	}
	s = ParseL2Index((byte_t*)str.Data(), L2index_size);
	if(s != OK)
	{
		return s;
	}
	
	assert(!m_L1indexs.empty());
	m_meta.lowest_key = m_L1indexs.begin()->start_key;
	return OK;
}

bool IndexReader::ParseObjectStat(const byte_t* &data, const byte_t* data_end)
{
	uint32_t cnt = DecodeV32(data, data_end);
	
	for(uint32_t i = 0; i < cnt; ++i)
	{
		byte_t type = *data++;
		ObjectStatItem* object_stat = (type == SetType) ? &m_meta.object_stat.set_stat : &m_meta.object_stat.delete_stat;

		object_stat->count = DecodeV64(data, data_end);
		object_stat->key_size = DecodeV64(data, data_end);
		object_stat->value_size = DecodeV64(data, data_end);
	}
	return true;
}

bool IndexReader::ParseDeletedSegment(const byte_t* &data, const byte_t* data_end)
{
	uint32_t cnt = DecodeV32(data, data_end);
	
	m_meta.deleted_segment_fileids.resize(cnt);
	for(uint32_t i = 0; i < cnt; ++i)
	{
		m_meta.deleted_segment_fileids[i] = DecodeV64(data, data_end);
	}
	return true;
}

bool IndexReader::ParseOtherMeta(const byte_t* &data, const byte_t* data_end)
{	
	//其他属性
	uint32_t id = DecodeV32(data, data_end);
	while(id != MID_END)
	{
		switch(id)
		{
		case MID_UPMOST_KEY:
			{
				StrView key = DecodeString(data, data_end);
				m_meta.upmost_key = CloneKey(key);
			}
			break;
		case MID_MAX_OBJECTID:
			m_meta.max_objectid = DecodeV64(data, data_end);
			break;
		//case MID_BLOOM_BITNUM:
		//	m_conf.bloom_bitnum = *data++;
		//	break;
		//case MID_COMPRESS_TYPE:
		//	m_conf.compress_type = (CompressionType)(*data++);
		//	break;
		default:
			return false;
			break;
		}
		id = DecodeV32(data, data_end);
	}
	return true;
}

StrView IndexReader::CloneKey(const StrView& str)
{
	byte_t *p = m_buf.Write((byte_t*)str.data, str.size);
	return StrView((char*)p, str.size);
}

bool IndexReader::ParseKeyIndex(const byte_t* &data, const byte_t* data_end, uint64_t& last_offset, SegmentL1Index& L1Index)
{
	StrView key = DecodeString(data, data_end);
	L1Index.start_key = CloneKey(key);

	L1Index.L1offset = last_offset;
	
	//if(m_conf.bloom_bitnum != 0)
	//{
	//	L1Index.bloom_size = DecodeV32(data, data_end);
	//}
	//else
	//{
		L1Index.bloom_size = 0;
	//}
	L1Index.L1compress_size = DecodeV32(data, data_end);
	
	uint32_t diff_size = DecodeV32(data, data_end);
	L1Index.L1origin_size = L1Index.L1compress_size + diff_size;
		
	L1Index.L1index_size = DecodeV32(data, data_end);
	
	last_offset += L1Index.L1compress_size;

	return true;
}

Status IndexReader::ParseL2Index(const byte_t* data, uint32_t L2index_size)
{
	const byte_t* data_end = data + L2index_size;

	uint64_t last_offset = DecodeV64(data, data_end);
	
	uint32_t key_cnt = DecodeV32(data, data_end);
	
	m_L1indexs.resize(key_cnt);
	for(uint32_t i = 0; i < key_cnt; ++i)
	{
		ParseKeyIndex(data, data_end, last_offset, m_L1indexs[i]);
	}

	//FIXME:未校验crc
	/*uint32_t crc = */Decode32(data);
	
	return OK;
}

Status IndexReader::ParseMeta(const byte_t* data, uint32_t meta_size)
{
	const byte_t* data_end = data + meta_size;
	
	if(!ParseDeletedSegment(data, data_end))
	{
		return ERR_FILE_FORMAT;
	}
	if(!ParseObjectStat(data, data_end))
	{
		return ERR_FILE_FORMAT;
	}
	if(!ParseOtherMeta(data, data_end))
	{
		return ERR_FILE_FORMAT;
	}
	//FIXME:未校验crc
	/*uint32_t crc = */Decode32(data);
	
	return OK;
}

Status IndexReader::SearchGroup(const byte_t* group, uint32_t group_size, const GroupIndex& group_index, const StrView& key, SegmentL0Index& L0_index) const
{
	assert(group_size != 0);
	const byte_t* group_end = group + group_size;

	String prev_str1, prev_str2;
	StrView prev_key = group_index.start_key;

	const byte_t* data_ptr = group;
	
	L0_index.L0offset = DecodeV64(data_ptr, group_end);

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
		
		//L0_index.start_key = curr_key;
		L0_index.L0compress_size = DecodeV32(data_ptr, group_end);
		uint32_t diff_size = DecodeV32(data_ptr, group_end);
		L0_index.L0origin_size = L0_index.L0compress_size + diff_size;
		L0_index.L0index_size = DecodeV32(data_ptr, group_end);

		if(ret == 0)
		{
			return OK;
		}
		L0_index.L0offset += L0_index.L0compress_size;
		
		prev_key = curr_key;
		
	}
	L0_index.L0offset -= L0_index.L0compress_size;
	return OK;
}

Status IndexReader::SearchChunk(const byte_t* chunk, uint32_t chunk_size, const ChunkIndex& chunk_index, const StrView& key, SegmentL0Index& L0_index) const
{
	assert(chunk_size != 0);
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
		//解group_size
		group_index.start_key = curr_key;
		group_index.group_size = DecodeV32(index_ptr, chunk_end);
		
		if(ret == 0)
		{
			return SearchGroup(group_ptr, group_index.group_size, group_index, key, L0_index);
		}
		group_ptr += group_index.group_size;

		prev_key = curr_key;
	}
	return SearchGroup(group_ptr-group_index.group_size, group_index.group_size, group_index, key, L0_index);
}

Status IndexReader::SearchBlock(const byte_t* block, uint32_t block_size, const SegmentL1Index* L1Index, const StrView& key, SegmentL0Index& L0_index) const
{
	assert(block_size != 0);
	const byte_t* block_end = block + block_size;
	const byte_t* index_ptr = block_end - L1Index->L1index_size;
	const byte_t* chunk_ptr = block;
	
	//找到第1个大于key的chunk
	ChunkIndex chunk_index;

	String prev_str1, prev_str2;
	StrView prev_key = L1Index->start_key;

	const byte_t* index_end = block_end - sizeof(uint32_t);//crc长度
	while(index_ptr < index_end)
	{
		//解 chunk key
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

		if(ret == 0)
		{
			return SearchChunk(chunk_ptr, chunk_index.chunk_size, chunk_index, key, L0_index);
		}
		
		chunk_ptr += chunk_index.chunk_size;

		prev_key = curr_key;
	}
	
	return SearchChunk(chunk_ptr-chunk_index.chunk_size, chunk_index.chunk_size, chunk_index, key, L0_index);
}

const SegmentL1Index* IndexReader::Search(const StrView& key) const
{
	if(key.Compare(m_meta.lowest_key) < 0 || key.Compare(m_meta.upmost_key) > 0)
	{
		return nullptr;
	}
	
	//二分查找
	uint32_t pos = std::upper_bound(m_L1indexs.begin(), m_L1indexs.end(), key, UpperCmp) - m_L1indexs.begin();
	assert(pos > 0 && pos <= m_L1indexs.size());
	return &m_L1indexs[pos-1];
}

Status IndexReader::Search(const StrView& key, SegmentL0Index& L0_index) const
{
	const SegmentL1Index* L1Index = Search(key);
	if(L1Index == nullptr)
	{
		return ERR_OBJECT_NOT_EXIST;
	}

	//TODO: 读取bloom cache
	//TODO: 读取L1块 cache
	
	//读取L1索引
	//printf("L1Index->L1compress_size:%u\n", L1Index->L1compress_size);
	byte_t* buf = m_pool.Alloc();
	if(buf == nullptr)
	{
		return ERR_MEMORY_NOT_ENOUGH;
	}
	int64_t r_size = m_file.Read(L1Index->L1offset, buf, L1Index->L1compress_size);
	if((uint64_t)r_size != L1Index->L1compress_size)
	{
		m_pool.Free(buf);
		return ERR_FILE_READ;
	}
	//TODO: 判断是否需要解压
	//TODO: 将bloom和data放入cache中
	//TODO: 判断是否有bloom，有则判断bloom是否命中
	
	const byte_t* block = buf + L1Index->bloom_size;
	uint32_t block_size = r_size - L1Index->bloom_size;

	Status s = SearchBlock(block, block_size, L1Index, key, L0_index);
	m_pool.Free(buf);
	return s;
}


////////////////////////////////////////////////////////////
IndexWriter::IndexWriter(const DBConfig& db_conf, BlockPool& pool)
	: m_db_conf(db_conf), m_pool(pool), m_L1key_buf(pool), m_L0key_buf(pool)
{
	m_offset = 0;
	m_L1offset_start = 0;
	m_L2offset_start = 0;
	
	m_block_start = pool.Alloc();
	m_block_end = m_block_start + m_pool.BlockSize();
	m_block_ptr = m_block_start;
	m_L0indexs = new SegmentL0Index[MAX_OBJECT_NUM_OF_BLOCK];
	m_L0index_cnt = 0;
	
	m_writing_size = 0;
}

IndexWriter::~IndexWriter()
{
	m_file.Close();
	
	char file_path[MAX_PATH_LEN], tmp_file_path[MAX_PATH_LEN];
	MakeTmpIndexFilePath(m_bucket_path, m_segment_fileid, tmp_file_path);
	MakeIndexFilePath(m_bucket_path, m_segment_fileid, file_path);

	File::Rename(tmp_file_path, file_path);

	m_pool.Free(m_block_start);
	delete[] m_L0indexs;
}

Status IndexWriter::Create(const char* bucket_path, fileid_t fileid)
{
	StrCpy(m_bucket_path, sizeof(m_bucket_path), bucket_path);
	m_segment_fileid = fileid;
	
	char file_path[MAX_PATH_LEN];
	MakeTmpIndexFilePath(bucket_path, fileid, file_path);
	
	if(!m_file.Open(file_path, OF_CREATE|OF_WRITEONLY))
	{
		return ERR_FILE_WRITE;
	}
	
	m_block_ptr = FillIndexFileHeader(m_block_start);
	m_L1offset_start = m_block_ptr - m_block_start;
	
	//写head
	if(m_file.Write(m_block_start, m_L1offset_start) != (int64_t)m_L1offset_start)
	{
		return ERR_FILE_WRITE;
	}
	m_offset = m_L1offset_start;
	
	return OK;
}

StrView IndexWriter::CloneKey(const StrView& str, WriteBuffer& buf)
{
	byte_t *p = buf.Write((byte_t*)str.data, str.size);
	return StrView((char*)p, str.size);
}

Status IndexWriter::FillGroup(uint32_t& L0_idx, GroupIndex& gi)
{	
	if(L0_idx >= m_L0index_cnt)
	{
		return ERR_NOMORE_DATA;
	}
	gi.start_key = CloneKey(m_L0indexs[L0_idx].start_key, m_L1key_buf);	
	StrView prev_key = gi.start_key;
	
	Status s = OK;
	byte_t* group_start = m_block_ptr;

	m_block_ptr = EncodeV64(m_block_ptr, m_L0indexs[L0_idx].L0offset);
	for(int i = 0; i < MAX_OBJECT_NUM_OF_GROUP && L0_idx < m_L0index_cnt; ++i)
	{
		const SegmentL0Index& L0indexs = m_L0indexs[L0_idx];
		
		//当剩余空间不足时，结束掉该block
		uint32_t need_size = L0indexs.start_key.size + EXTRA_OBJECT_SIZE;
		if(m_block_end - m_block_ptr <= need_size)
		{
			s = ERR_BUFFER_FULL;
			break;
		}
		
		StrView key = L0indexs.start_key;
		uint32_t shared_keysize = prev_key.GetPrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);
		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);
		
		m_block_ptr = EncodeV32(m_block_ptr, L0indexs.L0compress_size);
		m_block_ptr = EncodeV32(m_block_ptr, L0indexs.L0origin_size - L0indexs.L0compress_size);
		m_block_ptr = EncodeV32(m_block_ptr, L0indexs.L0index_size);

		//此key非临时key，无需clone
		prev_key = key;
		++L0_idx;

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

Status IndexWriter::FillGroupIndex(const GroupIndex* group_indexs, int index_cnt)
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

Status IndexWriter::FillChunk(uint32_t& L0_idx, ChunkIndex& ci)
{
	if(L0_idx >= m_L0index_cnt)
	{
		return ERR_NOMORE_DATA;
	}
	ci.start_key = CloneKey(m_L0indexs[L0_idx].start_key, m_L1key_buf);
	
	Status s = ERR_BUFFER_FULL;
	byte_t* chunk_start = m_block_ptr;

	GroupIndex gis[MAX_GROUP_NUM_OF_CHUNK];
	int gi_cnt = 0;
	for(; gi_cnt < MAX_GROUP_NUM_OF_CHUNK && L0_idx < m_L0index_cnt; ++gi_cnt)
	{
		s = FillGroup(L0_idx, gis[gi_cnt]);
		if(s != OK)
		{
			if(gis[gi_cnt].group_size != 0)
			{
				++gi_cnt;
			}
			break;
		}
	}
	byte_t* chunk_index_start = m_block_ptr;
	
	FillGroupIndex(gis, gi_cnt);

	ci.chunk_size = m_block_ptr - chunk_start;
	ci.index_size = m_block_ptr - chunk_index_start;
	return s;	
}

Status IndexWriter::FillChunkIndex(const ChunkIndex* chunk_indexs, int index_cnt)
{
	StrView prev_key = chunk_indexs[0].start_key;
	for(int i = 0; i < index_cnt; ++i)
	{
		StrView key = chunk_indexs[i].start_key;
		
		uint32_t shared_keysize = prev_key.GetPrefixLength(key);
		m_block_ptr = EncodeV32(m_block_ptr, shared_keysize);
		
		uint32_t nonshared_size = key.size - shared_keysize;
		m_block_ptr = EncodeString(m_block_ptr, &key.data[shared_keysize], nonshared_size);

		m_block_ptr = EncodeV32(m_block_ptr, chunk_indexs[i].chunk_size);
		m_block_ptr = EncodeV32(m_block_ptr, chunk_indexs[i].index_size);

		prev_key = key;
	}
	//FIXME:crc填0
	m_block_ptr = Encode32(m_block_ptr, 0);
	return OK;
}

Status IndexWriter::FillBlock(uint32_t& index_size)
{	
	assert(m_L0index_cnt != 0);
	
	uint32_t L0_idx = 0;
	Status s = ERR_BUFFER_FULL;
	
	ChunkIndex cis[MAX_CHUNK_NUM_OF_BLOCK];
	int ci_cnt = 0;
	for(; ci_cnt < MAX_CHUNK_NUM_OF_BLOCK && L0_idx < m_L0index_cnt; ++ci_cnt)
	{
		s = FillChunk(L0_idx, cis[ci_cnt]);
		if(s != OK)
		{
			if(cis[ci_cnt].chunk_size != 0)
			{
				++ci_cnt;
			}
			break;
		}
	}
	byte_t* index_start = m_block_ptr;
	
	FillChunkIndex(cis, ci_cnt);

	index_size = m_block_ptr - index_start;
	return s;
}

Status IndexWriter::WriteBlock()
{
	m_block_ptr = m_block_start;

	SegmentL1Index L1_index;
	L1_index.start_key = CloneKey(m_L0indexs[0].start_key, m_L1key_buf);
	L1_index.L1offset = m_offset;

	uint32_t index_size;
	FillBlock(index_size);

	uint32_t block_size = m_block_ptr - m_block_start;
	
	//TODO: 压缩
	
	L1_index.bloom_size = 0;
	L1_index.L1compress_size = block_size;
	L1_index.L1origin_size = block_size;
	L1_index.L1index_size = index_size;

	//写block
	if(m_file.Write(m_block_start, block_size) != block_size)
	{
		return ERR_FILE_WRITE;
	}
	m_L1indexs.push_back(L1_index);
	
	m_offset += block_size;
	
	return OK;
}

Status IndexWriter::Write(const SegmentL0Index& L0_index)
{
	//TODO:构建最短key
	m_L0indexs[m_L0index_cnt] = L0_index;
	m_L0indexs[m_L0index_cnt].start_key = CloneKey(L0_index.start_key, m_L0key_buf);

	++m_L0index_cnt;
	m_writing_size += L0_index.start_key.size + MAX_V32_SIZE*3;
	
	//则限制1024个+128KB(压缩时，如果不压缩，则为64KB)
	//TODO:如果有布隆，则限制元素数量？
	if(m_L0index_cnt >= MAX_OBJECT_NUM_OF_BLOCK || m_writing_size >= MAX_UNCOMPRESS_BLOCK_SIZE)
	{
		WriteBlock();
		
		m_L0index_cnt = 0;
		m_writing_size = 0;
		m_L0key_buf.Clear();
	}
	return OK;
}

Status IndexWriter::WriteL2Index(uint32_t& L2index_size)
{
	L2index_size = 0;

	m_block_ptr = m_block_start;

	m_block_ptr = EncodeV64(m_block_ptr, m_L1offset_start);
	
	m_block_ptr = EncodeV32(m_block_ptr, m_L1indexs.size());
	for(auto it = m_L1indexs.begin(); it != m_L1indexs.end(); ++it)
	{
		//剩余空间不足时刷盘
		if(m_block_ptr - m_block_start <= (ssize_t)it->start_key.size + EXTRA_OBJECT_SIZE)
		{
			uint32_t size = m_block_ptr - m_block_start;
			if(m_file.Write(m_block_start, size) != size)
			{
				return ERR_FILE_WRITE;
			}
			m_offset += size;
			L2index_size += size;

			m_block_ptr = m_block_start;
		}
		
		m_block_ptr = EncodeString(m_block_ptr, it->start_key.data, it->start_key.size);
		//如果bloom_len不为空
		//m_block_ptr = EncodeV32(m_block_ptr, it->bloom_size);
		m_block_ptr = EncodeV32(m_block_ptr, it->L1compress_size);
		m_block_ptr = EncodeV32(m_block_ptr, it->L1origin_size - it->L1compress_size);
		m_block_ptr = EncodeV32(m_block_ptr, it->L1index_size);

	}
	m_L1indexs.clear();
	
	m_block_ptr = Encode32(m_block_ptr, 0);//FIXME:crc填0

	uint32_t size = m_block_ptr - m_block_start;
	if(m_file.Write(m_block_start, size) != size)
	{
		return ERR_FILE_WRITE;
	}
	m_offset += size;
	L2index_size += size;
	
	return OK;
}

void IndexWriter::FillObjectStat(ObjectType type, const ObjectStatItem& stat)
{
	*m_block_ptr++ = (byte_t)type;
	m_block_ptr = EncodeV64(m_block_ptr, stat.count);
	m_block_ptr = EncodeV64(m_block_ptr, stat.key_size);
	m_block_ptr = EncodeV64(m_block_ptr, stat.value_size);
}

void IndexWriter::FillMeta(const SegmentMeta& meta)
{
	m_block_ptr = EncodeV32(m_block_ptr, meta.deleted_segment_fileids.size());
	for(auto id : meta.deleted_segment_fileids)
	{
		m_block_ptr = EncodeV64(m_block_ptr, id);
	}
	
	m_block_ptr = EncodeV32(m_block_ptr, 2);//set+delete
	FillObjectStat(SetType, meta.object_stat.set_stat);
	FillObjectStat(DeleteType, meta.object_stat.delete_stat);
	
	m_block_ptr = EncodeString(m_block_ptr, MID_UPMOST_KEY, meta.upmost_key.data, meta.upmost_key.size);
	m_block_ptr = EncodeV64(m_block_ptr, MID_MAX_OBJECTID, meta.max_objectid);
	m_block_ptr = EncodeV32(m_block_ptr, MID_END);
	
	m_block_ptr = Encode32(m_block_ptr, 0);//FIXME:crc填0
}

Status IndexWriter::WriteMeta(uint32_t L2index_size, const SegmentMeta& meta)
{
	m_block_ptr = m_block_start;

	FillMeta(meta);
	
	uint32_t meta_size = m_block_ptr - m_block_start;
	
	m_block_ptr = Encode32(m_block_ptr, L2index_size);
	m_block_ptr = Encode32(m_block_ptr, meta_size);

	uint32_t size = m_block_ptr - m_block_start;
	if(m_file.Write(m_block_start, size) != size)
	{
		return ERR_FILE_WRITE;
	}
	m_offset += size;

	return OK;
}

Status IndexWriter::WriteL2IndexMeta(const SegmentMeta& meta)
{
	m_L2offset_start = m_offset;

	uint32_t L2index_size;
	Status s = WriteL2Index(L2index_size);
	if(s != OK)
	{
		return s;
	}

	return WriteMeta(L2index_size, meta);
}

Status IndexWriter::Finish(const SegmentMeta& meta)
{
	if(m_L0index_cnt != 0)
	{
		Status s = WriteBlock();
		if(s != OK)
		{
			return s;
		}
	}
	Status s = WriteL2IndexMeta(meta);
	if(s != OK)
	{
		return s;
	}
	if(!m_file.Sync())
	{
		return ERR_FILE_WRITE;
	}
	return OK;
}
	
Status IndexWriter::Remove(const char* bucket_path, fileid_t fileid)
{
	char index_path[MAX_PATH_LEN];
	MakeIndexFilePath(bucket_path, fileid, index_path);

	return File::Remove(index_path) ? OK : ERR_PATH_DELETE;
}
	


}  


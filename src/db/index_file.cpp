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
#include "index_block.h"
#include "engine.h"
#include "bloom_filter.h"

using namespace xfutil;

namespace xfdb 
{


enum
{
	MID_BLOOM_FILTER_BITNUM = MID_START,
	//MID_COMPRESS_TYPE,

    MID_MAX_KEY = 100,
    MID_MAX_OBJECT_ID,
    MID_MAX_MERGE_SEGMENT_ID,
};

IndexReader::IndexReader() : m_large_block_pool(Engine::GetEngine()->GetLargeBlockPool()), m_buf(m_large_block_pool)
{
}

IndexReader::~IndexReader()
{
}

Status IndexReader::Open(const char* bucket_path, const SegmentStat& info)
{
	char index_path[MAX_PATH_LEN];
	MakeIndexFilePath(bucket_path, info.segment_fileid, index_path);

	if(!m_file.Open(index_path, OF_READONLY))
	{
		return ERR_FILE_READ;
	}
	m_path = index_path;

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
	return OK;
}

bool IndexReader::ParseObjectStat(const byte_t* &data, const byte_t* data_end)
{
	uint32_t cnt = DecodeV32(data, data_end);
	
    TypeObjectStat* object_stat;
	for(uint32_t i = 0; i < cnt; ++i)
	{
		byte_t type = *data++;
        switch (type)
        {
        case SetType:
            object_stat = &m_meta.object_stat.set_stat;
            break;
        case DeleteType:
            object_stat = &m_meta.object_stat.delete_stat;
            break;
        default:
            assert(type == AppendType);
            object_stat = &m_meta.object_stat.append_stat;
            break;
        }

		object_stat->count = DecodeV64(data, data_end);
		object_stat->key_size = DecodeV64(data, data_end);
		object_stat->value_size = DecodeV64(data, data_end);
	}
	return true;
}

bool IndexReader::ParseKeyIndex(const byte_t* &data, const byte_t* data_end, uint64_t& last_offset, SegmentL1Index& L1Index)
{
	StrView key = DecodeString(data, data_end);
	L1Index.start_key = CloneKey(m_buf, key);

	L1Index.L1offset = last_offset;
	
	if(m_meta.bloom_filter_bitnum != 0)
	{
		L1Index.bloom_filter_size = DecodeV32(data, data_end);
	}
	else
	{
		L1Index.bloom_filter_size = 0;
	}
	L1Index.L1compress_size = DecodeV32(data, data_end);
	
	uint32_t diff_size = DecodeV32(data, data_end);
	assert(diff_size == 0);	//暂时未使用压缩功能
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
	
	if(!ParseObjectStat(data, data_end))
	{
		return ERR_FILE_FORMAT;
	}

	//其他属性
    uint32_t id;
	do
	{
		id = DecodeV32(data, data_end);
		switch(id)
		{
		case MID_BLOOM_FILTER_BITNUM:
			m_meta.bloom_filter_bitnum = *data++;
			break;
		//case MID_COMPRESS_TYPE:
		//	m_meta.compress_type = (CompressionType)(*data++);
		//	break;
        case MID_MAX_KEY:
            {
                StrView key = DecodeString(data, data_end);
                m_meta.max_key = CloneKey(m_buf, key);
                assert(m_meta.max_key.size != 0);
            }
            break;
        case MID_MAX_OBJECT_ID:
            m_meta.max_object_id = DecodeV64(data, data_end);
            break;
        case MID_MAX_MERGE_SEGMENT_ID:
            m_meta.max_merge_segment_id = DecodeV64(data, data_end);
            break;
		case MID_END:
			break;
		default:
			return ERR_FILE_FORMAT;
			break;
		}
	}while (id != MID_END);    

	//FIXME:未校验crc
	/*uint32_t crc = */Decode32(data);
	
	return OK;
}

bool IndexReader::Read(const SegmentL1Index* L1Index, std::string& bf_data, std::string& index_data) const
{
	byte_t* buffer;
	if(L1Index->L1compress_size <= m_large_block_pool.BlockSize())
	{
		buffer = m_large_block_pool.Alloc();
	}
	else
	{
		buffer = xmalloc(L1Index->L1compress_size);
	}

	int64_t r_size = m_file.Read(L1Index->L1offset, (void*)buffer, L1Index->L1compress_size);
	if((uint64_t)r_size != L1Index->L1compress_size)
	{
		if(L1Index->L1compress_size <= m_large_block_pool.BlockSize())
		{
			m_large_block_pool.Free(buffer);
		}
		else
		{
			xfree(buffer);
		}
		return false;
	}
	//TODO: 解压缩

	if(L1Index->bloom_filter_size != 0)
	{
		std::string cache_key = m_path;
		cache_key.append((char*)&L1Index->L1offset, sizeof(L1Index->L1offset));

		auto& bf_cache = Engine::GetEngine()->GetBloomFilterCache();
		bf_data.assign((char*)buffer, L1Index->bloom_filter_size);
		bf_cache.Add(cache_key, bf_data, bf_data.size());
	}

	std::string cache_key = m_path;
	uint64_t offset = L1Index->L1offset + L1Index->bloom_filter_size;
	cache_key.append((char*)&offset, sizeof(offset));

	auto& index_cache = Engine::GetEngine()->GetIndexCache();
	index_data.assign((char*)buffer+L1Index->bloom_filter_size, L1Index->L1origin_size-L1Index->bloom_filter_size);
	index_cache.Add(cache_key, index_data, index_data.size());

	if(L1Index->L1compress_size <= m_large_block_pool.BlockSize())
	{
		m_large_block_pool.Free(buffer);
	}
	else
	{
		xfree(buffer);
	}
	return true;
}

bool IndexReader::CheckBloomFilter(const SegmentL1Index* L1Index, const StrView& key) const
{
	std::string cache_key = m_path;
	cache_key.append((char*)&L1Index->L1offset, sizeof(L1Index->L1offset));

	auto& cache = Engine::GetEngine()->GetBloomFilterCache();
	std::string bf_data;
	if(!cache.Get(cache_key, bf_data) || bf_data.size() != L1Index->bloom_filter_size)
	{
		std::string index_data;
		Read(L1Index, bf_data, index_data);
	}
	//判断是否有bloom，有则判断bloom是否命中
	BloomFilter bf(m_meta.bloom_filter_bitnum);
	bf.Attach(bf_data);
	
	uint32_t hc = Hash32((byte_t*)key.data, key.size);
	return bf.Check(hc);
}

static bool UpperCmp(const StrView& key, const SegmentL1Index& index)
{
	return key < index.start_key;
}

ssize_t IndexReader::Find(const StrView& key) const
{
	if(key.Compare(m_L1indexs[0].start_key) < 0 || key.Compare(m_meta.max_key) > 0)
	{
		return -1;
	}
	ssize_t pos = std::upper_bound(m_L1indexs.begin(), m_L1indexs.end(), key, UpperCmp) - m_L1indexs.begin();
	assert(pos > 0 && pos <= (ssize_t)m_L1indexs.size());
    return pos - 1;
}

const SegmentL1Index* IndexReader::Search(const StrView& key) const
{
	ssize_t idx = Find(key);
    if(idx < 0)
    {
        return nullptr;
    }
    
	const SegmentL1Index* L1Index = &m_L1indexs[idx];

	//判断布隆
	if(L1Index->bloom_filter_size != 0 && !CheckBloomFilter(L1Index, key))
	{
		return nullptr;
	}

	return L1Index;
}


Status IndexReader::Search(const StrView& key, SegmentL0Index& L0_index) const
{
	const SegmentL1Index* L1Index = Search(key);
	if(L1Index == nullptr)
	{
		return ERR_OBJECT_NOT_EXIST;
	}
		
	IndexBlockReader block(*this);
	Status s = block.Read(*L1Index);
	if(s != OK)
	{
		return s;
	}

	return block.Search(key, L0_index);
}


////////////////////////////////////////////////////////////
IndexWriter::IndexWriter(const BucketConfig& bucket_conf, BlockPool& pool)
	: m_bucket_conf(bucket_conf), m_large_block_pool(pool), m_L1key_buf(pool), m_L0key_buf(pool)
{
	m_offset = 0;
	m_L1offset_start = 0;
	m_L2offset_start = 0;
	
	m_L0indexs.reserve(MAX_OBJECT_NUM_OF_BLOCK);
	m_writing_size = 0;
	
	m_block_start = pool.Alloc();
	m_block_end = m_block_start + m_large_block_pool.BlockSize();
	m_block_ptr = m_block_start;

    //m_prev_key.Reserve(1024);
}

IndexWriter::~IndexWriter()
{
	m_file.Close();
	
	char file_path[MAX_PATH_LEN], tmp_file_path[MAX_PATH_LEN];
	MakeTmpIndexFilePath(m_bucket_path, m_segment_fileid, tmp_file_path);
	MakeIndexFilePath(m_bucket_path, m_segment_fileid, file_path);

	File::Rename(tmp_file_path, file_path);

	m_large_block_pool.Free(m_block_start);
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
	
	m_block_ptr = WriteIndexFileHeader(m_block_start);
	m_L1offset_start = m_block_ptr - m_block_start;
	
	//写head
	if(m_file.Write(m_block_start, m_L1offset_start) != (int64_t)m_L1offset_start)
	{
		return ERR_FILE_WRITE;
	}
	m_offset = m_L1offset_start;
	
	return OK;
}


Status IndexWriter::WriteGroup(uint32_t& L0_idx, L0GroupIndex& gi)
{	
	if(L0_idx >= m_L0indexs.size())
	{
		return ERR_NOMORE_DATA;
	}
	gi.start_key = CloneKey(m_L1key_buf, m_L0indexs[L0_idx].start_key);	
	StrView prev_key = gi.start_key;
	uint64_t prev_offset = 0;
	
	Status s = OK;
	byte_t* group_start = m_block_ptr;
	
	for(int i = 0; i < MAX_OBJECT_NUM_OF_GROUP && L0_idx < m_L0indexs.size(); ++i)
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

		assert(L0indexs.L0offset > prev_offset);
		m_block_ptr = EncodeV64(m_block_ptr, L0indexs.L0offset-prev_offset);
		
		m_block_ptr = EncodeV32(m_block_ptr, L0indexs.L0compress_size);
		m_block_ptr = EncodeV32(m_block_ptr, L0indexs.L0origin_size - L0indexs.L0compress_size);
		m_block_ptr = EncodeV32(m_block_ptr, L0indexs.L0index_size);

		//此key非临时key，无需clone
		prev_key = key;
		prev_offset = L0indexs.L0offset;
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

Status IndexWriter::WriteGroupIndex(const L0GroupIndex* group_indexs, int index_cnt)
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

Status IndexWriter::WriteL2Group(uint32_t& L0_idx, LnGroupIndex& ci)
{
	if(L0_idx >= m_L0indexs.size())
	{
		return ERR_NOMORE_DATA;
	}
	ci.start_key = CloneKey(m_L1key_buf, m_L0indexs[L0_idx].start_key);
	
	Status s = ERR_BUFFER_FULL;
	byte_t* group_start = m_block_ptr;

	L0GroupIndex gis[MAX_OBJECT_NUM_OF_GROUP];
	int gi_cnt = 0;
	for(; gi_cnt < MAX_OBJECT_NUM_OF_GROUP && L0_idx < m_L0indexs.size(); ++gi_cnt)
	{
		s = WriteGroup(L0_idx, gis[gi_cnt]);
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

Status IndexWriter::WriteL2GroupIndex(const LnGroupIndex* group_indexs, int index_cnt)
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
		m_block_ptr = EncodeV32(m_block_ptr, group_indexs[i].index_size);

		prev_key = key;
	}
	return OK;
}

Status IndexWriter::WriteBlock(uint32_t& index_size)
{	
	assert(!m_L0indexs.empty());
	
	uint32_t L0_idx = 0;
	Status s = ERR_BUFFER_FULL;
	
	LnGroupIndex cis[MAX_OBJECT_NUM_OF_GROUP];
	int ci_cnt = 0;
	for(; ci_cnt < MAX_OBJECT_NUM_OF_GROUP && L0_idx < m_L0indexs.size(); ++ci_cnt)
	{
		s = WriteL2Group(L0_idx, cis[ci_cnt]);
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
	
	//FIXME:crc填0
	m_block_ptr = Encode32(m_block_ptr, 0);
	
	return s;
}

Status IndexWriter::WriteBlock(std::deque<uint32_t>& key_hashcodes)
{
	m_block_ptr = m_block_start;

	SegmentL1Index L1_index;
	L1_index.start_key = CloneKey(m_L1key_buf, m_L0indexs[0].start_key);
	L1_index.L1offset = m_offset;

	std::string bloom_filter_data;
	if(!key_hashcodes.empty())
	{
		BloomFilter bf(m_bucket_conf.bloom_filter_bitnum);
		bf.Create(key_hashcodes);
		bloom_filter_data = bf.Data();
	}	

	uint32_t index_size;
	WriteBlock(index_size);

	uint32_t block_size = m_block_ptr - m_block_start;
	
	//TODO: 压缩
	ssize_t total_size = bloom_filter_data.size() + block_size;
	
	L1_index.bloom_filter_size = bloom_filter_data.size();
	L1_index.L1compress_size = total_size;
	L1_index.L1origin_size = total_size;
	L1_index.L1index_size = index_size;

	//写block
	if(m_file.Write(bloom_filter_data.data(), bloom_filter_data.size(), m_block_start, block_size) != total_size)
	{
		return ERR_FILE_WRITE;
	}
	m_L1indexs.push_back(L1_index);
	
	m_offset += total_size;
	
	return OK;
}

Status IndexWriter::Write(const SegmentL0Index& L0_index, std::deque<uint32_t>& key_hashcodes)
{
	//TODO:构建最短key
	m_L0indexs.push_back(L0_index);
	m_L0indexs.back().start_key = CloneKey(m_L0key_buf, L0_index.start_key);

	m_writing_size += L0_index.start_key.size + MAX_V32_SIZE*3;
	
	//则限制1024个+128KB(压缩时，如果不压缩，则为64KB)
	//FIXME:这里可以限制布隆值的数量，以减少内存占用
	if(m_L0indexs.size() >= MAX_OBJECT_NUM_OF_BLOCK || m_writing_size >= MAX_UNCOMPRESS_BLOCK_SIZE)
	{
		WriteBlock(key_hashcodes);
		
		m_L0indexs.clear();
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
		if(m_bucket_conf.bloom_filter_bitnum != 0)
		{
			m_block_ptr = EncodeV32(m_block_ptr, it->bloom_filter_size);
		}
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

void IndexWriter::WriteObjectStat(ObjectType type, const TypeObjectStat& stat)
{
	*m_block_ptr++ = (byte_t)type;
	m_block_ptr = EncodeV64(m_block_ptr, stat.count);
	m_block_ptr = EncodeV64(m_block_ptr, stat.key_size);
	m_block_ptr = EncodeV64(m_block_ptr, stat.value_size);
}

void IndexWriter::WriteMeta(const SegmentMeta& meta)
{	
	m_block_ptr = EncodeV32(m_block_ptr, MaxObjectType);
	WriteObjectStat(SetType, meta.object_stat.set_stat);
	WriteObjectStat(DeleteType, meta.object_stat.delete_stat);
	WriteObjectStat(AppendType, meta.object_stat.append_stat);

	if(m_bucket_conf.bloom_filter_bitnum != 0)
	{
		m_block_ptr = EncodeV32(m_block_ptr, MID_BLOOM_FILTER_BITNUM, m_bucket_conf.bloom_filter_bitnum);
	}

    assert(meta.max_key.size != 0);
	m_block_ptr = EncodeString(m_block_ptr, MID_MAX_KEY, meta.max_key.data, meta.max_key.size);
	m_block_ptr = EncodeV64(m_block_ptr, MID_MAX_OBJECT_ID, meta.max_object_id);
	m_block_ptr = EncodeV64(m_block_ptr, MID_MAX_MERGE_SEGMENT_ID, meta.max_merge_segment_id);

	m_block_ptr = EncodeV32(m_block_ptr, MID_END);
	
	m_block_ptr = Encode32(m_block_ptr, 0);//FIXME:crc填0
}

Status IndexWriter::WriteMeta(uint32_t L2index_size, const SegmentMeta& meta)
{
	m_block_ptr = m_block_start;

	WriteMeta(meta);
	
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

Status IndexWriter::Finish(std::deque<uint32_t>& key_hashcodes, const SegmentMeta& meta)
{
	if(!m_L0indexs.empty())
	{
		Status s = WriteBlock(key_hashcodes);
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
    if(m_bucket_conf.sync_data)
    {
        if(!m_file.Sync())
        {
            return ERR_FILE_WRITE;
        }
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


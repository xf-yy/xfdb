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

#include "db_types.h"
#include "segment_file.h"
#include "object_writer_list.h"
#include "object_writer.h"
#include "object_reader_snapshot.h"
#include "engine.h"

namespace xfdb 
{

SegmentReader::SegmentReader()
{
}

SegmentReader::~SegmentReader()
{
}

Status SegmentReader::Open(const char* bucket_path, const SegmentIndexInfo& info)
{
	m_index_info = info;
	Status s = m_index_reader.Open(bucket_path, info);
	if(s != OK)
	{
		return s;
	}
	s = m_data_reader.Open(bucket_path, info.segment_fileid);
    if(s != OK)
    {
        return s;
    }
    m_max_key = m_index_reader.MaxKey();
    return s;
}

Status SegmentReader::Get(const StrView& key, objectid_t obj_id, ObjectType& type, std::string& value) const
{
	SegmentL0Index L0index;
	Status s = m_index_reader.Search(key, L0index);
	if(s != OK)
	{
		return s;
	}
	assert(L0index.start_key.Empty());
	return m_data_reader.Search(L0index, key, type, value);
}

IteratorImplPtr SegmentReader::NewIterator(objectid_t max_objid)
{
	SegmentReaderPtr ptr = std::dynamic_pointer_cast<SegmentReader>(shared_from_this());

	return NewSegmentReaderIterator(ptr);
}

uint64_t SegmentReader::Size() const
{
	return m_index_info.index_filesize + m_index_info.data_filesize;
}

void SegmentReader::GetStat(BucketStat& stat) const
{
	stat.segment_stat.Add(Size());
	stat.object_stat.Add(m_index_reader.GetMeta().object_stat);
}

// /////////////////////////////////////////////////////////////////////////////////////////////
SegmentReaderIterator::SegmentReaderIterator(SegmentReaderPtr& segment_reader) 
 	: m_segment_reader(segment_reader), 
      m_L1index_count(segment_reader->m_index_reader.m_L1indexs.size()),
	  m_index_block_reader(segment_reader->m_index_reader),
	  m_data_block_reader(segment_reader->m_data_reader.m_file, segment_reader->m_data_reader.m_path)
{
    m_max_key = m_segment_reader->MaxKey();
    assert(m_max_key.size != 0);    
	First();
}

Status SegmentReaderIterator::SeekL1Index(size_t idx, const StrView* key)
{
    m_L1index_idx = idx;
    m_data_block_iter.reset();

	Status s = m_index_block_reader.Read(m_segment_reader->m_index_reader.m_L1indexs[idx]);
    if(s != OK)
    {
        return s;
    }
	m_index_block_iter = m_index_block_reader.NewIterator();
    if(key != nullptr)
    {
        m_index_block_iter->Seek(*key);
    }

	s = m_data_block_reader.Read(m_index_block_iter->L0Index());
    if(s != OK)
    {
        return s;
    }
	m_data_block_iter = m_data_block_reader.NewIterator();
    if(key != nullptr)
    {
        m_data_block_iter->Seek(*key);

        //继续找第一个满足>=key
        while(Valid_() && m_data_block_iter->object().key < *key)
        {
            s = Next_();
            if(s != OK)
            {
                return s;
            }
        }
    }

	return OK;
}

/**移到第1个元素处*/
void SegmentReaderIterator::First()
{
    SeekL1Index(0);
    if(Valid_())
    {
        m_obj_ptr = &m_data_block_iter->object();
    }
}


/**移到到>=key的地方*/
void SegmentReaderIterator::Seek(const StrView& key)
{
    m_data_block_iter.reset();
    ssize_t idx = m_segment_reader->m_index_reader.Find(key);
    if(idx < 0)
    {
        return;
    }
    SeekL1Index(idx, &key);
    if(Valid_())
    {
        m_obj_ptr = &m_data_block_iter->object();
    }
}


/**向后移到一个元素*/
void SegmentReaderIterator::Next()
{
    if(Next_() != OK)
    {
        m_data_block_iter.reset();
        return;
    }
    if(Valid_())
    {
        m_obj_ptr = &m_data_block_iter->object();
    }
}

Status SegmentReaderIterator::Next_()
{
	m_data_block_iter->Next();
	if(m_data_block_iter->Valid())
	{
		return OK;
	}

	//读取下一个data block
	m_index_block_iter->Next();
	if(m_index_block_iter->Valid())
	{
		Status s = m_data_block_reader.Read(m_index_block_iter->L0Index());
        if(s != OK)
        {
            return s;
        }
		m_data_block_iter = m_data_block_reader.NewIterator();
		return OK;
	}

	if(++m_L1index_idx < m_L1index_count)
	{
		Status s = m_index_block_reader.Read(m_segment_reader->m_index_reader.m_L1indexs[m_L1index_idx]);
        if(s != OK)
        {
            return s;
        }
		m_index_block_iter = m_index_block_reader.NewIterator();

		s = m_data_block_reader.Read(m_index_block_iter->L0Index());
        if(s != OK)
        {
            return s;
        }
		m_data_block_iter = m_data_block_reader.NewIterator();
	}
    return OK;
}

/**是否还有下一个元素*/
bool SegmentReaderIterator::Valid() const
{
	return Valid_();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
SegmentWriter::SegmentWriter(const BucketConfig& bucket_conf, BlockPool& pool)
	: m_index_writer(bucket_conf, pool), m_data_writer(bucket_conf, pool, m_index_writer)
{
}

SegmentWriter::~SegmentWriter()
{
}

Status SegmentWriter::Create(const char* bucket_path, fileid_t fileid)
{
	Status s = m_index_writer.Create(bucket_path, fileid);
	if(s != OK)
	{
		return s;
	}
	return m_data_writer.Create(bucket_path, fileid);
}

Status SegmentWriter::Write(IteratorImplPtr& iter, const BucketStat& stat, SegmentIndexInfo& seginfo)
{	
	Status s = m_data_writer.Write(*iter);
	if(s != OK)
	{
		return s;
	}
	s = m_data_writer.Finish();
	if(s != OK)
	{
		return s;
	}

	SegmentMeta meta;
	meta.object_stat = stat.object_stat;

	s = m_index_writer.Finish(m_data_writer.m_key_hashs, iter->MaxKey(), meta);
	if(s != OK)
	{
		return s;
	}
	seginfo.data_filesize = m_data_writer.FileSize();
	seginfo.index_filesize = m_index_writer.FileSize();
	seginfo.L2index_meta_size = m_index_writer.L2IndexMetaSize();
	return OK;
}

Status SegmentWriter::Write(const ObjectWriterListPtr& object_writer_list, SegmentIndexInfo& seginfo)
{	
	//FIXME:与Merge相似，可以合并
	IteratorImplPtr iter = object_writer_list->NewIterator();

	BucketStat stat = {0};
	object_writer_list->GetStat(stat);

	return Write(iter, stat, seginfo);
}

Status SegmentWriter::Write(const MergingSegmentInfo& msinfo, SegmentIndexInfo& seginfo)
{
	std::map<fileid_t, ObjectReaderPtr> segment_readers;
	msinfo.GetMergingReaders(segment_readers);

    BucketMetaFilePtr meta_file;
	ObjectReaderSnapshot tmp_reader_snapshot(meta_file, segment_readers);
	IteratorImplPtr iter = tmp_reader_snapshot.NewIterator();

	BucketStat stat = {0};
	tmp_reader_snapshot.GetStat(stat);

	return Write(iter, stat, seginfo);
}
		
Status SegmentWriter::Remove(const char* bucket_path, fileid_t fileid)
{
	Status s = IndexWriter::Remove(bucket_path, fileid);
	if(s != OK)
	{
		return s;
	}
	return DataWriter::Remove(bucket_path, fileid);
}


fileid_t MergingSegmentInfo::NewSegmentFileID() const
{
	//算法：选用最小的seqid，将其level+1，如果level+1已是最大level，则从begin->end中选个未用的seqid
	//     如果都用了，则选用未使用的最小segment id
	assert(merging_segment_fileids.size() > 1);
	fileid_t fileid = *merging_segment_fileids.begin();
	
	if(LEVEL_ID(fileid) < MAX_LEVEL_ID)
	{
		fileid_t segment_id = SEGMENT_ID(fileid);
		uint32_t new_level = LEVEL_ID(fileid) + 1;

		return SEGMENT_FILEID(segment_id, new_level);
	}
	else if(FULLMERGE_COUNT(fileid) < MAX_FULLMERGE_NUM)
	{
		fileid_t segment_id = SEGMENT_ID(fileid);
		uint32_t fullmerge_cnt = FULLMERGE_COUNT(fileid) + 1;
		uint32_t level = LEVEL_ID(fileid);

		return SEGMENT_FILEID2(segment_id, fullmerge_cnt, level);
	}
	assert(false);
	return INVALID_FILE_ID;
}

void MergingSegmentInfo::GetMergingReaders(std::map<fileid_t, ObjectReaderPtr>& segment_readers) const
{
	assert(reader_snapshot);
	const auto& readers = reader_snapshot->Readers();

	for(auto id_it = merging_segment_fileids.begin(); id_it != merging_segment_fileids.end(); ++id_it)
	{
		auto reader_it = readers.find(*id_it);
		assert(reader_it != readers.end());

		segment_readers[*id_it] = reader_it->second;
	}		
}

uint64_t MergingSegmentInfo::GetMergingSize() const
{
	uint64_t total_size = 0;

	assert(reader_snapshot);
	const auto& readers = reader_snapshot->Readers();

	for(auto id_it = merging_segment_fileids.begin(); id_it != merging_segment_fileids.end(); ++id_it)
	{
		auto reader_it = readers.find(*id_it);
		assert(reader_it != readers.end());

		total_size += reader_it->second->Size();
	}		
	return total_size;
}


}  



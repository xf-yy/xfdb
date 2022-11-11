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

#include "types.h"
#include "segment_file.h"
#include "table_writer_snapshot.h"
#include "memwriter_iterator.h"
#include "table_writer.h"
#include "table_reader_snapshot.h"
#include "segment_iterator.h"
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
	return m_data_reader.Open(bucket_path, info.segment_fileid);
}

Status SegmentReader::Get(const StrView& key, ObjectType& type, String& value) const
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

IteratorPtr SegmentReader::NewIterator()
{
	SegmentReaderPtr ptr = std::dynamic_pointer_cast<SegmentReader>(shared_from_this());

	return NewSegmentReaderIterator(ptr);
}

//最大key
StrView SegmentReader::UpmostKey() const
{
	return m_index_reader.UpmostKey();
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

Status SegmentWriter::Write(IteratorPtr& iter, const BucketStat& stat, SegmentIndexInfo& seginfo)
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

	s = m_index_writer.Finish(m_data_writer.m_key_hashs, iter->UpmostKey(), meta);
	if(s != OK)
	{
		return s;
	}
	seginfo.data_filesize = m_data_writer.FileSize();
	seginfo.index_filesize = m_index_writer.FileSize();
	seginfo.L2index_meta_size = m_index_writer.L2IndexMetaSize();
	return OK;
}

Status SegmentWriter::Write(const TableWriterSnapshotPtr& table_writer_snapshot, SegmentIndexInfo& seginfo)
{	
	//FIXME:与Merge相似，可以合并
	IteratorPtr iter = table_writer_snapshot->NewIterator();

	BucketStat stat = {0};
	table_writer_snapshot->GetStat(stat);

	return Write(iter, stat, seginfo);
}

Status SegmentWriter::Merge(const MergingSegmentInfo& msinfo, SegmentIndexInfo& seginfo)
{
	std::map<fileid_t, TableReaderPtr> segment_readers;
	msinfo.GetMergingReaders(segment_readers);

	TableReaderSnapshot tmp_reader_snapshot(segment_readers);
	IteratorPtr iter = tmp_reader_snapshot.NewIterator();

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
	return INVALID_FILEID;
}

void MergingSegmentInfo::GetMergingReaders(std::map<fileid_t, TableReaderPtr>& segment_readers) const
{
	assert(reader_snapshot.readers);
	const auto& readers = reader_snapshot.readers->Readers();

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

	assert(reader_snapshot.readers);
	const auto& readers = reader_snapshot.readers->Readers();

	for(auto id_it = merging_segment_fileids.begin(); id_it != merging_segment_fileids.end(); ++id_it)
	{
		auto reader_it = readers.find(*id_it);
		assert(reader_it != readers.end());

		total_size += reader_it->second->Size();
	}		
	return total_size;
}


}  



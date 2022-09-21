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
#include "mem_writer_iterator.h"
#include "table_writer.h"

namespace xfdb 
{

SegmentReader::SegmentReader(BlockPool& pool) : m_index_reader(pool), m_data_reader(pool)
{
}

SegmentReader::~SegmentReader()
{
}

Status SegmentReader::Open(const char* bucket_path, const SegmentFileIndex& info)
{
	m_fileinfo = info;
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
	assert(false);
	return nullptr;
}

//最大key
StrView SegmentReader::UpmostKey() const
{
	return m_index_reader.GetMeta().upmost_key;
}

//最小key
StrView SegmentReader::LowestKey() const
{
	return m_index_reader.GetMeta().lowest_key;
}

uint64_t SegmentReader::Size() const
{
	return m_fileinfo.index_filesize + m_fileinfo.data_filesize;
}

void SegmentReader::GetStat(BucketStat& stat) const
{
	stat.segment_stat.Add(Size());
	stat.object_stat.Add(m_index_reader.GetMeta().object_stat);
}


SegmentWriter::SegmentWriter(const DBConfig& db_conf, BlockPool& pool)
	: m_index_writer(db_conf, pool), m_data_writer(db_conf, pool, m_index_writer)
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

Status SegmentWriter::Write(const TableWriterSnapshotPtr& table_writer_snapshot, SegmentFileIndex& seginfo)
{	
	IteratorPtr iter = table_writer_snapshot->NewIterator();
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
	meta.max_objectid = table_writer_snapshot->GetMaxObjectID();
	meta.lowest_key = table_writer_snapshot->LowestKey();
	meta.upmost_key = table_writer_snapshot->UpmostKey();

	BucketStat stat = {0};
	table_writer_snapshot->GetStat(stat);
	meta.object_stat = stat.object_stat;
	
	s = m_index_writer.Finish(meta);
	if(s != OK)
	{
		return s;
	}
	seginfo.data_filesize = m_data_writer.FileSize();
	seginfo.index_filesize = m_index_writer.FileSize();
	seginfo.L2index_meta_size = m_index_writer.L2IndexMetaSize();
	return OK;
}
	
Status SegmentWriter::Merge(const std::map<fileid_t, SegmentReaderPtr>& segment_readers, SegmentFileIndex& seginfo)
{
	assert(false);
	//IteratorPtr iter = segment_readers->NewIterator();
	//m_data_writer.Write(*iter);

	//m_index_writer.Finish(table_writer_snapshot->);

	return ERR_INVALID_MODE;
}
		
Status SegmentWriter::Remove(const char* bucket_path, fileid_t fileid)
{
	Status s = IndexWriter::Remove(bucket_path, fileid);
	if(s != OK)
	{
		assert(s != ERR_FILE_READ);
		return s;
	}
	return DataWriter::Remove(bucket_path, fileid);
}

}  



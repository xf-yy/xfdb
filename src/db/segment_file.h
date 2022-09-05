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

#ifndef __xfdb_segment_file_h__
#define __xfdb_segment_file_h__

#include "types.h"
#include "data_file.h"
#include "index_file.h"
#include "segment_iterator.h"
#include "mem_writer_iterator.h"
#include "file.h"
#include "table_reader.h"

namespace xfdb 
{

class SegmentReader : public TableReader
{
public:
	SegmentReader(BlockPool& pool);
	virtual ~SegmentReader();

public:
	Status Open(const char* bucket_path, const SegmentFileIndex& info);

	inline const SegmentFileIndex& FileIndex() const
	{
		return m_fileinfo;
	}
	
	Status Get(const StrView& key, ObjectType& type, String& value) const override;
	
	IteratorPtr NewIterator() override;
	
	//最大key
	StrView UpmostKey() const override;
	//最小key
	StrView LowestKey() const override;

	/**返回segment文件总大小*/
	uint64_t Size() const override;
	
	const ObjectStat& GetObjectStat() const override;	
	
private:
	SegmentFileIndex m_fileinfo;
	IndexReader m_index_reader;
	DataReader m_data_reader;

private:
	SegmentReader(const SegmentReader&) = delete;
	SegmentReader& operator=(const SegmentReader&) = delete;
	
};

class SegmentWriter
{
public:
	SegmentWriter(const DBConfig& db_conf, BlockPool& pool);
	~SegmentWriter();
	
public:	
	Status Create(const char* bucket_path, fileid_t fileid);
	
	Status Write(const TableWriterPtr& table_writer, SegmentFileIndex& seginfo);
	Status Write(const TableWriterSnapshotPtr& table_writer_snapshot, SegmentFileIndex& seginfo);
	Status Merge(const std::map<fileid_t, SegmentReaderPtr>& segment_readers, SegmentFileIndex& seginfo);

	static Status Remove(const char* bucket_path, fileid_t fileid);

private:
	IndexWriter m_index_writer;
	DataWriter m_data_writer;

private:
	SegmentWriter(const SegmentWriter&) = delete;
	SegmentWriter& operator=(const SegmentWriter&) = delete;
	
};


}  

#endif


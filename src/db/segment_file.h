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

#include "db_types.h"
#include "data_file.h"
#include "index_file.h"
#include "iterator_impl.h"
#include "file.h"
#include "object_reader.h"
#include "index_block.h"
#include "data_block.h"

namespace xfdb 
{

class SegmentReader : public ObjectReader
{
public:
	SegmentReader();
	virtual ~SegmentReader();

public:
	Status Open(const char* bucket_path, const SegmentIndexInfo& info);
	
	Status Get(const StrView& key, objectid_t obj_id, ObjectType& type, String& value) const override;
	
	IteratorImplPtr NewIterator(objectid_t max_objid = MAX_OBJECT_ID) override;
	
	//最大key
	StrView UpmostKey() const override;

	/**返回segment文件总大小*/
	uint64_t Size() const override;
	
	void GetStat(BucketStat& stat) const override;	
	
	inline const SegmentIndexInfo& IndexInfo() const
	{
		return m_index_info;
	}
	
private:
	SegmentIndexInfo m_index_info;
	IndexReader m_index_reader;
	DataReader m_data_reader;
	
private:
	friend class SegmentReaderIterator;
	SegmentReader(const SegmentReader&) = delete;
	SegmentReader& operator=(const SegmentReader&) = delete;
	
};

class SegmentReaderIterator : public IteratorImpl 
{
public:
	explicit SegmentReaderIterator(SegmentReaderPtr& segment_reader);
	virtual ~SegmentReaderIterator()
	{}

public:
	/**移到第1个元素处*/
	virtual void First() override;
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) override;
	
	/**向后移到一个元素*/
	virtual void Next() override;

	/**是否还有下一个元素*/
	virtual bool Valid() const override;
	
	virtual const Object& object() const override;	

	virtual StrView UpmostKey() const override;
	
private:
    Status SeekL1Index(size_t idx, const StrView* key = nullptr);  
        
private:
	SegmentReaderPtr m_segment_reader;
	uint32_t m_L1index_idx;
	uint32_t m_L1index_count;
	IndexBlockReader m_index_block_reader;
	IndexBlockReaderIteratorPtr m_index_block_iter;
	DataBlockReader m_data_block_reader;
	DataBlockReaderIteratorPtr m_data_block_iter;

private:
	SegmentReaderIterator(const SegmentReaderIterator&) = delete;
	SegmentReaderIterator& operator=(const SegmentReaderIterator&) = delete;
};

class SegmentWriter
{
public:
	SegmentWriter(const BucketConfig& bucket_conf, BlockPool& pool);
	~SegmentWriter();
	
public:	
	Status Create(const char* bucket_path, fileid_t fileid);
	
	Status Write(const ObjectWriterListPtr& object_writer_list, SegmentIndexInfo& seginfo);
	Status Write(const MergingSegmentInfo& msinfo, SegmentIndexInfo& seginfo);

	static Status Remove(const char* bucket_path, fileid_t fileid);

private:
	Status Write(IteratorImplPtr& iter, const BucketStat& stat, SegmentIndexInfo& seginfo);

private:
	IndexWriter m_index_writer;
	DataWriter m_data_writer;

private:
	SegmentWriter(const SegmentWriter&) = delete;
	SegmentWriter& operator=(const SegmentWriter&) = delete;
	
};


}  

#endif


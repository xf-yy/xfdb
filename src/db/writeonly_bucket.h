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

#ifndef __xfdb_writeonly_bucket_h__
#define __xfdb_writeonly_bucket_h__

#include "types.h"
#include "write_engine.h"
#include "writeonly_mem_writer.h"
#include "table_writer_snapshot.h"
#include "table_reader_snapshot.h"
#include "bucket.h"
#include <deque>
#include <mutex>
#include <set>

namespace xfdb 
{

class WriteOnlyBucket : public Bucket
{	
public:
	WriteOnlyBucket(WritableEngine* engine, DBImplPtr db, const BucketInfo& info);
	virtual ~WriteOnlyBucket();
	
public:	
	virtual Status Open() override;

	virtual void GetStat(BucketStat& stat) const override;

	//OK, check
	Status Write(const Object* object);
	
	//异步
	virtual Status TryFlush() override;
	virtual Status Flush()   override;
	virtual Status Merge() override;
		
	static Status Remove(const char* bucket_path);

protected:	
	virtual TableWriterPtr NewTableWriter(WritableEngine* engine);

	//清理内存table
	void Clear();
	Status Flush(bool force);
	    
private:
	Status Create();

	Status WriteSegment();			//同步刷盘
	Status WriteSegment(TableReaderPtr& reader, fileid_t new_segment_fileid, SegmentFileIndex& seginfo);
	Status WriteSegment(std::map<fileid_t, TableReaderPtr>& readers);
	Status WriteBucketMeta();		//同步刷盘
	void FlushMemWriter();

	Status Merge(MergeSegmentInfo& msinfo);
	Status FullMerge();		//同步merge
	Status PartMerge();		//同步merge，写入时合并降低速度？
	
	Status Clean();

private:
	fileid_t SelectNewSegmentFileId(MergeSegmentInfo& msinfo);
	void FillAliveSegmentInfos(TableReaderSnapshotPtr& trs_ptr, std::vector<fileid_t>& writed_segment_fileids, BucketMetaData& md);
	void UpdateReaderSnapshot(std::map<fileid_t, TableReaderPtr>& readers, TableReaderSnapshotPtr& prev_reader_snapshot);
	void UpdateReaderSnapshot(MergeSegmentInfo& msinfo);

protected:
	friend class WritableDB;
	friend class WritableEngine;
	WritableEngine* m_engine;
				
	//流程：memwriter->table_writer_snapshot->segment_snapshot;
	//TODO: 可采用几组memwriter达到并发写的目的(根据key hash)
	objectid_t m_next_objectid;					//下个object的 seqid
	TableWriterPtr m_memwriter;						//当前可写的memwriter
	TableWriterSnapshotPtr m_memwriter_snapshot;	//只读待落盘的memwriter集

	//FIXME:segment文件生成了，但没有写入segment list，怎么淘汰？
	//对于大于segment list中的segment都要淘汰？还是重新加入segment list？

	
	//std::set<fileid_t> m_merging_segment_fileids;	//正在合并的段ID，		防止重复合并
	
	
	std::vector<fileid_t> m_merged_segment_fileids;		//合并后待删除的段，需写入segment list
	std::vector<fileid_t> m_writed_segment_fileids;
	std::map<fileid_t, bool> m_writing_segment_fileids;
	
	//待清除的segment list文件	
	std::deque<FileName> m_deleting_bucket_meta_names;
		
};


}  

#endif


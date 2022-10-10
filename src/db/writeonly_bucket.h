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
	Status WriteSegment(TableWriterSnapshotPtr& memwriter_snapshot, fileid_t fileid, SegmentReaderPtr& new_segment_reader);
	Status WriteBucketMeta();		//同步刷盘
	void FlushMemWriter();

	Status Merge(MergingSegmentInfo& msinfo);
	Status FullMerge();		//同步merge
	Status PartMerge();		//同步merge，写入时合并降低速度？
	
	Status Clean();

private:
	fileid_t SelectNewSegmentFileID(MergingSegmentInfo& msinfo);
	void WriteAliveSegmentInfos(TableReaderSnapshotPtr& trs_ptr, BucketMetaData& md);

protected:
	WritableEngine* m_engine;
				
	TableWriterPtr m_memwriter;							//当前可写的memwriter
	TableWriterSnapshotPtr m_memwriter_snapshot;		//只读待落盘的memwriter集

	//FIXME:segment文件生成了，但没有写入bucket meta，怎么淘汰？
	//对于大于bucket meta中的segment都要淘汰？还是重新加入bucket meta？
	std::map<fileid_t, bool> m_writing_segment_fileids;	//正在写的segment
	int m_writed_segment_cnt;							//已写完的segment数
	
	//TODO: 所有level层的segment，用于merge
	std::set<fileid_t> m_tobe_merge_segments[MAX_LEVEL_NUM];
	std::map<fileid_t, bool> m_merging_segment_fileids[MAX_LEVEL_NUM];
	std::vector<fileid_t> m_merged_segment_fileids;		//合并后待删除的段，需写入bucket meta

	//待清除的bucket meta文件	
	std::deque<FileName> m_tobe_delete_bucket_meta_names;
		
	friend class WritableDB;
	friend class WritableEngine;
};


}  

#endif


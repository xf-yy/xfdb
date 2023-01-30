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

#include "db_types.h"
#include "writable_engine.h"
#include "writeonly_writer.h"
#include "object_writer_snapshot.h"
#include "object_reader_snapshot.h"
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

	Status Write(const Object* object);
	Status Write(const WriteOnlyWriterPtr& memtable);
	
	//异步
	virtual Status TryFlush() override;
	virtual Status Flush() override;
	virtual Status Merge() override;

	virtual	Status Clean() override;

	static Status Remove(const char* bucket_path);

protected:	
	virtual ObjectWriterPtr NewObjectWriter(WritableEngine* engine);

	Status Flush(bool force);

	//清理内存table
	void Clear();
	    
private:
	Status Create();
	Status RemoveMetaFile();

	Status WriteSegment();			//同步刷盘
	Status WriteSegment(ObjectWriterSnapshotPtr& memwriter_snapshot, fileid_t fileid, SegmentReaderPtr& new_segment_reader);
	Status WriteBucketMeta();		//同步刷盘
	void FlushMemWriter();

	Status Merge(MergingSegmentInfo& msinfo);
	Status FullMerge();				//同步merge
	Status PartMerge();				//同步merge，写入时合并降低速度？
	bool AddMerging(MergingSegmentInfo& msinfo);
	
private:
	void GetAliveSegmentStat(ObjectReaderSnapshotPtr& ors_ptr, BucketMeta& bm);

protected:
	WritableEngine* m_engine;
	const uint32_t m_max_memtable_size;									//1~1024
	const uint32_t m_max_memtable_objects;								//1000~100*10000
    const uint32_t m_merged_reserve_size;
				
	ObjectWriterPtr m_memwriter;										//当前正在写的memwriter
	ObjectWriterSnapshotPtr m_memwriter_snapshot;						//只读待落盘的memwriter集

	//FIXME:segment文件生成了，但没有写入bucket meta，怎么淘汰？
	//对于大于bucket meta中的segment都要淘汰？还是重新加入bucket meta？
	std::map<fileid_t, uint64_t> m_writing_segments;					//正在写的segment
	int64_t m_writed_segment_cnt;										//已写完的segment数
    std::vector<fileid_t> m_new_segment_fileid;                         //新增的segment fileid
	
	std::map<fileid_t, uint64_t> m_tobe_merge_segments[MAX_LEVEL_ID+1];	    //所有level层的segment，用于merge
	std::map<fileid_t, uint64_t> m_merging_segment_fileids[MAX_LEVEL_ID+1];	//正在合并的segment
	std::vector<fileid_t> m_merged_segment_fileids;						    //已合并并待删除的segment，需写入bucket meta

	std::deque<fileid_t> m_tobe_delete_bucket_meta_fileids;				//待删除的bucket meta文件
	fileid_t m_tobe_clean_bucket_meta_fileid;							//待清理的bucket meta文件

private:		
	friend class WritableDB;
	friend class WritableEngine;

	WriteOnlyBucket(const WriteOnlyBucket&) = delete;
  	WriteOnlyBucket& operator=(const WriteOnlyBucket&) = delete;	
};


}  

#endif


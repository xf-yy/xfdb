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

#include "bucket.h"
#include "bucket_metafile.h"
#include "table_reader_snapshot.h"
#include "path.h"
#include "logger.h"
#include "engine.h"

namespace xfdb 
{

Bucket::Bucket(DBImplPtr db, const BucketInfo& info) 
	: m_db(db), m_info(info)
{
	char bucket_path[MAX_PATH_LEN];
	MakeBucketPath(db->GetPath().c_str(), info.name.c_str(), info.id, bucket_path);
	m_bucket_path = bucket_path;

	m_next_object_id = MIN_OBJECTID;

	m_next_segment_id = MIN_FILEID;
	m_next_bucket_meta_fileid = MIN_FILEID;
	m_max_level_num = MAX_LEVEL_ID;
}

Status Bucket::Open(const char* bucket_meta_filename)
{	
	assert(bucket_meta_filename != nullptr);

	//读锁打开最新的segment list
	BucketMetaFilePtr bucket_meta_file = NewBucketMetaFile();
	Status s = bucket_meta_file->Open(m_bucket_path.c_str(), bucket_meta_filename, LF_TRY_READ);
	if(s != OK) 
	{
		return s;
	}
	BucketMetaData bmd;
	s = bucket_meta_file->Read(bmd);
	if(s != OK)
	{
		return s;
	}
	fileid_t fileid = strtoull(bucket_meta_filename, nullptr, 10);

	//保证一次只有一个线程在执行reload操作
	std::lock_guard<std::mutex> lock(m_mutex);
	if(fileid < m_next_bucket_meta_fileid)
	{
		assert(false);
		return ERROR;
	}	
	
	m_segment_rwlock.ReadLock();
	BucketReaderSnapshot reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	std::map<fileid_t, TableReaderPtr> new_readers;
	OpenSegments(bmd, reader_snapshot.readers.get(), new_readers);
	
	TableReaderSnapshotPtr new_ss_ptr = NewTableReaderSnapshot(new_readers);
	
	m_segment_rwlock.WriteLock();
	m_next_bucket_meta_fileid = fileid+1;
	m_next_segment_id = bmd.next_segment_id;
	m_next_object_id = bmd.next_object_id;
	m_max_level_num = bmd.max_level_num;

	m_reader_snapshot.readers.swap(new_ss_ptr);
	m_reader_snapshot.meta_file.swap(bucket_meta_file);
	m_segment_rwlock.WriteUnlock();

	return OK;
}

Status Bucket::OpenSegment(const char* bucket_path, const SegmentIndexInfo& sfi, SegmentReaderPtr& sr_ptr)
{
	DBImplPtr db = m_db.lock();
	sr_ptr = NewSegmentReader(db->GetEngine()->GetBlockPool());
	
	return sr_ptr->Open(bucket_path, sfi);
}

void Bucket::OpenSegments(const BucketMetaData& bmd, const TableReaderSnapshot* last_snapshot, std::map<fileid_t, TableReaderPtr>& readers)
{
	static const std::map<fileid_t, TableReaderPtr> empty_readers;
	const std::map<fileid_t, TableReaderPtr>& last_readers = (last_snapshot != nullptr) ? last_snapshot->Readers() : empty_readers;
	
	SegmentReaderPtr sr_ptr;
	for(const auto& seginfo : bmd.alive_segment_infos)
	{		
		auto it = last_readers.find(seginfo.segment_fileid);
		if(it != last_readers.end())
		{
			readers[seginfo.segment_fileid] = it->second;
		}
		else if(OpenSegment(m_bucket_path.c_str(), seginfo, sr_ptr) == OK)
		{
			readers[seginfo.segment_fileid] = sr_ptr;
		}
	}
}


}  // namespace xfdb



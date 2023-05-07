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
#include "bucketmeta_file.h"
#include "object_reader_snapshot.h"
#include "path.h"
#include "logger.h"
#include "engine.h"
#include "directory.h"

namespace xfdb 
{

Bucket::Bucket(DBImplPtr& db, const BucketInfo& info) 
	: m_db(db), m_info(info), m_conf(db->GetConfig().GetBucketConfig(info.name))
{
	char bucket_path[MAX_PATH_LEN];
	MakeBucketPath(db->GetPath().c_str(), info.name.c_str(), info.id, bucket_path);
	m_bucket_path = bucket_path;

	m_next_object_id = MIN_OBJECT_ID;

	m_next_segment_id = MIN_FILE_ID;
	m_next_bucket_meta_fileid = MIN_FILE_ID;
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
	BucketMeta bm;
	s = bucket_meta_file->Read(bm);
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
	ObjectReaderSnapshotPtr reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	std::map<fileid_t, ObjectReaderPtr> new_readers;
    if(reader_snapshot)
    {
	    OpenSegment(bm, reader_snapshot.get(), new_readers);
    }
    else
    {
	    OpenSegment(bm, new_readers);
    }
	
	ObjectReaderSnapshotPtr new_ss_ptr = NewObjectReaderSnapshot(bucket_meta_file, new_readers);
	
	m_segment_rwlock.WriteLock();

	m_conf.max_level_num = bm.max_level_num;
	m_next_bucket_meta_fileid = fileid+1;
	m_next_segment_id = bm.next_segment_id;
	m_next_object_id = bm.next_object_id;
	m_reader_snapshot.swap(new_ss_ptr);

	m_segment_rwlock.WriteUnlock();

	return OK;
}

void Bucket::OpenSegment(const BucketMeta& bm, std::map<fileid_t, ObjectReaderPtr>& readers)
{	
	DBImplPtr db = m_db.lock();
	assert(db);
	
	for(const auto& seg_stat : bm.alive_segment_stats)
	{		
        SegmentReaderPtr sr_ptr = NewSegmentReader();
        if(sr_ptr->Open(m_bucket_path.c_str(), seg_stat) == OK)
        {
            readers[seg_stat.segment_fileid] = sr_ptr;
        }
	}
}

void Bucket::OpenSegment(const BucketMeta& bm, const ObjectReaderSnapshot* last_snapshot, std::map<fileid_t, ObjectReaderPtr>& readers)
{
    assert(last_snapshot != nullptr);
	const std::map<fileid_t, ObjectReaderPtr>& last_readers = last_snapshot->Readers();
	
	DBImplPtr db = m_db.lock();
	assert(db);
	
	for(const auto& seg_stat : bm.alive_segment_stats)
	{		
		auto it = last_readers.find(seg_stat.segment_fileid);
		if(it != last_readers.end())
		{
			readers[seg_stat.segment_fileid] = it->second;
		}
		else
		{
			SegmentReaderPtr sr_ptr = NewSegmentReader();
			if(sr_ptr->Open(m_bucket_path.c_str(), seg_stat) == OK)
			{
				readers[seg_stat.segment_fileid] = sr_ptr;
			}
		}
	}
}

Status Bucket::Backup(const std::string& db_dir)
{
    char bucket_path[MAX_PATH_LEN];
    MakeBucketPath(db_dir.c_str(), m_info.name.c_str(), m_info.id, bucket_path);
    if(!xfutil::Directory::Create(bucket_path))
    {
        return ERR_PATH_CREATE;
    }

    char src_path[MAX_PATH_LEN];
    char dst_path[MAX_PATH_LEN];

	m_segment_rwlock.ReadLock();
	ObjectReaderSnapshotPtr reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

    //先拷贝segment文件，再拷贝meta文件
    if(reader_snapshot)
    {
        const std::map<fileid_t, ObjectReaderPtr>& readers = reader_snapshot->Readers();

        for(auto it = readers.begin(); it != readers.end(); ++it)
        {
	        MakeDataFilePath(m_bucket_path.c_str(), it->first, src_path);
	        MakeDataFilePath(bucket_path, it->first, dst_path);
            File::Copy(src_path, dst_path);

            MakeIndexFilePath(m_bucket_path.c_str(), it->first, src_path);
            MakeIndexFilePath(bucket_path, it->first, dst_path);
            File::Copy(src_path, dst_path);
        }
    }
    fileid_t meta_fileid = reader_snapshot->MetaFile()->FileID();
    MakeBucketMetaFilePath(m_bucket_path.c_str(), meta_fileid, src_path);
    MakeBucketMetaFilePath(bucket_path, meta_fileid, dst_path);
    File::Copy(src_path, dst_path);

    return OK;
}

}  // namespace xfdb



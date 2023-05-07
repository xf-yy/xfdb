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
#include "logger.h"
#include "writeonly_bucket.h"
#include "readwrite_objectwriter.h"
#include "object_writer_snapshot.h"
#include "bucketmeta_file.h"
#include "notify_file.h"
#include "object_reader_snapshot.h"
#include "writable_db.h"

using namespace xfutil;

namespace xfdb 
{

WriteOnlyBucket::WriteOnlyBucket(WritableEngine* engine, DBImplPtr db, const BucketInfo& info) 
	: Bucket(db, info), m_engine(engine), 
	  m_max_memtable_size(engine->GetConfig().max_memtable_size), 
	  m_max_memtable_objects(engine->GetConfig().max_memtable_objects),
      m_merged_reserve_size(engine->GetConfig().merge_factor * engine->GetConfig().part_merge_thread_num)
{	
	m_merged_segment_fileids.reserve(m_merged_reserve_size);
	m_writed_segment_cnt = 0;
	m_tobe_clean_bucket_meta_fileid = INVALID_FILE_ID;
}

WriteOnlyBucket::~WriteOnlyBucket()
{
	assert(!m_memwriter);
	assert(!m_memwriter_snapshot);
}

ObjectWriterPtr WriteOnlyBucket::NewObjectWriter(WritableEngine* engine)
{
	assert(!(engine->GetConfig().mode & MODE_READONLY));
	return NewWriteOnlyObjectWriter(engine->GetLargePool(), m_max_memtable_objects);
}

Status WriteOnlyBucket::Create()
{
	if(!Directory::Create(m_bucket_path.c_str()))
	{
		return ERR_PATH_CREATE;
	}
	
	//写入空的bucket meta文件
	BucketMeta bm;
	fileid_t bucket_meta_fileid;
	{
		std::lock_guard<std::mutex> lock(m_mutex);

		bm.next_segment_id = m_next_segment_id;
		bm.next_object_id = m_next_object_id;
		bm.max_level_num = m_conf.max_level_num;
		bucket_meta_fileid = m_next_bucket_meta_fileid;
	}
	return BucketMetaFile::Write(m_bucket_path.c_str(), bucket_meta_fileid, bm);
}

Status WriteOnlyBucket::Open()
{
	//判断文件夹是否存在
	if(!Directory::Exist(m_bucket_path.c_str()))
	{
		Status s = Create();
		if(s != OK)
		{
			return s;
		}
	}
	//获取所有的meta file
	std::vector<FileName> file_names;
	Status s = ListBucketMetaFile(m_bucket_path.c_str(), file_names);
	if(s != OK)
	{
		return s;
	}
	//至少有一个meta文件
	if(file_names.empty()) 
	{
		return ERR_BUCKET_EMPTY;
	}
	for(size_t i = 0; i < file_names.size() - 1; ++i)
	{
		fileid_t id = atoll(file_names[i].str);
		m_tobe_delete_bucket_meta_fileids.push_back(id);
	}
	const char* curr_filename = file_names.back().str;
	m_tobe_clean_bucket_meta_fileid = atoll(curr_filename);

	s = Bucket::Open(curr_filename);
	if(s != OK)
	{
		return s;
	}

	//将已读的segment加入tobe merge队列
	std::lock_guard<std::mutex> lock(m_mutex);

	m_segment_rwlock.ReadLock();
	ObjectReaderSnapshotPtr reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();	
	
	const auto& readers = reader_snapshot->Readers();
	for(auto it = readers.begin(); it != readers.end(); ++it)
	{
		uint8_t level = GetLevelID(MERGE_COUNT(it->first));
		assert(level <= m_conf.max_level_num);
		m_tobe_merge_segments[level][it->first] = it->second->Size();
	}
	return OK;
}

void WriteOnlyBucket::Clear()
{	
	ObjectWriterPtr memwriter_ptr;
	ObjectWriterSnapshotPtr writer_snapshot;

	//清除内存表	
	m_segment_rwlock.WriteLock();
	memwriter_ptr.swap(m_memwriter);
	writer_snapshot.swap(m_memwriter_snapshot);
	m_segment_rwlock.WriteUnlock();
}

void WriteOnlyBucket::GetStat(BucketStat& stat) const
{
	memset(&stat, 0x00, sizeof(stat));
	
	m_segment_rwlock.ReadLock();
	ObjectWriterPtr memwriter_ptr = m_memwriter;
	ObjectWriterSnapshotPtr writer_snapshot = m_memwriter_snapshot;
	ObjectReaderSnapshotPtr reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	if(memwriter_ptr)
	{
		memwriter_ptr->GetBucketStat(stat);
	}
	if(writer_snapshot)
	{
		writer_snapshot->GetBucketStat(stat);
	}
	if(reader_snapshot)
	{
		reader_snapshot->GetBucketStat(stat);
	}
}

Status WriteOnlyBucket::Write(const Object* object)
{
	WriteLockGuard lock_guard(m_segment_rwlock);
	if(!m_memwriter)
	{			
		m_memwriter = NewObjectWriter(m_engine);
		assert(m_memwriter);

		DBImplPtr db = m_db.lock();
		assert(db);
		m_engine->NotifyTryFlush(db, shared_from_this());
	}
	Status s = m_memwriter->Write(m_next_object_id, object);	//数量+大小
	if(s != OK)
	{
		return s;
	}
	++m_next_object_id;
	
	if(m_memwriter->Size() >= m_max_memtable_size || m_memwriter->GetObjectCount() >= m_max_memtable_objects)
	{
		FlushMemWriter();
	}
	return OK;
}

Status WriteOnlyBucket::Write(const WriteOnlyObjectWriterPtr& memtable)
{
	WriteLockGuard lock_guard(m_segment_rwlock);
	if(!m_memwriter)
	{			
		m_memwriter = NewObjectWriter(m_engine);
		assert(m_memwriter);

		DBImplPtr db = m_db.lock();
		assert(db);
		m_engine->NotifyTryFlush(db, shared_from_this());
	}
	Status s = m_memwriter->Write(m_next_object_id, memtable);	//数量+大小
	if(s != OK)
	{
		return s;
	}
	m_next_object_id += memtable->GetObjectCount();
	
	if(m_memwriter->Size() >= m_max_memtable_size || m_memwriter->GetObjectCount() >= m_max_memtable_objects)
	{
		FlushMemWriter();
	}
	return OK;
}

//OK表示有数据待落盘，NOMORE_DATA表示没有数据
Status WriteOnlyBucket::TryFlush()
{
	WriteLockGuard lock_guard(m_segment_rwlock);
	return Flush(false);
}

Status WriteOnlyBucket::Flush()
{
	WriteLockGuard lock_guard(m_segment_rwlock);
	return Flush(true);
}

Status WriteOnlyBucket::Remove(const char* bucket_path)
{
	std::vector<FileName> file_names;
	Status s = ListBucketMetaFile(bucket_path, file_names);
	if(s != OK)
	{		
		return s;
	}
	for(size_t i = 0; i < file_names.size(); ++i)
	{
		s = BucketMetaFile::Remove(bucket_path, file_names[i].str);
		if(s != OK)
		{
			return s;
		}
	}

	//最后删除目录(包括未删除的segment)
	Directory::Remove(bucket_path);
	return OK;
}

Status WriteOnlyBucket::RemoveMetaFile()
{
	for(;;)
	{		
		fileid_t clean_fileid;
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(m_tobe_delete_bucket_meta_fileids.empty()) 
			{
				return ERR_NOMORE_DATA;
			}
			clean_fileid = m_tobe_delete_bucket_meta_fileids.front();
		}
		char filename[MAX_FILENAME_LEN];
		MakeBucketMetaFileName(clean_fileid, filename);
		Status s = BucketMetaFile::Remove(m_bucket_path.c_str(), filename);
		if(s != OK)
		{
			return s;
		}
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(!m_tobe_delete_bucket_meta_fileids.empty()) 
			{
				m_tobe_delete_bucket_meta_fileids.pop_front();
			}
		}
	}

	return OK;
}

Status WriteOnlyBucket::Clean()
{
	//NOTE:保证一次只有一个线程在执行clean操作
	Status s = RemoveMetaFile();
	if(s == ERR_NOMORE_DATA)
	{
		fileid_t clean_bucket_meta_fileid = INVALID_FILE_ID;
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(m_tobe_delete_bucket_meta_fileids.empty() && m_tobe_clean_bucket_meta_fileid != INVALID_FILE_ID)
			{
				clean_bucket_meta_fileid = m_tobe_clean_bucket_meta_fileid;
				m_tobe_clean_bucket_meta_fileid = INVALID_FILE_ID;
			}
		}
		if(clean_bucket_meta_fileid != INVALID_FILE_ID)
		{
			char filename[MAX_FILENAME_LEN];
			MakeBucketMetaFileName(clean_bucket_meta_fileid, filename);
			s = BucketMetaFile::Clean(m_bucket_path.c_str(), filename);
		} 
	}
	return s;
}

Status WriteOnlyBucket::WriteSegment(ObjectWriterSnapshotPtr& memwriter_snapshot, fileid_t fileid, SegmentReaderPtr& new_segment_reader)
{
	memwriter_snapshot->Finish();
	
	SegmentStat seg_stat;
	seg_stat.segment_fileid = fileid;

	DBImplPtr db = m_db.lock();
	assert(db);
	{
		auto& bucket_conf = db->GetConfig().GetBucketConfig(m_info.name);
		SegmentWriter segment_writer(bucket_conf, m_engine->GetLargePool());
		Status s = segment_writer.Create(m_bucket_path.c_str(), fileid);
		if(s != OK)
		{
			return s;
		}
		s = segment_writer.Write(memwriter_snapshot, seg_stat);
		if(s != OK)
		{
			return s;
		}
	}
	new_segment_reader = NewSegmentReader();
	return new_segment_reader->Open(m_bucket_path.c_str(), seg_stat);
}

//多线程同时执行
Status WriteOnlyBucket::WriteSegment()
{	
	ObjectWriterSnapshotPtr memwriter_snapshot;
	fileid_t fileid;
	
	{
        ObjectReaderSnapshotPtr reader_snapshot;

		std::lock_guard<std::mutex> lock(m_mutex);
		if(m_next_segment_id >= MAX_SEGMENT_ID)
		{
			return ERR_RES_EXHAUST;
		}
		WriteLockGuard lock_guard(m_segment_rwlock);
		if(!m_memwriter_snapshot)
		{
			return OK;
		}
		memwriter_snapshot.swap(m_memwriter_snapshot);

		fileid_t new_segment_id = m_next_segment_id++;
		fileid = SEGMENT_FILEID(new_segment_id, 0);
		m_writing_segments[fileid] = 0;

		std::map<fileid_t, ObjectReaderPtr> new_readers = m_reader_snapshot->Readers();
		assert(new_readers.find(fileid) == new_readers.end());
		new_readers[fileid] = memwriter_snapshot;

		reader_snapshot = NewObjectReaderSnapshot(m_reader_snapshot->MetaFile(), new_readers);
		m_reader_snapshot.swap(reader_snapshot);
	}

	SegmentReaderPtr new_segment_reader;
	Status s = WriteSegment(memwriter_snapshot, fileid, new_segment_reader);
    if(s != OK)
    {
        LogWarn("write segment(id=%ld) of bucket(%s) failed, status: %u", fileid, m_bucket_path.c_str(), s);
        return s;
    }
	
	int writed_segment_inc = 0;
	{
        ObjectReaderSnapshotPtr reader_snapshot;

		std::lock_guard<std::mutex> lock(m_mutex);

		m_writing_segments[fileid] = new_segment_reader->Size();
		
		//判断m_writing_segments已经确认的segment
		for(auto it = m_writing_segments.begin(); it != m_writing_segments.end(); )
		{
			if(it->second == 0)
			{
				break;
			}
			m_tobe_merge_segments[0][it->first] = it->second;
            m_new_segment_fileid.push_back(it->first);
			++m_writed_segment_cnt;
			++writed_segment_inc;

			m_writing_segments.erase(it++);
		}
		
		WriteLockGuard lock_guard(m_segment_rwlock);
		std::map<fileid_t, ObjectReaderPtr> new_readers = m_reader_snapshot->Readers();
		assert(new_readers.find(fileid) != new_readers.end());		
		new_readers[fileid] = new_segment_reader;

		reader_snapshot = NewObjectReaderSnapshot(m_reader_snapshot->MetaFile(), new_readers);
		m_reader_snapshot.swap(reader_snapshot);
	}
	
	//判断是否有可写的段信息
	if(writed_segment_inc != 0)
	{
		DBImplPtr db = m_db.lock();
		assert(db);
		
		m_engine->NotifyWriteBucketMeta(db, shared_from_this());
		//FIXME:判断是否需要merge
		m_engine->NotifyPartMerge(db, shared_from_this());
	}
	return OK;
}

///////////////////////////////////////////////////////////////////////////////

Status WriteOnlyBucket::Merge()
{
	DBImplPtr db = m_db.lock();
	m_engine->NotifyFullMerge(db, shared_from_this());
	return OK;
}

Status WriteOnlyBucket::Merge(MergingSegmentInfo& msinfo)
{		
	if(msinfo.merging_segment_fileids.size() <= 1)
	{
		return OK;
	}

	DBImplPtr db = m_db.lock();
	assert(db);

	SegmentStat seg_stat;
	seg_stat.segment_fileid = msinfo.new_segment_fileid;
	{
		BucketConfig tmp_bucket_conf = m_conf;
		//超过一定大小的段不用布隆
		if(msinfo.GetMergingSize() >= m_engine->GetConfig().max_merge_size)
		{
			tmp_bucket_conf.bloom_filter_bitnum = 0;
		}

		SegmentWriter segment_writer(tmp_bucket_conf, m_engine->GetLargePool());
		Status s = segment_writer.Create(m_bucket_path.c_str(), msinfo.new_segment_fileid);
		if(s != OK)
		{
            LogWarn("open create segment(id=%ld) of bucket(%s) failed, status: %u", msinfo.new_segment_fileid, m_bucket_path.c_str(), s);
			return s;
		}
		
		s = segment_writer.Write(msinfo, seg_stat);
		if(s != OK)
		{
            LogWarn("open write segment(id=%ld) of bucket(%s) failed, status: %u", msinfo.new_segment_fileid, m_bucket_path.c_str(), s);
			return s;
		}
	}

	msinfo.new_segment_reader = NewSegmentReader();
	Status s = msinfo.new_segment_reader->Open(m_bucket_path.c_str(), seg_stat);
	if(s != OK)
	{
        LogWarn("open new segment(id=%ld) of bucket(%s) failed, status: %u", seg_stat.segment_fileid, m_bucket_path.c_str(), s);
		return s;
	}

	{
		std::lock_guard<std::mutex> lock(m_mutex);

		for(auto it = msinfo.merging_segment_fileids.begin(); it != msinfo.merging_segment_fileids.end(); ++it)
		{
			m_merged_segment_fileids.push_back(*it);
		}
		uint8_t level = GetLevelID(MERGE_COUNT(msinfo.new_segment_fileid));
		assert(level <= m_conf.max_level_num);

		m_merging_segment_fileids[level][msinfo.new_segment_fileid] = msinfo.new_segment_reader->Size();
		for(auto it = m_merging_segment_fileids[level].begin(); it != m_merging_segment_fileids[level].end();)
		{
			if(it->second == 0)
			{
				break;
			}
			m_tobe_merge_segments[level][it->first] = it->second;;
			m_merging_segment_fileids[level].erase(it++);
		}

		m_segment_rwlock.ReadLock();
		ObjectReaderSnapshotPtr reader_snapshot = m_reader_snapshot;
		m_segment_rwlock.ReadUnlock();

		std::map<fileid_t, ObjectReaderPtr> new_readers = reader_snapshot->Readers();
		for(auto it = msinfo.merging_segment_fileids.begin(); it != msinfo.merging_segment_fileids.end(); ++it)
		{
			assert(new_readers.find(*it) != new_readers.end());
			new_readers.erase(*it);
		}
		assert(new_readers.find(msinfo.new_segment_fileid) == new_readers.end());
		new_readers[msinfo.new_segment_fileid] = msinfo.new_segment_reader;

		m_segment_rwlock.WriteLock();
		ObjectReaderSnapshotPtr new_reader_snapshot = NewObjectReaderSnapshot(reader_snapshot->MetaFile(), new_readers);
		m_reader_snapshot.swap(new_reader_snapshot);
		m_segment_rwlock.WriteUnlock();
	}
	
	m_engine->NotifyWriteBucketMeta(db, shared_from_this());
	//FIXME:判断是否需要merge
	m_engine->NotifyPartMerge(db, shared_from_this());

	return OK;
}

bool WriteOnlyBucket::AddMerging(MergingSegmentInfo& msinfo)
{
	if(msinfo.merging_segment_fileids.size() <= 1)
	{
		return false;
	}

	m_segment_rwlock.ReadLock();
	msinfo.reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	msinfo.new_segment_fileid = msinfo.NewSegmentFileID();
	
	uint8_t new_level = GetLevelID(MERGE_COUNT(msinfo.new_segment_fileid));
	assert(new_level <= m_conf.max_level_num);
	assert(m_merging_segment_fileids[new_level].find(msinfo.new_segment_fileid) == m_merging_segment_fileids[new_level].end());
	m_merging_segment_fileids[new_level][msinfo.new_segment_fileid] = 0;
	return true;
}

//单线程执行
Status WriteOnlyBucket::FullMerge()
{
	std::vector<MergingSegmentInfo> msinfos;
	{		
		std::lock_guard<std::mutex> lock(m_mutex);

		//注：如果当前有合并操作则返回错误
		for(uint8_t level = 0; level <= m_conf.max_level_num; ++level)
		{
			if(!m_merging_segment_fileids[level].empty())
			{
				return ERR_IN_PROCESSING;
			}
		}

		//搜集所有的m_tobe_merge_segments
		std::map<fileid_t, uint64_t> total_tobe_merge_segments;
		for(uint8_t level = 0; level <= m_conf.max_level_num; ++level)
		{
			total_tobe_merge_segments.insert(m_tobe_merge_segments[level].begin(), m_tobe_merge_segments[level].end());
		}

		MergingSegmentInfo msinfo;	
		for(auto it = total_tobe_merge_segments.begin(); it != total_tobe_merge_segments.end(); ++it)
		{
			if(it->second < m_engine->GetConfig().max_merge_size && MERGE_COUNT(it->first) < MAX_MERGE_COUNT)
			{
				msinfo.merging_segment_fileids.insert(it->first);

				uint8_t level = GetLevelID(MERGE_COUNT(it->first));
				assert(level <= m_conf.max_level_num);
				m_tobe_merge_segments[level].erase(it->first);
			}
			else
			{
				if(AddMerging(msinfo))
				{
					msinfos.push_back(msinfo);
				}
				msinfo.merging_segment_fileids.clear();
			}
		}
		if(AddMerging(msinfo))
		{
			msinfos.push_back(msinfo);
		}
	}
	
	for(auto& msinfo : msinfos)
	{
		Status s = Merge(msinfo);
		if(s != OK)
		{
			return s;
		}
	}

	return OK;
}

//多线程执行
Status WriteOnlyBucket::PartMerge()
{	
	const uint32_t merge_factor = m_engine->GetConfig().merge_factor;

	//每层都尝试合并一下
	for(uint8_t level = 0; level <= m_conf.max_level_num; ++level)
	{
		for(;;)
		{
			MergingSegmentInfo msinfo;
			{
				std::lock_guard<std::mutex> lock(m_mutex);
				if(m_tobe_merge_segments[level].size() < merge_factor)
				{
					break;
				}

				auto it = m_tobe_merge_segments[level].begin();
				for(uint32_t m = 0; m < merge_factor; ++m)
				{
					//如果segment大小超过阈值或level已达最大值，则不参与合并
					if(it->second >= m_engine->GetConfig().max_merge_size || GetLevelID(MERGE_COUNT(it->first)) >= m_conf.max_level_num)
					{
						break;
					}
					msinfo.merging_segment_fileids.insert(it->first);
					it = m_tobe_merge_segments[level].erase(it);
				}
				AddMerging(msinfo);
			}

			Status s = Merge(msinfo);
			if(s != OK)
			{
				return s;
			}
		}
	}
	return OK;
}

void WriteOnlyBucket::GetAliveSegmentStat(ObjectReaderSnapshotPtr& ors_ptr, BucketMeta& bm)
{
	const std::map<fileid_t, ObjectReaderPtr>& readers = ors_ptr->Readers();
	assert(!readers.empty());

	bm.alive_segment_stats.reserve(readers.size());
	for(auto it = readers.begin(); it != readers.end(); ++it)
	{
		SegmentReaderPtr seg_reader = std::dynamic_pointer_cast<SegmentReader>(it->second);
		if(!seg_reader)
		{
			break;
		}
		bm.alive_segment_stats.push_back(seg_reader->Stat());
	}
}

//保证只被1个线程调用
Status WriteOnlyBucket::WriteBucketMeta()
{
	ObjectReaderSnapshotPtr reader_snapshot;
	fileid_t bucket_meta_fileid;
	BucketMeta bm;
	
	//将segment snapshot和deleted segmentid合并写入segment list
	//同时只读打开segment list，然后更新它们
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		if(m_writed_segment_cnt == 0 && m_merged_segment_fileids.empty())
		{
			return ERR_NOMORE_DATA;
		}
		bucket_meta_fileid = m_next_bucket_meta_fileid++;

		m_writed_segment_cnt = 0;
		if(!m_merged_segment_fileids.empty())
		{
			bm.merged_segment_fileids.swap(m_merged_segment_fileids);
			m_merged_segment_fileids.reserve(m_merged_reserve_size);
		}
        if(!m_new_segment_fileid.empty())
        {
            bm.new_segment_fileids.swap(m_new_segment_fileid);
        }

		bm.next_segment_id = m_next_segment_id;
		bm.next_object_id = m_next_object_id;
		bm.max_level_num = m_conf.max_level_num;
		
		ReadLockGuard lock_guard(m_segment_rwlock);
		reader_snapshot = m_reader_snapshot;
	}
	
	GetAliveSegmentStat(reader_snapshot, bm);
	Status s = BucketMetaFile::Write(m_bucket_path.c_str(), bucket_meta_fileid, bm);
	if(s != OK)
	{
		//FIXME:恢复md.deleted_segment_fileids到m_merged_segment_fileids?
        LogWarn("write meta(id=%ld) of bucket(%s) failed, status: %u", bucket_meta_fileid, m_bucket_path.c_str(), s);
		return s;
	}
	//只读打开segment list file，并替换当前list file
	BucketMetaFilePtr bucket_meta_file = NewBucketMetaFile();
	bucket_meta_file->Open(m_bucket_path.c_str(), bucket_meta_fileid, xfutil::LF_TRY_READ);
	{
        ObjectReaderSnapshotPtr reader_snapshot;
		WriteLockGuard lock_guard(m_segment_rwlock);
        
        reader_snapshot = NewObjectReaderSnapshot(bucket_meta_file, m_reader_snapshot->Readers());
		m_reader_snapshot.swap(reader_snapshot);
	}

	DBImplPtr db = m_db.lock();
	assert(db);

	//加入清理队列
	if(bucket_meta_file)
	{
		{
			std::lock_guard<std::mutex> lock(m_mutex);
			m_tobe_clean_bucket_meta_fileid = bucket_meta_fileid;
			m_tobe_delete_bucket_meta_fileids.push_back(bucket_meta_file->FileID());
		}
		m_engine->NotifyClean(db);
	}	

	//写通知文件
	NotifyData nd(NOTIFY_UPDATE_BUCKET_META, db->GetPath(), m_info.name, bucket_meta_fileid);
	m_engine->WriteNotifyFile(nd);
	
	return OK;
}

void WriteOnlyBucket::FlushMemWriter()
{
	assert(m_memwriter);
	ObjectWriterSnapshotPtr new_snapshot = NewObjectWriterSnapshot(m_memwriter, m_memwriter_snapshot.get());
	
	m_memwriter.reset();
	m_memwriter_snapshot = new_snapshot;
	
	DBImplPtr db = m_db.lock();
	assert(db);
	m_engine->NotifyWriteSegment(db, shared_from_this());
}

Status WriteOnlyBucket::Flush(bool force)
{		
	//已经获取锁了
	if(!m_memwriter)
	{
		return OK;
	}
	if(force || m_memwriter->ElapsedTime() >= m_engine->GetConfig().flush_interval_s)
	{
		FlushMemWriter();
	}
	return OK;
}
	

}  



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
#include "writeonly_bucket.h"
#include "readwrite_mem_writer.h"
#include "table_writer_snapshot.h"
#include "bucket_meta_file.h"
#include "notify_file.h"
#include "table_reader_snapshot.h"
#include "write_db.h"

using namespace xfutil;

namespace xfdb 
{

WriteOnlyBucket::WriteOnlyBucket(WritableEngine* engine, DBImplPtr db, const BucketInfo& info) 
	: Bucket(db, info), m_engine(engine)
{	
	m_merged_segment_fileids.reserve(engine->GetConfig().merge_factor * engine->GetConfig().part_merge_thread_num);
}

WriteOnlyBucket::~WriteOnlyBucket()
{
	assert(!m_memwriter);
	assert(!m_memwriter_snapshot);
}

TableWriterPtr WriteOnlyBucket::NewTableWriter(WritableEngine* engine)
{
	assert(!(engine->m_conf.mode & MODE_READONLY));
	return NewWriteOnlyMemWriter(engine->m_pool, engine->m_conf.max_object_num_of_memtable);
}

Status WriteOnlyBucket::Create()
{
	if(!Directory::Create(m_bucket_path.c_str()))
	{
		return ERR_PATH_CREATE;
	}
	
	//写入空的bucket meta文件
	BucketMetaData md;
	fileid_t bucket_meta_fileid;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		md.next_segment_id = m_next_segment_id;
		md.next_object_id = m_next_object_id;
		md.max_level_num = m_max_level_num;
		bucket_meta_fileid = m_next_bucket_meta_fileid;
	}
	return BucketMetaFile::Write(m_bucket_path.c_str(), bucket_meta_fileid, md);
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
	//读取最新的segment list
	std::vector<FileName> file_names;
	Status s = ListBucketMetaFile(m_bucket_path.c_str(), file_names);
	if(s != OK)
	{
		return s;
	}
	if(file_names.empty()) 
	{
		return OK;
	}
	for(size_t i = 0; i < file_names.size() - 1; ++i)
	{
		m_tobe_delete_bucket_meta_filenames.push_back(file_names[i]);
	}
	
	return Bucket::Open(file_names.back().str);
}

void WriteOnlyBucket::Clear()
{	
	TableWriterPtr memwriter_ptr;
	TableWriterSnapshotPtr mts_ptr;

	//清除内存表	
	m_segment_rwlock.WriteLock();
	memwriter_ptr.swap(m_memwriter);
	mts_ptr.swap(m_memwriter_snapshot);
	m_segment_rwlock.WriteUnlock();
}

void WriteOnlyBucket::GetStat(BucketStat& stat) const
{
	memset(&stat, 0x00, sizeof(stat));
	
	m_segment_rwlock.ReadLock();
	TableWriterPtr memwriter_ptr = m_memwriter;
	TableWriterSnapshotPtr mts_ptr = m_memwriter_snapshot;
	BucketReaderSnapshot reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	if(memwriter_ptr)
	{
		memwriter_ptr->GetStat(stat);
	}
	if(mts_ptr)
	{
		mts_ptr->GetStat(stat);
	}
	if(reader_snapshot.readers)
	{
		reader_snapshot.readers->GetStat(stat);
	}
}

Status WriteOnlyBucket::Write(const Object* object)
{
	WriteLockGuard lock_guard(m_segment_rwlock);
	if(!m_memwriter)
	{			
		m_memwriter = NewTableWriter(m_engine);
		assert(m_memwriter);
		m_engine->NotifyTryFlush(m_db.lock(), shared_from_this());
	}
	Status s = m_memwriter->Write(m_next_object_id, object);	//数量+大小
	if(s != OK)
	{
		return s;
	}
	++m_next_object_id;
	
	if(m_memwriter->Size() >= m_engine->GetConfig().max_memtable_size || m_memwriter->GetObjectCount() >= m_engine->GetConfig().max_object_num_of_memtable)
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
		assert(s != ERR_FILE_READ);
		return s;
	}
	for(size_t i = 0; i < file_names.size(); ++i)
	{		
		const FileName& fn = file_names[i];

		bool remove_all = (i == file_names.size() - 1);
		s = BucketMetaFile::Remove(bucket_path, fn.str, remove_all);
		if(s != OK)
		{
			assert(s != ERR_FILE_READ);
			return s;
		}
	}

	//最后删除目录
	Directory::Remove(bucket_path);
	return OK;
}

Status WriteOnlyBucket::Clean()
{
	//保证一次只有一个线程在执行clean操作
	FileName clean_filename;
	for(;;)
	{		
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(m_tobe_delete_bucket_meta_filenames.empty()) 
			{
				return ERR_NOMORE_DATA;
			}
			clean_filename = m_tobe_delete_bucket_meta_filenames.front();
		}
		Status s = BucketMetaFile::Remove(m_bucket_path.c_str(), clean_filename.str, false);
		if(s != OK)
		{
			return s;
		}
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(!m_tobe_delete_bucket_meta_filenames.empty()) 
			{
				m_tobe_delete_bucket_meta_filenames.pop_front();
			}
		}
	}

	return OK;
}

Status WriteOnlyBucket::WriteSegment(TableWriterSnapshotPtr& memwriter_snapshot, fileid_t fileid, SegmentReaderPtr& new_segment_reader)
{
	memwriter_snapshot->Sort();
	
	SegmentIndexInfo seginfo;
	seginfo.segment_fileid = fileid;

	{
		DBImplPtr db = m_db.lock();
		assert(db);
		
		SegmentWriter segment_writer(db->GetConfig(), m_engine->m_pool);
		Status s = segment_writer.Create(m_bucket_path.c_str(), fileid);
		if(s != OK)
		{
			return s;
		}
		s = segment_writer.Write(memwriter_snapshot, seginfo);
		if(s != OK)
		{
			return s;
		}
	}

	return OpenSegment(m_bucket_path.c_str(), seginfo, new_segment_reader);
}

//多线程同时执行
Status WriteOnlyBucket::WriteSegment()
{	
	TableWriterSnapshotPtr memwriter_snapshot;
	fileid_t fileid;
	
	{
		std::lock_guard<std::mutex> lock(m_mutex);

		WriteLockGuard lock_guard(m_segment_rwlock);
		if(!m_memwriter_snapshot)
		{
			return ERR_NOMORE_DATA;
		}
		
		memwriter_snapshot.swap(m_memwriter_snapshot);

		fileid_t new_segment_id = m_next_segment_id++;
		fileid = SEGMENT_FILEID(new_segment_id, 0);
		m_writing_segment_fileids[fileid] = false;

		std::map<fileid_t, TableReaderPtr> new_readers = m_reader_snapshot.readers->Readers();
		new_readers[fileid] = memwriter_snapshot;

		TableReaderSnapshotPtr reader_snapshot = NewTableReaderSnapshot(new_readers);
		m_reader_snapshot.readers.swap(reader_snapshot);
	}

	SegmentReaderPtr new_segment_reader;
	WriteSegment(memwriter_snapshot, fileid, new_segment_reader);
	
	int writed_segment_inc = 0;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		m_writing_segment_fileids[fileid] = true;
		
		//判断m_writing_segments已经确认的segment
		for(auto it = m_writing_segment_fileids.begin(); it != m_writing_segment_fileids.end(); )
		{
			if(!it->second)
			{
				break;
			}
			m_tobe_merge_segments[0].insert(it->first);
			m_writing_segment_fileids.erase(it++);
			++m_writed_segment_cnt;
			++writed_segment_inc;
		}
		
		WriteLockGuard lock_guard(m_segment_rwlock);
		std::map<fileid_t, TableReaderPtr> new_readers = m_reader_snapshot.readers->Readers();
		new_readers[fileid] = new_segment_reader;

		TableReaderSnapshotPtr reader_snapshot = NewTableReaderSnapshot(new_readers);
		m_reader_snapshot.readers.swap(reader_snapshot);
	}
	
	//判断是否有可写的段信息
	if(writed_segment_inc != 0)
	{
		DBImplPtr db = m_db.lock();
		
		m_engine->NotifyWriteBucketMeta(db, shared_from_this());
		//FIXME:判断是否需要merge
		m_engine->NotifyPartMerge(db, shared_from_this());
	}
	return OK;
}

///////////////////////////////////////////////////////////////////////////////
fileid_t WriteOnlyBucket::SelectNewSegmentFileID(MergingSegmentInfo& msinfo)
{
	//算法：选用最小的seqid，将其level+1，如果level+1已是最大level，则从begin->end中选个未用的seqid
	//     如果都用了，则选用未使用的最小segment id
	assert(msinfo.merging_segment_fileids.size() > 1);
	auto it = msinfo.merging_segment_fileids.begin();
	if(LEVEL_NUM(*it) < MAX_LEVEL_NUM)
	{
		fileid_t segment_id = SEGMENT_ID(*it);
		int new_level = LEVEL_NUM(*it) + 1;

		return SEGMENT_FILEID(segment_id, new_level);
	}
	else
	{
		auto prev_it = it;
		for(++it; it != msinfo.merging_segment_fileids.end(); ++it)
		{
			fileid_t prev_segment_id = SEGMENT_ID(*prev_it);
			fileid_t segment_id = SEGMENT_ID(*it);

			if(prev_segment_id+1 < segment_id)
			{
				//FIXME: 曾经出现过怎么办？需记录并check一下？
				//将最高level的所有segment id记录下来，万一中途更改MAX_LEVEL_NUM值，怎么办？每个db记录最高level，不能改变
				int level = LEVEL_NUM(*it);
				return SEGMENT_FILEID(prev_segment_id+1, level);
			}

			prev_it = it;
		}
	}
	assert(false);
	return INVALID_FILEID;
}

Status WriteOnlyBucket::Merge()
{
	m_engine->NotifyFullMerge(m_db.lock(), shared_from_this());
	return OK;
}

Status WriteOnlyBucket::Merge(MergingSegmentInfo& msinfo)
{		
	if(msinfo.merging_segment_fileids.size() <= 1)
	{
		return OK;
	}

	DBImplPtr db = m_db.lock();

	SegmentIndexInfo seginfo;
	seginfo.segment_fileid = msinfo.new_segment_fileid;
	{
		SegmentWriter segment_writer(db->GetConfig(), m_engine->m_pool);
		Status s = segment_writer.Create(m_bucket_path.c_str(), msinfo.new_segment_fileid);
		if(s != OK)
		{
			return s;
		}
		
		s = segment_writer.Merge(msinfo, seginfo);
		if(s != OK)
		{
			return s;
		}
	}

	Status s = OpenSegment(m_bucket_path.c_str(), seginfo, msinfo.new_segment_reader);
	if(s != OK)
	{
		return s;
	}

	{
		std::lock_guard<std::mutex> lock(m_mutex);

		for(auto it = msinfo.merging_segment_fileids.begin(); it != msinfo.merging_segment_fileids.end(); ++it)
		{
			m_merged_segment_fileids.push_back(*it);
		}
		int level = LEVEL_NUM(msinfo.new_segment_fileid);
		m_merging_segment_fileids[level][msinfo.new_segment_fileid] = true;
		for(auto it = m_merging_segment_fileids[level].begin(); it != m_merging_segment_fileids[level].end();)
		{
			if(!it->second)
			{
				break;
			}
			m_tobe_merge_segments[level].insert(it->first);
			m_merging_segment_fileids[level].erase(it++);
		}

		m_segment_rwlock.ReadLock();
		BucketReaderSnapshot reader_snapshot = m_reader_snapshot;
		m_segment_rwlock.ReadUnlock();

		std::map<fileid_t, TableReaderPtr> new_readers = reader_snapshot.readers->Readers();
		for(auto it = msinfo.merging_segment_fileids.begin(); it != msinfo.merging_segment_fileids.end(); ++it)
		{
			assert(new_readers.find(*it) != new_readers.end());
			new_readers.erase(*it);
		}
		assert(new_readers.find(msinfo.new_segment_fileid) == new_readers.end());
		new_readers[msinfo.new_segment_fileid] = msinfo.new_segment_reader;

		m_segment_rwlock.WriteLock();
		TableReaderSnapshotPtr new_reader_snapshot = NewTableReaderSnapshot(new_readers);
		m_reader_snapshot.readers.swap(new_reader_snapshot);
		m_segment_rwlock.WriteUnlock();
	}
	
	m_engine->NotifyWriteBucketMeta(db, shared_from_this());
	//FIXME:判断是否需要merge
	m_engine->NotifyPartMerge(db, shared_from_this());

	return OK;
}

//单线程：超过一定大小的block不参与合并，低于1/10的小块不与大块合并
Status WriteOnlyBucket::FullMerge()
{	
	assert(false);
	return ERR_INVALID_MODE;

	std::vector<MergingSegmentInfo> msinfo;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		
		m_segment_rwlock.ReadLock();
		BucketReaderSnapshot reader_snapshot = m_reader_snapshot;
		m_segment_rwlock.ReadUnlock();

		//TODO:以超过阈值的segment为分割点，找到待合并的segment集
	}
	
	for(auto& seg_info : msinfo)
	{
		Status s = Merge(seg_info);
		if(s != OK)
		{
			return s;
		}
	}
	return OK;
}

//多线程
Status WriteOnlyBucket::PartMerge()
{	
	const uint32_t merge_factor = m_engine->GetConfig().merge_factor;

	//每层都尝试合并一下
	for(int i = 0; i < MAX_LEVEL_NUM; ++i)
	{
		for(;;)
		{
			MergingSegmentInfo msinfo;

			//TODO:从低层开始，按id升序找出待合并的segment集
			//相差很大的segment不要合并？比如10GB，100MB，即不超过10倍
			{
				std::lock_guard<std::mutex> lock(m_mutex);
				if(m_tobe_merge_segments[i].size() < merge_factor)
				{
					break;
				}

				//printf("level:%d, segment cnt:%zd, merge factor:%u\n", i, m_tobe_merge_segments[i].size(), merge_factor);
				//取前部分segment
				auto it = m_tobe_merge_segments[i].begin();
				for(uint32_t m = 0; m < merge_factor; ++m)
				{
					msinfo.merging_segment_fileids.insert(*it);
					it = m_tobe_merge_segments[i].erase(it);
				}

				msinfo.new_segment_fileid = SelectNewSegmentFileID(msinfo);

				m_merging_segment_fileids[LEVEL_NUM(msinfo.new_segment_fileid)][msinfo.new_segment_fileid] = false;

			}
			m_segment_rwlock.ReadLock();
			msinfo.reader_snapshot = m_reader_snapshot;
			m_segment_rwlock.ReadUnlock();

			Status s = Merge(msinfo);
			if(s != OK)
			{
				return s;
			}
		}
	}
	return OK;
}

void WriteOnlyBucket::WriteAliveSegmentInfos(TableReaderSnapshotPtr& trs_ptr, BucketMetaData& md)
{
	const std::map<fileid_t, TableReaderPtr>& readers = trs_ptr->Readers();
	assert(!readers.empty());

	md.alive_segment_infos.reserve(readers.size());
	for(auto it = readers.begin(); it != readers.end(); ++it)
	{
		SegmentReaderPtr seg_reader = std::dynamic_pointer_cast<SegmentReader>(it->second);
		if(!seg_reader)
		{
			break;
		}
		md.alive_segment_infos.push_back(seg_reader->IndexInfo());
	}
}

//保证只被1个线程调用
Status WriteOnlyBucket::WriteBucketMeta()
{
	BucketReaderSnapshot reader_snapshot;
	fileid_t bucket_meta_fileid;
	BucketMetaData md;
	
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
			md.deleted_segment_fileids.swap(m_merged_segment_fileids);
			m_merged_segment_fileids.reserve(m_engine->GetConfig().merge_factor * m_engine->GetConfig().part_merge_thread_num);
		}

		md.next_segment_id = m_next_segment_id;
		md.next_object_id = m_next_object_id;
		md.max_level_num = m_max_level_num;
		
		ReadLockGuard lock_guard(m_segment_rwlock);
		reader_snapshot = m_reader_snapshot;
	}
	
	WriteAliveSegmentInfos(reader_snapshot.readers, md);
	Status s = BucketMetaFile::Write(m_bucket_path.c_str(), bucket_meta_fileid, md);
	if(s != OK)
	{
		return s;
	}
	//只读打开segment list file，并替换当前list file
	BucketMetaFilePtr bucket_meta_file = NewBucketMetaFile();
	bucket_meta_file->Open(m_bucket_path.c_str(), bucket_meta_fileid, xfutil::LOCK_TRY_READ);
	{
		WriteLockGuard lock_guard(m_segment_rwlock);
		m_reader_snapshot.meta_file.swap(bucket_meta_file);
	}
	//写通知文件
	DBImplPtr db = m_db.lock();
	NotifyData nd(NOTIFY_UPDATE_BUCKET_META, db->GetPath(), m_info.name, bucket_meta_fileid);
	m_engine->WriteNotifyFile(nd);

	//加入清理队列
	if(bucket_meta_file)
	{
		FileName filename;
		MakeBucketMetaFileName(bucket_meta_file->FileID(), filename.str);

		{
			std::lock_guard<std::mutex> lock(m_mutex);
			m_tobe_delete_bucket_meta_filenames.push_back(filename);
		}
		m_engine->NotifyClean(db);
	}
	
	return OK;
}

void WriteOnlyBucket::FlushMemWriter()
{
	assert(m_memwriter);
	TableWriterSnapshotPtr new_snapshot = NewTableWriterSnapshot(m_memwriter, m_memwriter_snapshot.get());
	
	m_memwriter.reset();
	m_memwriter_snapshot = new_snapshot;
	
	m_engine->NotifyWriteSegment(m_db.lock(), shared_from_this());
}

Status WriteOnlyBucket::Flush(bool force)
{		
	//已经获取锁了
	if(!m_memwriter)
	{
		return OK;
	}
	if(force || m_memwriter->ElapsedTime() >= m_engine->GetConfig().memtable_ttl_s)
	{
		FlushMemWriter();
	}
	return OK;
}
	

}  



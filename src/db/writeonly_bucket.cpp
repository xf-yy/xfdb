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
#include "segment_list_file.h"
#include "notify_file.h"
#include "table_reader_snapshot.h"
#include "write_db.h"

using namespace xfutil;

namespace xfdb 
{

WriteOnlyBucket::WriteOnlyBucket(WritableEngine* engine, DBImplPtr db, const BucketInfo& info) 
	: Bucket(db, info), m_engine(engine)
{	
	m_next_objectid = MIN_OBJECTID;
}

WriteOnlyBucket::~WriteOnlyBucket()
{
	assert(!m_memwriter);
	assert(!m_memwriter_snapshot);
}

TableWriterPtr WriteOnlyBucket::NewTableWriter(WritableEngine* engine)
{
	assert(!(engine->m_conf.mode & MODE_READONLY));
	return NewWriteOnlyMemWriter(engine->m_pool, engine->m_conf.max_memwriter_object_num);
}

Status WriteOnlyBucket::Create()
{
	if(!Directory::Create(m_bucket_path.c_str()))
	{
		return ERR_PATH_CREATE;
	}
	
	//写入空的segmentlist文件
	SegmentListData md;
	fileid_t segment_list_fileid;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		md.next_segment_id = m_next_segment_id;
		segment_list_fileid = m_next_segmentlist_fileid;
	}
	return SegmentListFile::Write(m_bucket_path.c_str(), segment_list_fileid, md);
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
	Status s = ListSegmentListFile(m_bucket_path.c_str(), file_names);
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
		m_deleting_segmentlist_names.push_back(file_names[i]);
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
	TableReaderSnapshotPtr trs_ptr = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	if(memwriter_ptr)
	{
		++stat.memwriter_stat.count;
		stat.memwriter_stat.size += memwriter_ptr->Size();

		stat.object_stat.Add(memwriter_ptr->GetObjectStat());
	}
	if(mts_ptr)
	{
		const std::vector<TableWriterPtr>& writers = mts_ptr->TableWriters();
		
		stat.memwriter_stat.count += writers.size();
		for(auto writer : writers)
		{
			stat.memwriter_stat.size += writer->Size();
			stat.object_stat.Add(writer->GetObjectStat());
		}
	}
	if(trs_ptr)
	{
		trs_ptr->GetBucketStat(stat);
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
	Status s = m_memwriter->Write(m_next_objectid, object);	//数量+大小
	if(s != OK)
	{
		return s;
	}
	++m_next_objectid;
	
	if(m_memwriter->Size() >= m_engine->GetConfig().max_memwriter_size || m_memwriter->GetObjectStat().Count() >= m_engine->GetConfig().max_memwriter_object_num)
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
	Status s = ListSegmentListFile(bucket_path, file_names);
	if(s != OK)
	{		
		assert(s != ERR_FILE_READ);
		return s;
	}
	for(size_t i = 0; i < file_names.size(); ++i)
	{		
		const FileName& fn = file_names[i];

		bool remove_all = (i == file_names.size() - 1);
		s = SegmentListFile::Remove(bucket_path, fn.str, remove_all);
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
			if(m_deleting_segmentlist_names.empty()) 
			{
				return ERR_NOMORE_DATA;
			}
			clean_filename = m_deleting_segmentlist_names.front();
		}
		Status s = SegmentListFile::Remove(m_bucket_path.c_str(), clean_filename.str, false);
		if(s != OK)
		{
			return s;
		}
		{
			std::lock_guard<std::mutex> guard(m_mutex);
			if(!m_deleting_segmentlist_names.empty()) 
			{
				m_deleting_segmentlist_names.pop_front();
			}
		}
	}

	return OK;
}

Status WriteOnlyBucket::WriteSegment(TableReaderPtr& reader, fileid_t new_segment_fileid, SegmentFileIndex& seginfo)
{
	TableWriterPtr table_writer = std::dynamic_pointer_cast<TableWriter>(reader);
	table_writer->Sort();

	DBImplPtr db = m_db.lock();
	
	SegmentWriter segment_writer(db->GetConfig(), m_engine->m_pool);
	Status s = segment_writer.Create(m_bucket_path.c_str(), new_segment_fileid);
	if(s != OK)
	{
		return s;
	}
	return segment_writer.Write(table_writer, seginfo);
}

Status WriteOnlyBucket::WriteSegment(std::map<fileid_t, TableReaderPtr>& readers)
{
	//FIXME: 暂时一个table生成一个segment
	for(auto it = readers.begin(); it != readers.end(); ++it)
	{
		SegmentFileIndex seginfo;
		seginfo.segment_fileid = it->first;

		Status s = WriteSegment(it->second, it->first, seginfo);
		if(s != OK)
		{
			continue;
		}
		SegmentReaderPtr new_segment_reader;
		s = OpenSegment(m_bucket_path.c_str(), seginfo, new_segment_reader);
		if(s == OK)
		{
			it->second = new_segment_reader;
		}
		else
		{
			assert(false);
		}
	}
	return OK;
}

//多线程同时执行
Status WriteOnlyBucket::WriteSegment()
{	
	std::map<fileid_t, TableReaderPtr> readers;
	TableWriterSnapshotPtr table_writer_snapshot;
	
	{
		std::lock_guard<std::mutex> lock(m_mutex);

		{
			WriteLockGuard lock_guard(m_segment_rwlock);
			
			if(!m_memwriter_snapshot)
			{
				return ERR_NOMORE_DATA;
			}
			table_writer_snapshot.swap(m_memwriter_snapshot);
		}

		const std::vector<TableWriterPtr>& memwriters = table_writer_snapshot->TableWriters();
		for(size_t i = 0; i < memwriters.size(); ++i)
		{
			fileid_t fileid = (m_next_segment_id++ << LEVEL_BITNUM);
			assert(memwriters[i]);
			
			readers[fileid] = memwriters[i];
			m_writing_segment_fileids[fileid] = false;
		}

		UpdateReaderSnapshot(readers);
	}
	WriteSegment(readers);
	
	bool need_write_segment_list;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		for(auto it = readers.begin(); it != readers.end(); ++it)
		{
			m_writing_segment_fileids[it->first] = true;
		}
		//判断m_writing_segments已经确认的segment
		for(auto it = m_writing_segment_fileids.begin(); it != m_writing_segment_fileids.end(); )
		{
			if(!it->second)
			{
				break;
			}
			m_writed_segment_fileids.push_back(it->first);
			m_writing_segment_fileids.erase(it++);
		}
		need_write_segment_list = !m_writed_segment_fileids.empty();
		UpdateReaderSnapshot(readers);
	}
	
	//判断是否有可写的段信息
	if(need_write_segment_list)
	{
		m_engine->NotifyWriteSegmentList(m_db.lock(), shared_from_this());
	}
	//TODO: 判断是否需要merge
	return OK;
}

void WriteOnlyBucket::UpdateReaderSnapshot(std::map<fileid_t, TableReaderPtr>& readers)
{
	m_segment_rwlock.ReadLock();
	TableReaderSnapshotPtr rs_ptr = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();
	
	std::map<fileid_t, TableReaderPtr> new_readers;
	if(rs_ptr)
	{
		new_readers = rs_ptr->Readers();
	}
	for(auto it = readers.begin(); it != readers.end(); ++it)
	{
		new_readers[it->first] = it->second;
	}

	TableReaderSnapshotPtr new_segment_snapshot = NewTableReaderSnapshot(new_readers);

	m_segment_rwlock.WriteLock();
	m_reader_snapshot.swap(new_segment_snapshot);
	m_segment_rwlock.WriteUnlock();
}


///////////////////////////////////////////////////////////////////////////////
fileid_t WriteOnlyBucket::SelectNewSegmentFileId(MergeSegmentInfo& msinfo)
{
	//TODO:合并前先计算有没有可复用的segid，如果没有则重新选用segment集
	//     如果全部都没有，则选用未使用的最小segment id
	//原则：选用最小的seqid，将其level+1，如果level+1已是最大level，则选第2个seqid...
	for(auto it : msinfo.merging_segments)
	{
	}
	assert(false);
	return INVALID_FILEID;
}

Status WriteOnlyBucket::Merge()
{
	m_engine->NotifyFullMerge(m_db.lock(), shared_from_this());
	return OK;
}

void WriteOnlyBucket::UpdateReaderSnapshot(MergeSegmentInfo& msinfo)
{
	m_segment_rwlock.ReadLock();
	TableReaderSnapshotPtr rs_ptr = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();
	
	std::map<fileid_t, TableReaderPtr> new_readers = rs_ptr->Readers();
	for(auto it = msinfo.merging_segments.begin(); it != msinfo.merging_segments.end(); ++it)
	{
		new_readers.erase(it->first);
	}
	new_readers[msinfo.new_fileid] = msinfo.new_segment;

	TableReaderSnapshotPtr new_segment_snapshot = NewTableReaderSnapshot(new_readers);

	m_segment_rwlock.WriteLock();
	m_reader_snapshot.swap(new_segment_snapshot);
	m_segment_rwlock.WriteUnlock();
}

Status WriteOnlyBucket::Merge(MergeSegmentInfo& msinfo)
{		
	msinfo.new_fileid = SelectNewSegmentFileId(msinfo);

	DBImplPtr db = m_db.lock();
	SegmentWriter segment_writer(db->GetConfig(), m_engine->m_pool);
	Status s = segment_writer.Create(m_bucket_path.c_str(), msinfo.new_fileid);
	if(s != OK)
	{
		return s;
	}
	
	SegmentFileIndex seginfo;
	s = segment_writer.Merge(msinfo.merging_segments, seginfo);
	if(s != OK)
	{
		return s;
	}

	//更新segment snapshot，然后通知写segment list
	s = OpenSegment(m_bucket_path.c_str(), seginfo, msinfo.new_segment);
	if(s == OK)
	{
		return s;
	}
	UpdateReaderSnapshot(msinfo);

	{
		std::lock_guard<std::mutex> lock(m_mutex);
		m_merged_segment_fileids.reserve(m_merged_segment_fileids.size()+msinfo.merging_segments.size());
		for(auto it = msinfo.merging_segments.begin(); it != msinfo.merging_segments.end(); ++it)
		{
			m_merged_segment_fileids.push_back(it->first);
			//m_merging_segment_fileids.erase(it->first);
		}
	}
	
	m_engine->NotifyWriteSegmentList(db, shared_from_this());
	return OK;
}

//单线程：超过一定大小的block不参与合并，低于1/10的小块不与大块合并
Status WriteOnlyBucket::FullMerge()
{
	m_segment_rwlock.ReadLock();
	TableReaderSnapshotPtr bfs_ptr = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	//TODO:以超过阈值的segment为分割点，找到待合并的segment集
	std::vector<MergeSegmentInfo> msinfo;
	
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
	m_segment_rwlock.ReadLock();
	TableReaderSnapshotPtr bfs_ptr = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	//TODO:从低层开始，按id升序找出待合并的segment集
	MergeSegmentInfo msinfo;
	//...

	//相差很大的segment不要合并？比如10GB，100MB，即不超过10倍
	return Merge(msinfo);
	
	//bool need_merge = new_segment_snapshot->NeedMerge();
}

void WriteOnlyBucket::FillAliveSegmentInfos(TableReaderSnapshotPtr& trs_ptr, std::vector<fileid_t>& writed_segment_fileids, SegmentListData& md)
{
	const std::map<fileid_t, TableReaderPtr>& readers = trs_ptr->Readers();
	assert(!readers.empty());
	md.alive_segment_infos.reserve(readers.size());
	
	fileid_t max_fileid = writed_segment_fileids.empty() ? readers.rbegin()->first : writed_segment_fileids.back();
	for(auto it = readers.begin(); it != readers.end(); ++it)
	{
		if(it->first > max_fileid)
		{
			break;
		}
		SegmentReaderPtr seg_reader = std::dynamic_pointer_cast<SegmentReader>(it->second);
		assert(seg_reader);
		md.alive_segment_infos.push_back(seg_reader->FileIndex());
	}
}

//保证只被1个线程调用
Status WriteOnlyBucket::WriteSegmentList()
{
	TableReaderSnapshotPtr trs_ptr;
	std::vector<fileid_t> writed_segment_fileids;
	fileid_t segment_list_fileid;
	SegmentListData md;
	
	//将segment snapshot和deleted segmentid合并写入segment list
	//同时只读打开segment list，然后更新它们
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		if(m_writed_segment_fileids.empty() && m_merged_segment_fileids.empty())
		{
			return ERR_NOMORE_DATA;
		}

		segment_list_fileid = m_next_segmentlist_fileid++;
		if(!m_merged_segment_fileids.empty())
		{
			md.deleted_segment_fileids.swap(m_merged_segment_fileids);
		}
		if(!m_writed_segment_fileids.empty())
		{
			writed_segment_fileids.swap(m_writed_segment_fileids);
		}
		md.next_segment_id = m_next_segment_id;
		
		ReadLockGuard lock_guard(m_segment_rwlock);
		trs_ptr = m_reader_snapshot;
	}
	
	FillAliveSegmentInfos(trs_ptr, writed_segment_fileids, md);
	Status s = SegmentListFile::Write(m_bucket_path.c_str(), segment_list_fileid, md);
	if(s != OK)
	{
		return s;
	}
	//只读打开segment list file，并替换当前list file
	SegmentListFilePtr segment_list_file = NewSegmentListFile();
	segment_list_file->Open(m_bucket_path.c_str(), segment_list_fileid, xfutil::LOCK_TRY_READ);
	{
		WriteLockGuard lock_guard(m_segment_rwlock);
		m_segmentlist_file.swap(segment_list_file);
	}
	//写通知文件
	DBImplPtr db = m_db.lock();
	NotifyData nd(NOTIFY_UPDATE_BUCKET, db->GetPath(), m_info.name, segment_list_fileid);
	m_engine->WriteNotifyFile(nd);

	//加入清理队列
	if(segment_list_file)
	{
		FileName filename;
		MakeSegmentListFileName(segment_list_file->FileId(), filename.str);
		m_deleting_segmentlist_names.push_back(filename);

		m_engine->NotifyClean(db);
	}

	//判断是否需要merge
	//bool need_merge = new_segment_snapshot->NeedMerge();
	//if(need_merge)
	//{
	//	  m_engine->NotifyPartMerge(db, shared_from_this());
	//}
	
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
	if(force || m_memwriter->ElapsedTime() >= m_engine->GetConfig().memwriter_ttl_s)
	{
		FlushMemWriter();
	}
	return OK;
}
	

}  


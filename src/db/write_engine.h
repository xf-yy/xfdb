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

#ifndef __xfdb_write_engine_h__
#define __xfdb_write_engine_h__

#include <set>
#include "xfdb/strutil.h"
#include "types.h"
#include "notify_msg.h"
#include "file_util.h"
#include "thread.h"
#include "queue.h"
#include "file_notify.h"
#include "engine.h"
#include "db_impl.h"

namespace xfdb 
{

class WritableEngine : public Engine
{
public:
	explicit WritableEngine(const GlobalConfig& conf);
	virtual ~WritableEngine();
	
public:	
	virtual Status Start();
	virtual Status RemoveDB(const std::string& db_path) override;

public:	
	inline void NotifyWriteDbInfo(DBImplPtr db)
	{
		NotifyMsg msg(NOTIFY_WRITE_DB_META, db);
	
		uint32_t hc = Hash32((byte_t*)db->GetPath().data(), db->GetPath().size());
	
		m_write_metadata_queues[hc % m_conf.write_metadata_thread_num].Push(msg);
	}

	inline void NotifyWriteBucketMeta(DBImplPtr db, BucketPtr bucket)
	{
		NotifyMsg msg(NOTIFY_WRITE_BUCKET_META, db, bucket);
	
		uint32_t hc = Hash32((byte_t*)db->GetPath().data(), db->GetPath().size());
	
		m_write_metadata_queues[hc % m_conf.write_metadata_thread_num].Push(msg);
	}
	
    inline void NotifyWriteSegment(DBImplPtr db, BucketPtr bucket)
	{
		NotifyMsg msg(NOTIFY_WRITE_SEGMENT, db, bucket);
		
		m_write_segment_queue.Push(msg);
	}

	inline void NotifyTryFlush(DBImplPtr db, BucketPtr bucket)
	{
		NotifyMsg msg(NOTIFY_TRY_FLUSH, db, bucket);
		m_tryflush_queue.Push(msg);
	}

	inline void NotifyFullMerge(DBImplPtr db, BucketPtr bucket)
	{
		NotifyMsg msg(NOTIFY_FULL_MERGE, db, bucket);
		
		m_full_merge_queue.Push(msg);
	}
	inline void NotifyPartMerge(DBImplPtr db, BucketPtr bucket)
	{
		NotifyMsg msg(NOTIFY_PART_MERGE, db, bucket);
		
		m_part_merge_queue.Push(msg);
	}
	inline void NotifyClean(DBImplPtr db)
	{
		NotifyMsg msg(NOTIFY_CLEAN_DB, db);
		m_clean_queue.Push(msg);
	}

	void WriteNotifyFile(const NotifyData& nd);

protected:
	virtual DBImplPtr NewDB(const DBConfig& conf, const std::string& db_path);

private:
	static void WriteSegmentThread(size_t index, void* arg);
	static void WriteMetaThread(size_t index, void* arg);

	void TryFlush(std::set<std::string>& clean_dbs);
	bool TryFlush(const std::string& db_path);
	static void TryFlushThread(size_t index, void* arg);
	
	static void PartMergeThread(size_t index, void* arg);
	static void FullMergeThread(size_t index, void* arg);

	void CleanDB(std::set<std::string>& clean_dbs);
	bool CleanDB(const std::string& db_path);
	void CleanNotifyFile();
	void ScanNotifyFile();
	static void CleanThread(size_t index, void* arg);

private:	
	Status Close();
	
private:
	friend class WritableDB;
	friend class WriteOnlyBucket;
	friend class ReadWriteBucket;
	
	std::mutex m_mutex;
		
	BlockingQueue<NotifyMsg> m_tryflush_queue;
	Thread m_tryflush_thread;
	
	BlockingQueue<NotifyMsg> m_write_segment_queue;
	ThreadGroup m_write_segment_threadgroup;
	
	BlockingQueue<NotifyMsg> m_part_merge_queue;
	ThreadGroup m_part_merge_threadgroup;
	
	BlockingQueue<NotifyMsg> m_full_merge_queue;
	ThreadGroup m_full_merge_threadgroup;
	
	BlockingQueue<NotifyMsg>* m_write_metadata_queues;
	ThreadGroup m_write_metadata_threadgroup;	
		
	BlockingQueue<NotifyMsg> m_clean_queue;
	Thread m_clean_thread;
	std::deque<FileName> m_tobe_delete_notifyfiles;//待删除的通知文件，超过一定时间后被删除
	
private:	
	WritableEngine(const WritableEngine&) = delete;
	WritableEngine& operator=(const WritableEngine&) = delete;

};

}  

#endif


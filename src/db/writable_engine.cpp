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
#include "writable_engine.h"
#include "logger.h"
#include "hash.h"
#include "file.h"
#include "writable_db.h"
#include "writeonly_bucket.h"
#include "notify_file.h"

namespace xfdb 
{

WritableEngine::WritableEngine(const GlobalConfig& conf) : Engine(conf)
{
	m_write_metadata_queues = nullptr;
}

WritableEngine::~WritableEngine()
{
	//assert();
}

////////////////////////////////////////////////////////////////////////////////
Status WritableEngine::Start_()
{
	m_part_merge_threadgroup.Start(m_conf.part_merge_thread_num, PartMergeThread, this);
	m_full_merge_threadgroup.Start(m_conf.full_merge_thread_num, FullMergeThread, this);
	
	m_write_segment_threadgroup.Start(m_conf.write_segment_thread_num, WriteSegmentThread, this);
	
	m_write_metadata_queues = new BlockingQueue<NotifyMsg>[m_conf.write_metadata_thread_num];
	m_write_metadata_threadgroup.Start(m_conf.write_metadata_thread_num, WriteMetaThread, this);
	
	m_tryflush_thread.Start(TryFlushThread, this);
	
	m_clean_thread.Start(CleanThread, this);

	ScanNotifyFile();

	return OK;
}
 
void WritableEngine::Stop_()
{	
	NotifyMsg msg;

	m_clean_queue.Push(msg);
	m_clean_thread.Join();

	for(size_t i = 0; i < m_conf.part_merge_thread_num; ++i)
	{
		m_part_merge_queue.Push(msg);
	}
	m_part_merge_threadgroup.Join();

	for(size_t i = 0; i < m_conf.full_merge_thread_num; ++i)
	{
		m_full_merge_queue.Push(msg);
	}
	m_full_merge_threadgroup.Join();

	m_tryflush_queue.Push(msg);
	m_tryflush_thread.Join();
		
	for(size_t i = 0; i < m_conf.write_segment_thread_num; ++i)
	{
		m_write_segment_queue.Push(msg);
	}
	m_write_segment_threadgroup.Join();
	
	for(size_t i = 0; i < m_conf.write_metadata_thread_num; ++i)
	{
		m_write_metadata_queues[i].Push(msg);
	}
	m_write_metadata_threadgroup.Join();
	delete[] m_write_metadata_queues;
}

DBImplPtr WritableEngine::NewDB(const DBConfig& conf, const std::string& db_path)
{
	return NewWritableDB(conf, db_path);
}

Status WritableEngine::RemoveDB(const std::string& db_path)
{
	return WritableDB::Remove(db_path);
}

////////////////////////////////////////////////////////////////
void WritableEngine::PartMergeThread(size_t index, void* arg)
{	
	LogDebug("the %dst part merge thread started", index);

	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	assert(index < engine->m_conf.part_merge_thread_num);
	
	for(;;)
	{
		NotifyMsg msg;
		engine->m_part_merge_queue.Pop(msg);
		if(msg.type == NOTIFY_EXIT) 
		{
			break;
		}
		assert(msg.type == NOTIFY_PART_MERGE);
		WriteOnlyBucket* bucket = (WriteOnlyBucket*)msg.bucket.get();
		Status s = bucket->PartMerge();
        if(s != OK)
        {
            LogWarn("part merge for %s failed, status: %u", bucket->Info().name.c_str(), s);
        }
	}
	LogDebug("the %dst part merge thread exit", index);	
}

void WritableEngine::FullMergeThread(size_t index, void* arg)
{
	LogDebug("the %dst full merge thread started", index);
	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	assert(index < engine->m_conf.full_merge_thread_num);
	
	for(;;)
	{
		NotifyMsg msg;
		engine->m_full_merge_queue.Pop(msg);
		if(msg.type == NOTIFY_EXIT) 
		{
			break;
		}
		assert(msg.type == NOTIFY_FULL_MERGE);
		WriteOnlyBucket* bucket = (WriteOnlyBucket*)msg.bucket.get();
        Status s = bucket->FullMerge();
		if(s == ERR_IN_PROCESSING)
		{
			Thread::Sleep(10*1000);

			//重新放入合并队列
			engine->m_full_merge_queue.TryPush(msg);
		}
        else if(s != OK)
        {
            LogWarn("full merge for %s failed, status: %u", bucket->Info().name.c_str(), s);
        }
	}
	LogDebug("the %dst full merge thread started", index);
}

///////////////////////////////////////////////////////////////////

void WritableEngine::WriteMetaThread(size_t index, void* arg)
{
	LogDebug("the %dst write meta thread started", index);
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	int ms = 30*1000;
	
	for(;;)
	{
		NotifyMsg msg;
		if(!engine->m_write_metadata_queues[index].Pop(msg, ms))
		{
			continue;
		}
		if(msg.type == NOTIFY_EXIT)
		{
			break;
		}
		if(msg.type == NOTIFY_WRITE_BUCKET_META)
		{
			WriteOnlyBucketPtr bucket = std::dynamic_pointer_cast<WriteOnlyBucket>(msg.bucket);
			assert(bucket);
			bucket->WriteBucketMeta();
		}
		else if(msg.type == NOTIFY_WRITE_DB_META)
		{
			WritableDBPtr db = std::dynamic_pointer_cast<WritableDB>(msg.db);
			assert(db);
			db->WriteDBMeta();
		}
		else
		{
			LogWarn("invalid msg type: %d", msg.type);
		}
	}
	LogDebug("the %dst write meta thread exit", index);	
}


//////////////////////////////////////////////////////////////////////

void WritableEngine::WriteSegmentThread(size_t index, void* arg)
{
	LogDebug("the %dst write segment thread started", index);	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);

	for(;;)
	{
		NotifyMsg msg;
		engine->m_write_segment_queue.Pop(msg);
		if(msg.type == NOTIFY_EXIT) 
		{
			break;
		}
		WriteOnlyBucket* bucket = (WriteOnlyBucket*)msg.bucket.get();
		Status s = bucket->WriteSegment();
        if(s != OK)
        {
            LogWarn("write segment for %s failed, status: %u", bucket->Info().name.c_str(), s);
        }
	}
	LogDebug("the %dst write segment thread exit", index);	
	
}

////////////////////////////////////////////////////////////////////////

bool WritableEngine::TryFlush(const std::string& db_path)
{	
	DBImplPtr dbptr;
	if(!QueryDB(db_path, dbptr))
	{
		return false;
	}
	WritableDB* db = (WritableDB*)dbptr.get();
	Status s = db->TryFlush();
	return (s != ERR_NOMORE_DATA);
}

void WritableEngine::TryFlush(std::set<std::string>& clean_dbs)
{	
	for(auto it = clean_dbs.begin(); it != clean_dbs.end(); )
	{
		if(TryFlush(*it))
		{
			++it;
		}
		else
		{
			clean_dbs.erase(it++);
		}
	}
}


//try-flush结构体如何定义？
void WritableEngine::TryFlushThread(void* arg)
{
	LogDebug("try flush thread started");	
	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	uint16_t flush_interval_s = engine->m_conf.flush_interval_s / 3;
	if(flush_interval_s < 1)
	{
		flush_interval_s = 1;
	}

	const uint32_t timeout_ms = flush_interval_s * 1000;

	//key: db_path
	std::set<std::string> flush_dbs;

	second_t last_flush_time = time(nullptr);
	for(;;)
	{
		NotifyMsg nd;
		if(engine->m_tryflush_queue.Pop(nd, timeout_ms))
		{
			if(nd.type == NOTIFY_EXIT)
			{
				break;
			}
			flush_dbs.insert(nd.db->GetPath());

			//判断时间间隔是否已达到
			if((second_t)time(nullptr) < last_flush_time + flush_interval_s)
			{
				continue;
			}
		}
		last_flush_time = time(nullptr);
		engine->TryFlush(flush_dbs);
	}
	
	LogDebug("try flush thread exit");	
}

//////////////////////////////////////////////////////////////////////////
void WritableEngine::ScanNotifyFile()
{
	if(m_conf.notify_dir.empty())
	{
		return;
	}

	std::vector<FileName> filenames;
	ListNotifyFile(m_conf.notify_dir.c_str(), filenames);

	std::lock_guard<std::mutex> lock(m_mutex);
	for(size_t i = 0; i < filenames.size(); ++i)
	{
		m_tobe_delete_notifyfiles.push_back(filenames[i]);
	}
}

void WritableEngine::WriteNotifyFile(const NotifyData& nd)
{
	if(m_conf.notify_dir.empty())
	{
		return;
	}

	FileName filename;
	if(NotifyFile::Write(m_conf.notify_dir.c_str(), nd, filename) == OK)
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		m_tobe_delete_notifyfiles.push_back(filename);
	}
}

void WritableEngine::CleanNotifyFile()
{
	if(m_conf.notify_dir.empty())
	{
		return;
	}
	
	const second_t curr_time = time(nullptr);	
	
	for(;;)
	{		
		FileName filename;
		{
			std::lock_guard<std::mutex> lock(m_mutex);
			if(m_tobe_delete_notifyfiles.empty())
			{
				return;
			}
			filename = m_tobe_delete_notifyfiles.front();
		}
		char filepath[MAX_PATH_LEN];
		xfutil::Path::Combine(filepath, sizeof(filepath), m_conf.notify_dir.c_str(), filename.str);
		if(curr_time < File::ModifyTime(filepath) + m_conf.notify_file_ttl_s)
		{
			break;
		}
		File::Remove(filepath);
		
		{
			std::lock_guard<std::mutex> lock(m_mutex);
			if(!m_tobe_delete_notifyfiles.empty())
			{
				m_tobe_delete_notifyfiles.pop_front();
			}
		}
	}
}

bool WritableEngine::CleanDB(const std::string& db_path)
{	
	DBImplPtr dbptr;
	if(!QueryDB(db_path, dbptr))
	{
		return false;
	}
	WritableDB* db = (WritableDB*)dbptr.get();
	Status s = db->Clean();

	return (s != ERR_NOMORE_DATA);
}

void WritableEngine::CleanDB(std::set<std::string>& clean_dbs)
{
	for(auto it = clean_dbs.begin(); it != clean_dbs.end(); )
	{
		if(CleanDB(*it))
		{
			++it;
		}
		else
		{
			clean_dbs.erase(it++);
		}
	}
}

void WritableEngine::CleanThread(void* arg)
{
	LogDebug("clean thread started");	
	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	const uint32_t time_ms = engine->m_conf.clean_interval_s * 1000;
	second_t last_clean_time = time(nullptr);

	std::set<std::string> clean_dbs;    //待清理的db路径
    NotifyMsg nd;

	for(;;)
	{
		if(engine->m_clean_queue.Pop(nd, time_ms))
		{
			if(nd.type == NOTIFY_EXIT)
			{
				break;
			}
			assert(nd.type == NOTIFY_CLEAN_DB);
			clean_dbs.insert(nd.db->GetPath());
		}
        second_t now_s = time(nullptr);
        if(now_s >= last_clean_time + engine->m_conf.clean_interval_s)
        {
            last_clean_time = now_s;
            
            engine->CleanDB(clean_dbs);
            engine->CleanNotifyFile();

            engine->TryDeleteDB();
        }
	}
	
	LogDebug("clean thread exit");	
}


}  



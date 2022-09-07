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
#include "write_engine.h"
#include "logger.h"
#include "hash.h"
#include "file.h"
#include "write_db.h"
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
	Close();
}

////////////////////////////////////////////////////////////////////////////////
Status WritableEngine::Start()
{
	m_pool.Init(MEM_BLOCK_SIZE, 128);
	
	//TODO:读模式时需初始化cache
	if(m_conf.mode & MODE_READONLY)
	{
	}
	
	m_part_merge_threadgroup.Start(m_conf.part_merge_db_thread_num, PartMergeThread, this);
	m_full_merge_threadgroup.Start(m_conf.full_merge_db_thread_num, FullMergeThread, this);
	
	m_write_segment_threadgroup.Start(m_conf.write_segment_thread_num, WriteSegmentThread, this);
	
	m_write_metadata_queues = new BlockingQueue<NotifyMsg>[m_conf.write_metadata_thread_num];
	m_write_metadata_threadgroup.Start(m_conf.write_metadata_thread_num, WriteMetaThread, this);
	
	m_tryflush_thread.Start(TryFlushThread, this);
	
	m_clean_thread.Start(CleanThread, this);

	ScanNotifyFile();
	return OK;
}
 

Status WritableEngine::Close()
{
	assert(m_dbs.empty());
	
	//尝试关闭所有的db
	CloseAllDB();

	NotifyMsg msg;

	m_clean_queue.Push(msg);
	m_clean_thread.Join();

	for(size_t i = 0; i < m_part_merge_threadgroup.Size(); ++i)
	{
		m_part_merge_queue.Push(msg);
	}
	m_part_merge_threadgroup.Join();

	for(size_t i = 0; i < m_full_merge_threadgroup.Size(); ++i)
	{
		m_full_merge_queue.Push(msg);
	}
	m_full_merge_threadgroup.Join();

	m_tryflush_queue.Push(msg);
	m_tryflush_thread.Join();
		
	for(size_t i = 0; i < m_write_segment_threadgroup.Size(); ++i)
	{
		m_write_segment_queue.Push(msg);
	}
	m_write_segment_threadgroup.Join();
	
	for(size_t i = 0; i < m_write_metadata_threadgroup.Size(); ++i)
	{
		m_write_metadata_queues[i].Push(msg);
	}
	m_write_metadata_threadgroup.Join();
	delete[] m_write_metadata_queues;
	
	//TODO:读模式时释放cache
	if(m_conf.mode & MODE_READONLY)
	{
	}
	return OK;
}

Status WritableEngine::RemoveDB(const std::string& db_path)
{
	return WritableDB::Remove(db_path);
}

////////////////////////////////////////////////////////////////
void WritableEngine::PartMergeThread(size_t index, void* arg)
{
	LogInfo("merge db thread %d started", index);
	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	assert(index < engine->m_part_merge_threadgroup.Size());
	
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
		bucket->PartMerge();
	}
	LogInfo("merge db thread %d exit", index);
	
}

void WritableEngine::FullMergeThread(size_t index, void* arg)
{
	LogInfo("merge db thread %d started", index);
	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	assert(index < engine->m_full_merge_threadgroup.Size());
	
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
		bucket->FullMerge();
	}
	LogInfo("merge db thread %d exit", index);
	
}

///////////////////////////////////////////////////////////////////

void WritableEngine::WriteMetaThread(size_t index, void* arg)
{
	LogInfo("write meta thread started");
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	int ms = 30*1000;
	
	for(;;)
	{
		NotifyMsg msg;
		if(!engine->m_write_metadata_queues[index].Pop(msg, ms))
		{
			//clean操作;
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
		else if(msg.type == NOTIFY_WRITE_DB_INFO)
		{
			WritableDBPtr db = std::dynamic_pointer_cast<WritableDB>(msg.db);
			assert(db);
			db->WriteDbInfo();
		}
		else
		{
			LogWarn("invalid msg type: %d", msg.type);
		}
	}
	LogInfo("write meta thread exit");
	
}


//////////////////////////////////////////////////////////////////////

void WritableEngine::WriteSegmentThread(size_t index, void* arg)
{
	LogInfo("write segment file thread %zd started", index);
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
		bucket->WriteSegment();
	}
	LogInfo("write segment file thread %zd exit", index);
	
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
void WritableEngine::TryFlushThread(size_t index, void* arg)
{
	LogInfo("try flush thread started");
	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	const uint32_t time_ms = engine->m_conf.tryflush_interval_s * 1000;

	//key: db_path
	std::set<std::string> flush_dbs;

	second_t last_flush_time = time(nullptr);
	for(;;)
	{
		NotifyMsg nd;
		if(engine->m_tryflush_queue.Pop(nd, time_ms))
		{
			if(nd.type == NOTIFY_EXIT)
			{
				break;
			}
			flush_dbs.insert(nd.db->GetPath());

			//判断时间间隔是否已达到
			if((second_t)time(nullptr) < last_flush_time + engine->m_conf.tryflush_interval_s)
			{
				continue;
			}
		}
		last_flush_time = time(nullptr);
		engine->TryFlush(flush_dbs);
	}
	
	LogInfo("try flush thread exit");
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
		m_notifyfiles.push_back(filenames[i]);
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
		m_notifyfiles.push_back(filename);
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
			if(m_notifyfiles.empty())
			{
				return;
			}
			filename = m_notifyfiles.front();
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
			if(!m_notifyfiles.empty())
			{
				m_notifyfiles.pop_front();
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

void WritableEngine::CleanThread(size_t index, void* arg)
{
	LogInfo("clean thread started");
	
	WritableEngine* engine = (WritableEngine*)arg;
	assert(engine != nullptr);
	
	const uint32_t time_ms = engine->m_conf.clean_interval_s * 1000;

	//key: db path
	std::set<std::string> clean_dbs;
	
	second_t last_clean_time = time(nullptr);
	for(;;)
	{
		NotifyMsg nd;
		if(engine->m_clean_queue.Pop(nd, time_ms))
		{
			if(nd.type == NOTIFY_EXIT)
			{
				break;
			}
			assert(nd.type == NOTIFY_CLEAN_DB);
			clean_dbs.insert(nd.db->GetPath());

			if((second_t)time(nullptr) < last_clean_time + engine->m_conf.clean_interval_s)
			{
				continue;
			}
		}
		last_clean_time = time(nullptr);
		
		engine->CleanDB(clean_dbs);
		engine->CleanNotifyFile();
	}
	
	LogWarn("clean thread exit");
}


}  



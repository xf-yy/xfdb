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
#include "readonly_engine.h"
#include "logger.h"
#include "hash.h"
#include "file.h"
#include "readonly_db.h"
#include "notify_file.h"
#include "bucket.h"
#include "process.h"

namespace xfdb 
{

ReadOnlyEngine::ReadOnlyEngine(const GlobalConfig& conf) : Engine(conf)
{
	m_reload_queues = nullptr;
}

ReadOnlyEngine::~ReadOnlyEngine()
{
	//assert();
}

////////////////////////////////////////////////////////////////////////////////
Status ReadOnlyEngine::Start_()
{
	if(m_conf.auto_reload_db)
	{
		m_reload_queues = new BlockingQueue<NotifyData>[m_conf.reload_db_thread_num];
		m_reload_threadgroup.Start(m_conf.reload_db_thread_num, ProcessNotifyThread, this);

		if(!m_file_notify.AddPath(m_conf.notify_dir.c_str(), FE_RENAME, true))
		{
			return ERR_PATH_CREATE;
		}	
		m_notify_thread.Start(ReadNotifyThread, this);
	}	

	return OK;
}

void ReadOnlyEngine::Stop_()
{	
	if(m_conf.auto_reload_db)
	{
		//FIXME:写个关闭的通知文件
		NotifyData nd;
		NotifyFile::Write(m_conf.notify_dir.c_str(), nd);
		m_notify_thread.Join();
		
		//往reload队列中写入退出标记
		for(size_t i = 0; i < m_conf.reload_db_thread_num; ++i)
		{
			m_reload_queues[i].Push(nd);
		}
		m_reload_threadgroup.Join();
		delete[] m_reload_queues;
		m_reload_queues = nullptr;
	}
}

DBImplPtr ReadOnlyEngine::NewDB(const DBConfig& conf, const std::string& db_path)
{
	return NewReadOnlyDB(conf, db_path);
}

////////////////////////////////////////////////////////////////////////////////////
void ReadOnlyEngine::PostNotifyData(const NotifyData& nd)
{
	if(nd.db_path.empty())
	{
		return;
	}
	uint32_t hc = Hash32((byte_t*)nd.db_path.data(), nd.db_path.size());

	uint32_t idx = hc % m_conf.reload_db_thread_num;
	m_reload_queues[idx].Push(nd);
}

void ReadOnlyEngine::ProcessNotifyData(const NotifyData& nd)
{	
	DBImplPtr dbptr;
	if(!QueryDB(nd.db_path, dbptr))
	{
		return;
	}

	ReadOnlyDB* db = (ReadOnlyDB*)dbptr.get();
	char file_name[MAX_FILENAME_LEN];
	
	switch(nd.type)
	{
	case NOTIFY_UPDATE_BUCKET_META:
		assert(!nd.bucket_name.empty());
		if(!nd.bucket_name.empty())
		{
			BucketPtr bptr;
			if(db->GetBucket(nd.bucket_name, bptr))
			{
				MakeBucketMetaFileName(nd.file_id, file_name);
				Status s = bptr->Open(file_name);
                if(s != OK)
                {
                    LogWarn("open bucket(%s) of db(%s) failed, status: %d", nd.bucket_name.c_str(), nd.db_path.c_str(), s);
                }
			}
		}
		break;
		
	case NOTIFY_UPDATE_DB_META:
		if(nd.file_id != MIN_FILE_ID)
		{
			MakeDBMetaFileName(nd.file_id, file_name);
			Status s = db->OpenBucket(file_name);
            if(s != OK)
            {
                LogWarn("open bucket(%s) of db(%s) failed, status: %d", file_name, nd.db_path.c_str(), s);
            }
		}
		else
		{
			db->OpenBucket();
		}
		break;
		
	default:
		break;
	}
}

void ReadOnlyEngine::ProcessNotifyThread(size_t index, void* arg)
{
	LogDebug("the %dst process notify thread started", index);	

	ReadOnlyEngine* engine = (ReadOnlyEngine*)arg;
	assert(engine != nullptr);
	
	BlockingQueue<NotifyData>& reload_queue = engine->m_reload_queues[index];

	const uint32_t time_ms = engine->m_conf.clean_interval_s * 1000;
	second_t last_delete_time = time(nullptr);

    NotifyData nd;
    
	for(;;)
	{
		if(reload_queue.Pop(nd, time_ms))
        {
            if(nd.type == NOTIFY_EXIT)
            {
                break;
            }	
            engine->ProcessNotifyData(nd);
        }

        //FIXME: 借用这个线程删除无效db
        second_t now_s = time(nullptr);
        if(now_s >= last_delete_time + engine->m_conf.clean_interval_s)
        {
            last_delete_time = now_s;

            engine->TryDeleteDB();
        }
	}
	LogDebug("the %dst process notify thread exit", index);	
	
}

void ReadOnlyEngine::ReadNotifyThread(void* arg)
{
	LogDebug("read notify thread started");	
	
	ReadOnlyEngine* engine = (ReadOnlyEngine*)arg;
	assert(engine != nullptr);
	
	xfutil::tid_t pid = Process::GetPid();
	
	for(;;)
	{
		NotifyEvent event;
		int ret = engine->m_file_notify.Read(&event);
		if(ret > 0)
		{
			//判断文件进程id是否是本进程的
			xfutil::tid_t notify_pid = NotifyFile::GetNotifyPID(event.file_path);
			if(notify_pid == pid)
			{
				break;
			}
			
			NotifyData nd;
			Status s = NotifyFile::Read(event.file_path, nd);
			if(s == OK)
			{
				engine->PostNotifyData(nd);
			}	
			else
			{
				LogWarn("read notify file failed, status: %d", s);
			}
		}
		else if(ret < 0)
		{
			LogWarn("read notify failed, errno: %d", LastError);
		}
	}

	LogDebug("read notify thread exit");		
}


}  



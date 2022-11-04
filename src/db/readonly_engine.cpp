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
Status ReadOnlyEngine::Start()
{
	Status s = Init();
	if(s != OK)
	{
		return s;
	}

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
	LogInfo("xfdb started");

	m_started = true;
	return OK;
}

void ReadOnlyEngine::Stop()
{
	if(!m_started)
	{
		return;
	}
	
	//关闭所有的db
	CloseAllDB();
	
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
	Uninit();
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
				bptr->Open(file_name);
			}
		}
		break;
		
	case NOTIFY_UPDATE_DB_META:
		if(nd.file_id != MIN_FILEID)
		{
			MakeDBInfoFileName(nd.file_id, file_name);
			db->OpenBucket(file_name);
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
	LogInfo("reload thread_%zd started", index);
	
	ReadOnlyEngine* engine = (ReadOnlyEngine*)arg;
	assert(engine != nullptr);
	
	BlockingQueue<NotifyData>& reload_queue = engine->m_reload_queues[index];

	for(;;)
	{
		NotifyData nd;
		reload_queue.Pop(nd);
		if(nd.type == NOTIFY_EXIT)
		{
			break;
		}	
		engine->ProcessNotifyData(nd);
	}
	LogWarn("reload thread_%zd exit", index);
	
}

void ReadOnlyEngine::ReadNotifyThread(void* arg)
{
	LogInfo("notify thread started");
	
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
				LogWarn("found notify of current pid(%u)", notify_pid);
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

	LogWarn("notify thread exit");
	
}


}  



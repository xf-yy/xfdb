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

#ifndef __xfdb_readonly_engine_h__
#define __xfdb_readonly_engine_h__

#include "xfdb/strutil.h"
#include "types.h"
#include "notify_msg.h"
#include "file_util.h"
#include "thread.h"
#include "queue.h"
#include "file_notify.h"
#include "engine.h"
#include "notify_file.h"

namespace xfdb 
{

class ReadOnlyEngine : public Engine
{
public:
	explicit ReadOnlyEngine(const GlobalConfig& conf);
	virtual ~ReadOnlyEngine();

public:	
	virtual Status Start();
	
public:
	void PostNotifyData(const NotifyData& nd);

protected:
	virtual DBImplPtr NewDB(const DBConfig& conf, const std::string& db_path);
	
private:		
	static void ProcessNotifyThread(size_t index, void* arg);
	void ProcessNotifyData(const NotifyData& nd);
	
	static void ReadNotifyThread(void* arg);
	
	Status Close();
	
private:	
	BlockingQueue<NotifyData>* m_reload_queues;
	ThreadGroup m_reload_threadgroup;
	
	FileNotify m_file_notify;
	Thread m_notify_thread;
	
private:	
	ReadOnlyEngine(const ReadOnlyEngine&) = delete;
	ReadOnlyEngine& operator=(const ReadOnlyEngine&) = delete;

};

}  

#endif


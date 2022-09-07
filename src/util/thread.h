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

#ifndef __xfdb_thread_h__
#define __xfdb_thread_h__

#include <thread>
#include <mutex>
#include <vector>
#include "xfdb/util_types.h"
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

namespace xfutil
{

static inline uint32_t GetCpuCoreNum()
{
	return std::thread::hardware_concurrency();
	//sysconf (_SC_NPROCESSORS_CONF);
}
typedef void (*ThreadFunc)(size_t index, void* arg);	

class Thread
{
public:
	Thread(){}
		
public:
	void Start(ThreadFunc func, void* arg, size_t index = 0, bool detach = false)
	{
		std::thread t(func, index, arg);
		if(detach)
		{
			t.detach();
		}
		m_thread.swap(t);
	}
	
	void Join()
	{
		m_thread.join();
	}

	static void Sleep(uint32_t time_ms)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(time_ms));
	}
	static void Yield()
	{
		std::this_thread::yield();
	}
	static inline tid_t GetTid()
	{
		return syscall(SYS_gettid);
	}
	
private:
	std::thread m_thread;
	
private:
	Thread(const Thread&) = delete;
	Thread& operator=(const Thread&) = delete;
};

typedef std::shared_ptr<Thread> ThreadPtr;
#define NewThread 	std::make_shared<Thread>

class ThreadGroup
{
public:
	ThreadGroup()
	{}
		
public:
	void Start(size_t thread_count, ThreadFunc func, void* arg = nullptr, bool detach = false)
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		
		m_threads.reserve(thread_count);
		for(size_t i = 0; i < thread_count; ++i)
		{
			ThreadPtr thread = NewThread();
			thread->Start(func, arg, i, detach);
			m_threads.push_back(thread);
		}
	}
	
	void Join()
	{
		std::vector<ThreadPtr> tmp_threads;
		{
			std::lock_guard<std::mutex> lock(m_mutex);
			tmp_threads = m_threads;
		}
		
		for(size_t i = 0; i < tmp_threads.size(); ++i)
		{
			tmp_threads[i]->Join();
		}
	}
	
	size_t Size() const
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		return m_threads.size();
	}
	
private:
	mutable std::mutex m_mutex;
	std::vector<ThreadPtr> m_threads;
	
private:
  ThreadGroup(const ThreadGroup&) = delete;
  ThreadGroup& operator=(const ThreadGroup&) = delete;
		
};

}

#endif


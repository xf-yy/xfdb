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

#ifndef __xfutil_queue_h__
#define __xfutil_queue_h__

#include <mutex>
#include <condition_variable>
#include <deque>

namespace xfutil
{

template <typename T>	
class BlockingQueue
{
public:
	BlockingQueue(uint64_t capacity = (uint64_t)-1)
		: m_capacity(capacity)
	{}
	~BlockingQueue()
	{}

public:
	void SetCapacity(uint64_t capacity = (uint64_t)-1)
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		m_capacity = capacity;
	}
	size_t Size()
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		return m_queue.size();		
	}

	//只有数量限制
	void Push(const T& v)
	{
		std::unique_lock<std::mutex> lock(m_mutex);
		while(m_queue.size() >= m_capacity)
		{
			m_not_full_cond.wait(lock);
		}
        m_queue.push_back(v);
        m_not_empty_cond.notify_one();	
	}
	//只有数量限制
	bool Push(const T& v, uint32_t timeout_ms)
	{
		std::unique_lock<std::mutex> lock(m_mutex);
		while(m_queue.size() >= m_capacity)
		{
			if(m_not_full_cond.wait_for(lock, std::chrono::milliseconds(timeout_ms)) == std::cv_status::timeout)
			{
				return false;
			}
		}
		m_queue.push_back(v);
		m_not_empty_cond.notify_one();	
		return true;
	}

	bool TryPush(const T& v)
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		if(m_queue.size() >= m_capacity)
		{
			return false;
		}
        m_queue.push_back(v);
        m_not_empty_cond.notify_one();
        return true;
	}	
	
	bool TryPop(T& v)
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		if(m_queue.empty())
		{
			return false;
		}
        v = m_queue.front();
        m_queue.pop_front();
        m_not_full_cond.notify_one();
        return true;			
	}
	void Pop(T& v)
	{
		std::unique_lock<std::mutex> lock(m_mutex);
		while(m_queue.empty())
		{
			m_not_empty_cond.wait(lock);
		}
        v = m_queue.front();
        m_queue.pop_front();
        m_not_full_cond.notify_one();			
	}
	
	bool Pop(T& v, uint32_t timeout_ms)
	{
		std::unique_lock<std::mutex> lock(m_mutex);
		while(m_queue.empty())
		{
			if(m_not_empty_cond.wait_for(lock, std::chrono::milliseconds(timeout_ms)) == std::cv_status::timeout)
			{
				return false;
			}
		}
        v = m_queue.front();
        m_queue.pop_front();
        m_not_full_cond.notify_one();
        return true;
	}
	
private:
    mutable std::mutex m_mutex;
    std::condition_variable m_not_empty_cond;
    std::condition_variable m_not_full_cond;
    std::deque<T> m_queue;
    uint64_t m_capacity;
    	
private:
    BlockingQueue(const BlockingQueue<T>& other) = delete;
    BlockingQueue<T>& operator=(const BlockingQueue<T>& other) = delete;
		
};

}

#endif


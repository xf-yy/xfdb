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

#include "thread.h"

namespace xfutil
{

void ThreadGroup::Start(size_t thread_count, GThreadFunc func, void* arg/* = nullptr*/)
{
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for(size_t i = 0; i < thread_count; ++i)
    {
        std::thread th(func, i, arg);
        threads.push_back(std::move(th));
    } 

    std::lock_guard<std::mutex> lock(m_mutex);
    m_threads.swap(threads);
}

void ThreadGroup::Detach()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    
    for(size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i].detach();
    }
}

void ThreadGroup::Join()
{    
    std::lock_guard<std::mutex> lock(m_mutex);
    for(size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i].join();
    }
}

}



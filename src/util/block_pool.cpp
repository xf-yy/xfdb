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

#include <vector>
#include <malloc.h>
#include "block_pool.h"
#include "buffer.h"


namespace xfutil
{

bool BlockPool::Init(uint32_t block_size, uint32_t cache_num)
{
	assert(block_size % 4096 == 0);
	if(block_size % 4096 != 0)
	{
		return false;
	}
	SpinLockGuard guard(m_lock);
	if(m_cache_start != nullptr)
	{
		return false;
	}
	
	uint64_t size = (uint64_t)block_size * cache_num;
	byte_t* buf = xmalloc(size);
	if(buf == nullptr)
	{
		return false;
	}
	m_block_size = block_size;
	m_cache_start = buf;
	m_cache_end = buf + size;
	for(uint32_t i = 0; i < cache_num; ++i)
	{
		m_free_blocks.push_back(buf);
		buf += block_size;
	}
	return true;
}

byte_t* BlockPool::Alloc()
{
	{
		SpinLockGuard guard(m_lock);
		if(!m_free_blocks.empty())
		{
			byte_t* buf = m_free_blocks.front();
			m_free_blocks.pop_front();
			return buf;
		}
	}
	return xmalloc(m_block_size);
}

void BlockPool::Free(byte_t* block)
{
	{
		SpinLockGuard guard(m_lock);
		if(block >= m_cache_start && block < m_cache_end)
		{
			m_free_blocks.push_front(block);
			return;
		}
	}
	xfree(block);
}
	

}



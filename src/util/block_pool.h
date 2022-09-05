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

#ifndef __xfutil_block_pool_h__
#define __xfutil_block_pool_h__

#include <deque>
#include "xfdb/util_types.h"
#include "spinlock.h"

namespace xfutil
{

//大块内存池>=4096
class BlockPool
{
public:
	BlockPool()
	{
		spinlock_init(&m_lock);
		
		m_block_size = 0;
		m_cache_start = nullptr;
		m_cache_end = nullptr;
	}
	~BlockPool()
	{
		if(m_cache_start != nullptr)
		{
			xfree(m_cache_start);
			m_cache_start = nullptr;
			m_cache_end = nullptr;
		}
		
		spinlock_destroy(&m_lock);
	}
	
public:
	/**初始化内存块池
	 * block_size: 块大小，需对齐到4096
	 * cache_num: 缓存块的数量
	 */
	bool Init(uint32_t block_size, uint32_t cache_num);
	
	/**申请一个块，如果失败，返回nullptr*/
	byte_t* Alloc();
	
	/**释放一个块*/
	void Free(byte_t* block);

	/**块大小*/
	inline uint32_t BlockSize()
	{
		return m_block_size;
	}
private:
	spinlock_t m_lock;
	uint32_t m_block_size;
	byte_t* m_cache_start;
	byte_t* m_cache_end;
	std::deque<byte_t*> m_free_blocks;
	
private:
	BlockPool(const BlockPool&) = delete;
	BlockPool& operator=(const BlockPool&) = delete;
};

#if 0
//小块内存池
class MemPool
{
private:
	MemPool(const MemPool&) = delete;
	MemPool& operator=(const MemPool&) = delete;
};
#endif

}

#endif


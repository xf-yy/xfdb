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

#ifndef __xfutil_buffer_h__
#define __xfutil_buffer_h__

#include <vector>
#include <malloc.h>
#include <string.h>
#include "xfdb/strutil.h"

namespace xfutil
{

class BlockPool;
class WriteBuffer
{
public:
	explicit WriteBuffer(BlockPool& pool);
	~WriteBuffer();
	
public:
	/**申请空间，返回数据指针*/
	byte_t* Write(uint32_t size);
	
	/**申请空间，并写入数据，返回数据指针*/
	inline byte_t* Write(const byte_t* data, uint32_t size)
	{
		byte_t* buf = Write(size);
		memcpy(buf, data, size);
		return buf;
	}

	/**清除所有数据*/
	void Clear();
	
	//已分配空间的大小
	inline uint64_t Usage() const 
	{
		return m_usage;
	}
		
private:	
	BlockPool& m_large_block_pool;
	byte_t* m_ptr;				//当前待分配的指针
	uint32_t m_size;			//当前可用的空间大小
	
	uint64_t m_usage;
	std::vector<byte_t*> m_blocks;
	std::vector<byte_t*> m_bufs;

private:
	WriteBuffer(const WriteBuffer&) = delete;
	WriteBuffer& operator=(const WriteBuffer&) = delete;
};

class BufferGuard
{
public:
	explicit BufferGuard(size_t size)
	{
		m_buf = xmalloc(size);
        m_size = size;
	}
	~BufferGuard()
	{
		xfree(m_buf);
	}
	inline byte_t* Buffer()
    {
        return m_buf;
    }
    inline size_t Size()
    {
        return m_size;
    }
    
private:
	byte_t* m_buf;
    size_t m_size;
	
private:
	BufferGuard(const BufferGuard&) = delete;
	BufferGuard& operator=(const BufferGuard&) = delete;
};

}

#endif


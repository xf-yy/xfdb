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

#include "xfdb/strutil.h"

namespace xfutil
{

bool String::Reserve(size_t size)
{
	if(size <= m_capacity)
	{
		return true;
	}
	char* ptr = (char*)xmalloc(size);
	if(ptr == nullptr)
	{
		return false;
	}
	memcpy(ptr, m_buf, m_size);
	if(m_buf != m_cache)
	{
		xfree(m_buf);
	}
	m_buf = (char*)ptr;
	m_capacity = size;
	return true;
}

bool String::ReSize(size_t size)
{
	if(!Reserve(size))
	{
		return false;
	}
	m_size = size;
	return true;
}	

bool String::Append(const char* data, size_t size)
{
	if(m_size + size > m_capacity)
	{
		if(!Reserve(size + m_capacity*2))
		{
			return false;
		}
	}

	memcpy(m_buf+m_size, data, size);
	m_size += size;

	return true;
}

void String::Clear()
{
	if(m_buf != m_cache)
	{
		xfree(m_buf);
		m_buf = m_cache;
		m_capacity = CACHE_SIZE;
	}
	m_size = 0;
}

int StrView::Compare(const StrView& dst) const
{
	if(size == dst.size)
	{
		return memcmp(data, dst.data, size);
	}
	else if(size < dst.size)
	{
		int res = memcmp(data, dst.data, size);
		return (res == 0 ? -1 : res);
	}
	else
	{
		int res = memcmp(data, dst.data, dst.size);
		return (res == 0 ? 1 : res);
	}
}

uint32_t StrView::GetPrefixLength(const StrView& str) const
{
	uint32_t min_len = (size < str.size) ? size : str.size;
	for(uint32_t i = 0; i < min_len; ++i)
	{
		if(data[i] != str.data[i])
		{
			return i;
		}
	}
	return min_len;
}

} 


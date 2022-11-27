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

#ifndef __xfutil_strutil_h__
#define __xfutil_strutil_h__

#include <string.h>
#include <cassert>
#include <cstddef>
#include <errno.h>
#include <stdlib.h>

namespace xfutil
{

//定义整型类型
typedef unsigned char byte_t;
typedef unsigned char uint8_t;
typedef signed char int8_t;

typedef unsigned short uint16_t;
typedef signed short int16_t;

typedef unsigned int uint32_t;
typedef signed int int32_t;

#ifdef __linux__
typedef unsigned long uint64_t;
typedef signed long int64_t;

typedef uint32_t tid_t;

typedef int fd_t;
#define INVALID_FD ((fd_t)-1)

typedef uint64_t second_t;

#define	LastError	errno

#else
#error "not suppot this platform"
#define LastError	GetLastError()
#endif


static_assert(sizeof(void*) == 8, "invalid void*");
static_assert(sizeof(byte_t) == 1, "invalid byte_t");
static_assert(sizeof(int8_t) == 1, "invalid int8_t");
static_assert(sizeof(uint8_t) == 1, "invalid uint8_t");
static_assert(sizeof(int16_t) == 2, "invalid int16_t");
static_assert(sizeof(uint16_t) == 2, "invalid uint16_t");
static_assert(sizeof(int32_t) == 4, "invalid int32_t");
static_assert(sizeof(uint32_t) == 4, "invalid uint32_t");
static_assert(sizeof(int64_t) == 8, "invalid int64_t");
static_assert(sizeof(uint64_t) == 8, "invalid uint64_t");

#define ALIGN_UP(size, align) \
	((size + (align - 1)) & ~(align - 1))
		
#define ARRAY_SIZE(array) \
	(sizeof(array) / sizeof(array[0]))

#define KB(x)	(1024ULL * (x))
#define MB(x)	(1024ULL * KB(x))
#define GB(x)	(1024ULL * MB(x))

#define MAX(x, y)     (((x) > (y)) ? (x) : (y))
#define MIN(x, y)     (((x) < (y)) ? (x) : (y))

static inline byte_t* xmalloc(size_t size)
{
	return (byte_t*)malloc(size);
}
static inline void xfree(void* ptr)
{
	free(ptr);
}

#ifdef __linux__
static inline void StrCpy(char* dst, size_t dst_len, const char* src)
{
	memccpy(dst, src, '\0', dst_len);
	dst[dst_len-1] = '\0';
}
#else
static inline void StrCpy(char* dst, size_t dst_len, const char* src)
{
	_memccpy(dst, src, '\0', dst_len);
	dst[dst_len-1] = '\0';
}
#endif

static inline void StrCpy(char* dst, size_t dst_len, const char* src, size_t src_len)
{
	size_t min_len = MIN(dst_len, src_len);
	memcpy(dst, src, min_len);
	dst[min_len-1] = '\0';
}

/////////////////////////////////////////////////////////
class String
{	
public:
	String()
	{
		m_buf = m_cache;
		m_size = 0;
		m_capacity = CACHE_SIZE;
	}

	~String()
	{
		if(m_buf != m_cache)
		{
			xfree(m_buf);
		}
	}
	
public:
	inline char* Data() const 
	{
		return m_buf;
	}
	inline size_t Size() const
	{
		return m_size;
	}
	inline bool Empty() const
	{
		return (m_size == 0);
	}
	char& operator[](size_t idx) 
	{
		assert(idx < m_size);
		return m_buf[idx];
	}

	bool Reserve(size_t size);
	
	bool Resize(size_t size);
		
	bool Assign(const char* data, size_t size)
	{
		m_size = 0;
		return Append(data, size);
	}

	bool Append(const char* data, size_t size);
	
	void Clear();
		
private:
	enum{CACHE_SIZE=256};
	char m_cache[CACHE_SIZE];
	char* m_buf;
	size_t m_size;
	size_t m_capacity;

private:
	String(const String&) = delete;
	void operator=(const String&) = delete;
};


////////////////////////////////////////////////////////
struct StrView
{
	const char* data;
	size_t size;

public:
	StrView()
	{
		Reset();
	}

	explicit StrView(const char* data_)
	{
        Set(data_, strlen(data_));
	}

	StrView(const char* data_, size_t size_)
	{
        Set(data_, size_);
	}
	
	StrView(const String& str)
	{
        Set(str.Data(), str.Size());
	}

public:
	inline const char* Data() const 
	{
		return data;
	}

	inline size_t Size() const 
	{ 
		return size;
	}

	inline bool Empty() const 
	{ 
		return size == 0; 
	}
    
	char operator[](size_t idx) const
	{
		assert(idx < size);
		return data[idx];
	}

	inline void Reset() 
	{
		data = "";
		size = 0;
	}
	inline void Set(const char* data_, size_t size_)
	{
		data = data_;
		size = size_;
	}
	inline void Set(const String& str)
	{
		data = str.Data();
		size = str.Size();
	}
	inline void Set(const char* data_)
	{
		data = data_;
		size = strlen(data_);
	}
	inline void Skip(size_t n) 
	{
		assert(n <= size);
		data += n;
		size -= n;
	}

	int Compare(const StrView& dst) const;
	
	bool operator<(const StrView& dst) const
	{
		return Compare(dst) < 0;
	}
	bool operator==(const StrView& dst) const
	{
		return (size == dst.size) && (memcmp(data, dst.data, dst.size) == 0);
	}
	bool operator!=(const StrView& dst) const
	{
		return (size != dst.size) || (memcmp(data, dst.data, dst.size) != 0);
	}
	
	bool StartWith(const StrView& str) const 
	{
		return ((size >= str.size) && (memcmp(data, str.data, str.size) == 0));
	}
	bool EndWith(const StrView& str) const
	{
		return ((size >= str.size) && (memcmp(data+(size-str.size), str.data, str.size) == 0));
	}
	uint32_t GetPrefixLength(const StrView& str) const;

};


}

#endif


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

#ifndef __xfbase_types_h__
#define __xfbase_types_h__

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


}

#endif



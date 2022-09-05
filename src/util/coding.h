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

#ifndef __xfutil_coding_h__
#define __xfutil_coding_h__

#include <string.h>
#include "xfdb/util_types.h"
#include "xfdb/strutil.h"

namespace xfutil
{

#define MAX_V32_SIZE	(5)
#define MAX_V64_SIZE	(10)

static inline byte_t* Encode16(byte_t* buf, uint16_t value)
{
	buf[0] = (byte_t)(value);
	buf[1] = (byte_t)(value >> 8);
	return (buf + sizeof(uint16_t));
}

static inline uint16_t Decode16(const byte_t* &buf)
{
	uint16_t v =  ((uint16_t)buf[0]) | ((uint16_t)buf[1] << 8); 
	buf += sizeof(uint16_t);
	return v;
}

static inline byte_t* Encode32(byte_t* buf, uint32_t value)
{
	buf[0] = (byte_t)(value);
	buf[1] = (byte_t)(value >> 8);
	buf[2] = (byte_t)(value >> 16);
	buf[3] = (byte_t)(value >> 24);
	return (buf + sizeof(uint32_t));
}

static inline uint32_t Decode32(const byte_t* &buf)
{
	uint32_t v =  ((uint32_t)buf[0])
			     | ((uint32_t)buf[1] << 8) 
			     | ((uint32_t)buf[2] << 16) 
			     | ((uint32_t)buf[3] << 24);
	buf += sizeof(uint32_t);
	return v;
}

static inline byte_t* Encode64(byte_t* buf, uint64_t value)
{
	buf[0] = (byte_t)(value);
	buf[1] = (byte_t)(value >> 8);
	buf[2] = (byte_t)(value >> 16);
	buf[3] = (byte_t)(value >> 24);
	buf[4] = (byte_t)(value >> 32);
	buf[5] = (byte_t)(value >> 40);
	buf[6] = (byte_t)(value >> 48);
	buf[7] = (byte_t)(value >> 56);

	return (buf + sizeof(uint64_t));
}

static inline uint64_t Decode64(const byte_t* &buf)
{
	uint64_t v = ((uint64_t)buf[0])
			     	| ((uint64_t)buf[1] << 8) 
			     	| ((uint64_t)buf[2] << 16) 
			     	| ((uint64_t)buf[3] << 24)
			     	| ((uint64_t)buf[4] << 32) 
			     	| ((uint64_t)buf[5] << 40) 
			     	| ((uint64_t)buf[6] << 48)
			     	| ((uint64_t)buf[7] << 56);
	buf += sizeof(uint64_t);
	return v;
}

byte_t* EncodeV32(byte_t* buf, uint32_t v);
uint32_t DecodeV32(const byte_t* &buf, const byte_t* end);

byte_t* EncodeV64(byte_t* buf, uint64_t v);
uint64_t DecodeV64(const byte_t* &buf, const byte_t* end);

static inline byte_t* EncodeString(byte_t* buf, const char* str, uint32_t str_len)
{
	buf = EncodeV32(buf, str_len);
	memcpy(buf, str, str_len);
	return (buf + str_len);
}

static inline byte_t* EncodeString(byte_t* buf, const char* str)
{
	uint32_t str_len = (str != nullptr ? strlen(str) : 0);
	return EncodeString(buf, str, str_len);
}

static inline StrView DecodeString(const byte_t* &buf, const byte_t* end)
{
	uint32_t len = DecodeV32(buf, end);
	const char* data = (const char*)buf;
	buf += len;
	
	return StrView(data, len);
}

static inline byte_t* EncodeV32(byte_t* buf, uint32_t id, uint32_t v)
{
	buf = EncodeV32(buf, id);
	return EncodeV32(buf, v);
}
static inline byte_t* EncodeV64(byte_t* buf, uint32_t id, uint64_t v)
{
	buf = EncodeV32(buf, id);
	return EncodeV64(buf, v);
}
static inline byte_t* EncodeString(byte_t* buf, uint32_t id, const char* str, uint32_t str_len)
{
	buf = EncodeV32(buf, id);
	return EncodeString(buf, str, str_len);
}
static inline byte_t* EncodeString(byte_t* buf, uint32_t id, const char* str)
{
	buf = EncodeV32(buf, id);
	return EncodeString(buf, str);
}

}

#endif


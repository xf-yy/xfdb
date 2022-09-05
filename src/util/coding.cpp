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

#include "coding.h"

namespace xfutil
{

byte_t* EncodeV32(byte_t* buf, uint32_t v)
{
	while(v >= 0x80)
	{
		*(buf++) = 0x80 | (v & 0x7f);
		v >>= 7;
	}
	*(buf++) = v;
	return buf;
}

uint32_t DecodeV32(const byte_t* &buf, const byte_t* end)
{
	uint32_t v = 0;
	for (uint32_t shift = 0; shift <= 28 && buf < end; shift += 7) 
	{
		uint32_t b = *(buf++);
		if(b >= 0x80) 
		{
			v |= ((b & 0x7f) << shift);
		} 
		else 
		{
			v |= (b << shift);
			return v;
		}
	}
	++buf;	//通过判断(buf > end)表示非法
	return 0;
}


byte_t* EncodeV64(byte_t* buf, uint64_t v)
{
	while(v >= 0x80)
	{
		*(buf++) = 0x80 | (v & 0x7f);
		v >>= 7;
	}
	*(buf++) = v;
	return buf;
}

uint64_t DecodeV64(const byte_t* &buf, const byte_t* end)
{
	uint64_t v = 0;
	for (uint32_t shift = 0; shift <= 63 && buf < end; shift += 7) 
	{
		uint64_t b = *(buf++);
		if(b >= 0x80) 
		{
			v |= ((b & 0x7f) << shift);
		} 
		else 
		{
			v |= (b << shift);
			return v;
		}
	}
	++buf;	//通过判断(buf > end)表示非法
	return 0;
}



}



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

#include <algorithm>
#include "key_util.h"
#include "coding.h"
#include "xfdb/strutil.h"

using namespace xfutil;

namespace xfdb 
{

//找>=key的短串
void GetMaxShortKey(const StrView& key, String& str) 
{
	str.Assign(key.data, key.size);
	if(key.size <= MIN_SHORT_KEY_LEN)
	{
		return;
	}
	for(size_t i = MIN_SHORT_KEY_LEN; i < key.size; ++i)
	{
		if(str[i] != (char)0xFF)
		{
			++str[i];
			str.Resize(i+1);
			return;
		}
	}
}

//找<=key的短串
static void GetMinShortKey(String& str, uint32_t start_off) 
{
	for(size_t i = start_off; i < str.Size(); ++i)
	{
		if(str[i] != (char)0)
		{
			--str[i];
			str.Resize(i+1);
			return;
		}
	}
}

void GetMinShortKey(const StrView& key, String& str) 
{
	str.Assign(key.data, key.size);
	if(key.size <= MIN_SHORT_KEY_LEN)
	{
		return;
	}

	GetMinShortKey(str, MIN_SHORT_KEY_LEN);
}

//找比<=key且>prev_key的短串
void GetMidShortKey(const StrView& prev_key, const StrView& key, String& str)
{
	str.Assign(key.data, key.size);
	if(key.size <= MIN_SHORT_KEY_LEN)
	{
		return;
	}
	assert(prev_key.Compare(key) <= 0);
	
	uint32_t prefix_len = prev_key.GetPrefixLength(key);
	uint32_t min_len = MIN(prev_key.size, key.size);
	
	if(prefix_len < min_len)
	{
		if(str[prefix_len]-1 > prev_key[prefix_len])
		{
			--str[prefix_len];
			str.Resize(prefix_len+1);
			return;
		}
		++prefix_len;
	}
	if(prefix_len == 0)
	{
		prefix_len = MIN_SHORT_KEY_LEN;
	}
	return GetMinShortKey(str, prefix_len);
}

StrView MakeKey(StrView& prev_key, uint32_t shared_keysize, StrView& nonshared_key, String& prev_str1, String& prev_str2)
{
	if(prev_key.data == prev_str1.Data())
	{
		prev_str2.Assign(prev_key.data, shared_keysize);
		prev_str2.Append(nonshared_key.data, nonshared_key.size);
		return StrView(prev_str2.Data(), prev_str2.Size());
	}
	prev_str1.Assign(prev_key.data, shared_keysize);
	prev_str1.Append(nonshared_key.data, nonshared_key.size);
	return StrView(prev_str1.Data(), prev_str1.Size());
}

StrView CloneKey(WriteBuffer& buf, const StrView& str)
{
	byte_t *p = buf.Write((byte_t*)str.data, str.size);
	return StrView((char*)p, str.size);
}

}  



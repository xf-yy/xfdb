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

#ifndef __xfdb_key_util_h__
#define __xfdb_key_util_h__

#include "dbtypes.h"
#include "buffer.h"

namespace xfdb 
{

#define MIN_SHORT_KEY_LEN	16

void GetMaxShortKey(const StrView& key, String& str); 
void GetMinShortKey(const StrView& key, String& str);
void GetMidShortKey(const StrView& key1, const StrView& key2, String& str);

StrView MakeKey(StrView& prev_key, uint32_t shared_keysize, StrView& nonshared_key, String& prev_str1, String& prev_str2);
StrView CloneKey(WriteBuffer& buf, const StrView& str);


}  

#endif


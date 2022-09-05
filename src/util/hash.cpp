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

#include "hash.h"
#include "coding.h"

namespace xfutil 
{

static inline uint32_t Decode(const byte_t* data, size_t len)
{
	uint32_t v = 0;
	switch(len) 
	{
	case 3:	v |= data[2]<< 16; 	//透传
	case 2:	v |= data[1]<< 8 ; 	//透传
	case 1:	v |= data[0] ; 		//透传
	}
	return v;
}

//选个大质数 4118054813
uint32_t Hash32(const byte_t* data, size_t len, uint32_t seed) 
{
	uint32_t h = seed * len;
	 
	for (; len >= 4 ; len -= 4) 
	{
		h += Decode32(data);
		h *= seed;
	}

	h += Decode(data, len);
	h *= seed;
		 
	return h;
}



}



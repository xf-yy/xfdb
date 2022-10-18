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

#ifndef __xfutil_sysinfo_h__
#define __xfutil_sysinfo_h__

#include <thread>

namespace xfutil 
{
#ifdef __linux__

static inline uint32_t GetCpuCoreNum()
{
	return std::thread::hardware_concurrency();
	//sysconf (_SC_NPROCESSORS_CONF);
}

static inline uint64_t GetPhyMemSize()
{
	return (uint64_t)sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE);
}

static inline uint32_t GetPageSize()
{
	return sysconf(_SC_PAGESIZE);
}

#else
#endif



}

#endif



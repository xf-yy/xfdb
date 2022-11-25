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

#ifndef __xfutil_process_h__
#define __xfutil_process_h__

#include "xfdb/strutil.h"
#include <unistd.h>

namespace xfutil
{

typedef void (*ExitFunc)(void);

class Process
{
public:
	static inline tid_t GetPid()
	{
		return getpid();
	}
	static void Exit(int32_t exit_code)
	{
		_exit(exit_code);
	}
	static void AddExitFunc(ExitFunc func)
	{
		 atexit(func);
	}
};

}

#endif


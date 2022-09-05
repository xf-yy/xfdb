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

#ifndef __xfutil_path_h__
#define __xfutil_path_h__

#include <stdio.h>
#include "xfdb/util_types.h"
#include "xfdb/strutil.h"

namespace xfutil 
{

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN	256	//包含结束符'\0'
#endif

#define PATH_SEPARATOR	'/'

class Path
{
public:
	//static const char* GetFileName(const char* path);
	//static const char* GetExtName(const char* path);
	//static int GetDirPath(const char* path, char dir_path[MAX_PATH_LEN]);

	static void CombineEx(char* path, size_t path_size, const char* path1, const char* path2)
	{
		assert(path != nullptr);
		assert(path1 != nullptr && path1[0] != '\0');
		assert(path2 != nullptr && path2[0] != '/');
		if(path1[strlen(path1) - 1] == '/')
		{
			snprintf(path, path_size, "%s%s", path1, path2);
		}
		else
		{
			snprintf(path, path_size, "%s/%s", path1, path2);
		}
	}
	static void Combine(char* path, size_t path_size, const char* path1, const char* path2)
	{
		assert(path != nullptr);
		assert(path1 != nullptr && path1[0] != '\0');
		assert(path2 != nullptr && path2[0] != '/');
		snprintf(path, path_size, "%s/%s", path1, path2);
	}
	
	#if 0
	static AddBackslash();
	static RemoveBackslash();
	static RemoveExtName();
	static AddExtName();
	static RemoveFileName();
	static AddFileName();
	static Append();
	static GetTempDirPath(char* buf, int buf_len);
	static GetWorkPath(char* buf, int buf_len);
	static SetWorkPath(char* path);
	#endif
	
private:	
	Path(const Path&) = delete;
	Path& operator=(const Path&) = delete;
};

}

#endif



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

#ifndef __xfutil_directory_h__
#define __xfutil_directory_h__

#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "xfdb/strutil.h"

namespace xfutil 
{

typedef int (*ListCallback)(const char* dirname, const char* filename, void* arg);

class Directory
{
public:	
	//创建目录，必须保证父目录存在
	static inline bool Create(const char* path)
	{
		mode_t old_mask = umask(0);
		int ret = mkdir(path, 0644);
		umask(old_mask);
		return (ret == 0);
	}
	//static bool CreateR(const char* path);
	//递归移除目录
	static bool Remove(const char* path);
	static inline bool Rename(const char* old_path, const char* new_path)
	{
		return rename(old_path, new_path) == 0;
	}
	static inline bool Exist(const char* path)
	{
		struct stat st;
		return (stat(path, &st) == 0) && S_ISDIR(st.st_mode);
	}
	
	static bool GetWorkPath(char* buf, int buf_size)
	{
		return (getcwd(buf, buf_size) != nullptr);
	}
	static bool SetWorkPath(const char* path)
	{
		return (chdir(path) == 0);
	}
	
	//一层遍历
	static bool List(const char* path, const char* pattern, ListCallback cb, void* arg);
	//多层遍历
	static bool Find(const char* path, const char* pattern, ListCallback cb, void* arg);
};


}  

#endif



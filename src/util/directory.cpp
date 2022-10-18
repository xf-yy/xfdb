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

#include <fnmatch.h>
#include <dirent.h>
#include "xfdb/strutil.h"
#include "directory.h"
#include "path.h"


namespace xfutil 
{

//循环创建，先判断父目录是否存在，不存在则创建
#if 0
bool Directory::CreateR(const char* path)
{
	assert(path != nullptr);
	
	char tmp_path[MAX_PATH_LEN];
	StrCpy(tmp_path, sizeof(tmp_path), path);
	
	char* slash = strchr(tmp_path+1, '/');
	while(slash != nullptr)
	{
		*slash = '\0';
		
		if(access(tmp_path, F_OK) != 0)
		{
			if(mkdir(tmp_path, 0644) == -1 && LastError != EEXIST)
			{
				return false;
			}
		}
		*slash = '/';	//恢复
		slash = strchr(slash+1, '/');
	}
	return Create(tmp_path);
}
#endif

bool Directory::Remove(const char* path)
{
	DIR* dir = opendir(path);
	if(dir == nullptr)
	{
		return false;
	}
	struct dirent* entry;
	while((entry=readdir(dir)) != nullptr)
	{
		if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
		{
			continue;
		}	

		char full_path[MAX_PATH_LEN];
		Path::CombineEx(full_path, sizeof(full_path), path, entry->d_name);
		
		struct stat st;
		if(stat(full_path, &st) != 0)
		{
			closedir(dir);
			return false;
		}	
		if(S_ISDIR(st.st_mode))
		{
			Remove(full_path);
			rmdir(full_path);
		}
		else
		{
			unlink(full_path);
		}
	}
	
	rmdir(path);
	
	closedir(dir);
	return true;
}

bool Directory::List(const char* path, const char* pattern, ListCallback cb, void* arg)
{
	assert(cb != nullptr);

	DIR* dir = opendir(path);
	if(dir == nullptr)
	{
		return false;
	}
	struct dirent* entry;
	while((entry=readdir(dir)) != nullptr)
	{
		if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
		{
			continue;
		}
		int ret = fnmatch(pattern, entry->d_name, 0);//PathMatchSpecW
		if(ret == 0)
		{
			cb(path, entry->d_name, arg);
		}
	}
	
	closedir(dir);
	return true;
}

bool Directory::Find(const char* path, const char* pattern, ListCallback cb, void* arg)
{
	assert(cb != nullptr);

	DIR* dir = opendir(path);
	if(dir == nullptr)
	{
		return false;
	}
	struct dirent* entry;
	while((entry=readdir(dir)) != nullptr)
	{
		if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
		{
			continue;
		}
		char full_path[MAX_PATH_LEN];
		Path::CombineEx(full_path, sizeof(full_path), path, entry->d_name);
		
		struct stat st;
		if(stat(full_path, &st) == 0 && S_ISDIR(st.st_mode))
		{
			Find(full_path, pattern, cb, arg);
		}
		
		int ret = fnmatch(pattern, entry->d_name, 0);//PathMatchSpecW
		if(ret == 0)
		{			
			cb(full_path, entry->d_name, arg);
		}
	}
	
	closedir(dir);
	return true;
}


}  




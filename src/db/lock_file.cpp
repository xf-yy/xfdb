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

#include "lock_file.h"
#include "thread.h"

namespace xfdb 
{

bool LockFile::Create(const std::string& db_path)
{
	char file_path[MAX_PATH_LEN];
	MakeLockFilePath(db_path.c_str(), file_path);

	File file;
	return file.Open(file_path, OF_CREATE);
}

bool LockFile::Exist(const std::string& db_path)
{
	char file_path[MAX_PATH_LEN];
	MakeLockFilePath(db_path.c_str(), file_path);
	return File::Exist(file_path);
}

Status LockFile::Open(const std::string& db_path, LockFlag type/* = LF_NONE*/, int lock_timeout_ms/* = 0*/)
{
	char file_path[MAX_PATH_LEN];
	MakeLockFilePath(db_path.c_str(), file_path);
	
	if(!m_file.Open(file_path, OF_READONLY))
	{
		return ERR_FILE_READ;
	}
	while(type != LF_NONE)
	{
		if(m_file.Lock(type))
		{
			return OK;
		}
		if(lock_timeout_ms <= 0)
		{
			break;
		}
		Thread::Sleep(10);
		lock_timeout_ms -= 10;
	}

	return ERR_FILE_LOCK;
}
	
	

}  



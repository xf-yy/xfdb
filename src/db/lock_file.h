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

#ifndef __xfdb_lock_file_h__
#define __xfdb_lock_file_h__

#include "xfdb/strutil.h"
#include "path.h"
#include "file.h"
#include "file_util.h"

namespace xfdb 
{

class LockFile
{
public:
	LockFile(){}
	~LockFile(){}
	
public:	
	static bool Create(const std::string& db_path);
	static bool Exist(const std::string& db_path);
	
	Status Open(const std::string& db_path, LockFlag type = LF_NONE, int lock_timeout_ms = 0);

private:
	File m_file;
	
private:
	LockFile(const LockFile&) = delete;
	LockFile& operator=(const LockFile&) = delete;

};

}  

#endif


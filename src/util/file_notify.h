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

#ifndef __xfutil_file_notify_h__
#define __xfutil_file_notify_h__

#include <string>
#include "xfdb/util_types.h"
#include "path.h"
#include "directory.h"

namespace xfutil 
{

struct NotifyEvent
{
	uint32_t events;
	char file_path[MAX_PATH_LEN];
};

class FileNotify
{
public:
	FileNotify();
	~FileNotify();
	
public: 
	bool AddPath(const char* path, uint32_t events, bool created_if_missing = true);
	void RemovePath();
	int Read(NotifyEvent* event);
		
private:
	fd_t m_ifd;
	fd_t m_wfd;
	char m_path[MAX_PATH_LEN];
	
	char* m_buf;
	int m_data_size;
	int m_read_off;
};


}

#endif



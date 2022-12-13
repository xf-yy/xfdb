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

#include <unistd.h>
#include "file.h"
#ifdef __linux__
#include <sys/uio.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace xfutil 
{

File::File()
{
	m_fd = INVALID_FD;
}

File::~File()
{
	Close();
}

bool File::Open(const char* file_path, uint32_t flags)
{
    assert(file_path != nullptr && file_path[0] != '\0');
    
	int f = O_LARGEFILE;
	switch(flags & 3)
	{
	case OF_READONLY:	f |= O_RDONLY;	break;
	case OF_WRITEONLY:	f |= O_WRONLY;	break;
	case OF_READWRITE:	f |= O_RDWR;	break;
	}
	if(flags & OF_APPEND)	f |= O_APPEND;
	if(flags & OF_TRUNCATE)	f |= O_TRUNC;
	if(flags & OF_CREATE)	f |= O_CREAT;

    
	mode_t old_mask = umask(0);

	m_fd = open(file_path, f, 0644);
	umask(old_mask);

	return (m_fd != INVALID_FD);
}

//SetFilePointerEx() followed by SetEndOfFile()
	

} 




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

#include "file.h"
#include "file_notify.h"
#include "xfdb/strutil.h"
#include "buffer.h"
#ifdef __linux__
#include <sys/inotify.h>
#include <unistd.h>
#endif

enum {FW_BUF_SIZE = 4096};

namespace xfutil 
{

FileNotify::FileNotify()
{
	m_ifd = inotify_init();
	m_wfd = INVALID_FD;
	
	m_buf = (char*)xmalloc(FW_BUF_SIZE);
	m_data_size = 0;
	m_read_off = 0;
}

FileNotify::~FileNotify()
{
	RemovePath();
	if(m_ifd != INVALID_FD)
	{
		close(m_ifd);
	}
	if(m_buf != nullptr)
	{
		xfree(m_buf);
		m_buf = nullptr;
	}
}

bool FileNotify::AddPath(const char* path, uint32_t events, bool created_if_missing/* = true*/)
{
	assert(m_wfd == INVALID_FD);
	if(m_wfd != INVALID_FD)
	{
		return false;
	}
	if(m_buf == nullptr) 
	{
		return false;
	}
	if(!Directory::Exist(path))
	{
		if(!created_if_missing)
		{
			return false;
		}
		if(!Directory::Create(path))
		{
			return false;
		}
	}
	StrCpy(m_path, sizeof(m_path), path);

	uint32_t tmp_event = 0;
	if(events & FE_CREATE)	tmp_event |= IN_CREATE;
	if(events & FE_OPEN)	tmp_event |= IN_OPEN;
	if(events & FE_CLOSE)	tmp_event |= IN_CLOSE;
	if(events & FE_DELETE)	tmp_event |= IN_DELETE|IN_MOVED_FROM;
	if(events & FE_RENAME)	tmp_event |= IN_MOVED_TO;
	if(events & FE_MODIFY)	tmp_event |= IN_MODIFY;
	
	m_wfd = inotify_add_watch(m_ifd, m_path, tmp_event);
	return (m_wfd != INVALID_FD);
}

void FileNotify::RemovePath()
{
	if(m_wfd != INVALID_FD)
	{
		inotify_rm_watch (m_ifd, m_wfd);
		m_wfd = INVALID_FD;
	}
}

int FileNotify::Read(NotifyEvent* event)
{
	while(m_read_off + (int)sizeof(struct inotify_event) > m_data_size)
	{
		ssize_t n = read(m_ifd, m_buf, FW_BUF_SIZE);
		if(n <= 0)
		{
			return (ErrorNo == EBADF) ? - 1 : 0;
		}
		m_read_off = 0;
		m_data_size = n;
	}
	struct inotify_event* ev = (struct inotify_event*)&m_buf[m_read_off];
	m_read_off += sizeof(struct inotify_event) + ev->len;
	
	event->events = ev->mask;
	Path::Combine(event->file_path, sizeof(event->file_path), m_path, ev->name);

	return 1;
}
		

}  



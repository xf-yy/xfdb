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

#ifndef __xfutil_file_h__
#define __xfutil_file_h__

#include <memory>
#include <ctime>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/uio.h>
#include "xfdb/util_types.h"

namespace xfutil 
{


enum OpenFlag
{
	OF_READONLY = 0x0001,
	OF_WRITEONLY = 0x0002,
	OF_READWRITE = (OF_READONLY|OF_WRITEONLY),
	
	OF_CREATE = 0x0010,			//不存在时则创建
	OF_APPEND = 0x0020,			//追加文件写
	OF_TRUNCATE = 0x0040,		//文件存在+写方式时，文件截断为0
};

enum LockFlag : uint8_t
{
	LF_NONE = 0,
	LF_TRY_READ,
	LF_TRY_WRITE,
};	

struct FileTime
{
#ifdef _WIN32
	second_t create_time;
#else
	second_t change_time;
#endif
	second_t access_time;
	second_t modify_time;
};

enum FileEvent
{
	FE_NONE = 0x00,
	FE_CREATE = 0x01,
  	FE_OPEN = 0x02,
  	FE_CLOSE = 0x04,
	FE_DELETE = 0x10,
	FE_RENAME = 0x20,
	FE_MODIFY = 0x40,
};

class File
{
public:
	File();
	~File();
	
public:	
	inline fd_t GetFD()
	{
		return m_fd;
	}
	//-1:错误，0表示已存在，1表示成功
	bool Open(const char* file_path, uint32_t flags);
	inline void Close()
	{
		if(m_fd != INVALID_FD)
		{
			close(m_fd);
			m_fd = INVALID_FD;
		}
	}
	
	inline int64_t Read(void* buf, size_t buf_size) const
	{
		return read(m_fd, buf, buf_size);
	}
	inline int64_t Read(uint64_t offset, void* buf, size_t buf_size) const
	{
		return pread(m_fd, buf, buf_size, offset);
	}
	inline int64_t Read(uint64_t offset, void* buf1, size_t buf1_size, void* buf2, size_t buf2_size) const
	{
		struct iovec iov[2] = {{(void*)buf1, buf1_size}, {(void*)buf2, buf2_size}};

		return preadv(m_fd, iov, 2, offset);
	}
	
	inline int64_t Write(const void* buf, size_t buf_size)
	{
		return write(m_fd, buf, buf_size);
	}
	inline int64_t Write(const void* buf1, size_t buf1_size, const void* buf2, size_t buf2_size)
	{
		struct iovec iov[2] = {{(void*)buf1, buf1_size}, {(void*)buf2, buf2_size}};
		return writev(m_fd, iov, 2);
	}	
	inline int64_t Write(uint64_t offset, const void* buf, size_t buf_size)
	{
		return pwrite(m_fd, buf, buf_size, offset);
	}
	
	inline bool Sync()
	{
		return fsync(m_fd) == 0;
	}
	inline bool Truncate(size_t new_size)
	{
		return ftruncate(m_fd, new_size) == 0;
	}
	inline bool Allocate(size_t size)
	{
		return fallocate(m_fd, 0, 0, size);
	}
	
	inline bool Seek(int64_t offset)
	{
		return lseek(m_fd, offset, SEEK_SET);
	}
	
	inline int64_t Size() const
	{
		struct stat st;
		return (fstat(m_fd, &st) == 0) ? st.st_size : -1;
	}
	static inline int64_t Size(const char* path)
	{
		struct stat st;
		return (stat(path, &st) == 0) ? st.st_size : -1;
	}
	static inline second_t ModifyTime(const char* path)
	{
		struct stat st;
		return (stat(path, &st) == 0) ? st.st_mtime : 0;
	}
	inline bool Lock(LockFlag type = LF_TRY_READ)	//文件必须被打开
	{
		int oper = LOCK_NB;
		switch(type)
		{
		case LF_TRY_READ: 	oper |= LOCK_SH;	break;
		case LF_TRY_WRITE: 	oper |= LOCK_EX;	break;
		default: 				return true;		break;
		}
		return flock(m_fd, oper) == 0;
	}
	inline bool Unlock()
	{
		return flock(m_fd, LOCK_UN) == 0;
	}
	
	static inline bool Exist(const char *filepath)
	{
		return access(filepath, F_OK) == 0;
	}
	static inline bool Remove(const char *filepath)
	{
		return unlink(filepath) == 0 || errno == ENOENT;
	}
	static inline bool Rename(const char *src_filepath, const char *dst_filepath)
	{
		return rename(src_filepath, dst_filepath) == 0;
	}
	
protected:
	fd_t m_fd;
	
private:
	File(const File&) = delete;
	File& operator=(const File&) = delete;
};

} 

#endif



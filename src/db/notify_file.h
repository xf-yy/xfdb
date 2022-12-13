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

#ifndef __xfdb_notify_file_h__
#define __xfdb_notify_file_h__

#include <atomic>
#include "db_types.h"
#include "file_util.h"
#include "file.h"
#include "notify_msg.h"

namespace xfdb 
{

//文件名：type-pid-seqid
class NotifyFile
{
public:
	NotifyFile();
	~NotifyFile();
	
public:	
	static Status Read(const char* file_path, NotifyData& nd);
	static Status Write(const char* file_dir, const NotifyData& nd, FileName& filename);
	static inline Status Write(const char* file_dir, const NotifyData& nd)
	{
		FileName filename;
		return Write(file_dir, nd, filename);
	}
	static xfutil::tid_t GetNotifyPID(const char* file_path);

private:
	static Status Parse(const byte_t* data, uint32_t size, NotifyData& nd);
	static Status Serialize(const NotifyData& nd, String& str);
	static constexpr uint32_t EstimateSize(const NotifyData& bd);

private:
	static std::atomic<uint32_t> ms_id;
	static const tid_t ms_pid;
	
private:
	NotifyFile(const NotifyFile&) = delete;
	NotifyFile& operator=(const NotifyFile&) = delete;
	
};


}  

#endif


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

#ifndef __xfutil_logger_impl_h__
#define __xfutil_logger_impl_h__

#include <atomic>
#include <stdio.h>
#include <stdarg.h>
#include "path.h"
#include "logger.h"
#include "queue.h"
#include "thread.h"
#include "file.h"
#include "xfdb/strutil.h"
#include "buffer.h"

namespace xfutil 
{

#define MAX_LOG_LEN			(512)


class LoggerImpl
{
public:
	LoggerImpl()
	{
	}
public:
	bool Init(const LogConf& conf);
	void Close();
	
	void LogMsg(LogLevel level, const char *file_name, const char* func_name, int line_num, const char *msg);

private:
	bool ParseConf(const char *conf_file);
	bool Write(const StrView& buf);
	
	static void LogThread(size_t index, void* arg);

private:
	LogConf m_conf;
	FilePtr m_logfile;
	BlockingQueue<StrView> m_buf_queue;
	Thread m_thread;
};


}

#endif

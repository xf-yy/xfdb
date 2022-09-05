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

#include <atomic>
#include <stdio.h>
#include <stdarg.h>
#include "path.h"
#include "logger.h"
#include "logger_impl.h"
#include "queue.h"
#include "thread.h"
#include "file.h"
#include "xfdb/strutil.h"
#include "buffer.h"

namespace xfutil 
{

//格式: <time> <level-desc> [文件名:行号:函数名] <message>

LogLevel Logger::sm_level = LEVEL_MAX;

static std::mutex s_mutex;
static LoggerImpl s_logger;


//初始化，成功返回0，其他值错误码
bool Logger::Init(const LogConf& conf)
{
	std::lock_guard<std::mutex> lock(s_mutex);

	return s_logger.Init(conf);
}

//关闭
void Logger::Close()
{
	std::lock_guard<std::mutex> lock(s_mutex);
	s_logger.Close();
}

void Logger::LogMsg(LogLevel level, const char *file_name, const char* func_name, int line_num, const char *msg_fmt, ...)
{
	char msg[MAX_LOG_LEN];
	
	va_list args;
	va_start(args, msg_fmt);
	vsnprintf(msg, sizeof(msg), msg_fmt, args);
	va_end(args);
	
	s_logger.LogMsg(level, file_name, func_name, line_num, msg);
}




}


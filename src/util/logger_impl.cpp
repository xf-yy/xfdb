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
#include "logger_impl.h"
#include "queue.h"
#include "thread.h"
#include "file.h"
#include "xfdb/strutil.h"
#include "buffer.h"

namespace xfutil 
{

const char* LEVEL_DESC[LogLevel::LEVEL_MAX] = 
{
	"DEBUG",
	"INFO",
	"WARN",
	"ERROR"
	"FATAL",
};

bool LoggerImpl::Write(const StrView& buf)
{
	m_logfile.Write(buf.data, buf.size);
	
	if(m_logfile.Size() >= m_conf.max_file_size)
	{
		m_logfile.Close();
		
		char dst_path[MAX_PATH_LEN];
		Path::Combine(dst_path, sizeof(dst_path), m_conf.file_path.c_str(), ".1");
		File::Remove(dst_path);
		File::Rename(m_conf.file_path.c_str(), dst_path);

		m_logfile.Open(m_conf.file_path.c_str(), OF_WRITEONLY|OF_APPEND|OF_CREATE);
	}
	return true;
}

//一个日志队列，一个单独的写线程(每隔50ms输出下)
void LoggerImpl::LogThread(size_t index, void* arg)
{
	LoggerImpl* logger = (LoggerImpl*)arg;
	
	for(;;)
	{		
		StrView buf;
		logger->m_buf_queue.Pop(buf);
		if(buf.size == 0)	//退出标记
		{
			break;
		}
		logger->Write(buf);
		xfree((void*)buf.data);
	}
}

bool LoggerImpl::Init(const LogConf& conf)
{
	Logger::SetLevel(m_conf.level);
	
	//打开日志文件
	if(!m_logfile.Open(m_conf.file_path.c_str(), OF_WRITEONLY|OF_APPEND|OF_CREATE))
	{
		return false;
	}
	//初始化队列
	m_buf_queue.SetCapacity(100000);
	
	//启用线程
	m_thread.Start(LogThread, nullptr);
	
	return true;
}

void LoggerImpl::Close()
{
	//队列中放入退出标记
	StrView buf;
	m_buf_queue.Push(buf);
	
	//等待线程结束
	m_thread.Join();

	//关闭日志文件
	m_logfile.Close();
}

//格式: [date time] [level-desc] [文件名:行号:函数名] [message]
void LoggerImpl::LogMsg(LogLevel level, const char *file_name, const char* func_name, int line_num, const char *msg)
{
	time_t now = time(nullptr);
	struct tm t_now;
	localtime_r(&now, &t_now);
	
	char name_str[256];
	switch(m_conf.options)
	{
	case OPTION_FILENAME:
		snprintf(name_str, sizeof(name_str), "[%s] ", file_name);
		break;
	case 0: 
	default:
		name_str[0] = '\0';
		break;
	}

	StrView buf;
    buf.data = (char*)xmalloc(MAX_LOG_LEN);
    buf.size = snprintf((char*)buf.data, MAX_LOG_LEN, 
    	"%04d-%02d-%02d %02d:%02d:%02d [%s] %s%s",
    	t_now.tm_year+1900, t_now.tm_mon+1, t_now.tm_mday, 
    	t_now.tm_hour, t_now.tm_min, t_now.tm_sec, 
    	LEVEL_DESC[level],
    	name_str,
    	msg);
    
	m_buf_queue.Push(buf);
}




}


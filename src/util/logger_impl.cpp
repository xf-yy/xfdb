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
#include <sys/time.h>
#include "path.h"
#include "logger_impl.h"
#include "queue.h"
#include "thread.h"
#include "file.h"
#include "xfdb/strutil.h"
#include "buffer.h"
#include "directory.h"

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

#undef BLOCK_SIZE
#define BLOCK_SIZE      8192

#undef CACHE_NUM 
#define CACHE_NUM       1024

#undef QUEUE_CAPACITY
#define QUEUE_CAPACITY  10000

void LoggerImpl::Rename()
{
    //尝试删除最老的，然后更名其他的
    char path[MAX_PATH_LEN], dst_path[MAX_PATH_LEN];
    snprintf(path, sizeof(path), "%s.%u", m_conf.file_path.c_str(), m_conf.max_file_num);
    File::Remove(path);

    for(uint16_t i = m_conf.max_file_num-1; i >= 1; --i)
    {
        snprintf(path, sizeof(path), "%s.%u", m_conf.file_path.c_str(), i);
        snprintf(dst_path, sizeof(dst_path), "%s.%u", m_conf.file_path.c_str(), i+1);

        File::Rename(path, dst_path);
    }

    snprintf(dst_path, sizeof(dst_path), "%s.%u", m_conf.file_path.c_str(), 1);
    File::Rename(m_conf.file_path.c_str(), dst_path);

}

bool LoggerImpl::Reopen()
{
    m_logfile.Close();
    
    Rename();

    m_filesize = 0;
    return m_logfile.Open(m_conf.file_path.c_str(), OF_CREATE|OF_WRITEONLY|OF_APPEND);
}

bool LoggerImpl::Write(const LogData& data)
{
	m_logfile.Write(data.buf, data.len);
    m_filesize += data.len;

    m_pool.Free((byte_t*)data.buf);

	if(m_filesize >= m_conf.max_file_size)
	{
        Reopen();
	}
	return true;
}

//日志线程
void LoggerImpl::LogThread(void* arg)
{
	LoggerImpl* logger = (LoggerImpl*)arg;
	
	for(;;)
	{		
		LogData data;
		logger->m_data_queue.Pop(data);
		if(data.buf == nullptr)	//退出标记
		{
			break;
		}
		logger->Write(data);
	}
}

bool LoggerImpl::Start(const LogConfig& conf)
{	
    m_conf = conf;

	//打开日志文件
	if(!m_logfile.Open(m_conf.file_path.c_str(), OF_CREATE|OF_WRITEONLY|OF_APPEND))
	{
		return false;
	}
    m_filesize = m_logfile.Size();

    m_pool.Init(BLOCK_SIZE, CACHE_NUM);

	//初始化队列
	m_data_queue.SetCapacity(QUEUE_CAPACITY);
	
	//启用线程
	m_thread.Start(LogThread, this);
	
	return true;
}

void LoggerImpl::Close()
{
	//队列中放入退出标记
	LogData data = {nullptr};
	m_data_queue.Push(data);
	
	//等待线程结束
	m_thread.Join();

	//关闭日志文件
	m_logfile.Close();
}

//格式: date time [level-desc] [文件名:行号:函数名] message
void LoggerImpl::LogMsg(LogLevel level, const char *file_name, const char* func_name, int line_num, const char *format, va_list args)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

	struct tm t_now;
	time_t sec = tv.tv_sec;
	localtime_r(&sec, &t_now);
	
    char line_buf[64];
    if(m_conf.flags & FLAG_OUT_LINENUM)
    {
        snprintf(line_buf, sizeof(line_buf), "%d", line_num);
    }
    else
    {
        line_buf[0] = '\0';
    }
    if(m_conf.flags & FLAG_OUT_FILENAME)
    {
        file_name = Path::GetFileName(file_name);
    }

    char* msg = (char*)m_pool.Alloc();
    vsnprintf(msg, BLOCK_SIZE, format, args);

    LogData data;
    data.buf = m_pool.Alloc();

    data.len = snprintf((char*)data.buf, BLOCK_SIZE, 
    	"%04d-%02d-%02d %02d:%02d:%02d-%03lu [%s] [%s:%s:%s] %s\n",
    	t_now.tm_year+1900, t_now.tm_mon+1, t_now.tm_mday, 
    	t_now.tm_hour, t_now.tm_min, t_now.tm_sec, (uint64_t)tv.tv_usec/1000,
    	LEVEL_DESC[level],
        (m_conf.flags & FLAG_OUT_FILENAME) ? file_name : "",
        (m_conf.flags & FLAG_OUT_LINENUM) ? line_buf : "",
        (m_conf.flags & FLAG_OUT_FUNCTION) ? func_name : "",
    	msg);
    
    m_pool.Free((byte_t*)msg);

	m_data_queue.Push(data);
}




}


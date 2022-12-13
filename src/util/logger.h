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

#ifndef __xfutil_logger_h__
#define __xfutil_logger_h__

#include <string>
#include "xfdb/strutil.h"

namespace xfutil 
{

//日志级别
enum LogLevel : uint8_t
{
	LEVEL_DEBUG = 0,
	LEVEL_INFO,
	LEVEL_WARN,
	LEVEL_ERROR,
	LEVEL_FATAL,
	LEVEL_MAX,
};

#define FLAG_OUT_FILENAME			0x01
#define FLAG_OUT_FUNCTION			0x02
#define FLAG_OUT_LINENUM			0x04

struct LogConfig
{
	std::string file_path;
	uint64_t max_file_size = 50 * 1024 * 1024;
	uint16_t max_file_num = 4;
	LogLevel level = LEVEL_INFO;
	uint32_t flags = FLAG_OUT_FILENAME | FLAG_OUT_LINENUM;
	
};

class Logger
{
public:
	//初始化，成功返回0，其他值错误
	static bool Start(const LogConfig& conf);

	//关闭，成功返回0，其他值错误
	static void Close();
	
	//设置日志级别
	static void SetLevel(LogLevel level);

	//获取日志级别
	static LogLevel GetLevel();

	//记录日志，单行长度不超8KB
	static void LogMsg(LogLevel level, const char *file_name, const char* func_name, int line_num, const char *msg_fmt, ...);
	
private:
	Logger(const Logger&) = delete;
	Logger& operator=(const Logger&) = delete;
};
	
#define LogDebug(msg_fmt, ...) \
	if(Logger::GetLevel() <= LEVEL_DEBUG) { \
		Logger::LogMsg(LEVEL_DEBUG, __FILE__, __FUNCTION__, __LINE__, msg_fmt, ##__VA_ARGS__); \
	}

#define LogInfo(msg_fmt, ...) \
	if(Logger::GetLevel() <= LEVEL_INFO) { \
		Logger::LogMsg(LEVEL_INFO, __FILE__, __FUNCTION__, __LINE__, msg_fmt, ##__VA_ARGS__); \
	}

#define LogWarn(msg_fmt, ...) \
    if(Logger::GetLevel() <= LEVEL_WARN) { \
        Logger::LogMsg(LEVEL_WARN, __FILE__, __FUNCTION__, __LINE__, msg_fmt, ##__VA_ARGS__); \
    }

#define LogError(msg_fmt, ...) \
    if(Logger::GetLevel() <= LEVEL_ERROR) { \
        Logger::LogMsg(LEVEL_ERROR, __FILE__, __FUNCTION__, __LINE__, msg_fmt, ##__VA_ARGS__); \
    }

#define LogFatal(msg_fmt, ...) \
	if(Logger::GetLevel() <= LEVEL_FATAL) { \
		Logger::LogMsg(LEVEL_FATAL, __FILE__, __FUNCTION__, __LINE__, msg_fmt, ##__VA_ARGS__); \
        exit(1); \
	}

}


#endif


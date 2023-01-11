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

#include "notify_file.h"
#include "process.h"
#include "coding.h"
#include "file_util.h"

using namespace xfutil;

namespace xfdb 
{

enum
{
	MID_FILE_TYPE = MID_START,
	MID_DB_PATH,
	MID_BUCKET_NAME,
	MID_FILE_ID,
};

std::atomic<uint32_t> NotifyFile::ms_id(1);
const tid_t NotifyFile::ms_pid = Process::GetPid();

Status NotifyFile::Read(const char* file_path, NotifyData& nd)
{
	String str;
	Status s = ReadFile(file_path, str);
	if(s != OK)
	{
		return s;
	}

	return Parse((byte_t*)str.Data(), str.Size(), nd);
}

static const bool ParseNotifyData(const byte_t* data, const byte_t* data_end, NotifyData& nd)
{
	StrView str;
	
	for(;;)
	{
		uint32_t id = DecodeV32(data, data_end);
		switch(id)
		{
		case MID_FILE_TYPE:
			nd.type = (NotifyType)DecodeV32(data, data_end);
			break;
		case MID_DB_PATH:
			str = DecodeString(data, data_end);
			nd.db_path.assign(str.data, str.size);
			break;
		case MID_BUCKET_NAME:
			str = DecodeString(data, data_end);
			nd.bucket_name.assign(str.data, str.size);
			break;
		case MID_FILE_ID:
			nd.file_id = DecodeV64(data, data_end);
			break;
		case MID_END:
			return true;
			break;
		default:
			return false;
			break;
		}
	}
	return false;
}

Status NotifyFile::Parse(const byte_t* data, uint32_t size, NotifyData& nd)
{
	const byte_t* data_end = data + size;
	
	FileHeader header;
	if(!ParseNotifyFileHeader(data, size, header))
	{
		return ERR_FILE_FORMAT;
	}
	return ParseNotifyData(data, data_end, nd) ? OK : ERR_FILE_FORMAT;
}

Status NotifyFile::Serialize(const NotifyData& nd, String& str)
{
	uint32_t esize = EstimateSize(nd);
	if(!str.Reserve(esize))
	{
		return ERR_MEMORY_NOT_ENOUGH;
	}

	byte_t* ptr = WriteNotifyFileHeader((byte_t*)str.Data());

	ptr = EncodeV32(ptr, MID_FILE_TYPE, nd.type);
	ptr = EncodeString(ptr, MID_DB_PATH, nd.db_path.data(), nd.db_path.size());
	ptr = EncodeString(ptr, MID_BUCKET_NAME, nd.bucket_name.data(), nd.bucket_name.size());
	ptr = EncodeV64(ptr, MID_FILE_ID, nd.file_id);
	ptr = EncodeV32(ptr, MID_END);

	ptr = Encode32(ptr, 0);	//FIXME:crc填0
	
	assert(ptr - (byte_t*)str.Data() <= esize);
	str.Resize(ptr - (byte_t*)str.Data());

	return OK;
}

Status NotifyFile::Write(const char* file_dir, const NotifyData& nd, FileName& filename)
{
	String str;
	Status s = Serialize(nd, str);
	if(s != OK)
	{
		return s;
	}
	
	MakeNotifyFileName(ms_pid, ms_id++, filename.str);
	return WriteFile(file_dir, filename.str, str.Data(), str.Size());
}
	
uint32_t constexpr NotifyFile::EstimateSize(const NotifyData& dm)
{
	return FILE_HEAD_SIZE
	      +	MAX_V32_SIZE+MAX_PATH_LEN+MAX_BUCKET_NAME_LEN+MAX_V64_SIZE
		  + MAX_V32_SIZE*5/*5个属性*/
		  + sizeof(uint32_t)/*crc*/;
}

xfutil::tid_t NotifyFile::GetNotifyPID(const char* file_path)
{
	const char* slash = strrchr(file_path, '/');
	const char* file_name = (slash != nullptr) ? slash+1 : file_path;
	
	return strtol(file_name, nullptr, 10);
}

}  


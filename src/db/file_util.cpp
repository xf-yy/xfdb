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

#include <algorithm>
#include "file_util.h"
#include "file.h"
#include "directory.h"
#include "path.h"
#include "coding.h"
#include "xfdb/strutil.h"

using namespace xfutil;

namespace xfdb 
{

#define HEADER_VERSION	1

bool CheckBucketName(const std::string& bucket_name)
{
	if(bucket_name.size() < MIN_BUCKET_NAME_LEN || bucket_name.size() >= MAX_BUCKET_NAME_LEN)
	{
		return false;
	}
	//小写英文数字开头，小写英文数字_-组成
	if(!(isdigit(bucket_name[0]) || islower(bucket_name[0])))
	{
		return false;
	}
	for(size_t i = 1; i < bucket_name.size(); ++i)
	{
		if(islower(bucket_name[i]) || isdigit(bucket_name[i]))
		{
			continue;
		}
		if(bucket_name[i] == '-' || bucket_name[i] == '_')
		{
			continue;
		}
		return false;
	}
	return true;
}

int ListFileCallback(const char* dirname, const char* filename, void* arg)
{
	std::vector<FileName>* names = (std::vector<FileName>*)arg;

	FileName name;
	StrCpy(name.str, sizeof(name.str), filename);
	names->push_back(name);
	return 0;
}

Status ListFile(const char* path, const char* pattern, std::vector<FileName>& names, bool sort)
{	
	names.reserve(16);
	if(!Directory::List(path, pattern, ListFileCallback, &names))
	{
		return ERR_FILE_READ;
	}
	if(sort && names.size() > 1)
	{
		std::sort(names.begin(), names.end(), std::less<FileName>());
	}
	return OK;
}

Status ReadFile(const char* file_path, String& str)
{
	File file;
	if(!file.Open(file_path, OF_READONLY))
	{
		return ERR_PATH_NOT_EXIST;
	}
	return ReadFile(file, str);
}

Status ReadFile(const File& file, String& str)
{
	int64_t file_size = file.Size();
	assert(file_size < 1024*1024*1024);

	return ReadFile(file, 0, file_size, str);
}

Status ReadFile(const File& file, uint64_t offset, int64_t size, String& str)
{
	if(!str.Resize(size))
	{
		return ERR_MEMORY_NOT_ENOUGH;
	}
	int64_t read_size = file.Read(offset, str.Data(), size);
	return (read_size == size) ? OK : ERR_FILE_READ;
}

Status WriteFile(const char* file_path, void* data, int64_t size)
{
	File file;
	if(!file.Open(file_path, OF_WRITEONLY|OF_CREATE))
	{
		return ERR_PATH_CREATE;
	}
	int64_t write_size = file.Write(data, size);
	if(write_size != size)
	{
		return ERR_FILE_WRITE;
	}
	return file.Sync() ? OK : ERR_FILE_WRITE;
}

Status WriteFile(const char* dir_path, const char* filename, void* data, int64_t size)
{
	char tmp_filename[MAX_FILENAME_LEN];
	snprintf(tmp_filename, sizeof(tmp_filename), "~%s", filename);
	
	char tmp_path[MAX_PATH_LEN];
	Path::Combine(tmp_path, sizeof(tmp_path), dir_path, tmp_filename);
	Status s = WriteFile(tmp_path, data, size);
	if(s != OK)
	{
		return s;
	}
	
	char path[MAX_PATH_LEN];
	Path::Combine(path, sizeof(path), dir_path, filename);
	return File::Rename(tmp_path, path) ? OK : ERR_PATH_CREATE;
}


bool ParseHeader(const byte_t* &data, size_t size, const char expect_magic[FILE_MAGIC_SIZE], uint16_t max_version, FileHeader& header)
{
	if(size <= FILE_HEAD_SIZE)
	{
		return false;
	}
	if(memcmp(data, expect_magic, FILE_MAGIC_SIZE) != 0)
	{
		return false;
	}
	memcpy(header.magic, data, FILE_MAGIC_SIZE);

	if(data[FILE_HEADER_VERSION_OFF] != HEADER_VERSION)
	{
		return false;
	}

	const byte_t* ptr = data + FILE_VERSION_OFF;
	header.version = Decode16(ptr);
	header.create_time_s = Decode64(ptr);

	data += FILE_HEAD_SIZE;
	return true;
}

byte_t* WriteHeader(byte_t* data, const char magic[FILE_MAGIC_SIZE], uint16_t version)
{
	memcpy(data, magic, FILE_MAGIC_SIZE);
	memset(data+FILE_MAGIC_SIZE, 0x00, FILE_HEAD_SIZE-FILE_MAGIC_SIZE);

	data[FILE_HEADER_VERSION_OFF] = HEADER_VERSION;

	byte_t* ptr = Encode16(data+FILE_VERSION_OFF, version);

	ptr = Encode64(ptr, (uint64_t)time(nullptr));
	
	return data + FILE_HEAD_SIZE;
}
	
int RemoveTempFile(const char* dir_path)
{
	std::vector<FileName> tmp_files;
	ListFile(dir_path, "~*", tmp_files);

	char path[MAX_PATH_LEN];
	for(const auto filename : tmp_files)
	{
		Path::Combine(path, sizeof(path), dir_path, filename.str);
		File::Remove(path);
	}
	return tmp_files.size();
}


}  



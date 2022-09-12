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

#include "types.h"
#include "coding.h"
#include "path.h"
#include "bucket_meta_file.h"
#include "file_util.h"
#include "segment_file.h"

using namespace xfutil;

namespace xfdb 
{

enum
{
	MID_NEXT_SEGMENT_ID = MID_START,
	MID_SEGMENT_ID,
	MID_L2INDEX_META_SIZE,
	MID_INDEX_FILESIZE,
	MID_DATA_FILESIZE,
};

BucketMetaFile::BucketMetaFile()
{
	m_id = INVALID_FILEID;
}

BucketMetaFile::~BucketMetaFile()
{
}

//
Status BucketMetaFile::Open(const char* bucket_path, fileid_t fileid, LockFlag flag/* = LOCK_NONE*/)
{
	char filename[MAX_FILENAME_LEN];
	MakeBucketMetaFileName(fileid, filename);
	
	return Open(bucket_path, filename, flag);
}

Status BucketMetaFile::Open(const char* bucket_path, const char* filename, LockFlag flag/* = LOCK_NONE*/)
{
	char filepath[MAX_PATH_LEN];
	MakeBucketMetaFilePath(bucket_path, filename, filepath);
	
	assert(m_file.GetFD() == INVALID_FD);
	if(!m_file.Open(filepath, OF_READONLY))
	{
		return ERR_PATH_NOT_EXIST;
	}
	if(!m_file.Lock(flag))
	{
		return ERR_FILE_LOCK;
	}
	m_id = strtoull(filename, nullptr, 10);
	return OK;
}

static const bool ParseSegmentFileInfo(const byte_t*& data, const byte_t* data_end, SegmentFileIndex& info)
{
	for(;;)
	{
		uint32_t id = DecodeV32(data, data_end);
		switch(id)
		{
		case MID_SEGMENT_ID:
			info.segment_fileid = DecodeV64(data, data_end);
			break;
		case MID_L2INDEX_META_SIZE:
			info.L2index_meta_size = DecodeV32(data, data_end);
			break;
		case MID_INDEX_FILESIZE:
			info.index_filesize = DecodeV64(data, data_end);
			break;
		case MID_DATA_FILESIZE:
			info.data_filesize = DecodeV64(data, data_end);
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

static const bool ParseSegmentMeta(const byte_t*& data, const byte_t* data_end, BucketMetaData& md)
{
	for(;;)
	{
		uint32_t id = DecodeV32(data, data_end);
		switch(id)
		{
		case MID_NEXT_SEGMENT_ID:
			md.next_segment_id = DecodeV64(data, data_end);
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

static const bool ParseBucketMetaData(const byte_t*& data, const byte_t* data_end, BucketMetaData& md)
{	
	uint32_t cnt = DecodeV32(data, data_end);
	md.alive_segment_infos.resize(cnt);
	for(uint32_t i = 0; i < cnt; ++i)
	{
		ParseSegmentFileInfo(data, data_end, md.alive_segment_infos[i]);
	}

	cnt = DecodeV32(data, data_end);
	md.deleted_segment_fileids.resize(cnt);
	for(uint32_t i = 0; i < cnt; ++i)
	{
		md.deleted_segment_fileids[i] = DecodeV64(data, data_end);
	}

	return ParseSegmentMeta(data, data_end, md);
}

Status BucketMetaFile::Parse(const byte_t* data, uint32_t size, BucketMetaData& md)
{
	const byte_t* data_end = data + size;
	
	FileHeader header;
	if(!ParseBucketMetaFileHeader(data, size, header))
	{
		return ERR_FILE_FORMAT;
	}
	return ParseBucketMetaData(data, data_end, md) ? OK : ERR_FILE_FORMAT;
}

Status BucketMetaFile::Read(BucketMetaData& md)
{
	String str;
	Status s = ReadFile(m_file, str);
	if(s != OK)
	{
		return s;
	}
	
	return Parse((byte_t*)str.Data(), str.Size(), md);
}

Status BucketMetaFile::Write(const char* bucket_path, fileid_t fileid, BucketMetaData& md)
{
	String str;
	Status s = Serialize(md, str);
	if(s != OK)
	{
		return s;
	}

	char name[MAX_FILENAME_LEN];
	MakeBucketMetaFileName(fileid, name);

	return WriteFile(bucket_path, name, str.Data(), str.Size());
}

Status BucketMetaFile::Serialize(const BucketMetaData& md, String& str)
{
	uint32_t esize = EstimateSize(md);
	if(!str.Reserve(esize))
	{
		return ERR_MEMORY_NOT_ENOUGH;
	}
	
	byte_t* ptr = FillBucketMetaFileHeader((byte_t*)str.Data());
	
	assert(md.alive_segment_infos.size() < 0xFFFFFFFF);
	uint32_t cnt = (uint32_t)md.alive_segment_infos.size();
	ptr = EncodeV32(ptr, cnt);
	for(uint32_t i = 0; i < cnt; ++i)
	{
		const SegmentFileIndex& sinfo = md.alive_segment_infos[i];
		ptr = EncodeV64(ptr, MID_SEGMENT_ID, sinfo.segment_fileid);
		ptr = EncodeV32(ptr, MID_L2INDEX_META_SIZE, sinfo.L2index_meta_size);
		ptr = EncodeV64(ptr, MID_INDEX_FILESIZE, sinfo.index_filesize);
		ptr = EncodeV64(ptr, MID_DATA_FILESIZE, sinfo.data_filesize);
		ptr = EncodeV32(ptr, MID_END);
	}

	assert(md.deleted_segment_fileids.size() < 0xFFFFFFFF);
	cnt = md.deleted_segment_fileids.size();
	ptr = EncodeV32(ptr, cnt);
	for(uint32_t i = 0; i < cnt; ++i)
	{
		ptr = EncodeV64(ptr, md.deleted_segment_fileids[i]);
	}
	
	ptr = EncodeV64(ptr, MID_NEXT_SEGMENT_ID, md.next_segment_id);
	ptr = EncodeV32(ptr, MID_END);

	ptr = Encode32(ptr, 0);	//FIXME:crc填0
	
	assert(ptr < esize + (byte_t*)str.Data());
	str.Resize(ptr - (byte_t*)str.Data());
	return OK;
}

static constexpr uint32_t EstimateSegmentFileInfoSize()
{
	return (MAX_V64_SIZE + MAX_V32_SIZE)*5 /*5个属性*/;
}	
static constexpr uint32_t EstimateSegmentMetaSize()
{
	return (MAX_V32_SIZE + MAX_V64_SIZE)*2 /*2个属性*/;
}	

uint32_t BucketMetaFile::EstimateSize(const BucketMetaData& md)
{
	uint32_t size = FILE_HEAD_SIZE;
	size += MAX_V32_SIZE + md.alive_segment_infos.size() * EstimateSegmentFileInfoSize();
	size += MAX_V32_SIZE + md.deleted_segment_fileids.size() * MAX_V64_SIZE;
	size += EstimateSegmentMetaSize();
	size += sizeof(uint32_t)/*crc*/;
	return size;
}

Status BucketMetaFile::Remove(const char* bucket_path, const char* file_name, bool remove_all)
{
	//尝试写锁打开snapshot，并读取已删除的block文件
	{
		BucketMetaFile mfile;
		Status s = mfile.Open(bucket_path, file_name, LOCK_TRY_WRITE);
		if(s != OK)
		{
			assert(s != ERR_FILE_READ);
			return s;
		}
		BucketMetaData md;
		s = mfile.Read(md);
		if(s != OK)
		{
			assert(s != ERR_FILE_READ);
			return s;
		}
		for(auto id : md.deleted_segment_fileids)
		{
			s = SegmentWriter::Remove(bucket_path, id);
			if(s != OK)
			{
				assert(s != ERR_FILE_READ);
				return s;
			}
		}
		if(remove_all)
		{
			for(auto info : md.alive_segment_infos)
			{
				s = SegmentWriter::Remove(bucket_path, info.segment_fileid);
				if(s != OK)
				{
					assert(s != ERR_FILE_READ);
					return s;
				}
			}
		}
	}
	char file_path[MAX_PATH_LEN];
	Path::Combine(file_path, sizeof(file_path), bucket_path, file_name);
	return File::Remove(file_path) ? OK : ERR_PATH_DELETE;
}

} 



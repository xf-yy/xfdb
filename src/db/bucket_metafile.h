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

#ifndef __xfdb_bucket_metafile_h__
#define __xfdb_bucket_metafile_h__

#include <set>
#include "db_types.h"
#include "file.h"
#include "buffer.h"

namespace xfdb 
{

struct BucketMeta
{		
	std::vector<SegmentStat> alive_segment_stats;
	std::vector<fileid_t> deleted_segment_fileids;
	std::set<fileid_t> max_level_segment_fileids;
	uint16_t max_level_num;							//记录最大level数，创建后固定
	fileid_t next_segment_id;
	objectid_t next_object_id;

	BucketMeta()
	{
		max_level_num = MAX_LEVEL_ID;
		next_segment_id = MIN_FILE_ID;
		next_object_id = MIN_OBJECT_ID;
	}
};

class BucketMetaFile
{
public:
	BucketMetaFile();
	~BucketMetaFile();
	
public:	
	Status Open(const char* bucket_path, fileid_t fileid, LockFlag type = LF_NONE);
	Status Open(const char* bucket_path, const char* filename, LockFlag type = LF_NONE);
	Status Read(BucketMeta& bm);
	inline fileid_t FileID()
	{
		return m_id;
	}
	
	//
	static Status Write(const char* bucket_path, fileid_t fileid, BucketMeta& bm);
	//清理meta中待删除的segment文件
	static Status Clean(const char* bucket_path, const char* file_name);
	//移除meta中待删除的segment文件，并删除meta文件
	static Status Remove(const char* bucket_path, const char* file_name);

private:
	static Status Parse(const byte_t* data, uint32_t size, BucketMeta& bm);
	static Status Serialize(const BucketMeta& bm, String& str);
	static uint32_t EstimateSize(const BucketMeta& bm);

	//清理meta中待删除的segment文件
	static Status Clean(const char* bucket_path, const char* file_name, LockFlag flag);

private:
	File m_file;
	fileid_t m_id;
	
private:
	BucketMetaFile(const BucketMetaFile&) = delete;
	BucketMetaFile& operator=(const BucketMetaFile&) = delete;

};


}  

#endif


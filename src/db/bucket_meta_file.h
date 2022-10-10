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

#ifndef __xfdb_bucket_meta_file_h__
#define __xfdb_bucket_meta_file_h__

#include <set>
#include "types.h"
#include "file.h"
#include "buffer.h"

namespace xfdb 
{

struct BucketMetaData
{		
	std::vector<SegmentIndexInfo> alive_segment_infos;
	std::vector<fileid_t> deleted_segment_fileids;
	std::set<fileid_t> max_level_segment_fileids;
	uint16_t max_level_num;							//记录最大level数，创建后固定
	fileid_t next_segment_id;
	objectid_t next_object_id;

	BucketMetaData()
	{
		max_level_num = MAX_LEVEL_NUM;
		next_segment_id = MIN_FILEID;
		next_object_id = MIN_OBJECTID;
	}
};

class BucketMetaFile
{
public:
	BucketMetaFile();
	~BucketMetaFile();
	
public:	
	Status Open(const char* bucket_path, fileid_t fileid, LockFlag type = LOCK_NONE);
	Status Open(const char* bucket_path, const char* filename, LockFlag type = LOCK_NONE);
	Status Read(BucketMetaData& bmd);
	inline fileid_t FileID()
	{
		return m_id;
	}
	
	//
	static Status Write(const char* bucket_path, fileid_t fileid, BucketMetaData& bmd);
	static Status Remove(const char* bucket_path, const char* file_name, bool remove_all);

private:
	static Status Parse(const byte_t* data, uint32_t size, BucketMetaData& md);
	static Status Serialize(const BucketMetaData& md, String& str);
	static uint32_t EstimateSize(const BucketMetaData& md);

private:
	File m_file;
	fileid_t m_id;
	
private:
	BucketMetaFile(const BucketMetaFile&) = delete;
	BucketMetaFile& operator=(const BucketMetaFile&) = delete;

};


}  

#endif


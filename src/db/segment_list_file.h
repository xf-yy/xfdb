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

#ifndef __xfdb_segment_list_file_h__
#define __xfdb_segment_list_file_h__

#include "types.h"
#include "file.h"
#include "buffer.h"

namespace xfdb 
{

struct SegmentListData
{		
	fileid_t next_segment_id;
	std::vector<SegmentFileIndex> alive_segment_infos;
	std::vector<fileid_t> deleted_segment_fileids;

	SegmentListData()
	{
		next_segment_id = MIN_FILEID;
	}
};

class SegmentListFile
{
public:
	SegmentListFile();
	~SegmentListFile();
	
public:	
	Status Open(const char* bucket_path, fileid_t fileid, LockFlag type = LOCK_NONE);
	Status Open(const char* bucket_path, const char* filename, LockFlag type = LOCK_NONE);
	Status Read(SegmentListData& info);
	inline fileid_t FileId()
	{
		return m_id;
	}
	
	//
	static Status Write(const char* bucket_path, fileid_t fileid, SegmentListData& info);
	static Status Remove(const char* bucket_path, const char* file_name, bool remove_all);

private:
	static Status Parse(const byte_t* data, uint32_t size, SegmentListData& md);
	static uint32_t Serialize(const SegmentListData& md, K4Buffer& buf);
	static uint32_t EstimateSize(const SegmentListData& md);

private:
	File m_file;
	fileid_t m_id;
	
private:
	SegmentListFile(const SegmentListFile&) = delete;
	SegmentListFile& operator=(const SegmentListFile&) = delete;

};


}  

#endif


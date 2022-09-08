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

#ifndef __xfdb_reader_snapshot_h__
#define __xfdb_reader_snapshot_h__

#include <algorithm>
#include <set>
#include "types.h"
#include "segment_file.h"
#include "bucket_meta_file.h"

namespace xfdb 
{

#if 0
class SegmentLevelInfo
{
	std::vector<fileid_t> m_segment_fileids[MAX_LEVEL_NUM];	//按层级fileid升序排完序
	void InitLevel(int segment_cnt[MAX_LEVEL_NUM]);
	//TODO:判断是否可以进行merge了
	bool NeedMerge(std::set<fileid_t>& merging_segment_fileids) const;

	//获取下个合并的段集
	bool GetMergeSegments(MergingSegmentInfo& msi) const;
};
#endif

class TableReaderSnapshot : public std::enable_shared_from_this<TableReaderSnapshot>
{
public:
	TableReaderSnapshot(std::map<fileid_t, TableReaderPtr>& readers);
	~TableReaderSnapshot();

public:	
	Status Get(const StrView& key, ObjectType& type, String& value) const;
	
	IteratorPtr NewIterator();
	
	void GetBucketStat(BucketStat& stat) const;
	
	inline const std::map<fileid_t, TableReaderPtr>& Readers() const
	{
		return m_readers;
	}
	
private:
	std::map<fileid_t, TableReaderPtr> m_readers;

private:
	TableReaderSnapshot(const TableReaderSnapshot&) = delete;
	TableReaderSnapshot& operator=(const TableReaderSnapshot&) = delete;

};


}  

#endif


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

#include "table_reader_snapshot.h"
#include "path.h"
#include "logger.h"
#include "table_writer_snapshot.h"

namespace xfdb 
{

TableReaderSnapshot::TableReaderSnapshot(std::map<fileid_t, TableReaderPtr>& readers)
	: m_readers(readers)
{
}

TableReaderSnapshot::~TableReaderSnapshot()
{
}

Status TableReaderSnapshot::Get(const StrView& key, ObjectType& type, String& value) const
{
	//逆序遍历
	for(auto it = m_readers.rbegin(); it != m_readers.rend(); ++it)
	{
		if(it->second->Get(key, type, value) == OK)
		{
			return OK;
		}
	}
	return ERR_OBJECT_NOT_EXIST;
}

void TableReaderSnapshot::GetBucketStat(BucketStat& stat) const
{	
	stat.segment_stat.count += m_readers.size();
	for(auto it = m_readers.begin(); it != m_readers.end(); ++it)
	{
		stat.segment_stat.size += it->second->Size();
		
		stat.object_stat.Add(it->second->GetObjectStat());
	}
}


#if 0
//判断是否可以进行merge了
bool SegmentLevelInfo::NeedMerge(std::set<fileid_t>& merging_segment_fileids) const
{
	for(int i = 0; i < MAX_LEVEL_NUM; ++i)
	{
		//
	}
	return false;
}

void SegmentLevelInfo::InitLevel(int segment_cnt[MAX_LEVEL_NUM])
{
	//预分配空间
	for(int i = 0; i < MAX_LEVEL_NUM; ++i)
	{
		m_segment_fileids[i].reserve(segment_cnt[i]);
	}
	//for(auto it = m_readers.begin(); it != m_readers.end(); ++it)
	//{
	//	m_segment_fileids[LEVEL_NUM(it->first)].push_back(it->first);
	//}
	//FIXME:是不是不用排序?
	for(int i = 0; i < MAX_LEVEL_NUM; ++i)
	{
		for(int idx = 0; idx < (int)m_segment_fileids[i].size()-1; ++idx)
		{
			assert(m_segment_fileids[i][idx] < m_segment_fileids[i][idx+1]);
		}
	}
}

#endif

}  



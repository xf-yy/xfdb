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

static const std::map<fileid_t, TableReaderPtr> s_empty_readers;

TableReaderSnapshot::TableReaderSnapshot(TableReaderPtr reader, fileid_t fileid, TableReaderSnapshot* prev_snapshot/* = nullptr*/)
	: m_readers(prev_snapshot != nullptr ? prev_snapshot->m_readers : s_empty_readers)
{
	m_readers[fileid] = reader;
}

TableReaderSnapshot::TableReaderSnapshot(const std::map<fileid_t, TableReaderPtr>& new_readers) 
	: m_readers(new_readers)
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

void TableReaderSnapshot::GetStat(BucketStat& stat) const
{	
	for(auto it = m_readers.begin(); it != m_readers.end(); ++it)
	{
		//FIXME: 识别segment类型
		
		stat.segment_stat.Add(it->second->Size());
		it->second->GetStat(stat);
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



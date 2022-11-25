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

#include "dbtypes.h"
#include "table_writer.h"
#include "table_reader_iterator.h"
#include "table_writer_snapshot.h"

namespace xfdb 
{

TableWriterSnapshot::TableWriterSnapshot(TableWriterPtr& mem_table, TableWriterSnapshot* last_snapshot/* = nullptr*/)
{
	if(last_snapshot != nullptr)
	{
		m_memwriters.reserve(last_snapshot->m_memwriters.size() + 1);
		m_memwriters.insert(m_memwriters.end(), last_snapshot->m_memwriters.begin(), last_snapshot->m_memwriters.end());
	}
	m_memwriters.push_back(mem_table);
}

void TableWriterSnapshot::Finish()
{
	assert(!m_memwriters.empty());
	
	//先排序
	for(size_t i = 0; i < m_memwriters.size(); ++i)
	{
		m_memwriters[i]->Finish();
	}

	//找最大的key
	m_upmost_key = m_memwriters[0]->UpmostKey();
	for(size_t i = 1; i < m_memwriters.size(); ++i)
	{
		StrView cmp_key = m_memwriters[i]->UpmostKey();
		if(m_upmost_key.Compare(cmp_key) < 0)
		{
			m_upmost_key = cmp_key;
		}
	}
}

IteratorImplPtr TableWriterSnapshot::NewIterator()
{
	if(m_memwriters.size() == 1)
	{
		return m_memwriters[0]->NewIterator();
	}
	std::vector<IteratorImplPtr> iters;
	iters.reserve(m_memwriters.size());

	for(size_t i = 0; i < m_memwriters.size(); ++i)
	{
		iters.push_back(m_memwriters[i]->NewIterator());
	}
	return NewIteratorSet(iters);
}

Status TableWriterSnapshot::Get(const StrView& key, objectid_t obj_id, ObjectType& type, String& value) const
{
	for(ssize_t idx = (ssize_t)m_memwriters.size() - 1; idx >= 0; --idx)
	{
		if(m_memwriters[idx]->Get(key, obj_id, type, value) == OK)
		{
			return OK;
		}
	}
	return ERR_OBJECT_NOT_EXIST;
}

/**返回segment文件总大小*/
uint64_t TableWriterSnapshot::Size() const
{
	uint64_t size = 0;
	for(size_t i = 0; i < m_memwriters.size(); ++i)
	{
		size += m_memwriters[i]->Size();
	}
	return size;
}

/**获取统计*/
void TableWriterSnapshot::GetStat(BucketStat& stat) const
{
	for(size_t i = 0; i < m_memwriters.size(); ++i)
	{
		//FIXME: 识别memwriter类型？
		m_memwriters[i]->GetStat(stat);
	}
}

}  



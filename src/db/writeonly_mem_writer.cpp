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
#include "writeonly_mem_writer.h"
#include "mem_writer_iterator.h"

namespace xfdb
{

//批量写object的最大数
#define MAX_BATCH_NUM	(4096)

WriteOnlyMemWriter::WriteOnlyMemWriter(BlockPool& pool, uint32_t max_object_num)
	: TableWriter(pool)
{
	m_objects.reserve(max_object_num+MAX_BATCH_NUM/*额外数量*/);
	
#if _DEBUG
	m_sorted = false;
#endif
}

WriteOnlyMemWriter::~WriteOnlyMemWriter()
{
	for(size_t i = 0; i < m_objects.size(); ++i)
	{
		m_objects[i]->~Object();
	}
}


Status WriteOnlyMemWriter::Write(objectid_t start_seqid, const Object* object)
{
	Object* obj = CloneObject(start_seqid, object);
	m_objects.push_back(obj);
	return OK;
}

void WriteOnlyMemWriter::Sort()
{
#if _DEBUG
	m_sorted = true;
#endif
	if(m_objects.size() > 1)
	{
		std::sort(m_objects.begin(), m_objects.end(), ObjectCmp());
	}
}

IteratorPtr WriteOnlyMemWriter::NewIterator()
{
#if _DEBUG
	assert(m_sorted);
#endif
	WriteOnlyMemWriterPtr ptr = std::dynamic_pointer_cast<WriteOnlyMemWriter>(shared_from_this());

	return NewWriteOnlyMemWriterIterator(ptr);
}

//大于最大key的key
StrView WriteOnlyMemWriter::UpmostKey() const
{
#if _DEBUG
	assert(m_sorted);
#endif
	return m_objects.empty() ? StrView() : m_objects.back()->key;
}

//小于最小key的key
StrView WriteOnlyMemWriter::LowestKey() const
{
#if _DEBUG
	assert(m_sorted);
#endif
	return m_objects.empty() ? StrView() : m_objects[0]->key;
}

}  


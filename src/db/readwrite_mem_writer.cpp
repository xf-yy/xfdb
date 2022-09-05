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
#include "readwrite_mem_writer.h"
#include "mem_writer_iterator.h"

namespace xfdb
{

ReadWriteMemWriter::ReadWriteMemWriter(BlockPool& pool, uint32_t max_object_num)
	: TableWriter(pool)
{
}

Status ReadWriteMemWriter::Write(objectid_t start_seqid, const Object* object)
{
	Object* r = CloneObject(start_seqid, object);

	//FIXME:目前覆盖旧值(如果是append类型，则连接起来)
	m_object_map[r->key] = r;
	return OK;
}

void ReadWriteMemWriter::Sort()
{
	//map已经是排序好的，无需再排序
}

Status ReadWriteMemWriter::Get(const StrView& key, ObjectType& type, String& value) const
{
	auto it = m_object_map.find(key);
	if(it == m_object_map.end())
	{
		return ERR_OBJECT_NOT_EXIST;
	}
	type = it->second->type;
	value.Assign(it->second->value.data, it->second->value.size);
	return OK;
}

IteratorPtr ReadWriteMemWriter::NewIterator()
{
	ReadWriteMemWriterPtr ptr = std::dynamic_pointer_cast<ReadWriteMemWriter>(shared_from_this());

	return NewReadWriteMemWriterIterator(ptr);
}

//大于最大key的key
StrView ReadWriteMemWriter::UpmostKey() const
{
	return m_object_map.empty() ? StrView() : m_object_map.rbegin()->first;
}

//小于最小key的key
StrView ReadWriteMemWriter::LowestKey() const
{
	return m_object_map.empty() ? StrView() : m_object_map.begin()->first;
}

}  


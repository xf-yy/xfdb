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
#include "writeonly_writer.h"
#include "writer_iterator.h"

namespace xfdb
{

WriteOnlyWriter::WriteOnlyWriter(BlockPool& pool, uint32_t max_object_num)
	: ObjectWriter(pool)
{
	m_objects.reserve(max_object_num+1024/*额外数量*/);

#if _DEBUG
	m_finished = false;
#endif
}

WriteOnlyWriter::~WriteOnlyWriter()
{
	//FIXME: 无需析构
	#if 0
	for(size_t i = 0; i < m_objects.size(); ++i)
	{
		m_objects[i]->~Object();
	}
	#endif
}


Status WriteOnlyWriter::Write(objectid_t next_seqid, const Object* object)
{
	Object* obj = CloneObject(next_seqid, object);
	m_objects.push_back(obj);
	return OK;
}

Status WriteOnlyWriter::Write(objectid_t next_seqid, const WriteOnlyWriterPtr& memtable)
{
	AddWriter(memtable);

	auto& objs = memtable->m_objects;

	m_objects.reserve(objs.size() + m_objects.size());
	for(size_t i = 0; i < objs.size(); ++i)
	{
		objs[i]->id = next_seqid + i;
		m_objects.push_back(objs[i]);
	}
	return OK;
}

void WriteOnlyWriter::Finish()
{
	if(m_objects.size() > 1)
	{
		std::sort(m_objects.begin(), m_objects.end(), ObjectCmp());
	}
#if _DEBUG
	m_finished = true;
#endif
}

IteratorImplPtr WriteOnlyWriter::NewIterator(objectid_t max_objid)
{
#if _DEBUG
	assert(m_finished);
#endif
	WriteOnlyWriterPtr ptr = std::dynamic_pointer_cast<WriteOnlyWriter>(shared_from_this());

	return NewWriteOnlyWriterIterator(ptr);
}

//大于最大key的key
StrView WriteOnlyWriter::UpmostKey() const
{
#if _DEBUG
	assert(m_finished);
#endif
	return m_objects.empty() ? StrView() : m_objects.back()->key;
}

}  


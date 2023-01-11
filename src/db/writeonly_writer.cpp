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
#include "object_writer_snapshot.h"

namespace xfdb
{

WriteOnlyWriter::WriteOnlyWriter(BlockPool& pool, uint32_t max_object_num)
	: ObjectWriter(pool)
{
	m_objects.reserve(max_object_num+1024/*额外数量*/);
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

    m_max_objid = object->id;
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

    m_max_objid = next_seqid + objs.size() - 1;
	return OK;
}

void WriteOnlyWriter::Finish()
{
	if(m_objects.size() > 1)
	{
		std::sort(m_objects.begin(), m_objects.end(), ObjectCmp());
        m_max_key = m_objects.back()->key;
	}
}

IteratorImplPtr WriteOnlyWriter::NewIterator(objectid_t max_objid)
{
	WriteOnlyWriterPtr ptr = std::dynamic_pointer_cast<WriteOnlyWriter>(shared_from_this());

	return NewWriteOnlyWriterIterator(ptr);
}

WriteOnlyWriterIterator::WriteOnlyWriterIterator(WriteOnlyWriterPtr& table)
	: m_table(table), m_max_num(table->m_objects.size())
{
    m_value.reserve(4096);

    m_max_key = m_table->MaxKey();
    assert(m_max_key.size != 0);

    m_max_objid = m_table->MaxObjectID();
    
	First();
}

void WriteOnlyWriterIterator::First()
{
    m_begin_index = 0;
    GetObject();
}

void WriteOnlyWriterIterator::Seek(const StrView& key)
{
    //不支持seek
    assert(false);
}

void WriteOnlyWriterIterator::Next()
{
    assert(m_begin_index < m_max_num);

    m_begin_index = m_end_index;
    GetObject();
}

bool WriteOnlyWriterIterator::FindSameKey()
{    
	//如果key与后面的相同则跳过
	for(m_end_index = m_begin_index+1; m_end_index < m_max_num; ++m_end_index)
	{
        assert(m_table->m_objects[m_end_index]->key.Compare(m_table->m_objects[m_end_index-1]->key) >= 0);
        if(m_table->m_objects[m_end_index]->key != m_table->m_objects[m_begin_index]->key)
        {
            break;
        }
	}
    return m_begin_index < m_max_num;
};

void WriteOnlyWriterIterator::GetObject()
{
    if(!FindSameKey())
    {
        return;
    }
    m_obj_ptr = m_table->m_objects[m_begin_index];
    if(m_begin_index+1 == m_end_index || m_obj_ptr->type != AppendType)
    {
        return;
    }

    m_obj = *m_obj_ptr;

    ssize_t idx;
    for(idx = m_begin_index+1; idx < m_end_index; ++idx)
    {
        Object* obj = m_table->m_objects[idx];

        if(obj->type == SetType)
        {
            m_obj.type = SetType;
            ++idx;
            break;
        }
        else if(obj->type == DeleteType)
        {
            m_obj.type = SetType;
            break;
        }
    }
    assert(idx <= m_end_index);

    m_value.clear();
    for(--idx; idx >= m_begin_index; --idx)
    {
        Object* obj = m_table->m_objects[idx];
        m_value.append(obj->value.data, obj->value.size);
    }

    m_obj.value.Set(m_value.data(), m_value.size());

	m_obj_ptr = &m_obj;
}

}  


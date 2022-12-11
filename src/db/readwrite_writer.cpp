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

#include <math.h>
#include <algorithm>
#include "readwrite_writer.h"
#include "writeonly_writer.h"
#include "object_writer_list.h"

namespace xfdb
{

#define LEVEL_BRANCH 4

ReadWriteWriter::ReadWriteWriter(BlockPool& pool, uint32_t max_object_num) 
    : ObjectWriter(pool), LEVEL_BF(RAND_MAX / LEVEL_BRANCH), MAX_LEVEL_NUM(1 + logf(max_object_num) / logf(LEVEL_BRANCH))
{
    m_max_level = 1;

    m_head = NewNode(nullptr, MAX_LEVEL_NUM);
    for (int i = 0; i < MAX_LEVEL_NUM; ++i) 
    {
        m_head->SetNext(i, nullptr);
    }
}

Status ReadWriteWriter::Get(const StrView& key, objectid_t obj_id, ObjectType& type, String& value) const
{
    Object dst_obj(DeleteType, obj_id, key);

    SkipListNode* node = LowerBound(dst_obj);
    if(node == nullptr || key != node->object->key)
    {
        return ERR_OBJECT_NOT_EXIST;
    }
    type = node->object->type;
    value.Assign(node->object->value.data, node->object->value.size);
    return OK;
}

Status ReadWriteWriter::Write(objectid_t next_seqid, const Object* object)
{
    Object* obj = CloneObject(next_seqid, object);   
    return Write(obj);
}

Status ReadWriteWriter::Write(const Object* obj)
{	
    SkipListNode* prev[MAX_LEVEL_NUM];
    SkipListNode* node = LowerBound(*obj, prev);

    assert(node == nullptr || obj->Compare(node->object) != 0);

    int level = RandomLevel();
    if (level > GetMaxLevel()) 
    {
        for (int i = GetMaxLevel(); i < level; i++) 
        {
            prev[i] = m_head;
        }
        m_max_level.store(level, std::memory_order_relaxed);
    }

    node = NewNode(obj, level);
    for (int i = 0; i < level; ++i) 
    {
        node->NoBarrierSetNext(i, prev[i]->NoBarrierNext(i));
        prev[i]->SetNext(i, node);
    }
    return OK;
}

Status ReadWriteWriter::Write(objectid_t next_seqid, const WriteOnlyWriterPtr& memtable)
{
	AddWriter(memtable);

	auto& objs = memtable->m_objects;
	for(size_t i = 0; i < objs.size(); ++i)
	{
		objs[i]->id = next_seqid + i;
        Write(objs[i]);
	}
	return OK;
}

IteratorImplPtr ReadWriteWriter::NewIterator(objectid_t max_objid)
{
    ReadWriteWriterPtr ptr = std::dynamic_pointer_cast<ReadWriteWriter>(shared_from_this());
    return NewReadWriteWriterIterator(ptr, max_objid);
}

//大于最大key的key
StrView ReadWriteWriter::UpmostKey() const
{
    SkipListNode* node = Last();
    return (node != m_head) ? node->object->key : StrView();
}

SkipListNode* ReadWriteWriter::Last() const
{
    SkipListNode* node = m_head;

    for(int level = GetMaxLevel() - 1; level >= 0; --level)
    {
        SkipListNode* next = node->Next(level);
        while(next != nullptr) 
        {
            node = next;

            next = node->Next(level);
        }
    }

    return node;
}

SkipListNode* ReadWriteWriter::LowerBound(const Object& obj, SkipListNode** prev) const
{
    assert(prev != nullptr);

    SkipListNode* node = m_head;
    
    for(int level = GetMaxLevel() - 1; level >= 0; --level) 
    {
        SkipListNode* next = node->Next(level);
        while (next != nullptr && next->object->Compare(&obj) < 0) 
        {
            node = next;

            next = node->Next(level);
        } 
        prev[level] = node;
    }
    assert(node != nullptr);
    return node->Next(0);
}

int ReadWriteWriter::RandomLevel()
{
    int level = 1;
    while (level < MAX_LEVEL_NUM && rand() <= LEVEL_BF) 
    {
        ++level;
    }
    return level;
}

StrView ReadWriteWriterIterator::UpmostKey() const
{
	return m_table->UpmostKey();
}

/**移到第1个元素处*/
void ReadWriteWriterIterator::First()
{
    m_node = m_table->m_head->Next(0);
    while(m_node != nullptr && m_node->object->id > m_max_objid)
    {
        Next();
    }
}

void ReadWriteWriterIterator::Seek(const StrView& key)
{
    Object dst_obj(DeleteType, m_max_objid, key);
    m_node = m_table->LowerBound(dst_obj);

    //return (m_node != nullptr) ? OK : ERR_OBJECT_NOT_EXIST;
}

void ReadWriteWriterIterator::Next()
{
    assert(m_node != nullptr);

	//如果key与后面的相同或id大于m_max_objid则跳过
    SkipListNode* prev_node = m_node;
    m_node = m_node->Next(0);

	while(m_node != nullptr)
	{
        assert(m_node->object->key.Compare(prev_node->object->key) >= 0);
        if(m_node->object->id > m_max_objid || m_node->object->key == prev_node->object->key)
        {
            m_node = m_node->Next(0);
        }
        else
        {
            break;
        }
	}
}

const Object& ReadWriteWriterIterator::object() const
{
    return *m_node->object;
}


}  


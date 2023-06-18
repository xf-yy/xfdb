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
#include "readwrite_objectwriter.h"
#include "writeonly_objectwriter.h"
#include "object_writer_snapshot.h"

namespace xfdb
{

#define LEVEL_BRANCH 4

static const SkipListNode* FindSameKey(objectid_t max_object_id, const SkipListNode* node, std::vector<const SkipListNode*>& nodes)
{
    assert(node != nullptr);
    nodes.push_back(node);

	//如果key与后面的相同或id大于m_max_objid则跳过
    const SkipListNode* prev_node = node;
    node = node->Next(0);

	while(node != nullptr)
	{
        assert(node->object->key.Compare(prev_node->object->key) >= 0);
        if(node->object->id > max_object_id)
        {
            node = node->Next(0);
        }
        else if(node->object->key == prev_node->object->key)
        {
            nodes.push_back(node);
            node = node->Next(0);            
        }
        else
        {
            break;
        }
	}
    return node;
}

ReadWriteObjectWriter::ReadWriteObjectWriter(BlockPool& pool, uint32_t max_object_num) 
    : ObjectWriter(pool), LEVEL_BF(RAND_MAX / LEVEL_BRANCH), MAX_LEVEL_NUM(1 + logf(max_object_num) / logf(LEVEL_BRANCH))
{
    m_max_level = 1;

    m_head = NewNode(nullptr, MAX_LEVEL_NUM);
    for (int i = 0; i < MAX_LEVEL_NUM; ++i) 
    {
        m_head->SetNext(i, nullptr);
    }
}

Status ReadWriteObjectWriter::Get(const StrView& key, objectid_t obj_id, ObjectType& type, std::string& value) const
{
    Object dst_obj(MaxObjectType, obj_id, key);

    SkipListNode* node = LowerBound(dst_obj);
    if(node == nullptr || node->object->key != key)
    {
        return ERR_OBJECT_NOT_EXIST;
    }
    type = node->object->type;
    if(node->object->type != AppendType)
    {
        value.assign(node->object->value.data, node->object->value.size);
        return OK;
    }

    //append类型
    std::vector<const SkipListNode*> nodes;
    FindSameKey(obj_id, node, nodes);

    assert(!nodes.empty());

    ssize_t idx = 1;
    while(idx < (ssize_t)nodes.size())
    {
        const Object* obj = nodes[idx++]->object;

        if(obj->type == SetType)
        {
            type = SetType;
            break;
        }
        else if(obj->type == DeleteType)
        {
            type = SetType;
            --idx;
            break;
        }
    }
    assert(idx <= (ssize_t)nodes.size());

    value.clear();
    for(--idx; idx >= 0; --idx)
    {
        const Object* obj = nodes[idx]->object;
        value.append(obj->value.data, obj->value.size);
    }

    return OK;
}

Status ReadWriteObjectWriter::Write(objectid_t next_seqid, const Object* object)
{
    Object* obj = CloneObject(next_seqid, object);   
    return Write(obj);
}

Status ReadWriteObjectWriter::Write(const Object* obj)
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

    m_max_object_id = obj->id;
    return OK;
}

Status ReadWriteObjectWriter::Write(objectid_t next_seqid, const WriteOnlyObjectWriterPtr& memtable)
{
	AddWriter(memtable);

	auto& objs = memtable->m_objects;
	for(size_t i = 0; i < objs.size(); ++i)
	{
		objs[i]->id = next_seqid + i;
        Write(objs[i]);
	}
    m_max_object_id = next_seqid + objs.size() - 1;
	return OK;
}

IteratorImplPtr ReadWriteObjectWriter::NewIterator(objectid_t max_object_id)
{
    //NOTE: 可能Writer未Finish
    if(m_max_key.Empty())
    {
        Finish();
    }
    ReadWriteObjectWriterPtr ptr = std::dynamic_pointer_cast<ReadWriteObjectWriter>(shared_from_this());
    return NewReadWriteObjectWriterIterator(ptr, max_object_id);
}

//大于最大key的key
void ReadWriteObjectWriter::Finish()
{
    SkipListNode* node = Last();
    m_max_key = (node != m_head) ? node->object->key : StrView();
    assert(m_max_key.size != 0);
}

SkipListNode* ReadWriteObjectWriter::Last() const
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

SkipListNode* ReadWriteObjectWriter::LowerBound(const Object& obj, SkipListNode** prev) const
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

int ReadWriteObjectWriter::RandomLevel()
{
    int level = 1;
    while (level < MAX_LEVEL_NUM && rand() <= LEVEL_BF) 
    {
        ++level;
    }
    return level;
}

//////////////////////////////////////////////////////////////////////
ReadWriteObjectWriterIterator::ReadWriteObjectWriterIterator(ReadWriteObjectWriterPtr& table, objectid_t max_object_id) 
    : m_table(table)
{
    m_nodes.reserve(16);
    m_value.reserve(4096);

    m_max_key = m_table->MaxKey();
    assert(m_max_key.size != 0);    

    m_max_object_id = max_object_id;

    First();
}

/**移到第1个元素处*/
void ReadWriteObjectWriterIterator::First()
{
    m_next_node = m_table->m_head->Next(0);

    GetObject();
}

void ReadWriteObjectWriterIterator::Seek(const StrView& key)
{
    Object dst_obj(MaxObjectType, m_max_object_id, key);
    m_next_node = m_table->LowerBound(dst_obj);

    GetObject();
}

void ReadWriteObjectWriterIterator::Next()
{
    GetObject();
}

void ReadWriteObjectWriterIterator::GetObject()
{
    if(!FindSameKey())
    {
        return;
    }
    assert(!m_nodes.empty());
    m_obj_ptr = m_nodes[0]->object;
    if(m_nodes.size() == 1 || m_obj_ptr->type != AppendType)
    {
        return;
    }

    m_obj = *m_obj_ptr;

    ssize_t idx;
    for(idx = 1; idx < (ssize_t)m_nodes.size(); ++idx)
    {
        const Object* obj = m_nodes[idx]->object;

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
    assert(idx <= (ssize_t)m_nodes.size());

    m_value.clear();
    for(--idx; idx >= 0; --idx)
    {
        const Object* obj = m_nodes[idx]->object;
        m_value.append(obj->value.data, obj->value.size);
    }

    m_obj.value.Set(m_value.data(), m_value.size());

	m_obj_ptr = &m_obj;
}

bool ReadWriteObjectWriterIterator::FindSameKey()
{
    m_nodes.clear();
    if(m_next_node == nullptr)
    {
        return false;
    }
    m_next_node = xfdb::FindSameKey(m_max_object_id, m_next_node, m_nodes);
    return true;
}


}  


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

#ifndef __xfdb_readwrite_writer_h__
#define __xfdb_readwrite_writer_h__

#include <map>
#include <map>
#include "dbtypes.h"
#include "object_writer.h"


namespace xfdb
{

struct SkipListNode 
{
    const Object* object;
    std::atomic<SkipListNode*> next[0];   //level

    explicit SkipListNode(const Object* obj) : object(obj) 
    {}

    SkipListNode* Next(int level) 
    {
        return this->next[level].load(std::memory_order_acquire);
    }
    void SetNext(int level, SkipListNode* n) 
    {
        this->next[level].store(n, std::memory_order_release);
    }

    SkipListNode* NoBarrierNext(int level) 
    {
        return this->next[level].load(std::memory_order_relaxed);
    }
    void NoBarrierSetNext(int level, SkipListNode* n) 
    {
        this->next[level].store(n, std::memory_order_relaxed);
    }

};

class ReadWriteWriter : public ObjectWriter
{
public:
    ReadWriteWriter(BlockPool& pool, uint32_t max_object_num);

	virtual Status Get(const StrView& key, objectid_t obj_id, ObjectType& type, String& value) const override;

	virtual Status Write(objectid_t next_seqid, const Object* object) override;
	virtual Status Write(objectid_t next_seqid, const WriteOnlyWriterPtr& memtable) override;
	
	virtual IteratorImplPtr NewIterator(objectid_t max_objid = MAX_OBJECT_ID) override;
	
	//大于最大key的key
	virtual StrView UpmostKey() const override;

private:
    inline int GetMaxLevel() const 
    {
        return m_max_level.load(std::memory_order_relaxed);
    }

    int RandomLevel();

    SkipListNode* NewNode(const Object* obj, int level)
    {
        byte_t* node_buf = m_buf.Write(sizeof(SkipListNode) + sizeof(std::atomic<SkipListNode*>) * level);
        return new (node_buf) SkipListNode(obj);
    }

    SkipListNode* LowerBound(const Object& obj, SkipListNode** prev) const;
    SkipListNode* LowerBound(const Object& obj) const
    {
        SkipListNode* prev[MAX_LEVEL_NUM];
        return LowerBound(obj, prev);
    }
    
    SkipListNode* Last() const;

    Status Write(const Object* obj);

private:
    const int LEVEL_BF;
    const int MAX_LEVEL_NUM;

    std::atomic<int> m_max_level;
    SkipListNode* m_head;

private:
    friend class ReadWriteWriterIterator;
    ReadWriteWriter(const ReadWriteWriter&) = delete;
    ReadWriteWriter& operator=(const ReadWriteWriter&) = delete;
};



}  

#endif 


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
#include "db_types.h"
#include "object_writer.h"


namespace xfdb
{

struct SkipListNode 
{
    const Object* object;
    std::atomic<SkipListNode*> next[0];   //level

    explicit SkipListNode(const Object* obj) : object(obj) 
    {}

    SkipListNode* Next(int level) const
    {
        return this->next[level].load(std::memory_order_acquire);
    }
    void SetNext(int level, SkipListNode* n) 
    {
        this->next[level].store(n, std::memory_order_release);
    }

    SkipListNode* NoBarrierNext(int level) const
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

	virtual Status Get(const StrView& key, objectid_t obj_id, ObjectType& type, std::string& value) const override;

	virtual Status Write(objectid_t next_seqid, const Object* object) override;
	virtual Status Write(objectid_t next_seqid, const WriteOnlyWriterPtr& memtable) override;
	virtual void Finish() override;

	virtual IteratorImplPtr NewIterator(objectid_t max_objid = MAX_OBJECT_ID) override;

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

class ReadWriteWriterIterator : public IteratorImpl 
{
public:
	ReadWriteWriterIterator(ReadWriteWriterPtr& table, objectid_t max_objid);

	virtual ~ReadWriteWriterIterator()
    {}

public:	
	/**移到第1个元素处*/
	virtual void First() override;
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) override;
	
	/**向后移到一个元素*/
	virtual void Next() override;

	/**是否还有下一个元素*/
	virtual bool Valid() const override
    {
        return !m_nodes.empty();
    }

private:
    bool FindSameKey();
    void GetObject();

private:
	ReadWriteWriterPtr m_table;

	const SkipListNode* m_next_node;
	std::vector<const SkipListNode*> m_nodes;
    Object m_obj;
    std::string m_value;

private:
	ReadWriteWriterIterator(const ReadWriteWriterIterator&) = delete;
	ReadWriteWriterIterator& operator=(const ReadWriteWriterIterator&) = delete;
};

}  

#endif 


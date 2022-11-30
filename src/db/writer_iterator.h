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

#ifndef __xfdb_writer_iterator_h__
#define __xfdb_writer_iterator_h__

#include <map>
#include "dbtypes.h"
#include "iterator_impl.h"

namespace xfdb 
{

class WriteOnlyWriterIterator : public IteratorImpl 
{
public:
	WriteOnlyWriterIterator(WriteOnlyWriterPtr& table);
	virtual ~WriteOnlyWriterIterator()
    {}

public:
	/**移到第1个元素处*/
	virtual void First() override
	{
		m_index = 0;
	}
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) override
    {}
	
	/**向后移到一个元素*/
	virtual void Next() override;

	/**是否还有下一个元素*/
	virtual bool Valid() const override
	{
		return m_index < m_max_num;
	}
	virtual const Object& object() const override;

	virtual StrView UpmostKey() const override;

private:
	WriteOnlyWriterPtr m_table;

	const size_t m_max_num;
	size_t m_index;
	
private:
	WriteOnlyWriterIterator(const WriteOnlyWriterIterator&) = delete;
	WriteOnlyWriterIterator& operator=(const WriteOnlyWriterIterator&) = delete;
};


class ReadWriteWriterIterator : public IteratorImpl 
{
public:
	ReadWriteWriterIterator(ReadWriteWriterPtr& table, objectid_t max_objid) 
        : m_table(table), m_max_objid(max_objid)
    {
        First();
    }
	virtual ~ReadWriteWriterIterator()
    {}

public:	
	/**移到第1个元素处*/
	virtual void First() override;
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) override
    {}
	
	/**向后移到一个元素*/
	virtual void Next() override;

	/**是否还有下一个元素*/
	virtual bool Valid() const override
    {
        return m_node != nullptr;
    }

	virtual const Object& object() const override;	

	virtual StrView UpmostKey() const override;

private:
	ReadWriteWriterPtr m_table;
    objectid_t m_max_objid;
	SkipListNode* m_node;
	
private:
	ReadWriteWriterIterator(const ReadWriteWriterIterator&) = delete;
	ReadWriteWriterIterator& operator=(const ReadWriteWriterIterator&) = delete;
};


} 

#endif


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

#ifndef __xfdb_writeonly_writer_h__
#define __xfdb_writeonly_writer_h__

#include "dbtypes.h"
#include "buffer.h"
#include "object_writer.h"

namespace xfdb
{

//顺序追加
class WriteOnlyWriter : public ObjectWriter
{
public:
	explicit WriteOnlyWriter(BlockPool& pool, uint32_t max_object_num);
	~WriteOnlyWriter();

public:	
	virtual Status Write(objectid_t next_seqid, const Object* object) override;
	virtual Status Write(objectid_t next_seqid, const WriteOnlyWriterPtr& memtable) override;
	virtual void Finish() override;
	
	virtual IteratorImplPtr NewIterator(objectid_t max_objid = MAX_OBJECT_ID) override;
	
	//大于最大key的key
	virtual StrView UpmostKey() const override;

protected:
	std::vector<Object*> m_objects;

	#if _DEBUG
	bool m_finished;
	#endif

private:
	friend class WriteOnlyWriterIterator;
	friend class ReadWriteWriter;
	
	WriteOnlyWriter(const WriteOnlyWriter&) = delete;
	WriteOnlyWriter& operator=(const WriteOnlyWriter&) = delete;
};

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
	virtual void Seek(const StrView& key) override;
	
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

}  

#endif 


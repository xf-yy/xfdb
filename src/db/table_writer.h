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

#ifndef __xfdb_table_writer_h__
#define __xfdb_table_writer_h__

#include <list>
#include "buffer.h"
#include "xfdb/strutil.h"
#include "types.h"
#include "iterator.h"
#include "table_reader.h"

namespace xfdb
{

class TableWriter : public TableReader
{
public:
	explicit TableWriter(BlockPool& pool);
	virtual ~TableWriter();

public:
	virtual Status Write(objectid_t start_seqid, const Object* object) = 0;
	virtual Status Write(objectid_t start_seqid, const WriteOnlyMemWriterPtr& memtable) = 0;
	virtual void Finish() = 0;
	
	virtual Status Get(const StrView& key, ObjectType& type, String& value) const
	{
		return ERR_INVALID_MODE;
	}

	inline uint64_t Size() const
	{
		return m_buf.Usage() + m_ex_size;
	}	
	
	void GetStat(BucketStat& stat) const
	{
		stat.memwriter_stat.Add(Size());
		stat.object_stat.Add(m_object_stat);
	}

	///////////////////////////////////////////////////////
	inline uint64_t GetObjectCount()
	{
		return m_object_stat.Count();
	}
	
	/**返回消逝的时间，单位秒*/
	inline second_t ElapsedTime() const
	{
		return ((second_t)time(nullptr) - m_create_time);
	}
	
protected:	
	Object* CloneObject(objectid_t seqid, const Object* object);
	void AddWriter(TableWriterPtr writer)
	{
		m_ex_writers.push_back(writer);
		m_ex_size += writer->m_ex_size;
		m_object_stat.Add(writer->m_object_stat);
	}

private:	
	StrView CloneString(const StrView& str);

protected:
	const second_t m_create_time;		//创建时间，秒

	ObjectStat m_object_stat;			//统计值
	WriteBuffer m_buf;					//内存分配器

	uint64_t m_ex_size;
	std::list<TableWriterPtr> m_ex_writers;

private:
	TableWriter(const TableWriter&) = delete;
	TableWriter& operator=(const TableWriter&) = delete;
};

}  

#endif 


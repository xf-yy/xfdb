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

#ifndef __xfdb_table_writer_snapshot_h__
#define __xfdb_table_writer_snapshot_h__

#include <deque>
#include <map>
#include "types.h"
#include "xfdb/strutil.h"
#include "table_reader.h"

namespace xfdb 
{

class TableWriterSnapshot : public TableReader
{
public:
	TableWriterSnapshot(TableWriterPtr& mem_table,         TableWriterSnapshot* last_snapshot = nullptr);
	~TableWriterSnapshot(){}
	
public:	
	void Sort();
	
	Status Get(const StrView& key, ObjectType& type, String& value) const override;
	
	IteratorPtr NewIterator() override;

	/**最大key*/
	StrView UpmostKey() const override
	{
		return m_upmost_key;
	}
	/**最小key*/
	StrView LowestKey() const  override
	{
		return m_lowest_key;
	}
	
	/**返回segment文件总大小*/
	uint64_t Size() const override;

	/**获取统计*/
	void GetStat(BucketStat& stat) const override;

	/////////////////////////////////////////////////////////
	/**最大object id*/
	inline objectid_t GetMaxObjectID()
	{
		return m_max_objectid;
	}
		
private:
	std::vector<TableWriterPtr> m_memwriters;
	StrView m_upmost_key;
	StrView m_lowest_key;
	objectid_t m_max_objectid;
	
	friend class TableReadersIterator;
	
private:
	TableWriterSnapshot(const TableWriterSnapshot&) = delete;
	TableWriterSnapshot& operator=(const TableWriterSnapshot&) = delete;
};


}  

#endif


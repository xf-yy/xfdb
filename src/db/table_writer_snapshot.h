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

#ifndef __xfdb_memwriter_snapshot_h__
#define __xfdb_memwriter_snapshot_h__

#include <deque>
#include <map>
#include "types.h"
#include "xfdb/strutil.h"
#include "table_reader.h"

namespace xfdb 
{

class TableWriterSnapshot : public std::enable_shared_from_this<TableWriterSnapshot>
{
public:
	TableWriterSnapshot(TableWriterPtr& mem_table,        TableWriterSnapshot* last_snapshot = nullptr);
	~TableWriterSnapshot(){}
	
public:	
	Status Get(const StrView& key, ObjectType& type, String& value) const;
	
	IteratorPtr NewIterator();
	
	inline const std::vector<TableWriterPtr>& TableWriters() const
	{
		return m_memwriters;
	}
		
private:
	std::vector<TableWriterPtr> m_memwriters;
	friend class TableWriterSnapshotIterator;
	
private:
	TableWriterSnapshot(const TableWriterSnapshot&) = delete;
	TableWriterSnapshot& operator=(const TableWriterSnapshot&) = delete;
};


}  

#endif


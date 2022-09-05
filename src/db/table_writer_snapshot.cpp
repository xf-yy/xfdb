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

#include "table_writer_snapshot.h"
#include "table_writer.h"
#include "mem_writer_iterator.h"
#include "types.h"

namespace xfdb 
{

TableWriterSnapshot::TableWriterSnapshot(TableWriterPtr& mem_table,        TableWriterSnapshot* last_snapshot/* = nullptr*/)
{
	if(last_snapshot != nullptr)
	{
		m_memwriters.reserve(last_snapshot->m_memwriters.size() + 1);
		m_memwriters = last_snapshot->m_memwriters;
	}
	m_memwriters.push_back(mem_table);
}

IteratorPtr TableWriterSnapshot::NewIterator()
{
	TableWriterSnapshotPtr ptr = std::dynamic_pointer_cast<TableWriterSnapshot>(shared_from_this());
	return NewTableWriterSnapshotIterator(ptr);
}

Status TableWriterSnapshot::Get(const StrView& key, ObjectType& type, String& value) const
{
	for(ssize_t idx = (ssize_t)m_memwriters.size() - 1; idx >= 0; --idx)
	{
		if(m_memwriters[idx]->Get(key, type, value) == OK)
		{
			return OK;
		}
	}
	return ERR_OBJECT_NOT_EXIST;
}


}  



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

#include "types.h"
#include "mem_writer_iterator.h"
#include "writeonly_mem_writer.h"
#include "readwrite_mem_writer.h"
#include "table_writer_snapshot.h"

namespace xfdb 
{

WriteOnlyMemWriterIterator::WriteOnlyMemWriterIterator(WriteOnlyMemWriterPtr& table)
	: m_table(table)
{
	m_max_num = table->m_objects.size();
	First();
}

StrView WriteOnlyMemWriterIterator::UpmostKey()    const
{
	return m_table->UpmostKey();
}

//void WriteOnlyMemWriterIterator::Seek(const StrView& key)
//{};

void WriteOnlyMemWriterIterator::Next()
{
	//如果key与后面的相同则跳过
	while(m_index < m_max_num)
	{
		if(++m_index < m_max_num)
		{
			assert(m_table->m_objects[m_index]->key.Compare(m_table->m_objects[m_index-1]->key) >= 0);
			if(m_table->m_objects[m_index]->key != m_table->m_objects[m_index-1]->key)
			{
				break;
			}
		}
	}
};

const Object& WriteOnlyMemWriterIterator::object() const
{
	return *m_table->m_objects[m_index];
}

ReadWriteMemWriterIterator::ReadWriteMemWriterIterator(ReadWriteMemWriterPtr& table)
	: m_table(table)
{
	First();
}

StrView ReadWriteMemWriterIterator::UpmostKey()    const
{
	return m_table->UpmostKey();
}

/**移到第1个元素处*/
void ReadWriteMemWriterIterator::First()
{
	m_iter = m_table->m_objects.begin();
}
/**移到最后1个元素处*/
//virtual void Last() = 0;

/**移到到>=key的地方*/
//void ReadWriteMemWriterIterator::Seek(const StrView& key)
//{
//}

/**向后移到一个元素*/
void ReadWriteMemWriterIterator::Next()
{
	if(m_iter != m_table->m_objects.end())
	{
		++m_iter;
	}
};
//virtual void Prev() = 0;

/**是否还有下一个元素*/
bool ReadWriteMemWriterIterator::Valid() const
{
	return (m_iter != m_table->m_objects.end());
}

const Object& ReadWriteMemWriterIterator::object() const
{
	return *m_iter->second;
}


} 



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

#include "dbtypes.h"
#include "writer_iterator.h"
#include "writeonly_writer.h"
#include "readwrite_writer.h"
#include "object_writer_list.h"

namespace xfdb 
{

WriteOnlyWriterIterator::WriteOnlyWriterIterator(WriteOnlyWriterPtr& table)
	: m_table(table), m_max_num(table->m_objects.size())
{
	First();
}

StrView WriteOnlyWriterIterator::UpmostKey() const
{
	return m_table->UpmostKey();
}

//void WriteOnlyWriterIterator::Seek(const StrView& key)
//{};

void WriteOnlyWriterIterator::Next()
{
    assert(m_index < m_max_num);

	//如果key与后面的相同则跳过
    size_t prev_idx = m_index;
	while(++m_index < m_max_num)
	{
        assert(m_table->m_objects[m_index]->key.Compare(m_table->m_objects[prev_idx]->key) >= 0);
        if(m_table->m_objects[m_index]->key != m_table->m_objects[prev_idx]->key)
        {
            break;
        }
	}
};

const Object& WriteOnlyWriterIterator::object() const
{
    return *m_table->m_objects[m_index];
}

//////////////////////////////////////////////////////////////////////
StrView ReadWriteWriterIterator::UpmostKey() const
{
	return m_table->UpmostKey();
}

/**移到第1个元素处*/
void ReadWriteWriterIterator::First()
{
    m_node = m_table->m_head->Next(0);
    if(m_node != nullptr && m_node->object->id > m_max_objid)
    {
        Next();
    }
}

void ReadWriteWriterIterator::Next()
{
    assert(m_node != nullptr);

	//如果key与后面的相同或id大于m_max_objid则跳过
    SkipListNode* prev_node = m_node;
    m_node = m_node->Next(0);

	while(m_node != nullptr)
	{
        assert(m_node->object->key.Compare(prev_node->object->key) >= 0);
        if(m_node->object->id > m_max_objid || m_node->object->key == prev_node->object->key)
        {
            m_node = m_node->Next(0);
        }
        else
        {
            break;
        }
	}
}

const Object& ReadWriteWriterIterator::object() const
{
    return *m_node->object;
}

} 



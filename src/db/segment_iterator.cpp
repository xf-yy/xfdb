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

#include "segment_iterator.h"
#include "segment_file.h"
#include "engine.h"

namespace xfdb 
{

SegmentReaderIterator::SegmentReaderIterator(SegmentReaderPtr& segment_reader) 
 	: m_segment_reader(segment_reader), 
	  m_index_block_reader(segment_reader->m_index_reader),
	  m_data_block_reader(segment_reader->m_data_reader.m_file, segment_reader->m_data_reader.m_path)
{
	m_L1index_count = segment_reader->m_index_reader.m_L1indexs.size();
	First();
}

StrView SegmentReaderIterator::UpmostKey() const
{
	return m_segment_reader->UpmostKey();
}

/**移到第1个元素处*/
void SegmentReaderIterator::First()
{
	m_L1index_idx = 0;

	m_index_block_reader.Read(m_segment_reader->m_index_reader.m_L1indexs[0]);
	m_index_block_iter = m_index_block_reader.NewIterator();

	m_data_block_reader.Read(m_index_block_iter->L0Index());
	m_data_block_iter = m_data_block_reader.NewIterator();
}

/**移到到>=key的地方*/
//void SegmentReaderIterator::Seek(const StrView& key)
//{
//}

/**向后移到一个元素*/
void SegmentReaderIterator::Next()
{
	m_data_block_iter->Next();

	if(m_data_block_iter->Valid())
	{
		return;
	}
	//读取下一个data block
	m_index_block_iter->Next();
	if(m_index_block_iter->Valid())
	{
		m_data_block_reader.Read(m_index_block_iter->L0Index());
		m_data_block_iter = m_data_block_reader.NewIterator();
		return;
	}
	++m_L1index_idx;
	if(m_L1index_idx < m_L1index_count)
	{
		m_index_block_reader.Read(m_segment_reader->m_index_reader.m_L1indexs[m_L1index_idx]);
		m_index_block_iter = m_index_block_reader.NewIterator();

		m_data_block_reader.Read(m_index_block_iter->L0Index());
		m_data_block_iter = m_data_block_reader.NewIterator();
	}
}

/**是否还有下一个元素*/
bool SegmentReaderIterator::Valid() const
{
	return m_data_block_iter->Valid();
}

/**获取key和value*/
const StrView& SegmentReaderIterator::Key() const
{
	return m_data_block_iter->Key();
}
const StrView& SegmentReaderIterator::Value() const
{
	return m_data_block_iter->Value();
}
ObjectType SegmentReaderIterator::Type() const
{
	return m_data_block_iter->Type();
}

} 



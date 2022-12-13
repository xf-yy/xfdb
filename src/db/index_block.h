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

#ifndef __xfdb_index_block_h__
#define __xfdb_index_block_h__

#include <vector>
#include <map>
#include <list>
#include "db_types.h"
#include "buffer.h"
#include "xfdb/strutil.h"
#include "file.h"
#include "key_util.h"

namespace xfdb 
{

class IndexBlockReaderIterator;
typedef std::shared_ptr<IndexBlockReaderIterator> IndexBlockReaderIteratorPtr;
#define NewIndexBlockReaderIterator 	std::make_shared<IndexBlockReaderIterator>
class IndexReader;

class IndexBlockReader
{
public:
	IndexBlockReader(const IndexReader& index_reader);
	~IndexBlockReader();

public:	
	Status Read(const SegmentL1Index& L1Index);
	Status Search(const StrView& key, SegmentL0Index& L0_index);
	
	IndexBlockReaderIteratorPtr NewIterator();

private:
	Status SearchGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, const StrView& key, SegmentL0Index& L0_index) const;
	Status SearchL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, const StrView& key, SegmentL0Index& L0_index) const;
	Status SearchBlock(const StrView& key, SegmentL0Index& L0_index) const;

	Status ParseBlock(IndexBlockReaderIteratorPtr& iter_ptr) const;
	Status ParseL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, IndexBlockReaderIteratorPtr& iter_ptr) const;
	Status ParseGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, IndexBlockReaderIteratorPtr& iter_ptr) const;

private:
	const IndexReader& m_index_reader;

	std::string m_data;
	StrView m_L1Index_start_key;
	uint32_t m_L1Index_size;
	
private:
	IndexBlockReader(const IndexBlockReader&) = delete;
	IndexBlockReader& operator=(const IndexBlockReader&) = delete;
};

class IndexBlockReaderIterator 
{
public:
	IndexBlockReaderIterator();
	~IndexBlockReaderIterator()
	{
	}

public:
	/**移到第1个元素处*/
	inline void First()
	{
		m_idx = 0;
	}
	void Seek(const StrView& key);

	/**向后移到一个元素*/
	void Next()
	{
		if(m_idx < m_L0indexs.size())
		{
			++m_idx;
		}
	}

	/**是否还有下一个元素*/
	inline bool Valid() const
	{
		return m_idx < m_L0indexs.size();
	}
	
	/**获取method, key和value*/
	inline const SegmentL0Index& L0Index() const
	{
		return m_L0indexs[m_idx];
	}

private:
	inline void Add(SegmentL0Index& index)
	{
		index.start_key = CloneKey(m_buf, index.start_key);
		m_L0indexs.push_back(index);
	}
	
private:
	WriteBuffer m_buf;
	std::vector<SegmentL0Index> m_L0indexs;
	size_t m_idx;

private:
	friend class IndexBlockReader;
	IndexBlockReaderIterator(const IndexBlockReaderIterator&) = delete;
	IndexBlockReaderIterator& operator=(const IndexBlockReaderIterator&) = delete;
};

}  

#endif


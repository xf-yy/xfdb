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

#ifndef __xfdb_segment_iterator_h__
#define __xfdb_segment_iterator_h__

#include "iterator.h"
#include "index_block.h"
#include "data_block.h"

namespace xfdb 
{

class SegmentReaderIterator : public Iterator 
{
public:
	explicit SegmentReaderIterator(SegmentReaderPtr& segment_reader);
	virtual ~SegmentReaderIterator()
	{}

public:
	virtual StrView UpmostKey() const override;

	/**移到第1个元素处*/
	virtual void First() override;
	/**移到最后1个元素处*/
	//virtual void Last() = 0;
	
	/**移到到>=key的地方*/
	//virtual void Seek(const StrView& key) override;
	
	/**向后移到一个元素*/
	virtual void Next() override;
	//virtual void Prev() = 0;

	/**是否还有下一个元素*/
	virtual bool Valid() const override;
	
	/**获取key和value*/
	virtual const Object& object() const override;
	
private:
	SegmentReaderPtr m_segment_reader;
	uint32_t m_L1index_idx;
	uint32_t m_L1index_count;
	IndexBlockReader m_index_block_reader;
	IndexBlockReaderIteratorPtr m_index_block_iter;
	DataBlockReader m_data_block_reader;
	DataBlockReaderIteratorPtr m_data_block_iter;

private:
	SegmentReaderIterator(const SegmentReaderIterator&) = delete;
	SegmentReaderIterator& operator=(const SegmentReaderIterator&) = delete;
};

} 

#endif 


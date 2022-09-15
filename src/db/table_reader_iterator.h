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

#ifndef __xfdb_table_reader_iterator_h__
#define __xfdb_table_reader_iterator_h__

#include "types.h"
#include "iterator.h"

namespace xfdb 
{

//处理多个Iterator
class TableReadersIterator : public Iterator 
{
public:
	TableReadersIterator(const std::vector<IteratorPtr>& iters);
	virtual ~TableReadersIterator(){}

public:
	/**移到第1个元素处*/
	virtual void First() override;
	/**移到最后1个元素处*/
	//virtual void Last() = 0;
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) override 
	{assert(false);};
	
	/**向后移到一个元素*/
	virtual void Next() override;
	//virtual void Prev() = 0;

	/**是否还有下一个元素*/
	virtual bool Valid() override;
	
	/**获取key和value*/
	virtual ObjectType Type() override;
	virtual StrView Key() override;
	virtual StrView Value() override;

private:
	void GetMinKey();

private:
	std::vector<IteratorPtr> m_iters;
	size_t m_curr_idx;

private:
	TableReadersIterator(const TableReadersIterator&) = delete;
	TableReadersIterator& operator=(const TableReadersIterator&) = delete;
};


} 

#endif

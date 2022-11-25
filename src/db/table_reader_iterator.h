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

#include "dbtypes.h"
#include "iterator_impl.h"

namespace xfdb 
{

//处理多个Iterator
class IteratorSet : public IteratorImpl 
{
public:
	IteratorSet(const std::vector<IteratorImplPtr>& iters);
	virtual ~IteratorSet(){}

public:
	virtual StrView UpmostKey()    const override;

	/**移到第1个元素处*/
	virtual void First() override;
	
	/**移到到>=key的地方*/
	//virtual void Seek(const StrView& key) override 
	///{assert(false);};
	
	/**向后移到一个元素*/
	virtual void Next() override;

	/**是否还有下一个元素*/
	virtual bool Valid() const override;
	
	virtual const xfutil::StrView& Key() const override;
	virtual const xfutil::StrView& Value() const override;

    //object类型
	virtual ObjectType Type() const override;

private:
	void GetMinKey();
	void GetUpmostKey();

private:
	std::vector<IteratorImplPtr> m_iters;
	StrView m_upmost_key;
	size_t m_curr_idx;
	
private:
	IteratorSet(const IteratorSet&) = delete;
	IteratorSet& operator=(const IteratorSet&) = delete;
};


} 

#endif


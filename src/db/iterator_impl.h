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

#ifndef __xfdb_iterator_impl_h__
#define __xfdb_iterator_impl_h__

#include "xfdb/iterator.h"
#include "db_types.h"

namespace xfdb 
{

class IteratorImpl
{
public:
	IteratorImpl()
    {}
	virtual ~IteratorImpl()
    {}

public:
	/**移到第1个元素处*/
	virtual void First() = 0;
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) = 0;
	
	/**向后移到一个元素*/
	virtual void Next() = 0;

	/**是否还有下一个元素*/
	virtual bool Valid() const = 0;
	
	/**获取object*/
	const Object& object() const
    {
        return *m_obj_ptr;
    }

	//最大key
	const xfutil::StrView& MaxKey() const
    {
        return m_max_key;
    }
	
protected:
    const Object* m_obj_ptr;
    StrView m_max_key;

private:
	IteratorImpl(const IteratorImpl&) = delete;
	IteratorImpl& operator=(const IteratorImpl&) = delete;
};

//处理多个Iterator
class IteratorList : public IteratorImpl 
{
public:
	explicit IteratorList(const std::vector<IteratorImplPtr>& iters);
	virtual ~IteratorList(){}

public:
	/**移到第1个元素处*/
	virtual void First() override;
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) override;
	
	/**向后移到一个元素*/
	virtual void Next() override;

	/**是否还有下一个元素*/
	virtual bool Valid() const override;

private:
	bool GetMinKey();
	void GetMaxKey();
    void GetObject();

private:
	std::vector<IteratorImplPtr> m_iters;
    
    std::vector<size_t> m_minkey_idxs;
    Object m_obj;
    std::string m_value;
	
private:
	IteratorList(const IteratorList&) = delete;
	IteratorList& operator=(const IteratorList&) = delete;
};

} 

#endif 


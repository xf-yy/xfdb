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

#include "db_types.h"
#include "iterator_impl.h"

namespace xfdb 
{

IteratorList::IteratorList(const std::vector<IteratorImplPtr>& iters)
	: m_iters(iters)
{
    //NOTE: iters必须按逆序存放，即最新[0] -> [n]最老
	assert(iters.size() > 1);
    m_minkey_idxs.reserve(iters.size());
    m_value.reserve(4096);

	GetMaxKey();
	First();
}

/**移到第1个元素处*/
void IteratorList::First()
{
	for(size_t i = 0; i < m_iters.size(); ++i)
	{
		m_iters[i]->First();
	}
	GetObject();
}

void IteratorList::Seek(const StrView& key)
{
	for(size_t i = 0; i < m_iters.size(); ++i)
	{
		m_iters[i]->Seek(key);
	}
	GetObject();
}

/**向后移到一个元素*/
void IteratorList::Next()
{
    for(size_t i = 0; i < m_minkey_idxs.size(); ++i)
    {
        m_iters[m_minkey_idxs[i]]->Next();
    }

	GetObject();
}

bool IteratorList::Valid() const
{
	return !m_minkey_idxs.empty();
}

void IteratorList::GetObject()
{
    if(!GetMinKey())
    {
        return;
    }

    m_obj_ptr = &m_iters[m_minkey_idxs[0]]->object();
    if(m_minkey_idxs.size() == 1 || m_obj_ptr->type != AppendType)
    {
        return;
    }
    
    m_obj = *m_obj_ptr;

    //NOTE: 先找到最老的值，然后从最老的值开始append
    ssize_t idx;
    for(idx = 1; idx < (ssize_t)m_minkey_idxs.size(); ++idx)
    {
        const Object& obj = m_iters[m_minkey_idxs[idx]]->object();

        if(obj.type == SetType)
        {
            m_obj.type = SetType;
            ++idx;
            break;
        }
        else if(obj.type == DeleteType)
        {
            m_obj.type = SetType;
            break;
        }
    }
    assert(idx <= (ssize_t)m_minkey_idxs.size());

    m_value.clear();
    for(--idx; idx >= 0; --idx)
    {
        const Object& obj = m_iters[m_minkey_idxs[idx]]->object();
        m_value.append(obj.value.data, obj.value.size);
    }

    m_obj.value.Set(m_value.data(), m_value.size());

	m_obj_ptr = &m_obj;
}

bool IteratorList::GetMinKey()
{
    m_minkey_idxs.clear();

	size_t idx = m_iters.size();
	for(size_t i = 0; i < m_iters.size(); ++i) 
	{
		if (!m_iters[i]->Valid()) 
		{
            continue;
        }
        if(idx == m_iters.size()) 
        {
            idx = i;
            m_minkey_idxs.push_back(i);
        } 
        else 
        {
            const StrView& curr_key = m_iters[i]->object().key;
            int ret = curr_key.Compare(m_iters[idx]->object().key);
            if(ret < 0)
            {
                idx = i;
                m_minkey_idxs.clear();
                m_minkey_idxs.push_back(i);
            }
            else if(ret == 0)
            {
                m_minkey_idxs.push_back(i);
            }
        }
	}
    return !m_minkey_idxs.empty();
}

void IteratorList::GetMaxKey()
{
	assert(!m_iters.empty());
	m_max_key = m_iters[0]->MaxKey();
	for(size_t i = 1; i < m_iters.size(); ++i) 
	{
		const StrView& cmp_key = m_iters[i]->MaxKey();
		if(m_max_key.Compare(cmp_key) < 0)
		{
			m_max_key = cmp_key;
		}
	}
    assert(m_max_key.size != 0);
}

} 



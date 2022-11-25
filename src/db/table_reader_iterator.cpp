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
#include "table_reader_iterator.h"

namespace xfdb 
{

IteratorSet::IteratorSet(const std::vector<IteratorImplPtr>& iters)
	: m_iters(iters)
{
	assert(iters.size() > 1);
	GetUpmostKey();
	First();
}

StrView IteratorSet::UpmostKey() const 
{
	return m_upmost_key;
}

/**移到第1个元素处*/
void IteratorSet::First()
{
	for(size_t i = 0; i < m_iters.size(); ++i)
	{
		m_iters[i]->First();
	}
	GetMinKey();
}

/**向后移到一个元素*/
void IteratorSet::Next()
{
	m_iters[m_curr_idx]->Next();

	GetMinKey();
}

bool IteratorSet::Valid() const
{
	return m_curr_idx != m_iters.size();
}

const StrView& IteratorSet::Key() const
{
	return m_iters[m_curr_idx]->Key();
}

const StrView& IteratorSet::Value() const
{
	return m_iters[m_curr_idx]->Value();
}

ObjectType IteratorSet::Type() const
{
	return m_iters[m_curr_idx]->Type();
}

void IteratorSet::GetMinKey()
{
	ssize_t idx = m_iters.size();
	for(ssize_t i = m_iters.size()-1; i >= 0; --i) 
	{
		if (m_iters[i]->Valid()) 
		{
			if (idx == (ssize_t)m_iters.size()) 
			{
				idx = i;
			} 
			else 
			{
				StrView curr_key = m_iters[i]->Key();
				int ret = curr_key.Compare(m_iters[idx]->Key());
				if(ret < 0)
				{
					idx = i;
				}
				else if(ret == 0)
				{
					m_iters[i]->Next();	//剔除相同key的值
				}
			}
		}
	}
	m_curr_idx = idx;
}

void IteratorSet::GetUpmostKey()
{
	assert(!m_iters.empty());
	m_upmost_key = m_iters[0]->UpmostKey();
	for(size_t i = 1; i < m_iters.size(); ++i) 
	{
		StrView cmp_key = m_iters[i]->UpmostKey();
		if(m_upmost_key.Compare(cmp_key) < 0)
		{
			m_upmost_key = cmp_key;
		}
	}
}

} 



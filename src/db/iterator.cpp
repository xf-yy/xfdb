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

#include "xfdb/iterator.h"
#include "iterator_impl.h"

namespace xfdb 
{

Iterator::Iterator(IteratorImplPtr& iter) : m_iter(iter)
{}

/**移到第1个元素处*/
void Iterator::First()
{
    m_iter->First();
    while(m_iter->Valid() && m_iter->Type() == DeleteType)
    {
        m_iter->Next();
    }
}

/**移到到>=key的地方*/
//virtual void Seek(const StrView& key) = 0;

/**向后移到一个元素*/
void Iterator::Next()
{
    m_iter->Next();
    while(m_iter->Valid() && m_iter->Type() == DeleteType)
    {
        m_iter->Next();
    }
}

/**是否还有下一个元素*/
bool Iterator::Valid() const
{
    m_iter->Valid();
}

/**获取key和value*/
const xfutil::StrView& Iterator::Key() const
{
    m_iter->Key();
}
const xfutil::StrView& Iterator::Value() const
{
    m_iter->Value();
}


} 



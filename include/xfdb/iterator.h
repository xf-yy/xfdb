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

#ifndef __xfdb_iterator_h__
#define __xfdb_iterator_h__

#include "xfdb/types.h"
#include "xfdb/strutil.h"

namespace xfdb 
{

class Iterator 
{
public:
	~Iterator()
    {}

public:
	/**移到第1个元素处*/
	void First();
	
	/**移到到>=key的地方*/
	void Seek(const xfutil::StrView& key);
	
	/**向后移到一个元素*/
	void Next();

	/**是否还有下一个元素*/
	bool Valid() const;
	
	/**获取key*/
	const xfutil::StrView& Key() const;
    
	/**获取value*/
	const xfutil::StrView& Value() const;

private:
	Iterator(IteratorImplPtr& iter);
    
private:
	IteratorImplPtr m_iter;

private:
    friend class DB;
	Iterator(const Iterator&) = delete;
	Iterator& operator=(const Iterator&) = delete;
};

} 

#endif 


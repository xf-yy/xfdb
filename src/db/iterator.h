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

#include "types.h"
#include "xfdb/strutil.h"

namespace xfdb 
{

class Iterator 
{
public:
	Iterator(){}
	virtual ~Iterator(){}

public:
	/**移到第1个元素处*/
	virtual void First() = 0;
	/**移到最后1个元素处*/
	//virtual void Last() = 0;
	
	/**移到到>=key的地方*/
	virtual void Seek(const StrView& key) = 0;
	
	/**向后移到一个元素*/
	virtual void Next() = 0;
	//virtual void Prev() = 0;

	/**是否还有下一个元素*/
	virtual bool Valid() = 0;
	
	/**获取method, key和value*/
	virtual ObjectType Type() = 0;
	virtual StrView Key() = 0;
	virtual StrView Value() = 0;
	
private:
	Iterator(const Iterator&) = delete;
	Iterator& operator=(const Iterator&) = delete;
};

} 

#endif 


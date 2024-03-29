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

#ifndef __xfdb_object_reader_h__
#define __xfdb_object_reader_h__

#include "buffer.h"
#include "xfdb/strutil.h"
#include "db_types.h"
#include "iterator_impl.h"

namespace xfdb
{

class ObjectReader : public std::enable_shared_from_this<ObjectReader>
{
public:
	ObjectReader()
    {}
	virtual ~ObjectReader()
    {}

public:
	virtual Status Get(const StrView& key, objectid_t obj_id, ObjectType& type, std::string& value) const = 0;

	/**迭代器*/
	virtual IteratorImplPtr NewIterator(objectid_t max_object_id = MAX_OBJECT_ID) = 0;
	
	/**返回segment文件总大小*/
	virtual uint64_t Size() const = 0;

	/**获取统计*/
	virtual void GetBucketStat(BucketStat& stat) const = 0;
    
	/**最大key*/
	const StrView& MaxKey() const
    {
        return m_max_key;
    }
	/**最大key*/
	objectid_t MaxObjectID() const
    {
        return m_max_object_id;
    }	

protected:
	StrView m_max_key;
    objectid_t m_max_object_id;

private:
	ObjectReader(const ObjectReader&) = delete;
	ObjectReader& operator=(const ObjectReader&) = delete;
};


}

#endif 


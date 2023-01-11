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

#include "path.h"
#include "logger.h"
#include "object_reader_snapshot.h"
#include "object_writer_snapshot.h"
#include "iterator_impl.h"

namespace xfdb 
{

ObjectReaderSnapshot::ObjectReaderSnapshot(const BucketMetaFilePtr& meta_file, const std::map<fileid_t, ObjectReaderPtr>& new_readers) 
	: m_meta_file(meta_file), m_readers(new_readers)
{
	GetMaxKey();
}

ObjectReaderSnapshot::~ObjectReaderSnapshot()
{
}

Status ObjectReaderSnapshot::Get(const StrView& key, objectid_t obj_id, ObjectType& type, std::string& value) const
{
	//逆序遍历
    std::vector<std::string> values;
	for(auto it = m_readers.rbegin(); it != m_readers.rend(); ++it)
	{
		if(it->second->Get(key, obj_id, type, value) != OK)
		{
            continue;
        }
        if(type == DeleteType)
        {
            break;
        }
        std::string tmp(value);
        values.push_back(tmp);  
        if(type == SetType)
        {
            break;
        }
	
	}
    if(!values.empty())
    {
        value.clear();
        for(ssize_t idx = values.size() - 1; idx >= 0; --idx)
        {
            value.append(values[idx]);
        }
        return OK;
    }
	return (type == DeleteType) ? OK : ERR_OBJECT_NOT_EXIST;
}

IteratorImplPtr ObjectReaderSnapshot::NewIterator(objectid_t max_objid)
{
	if(m_readers.size() == 1)
	{
		return m_readers.begin()->second->NewIterator();
	}
	
	std::vector<IteratorImplPtr> iters;
	iters.reserve(m_readers.size());

	for(auto it = m_readers.rbegin(); it != m_readers.rend(); ++it)
	{
		iters.push_back(it->second->NewIterator());
	}
	return NewIteratorSet(iters);
}

/**返回segment文件总大小*/
uint64_t ObjectReaderSnapshot::Size() const
{
	uint64_t size = 0;
	for(auto it = m_readers.begin(); it != m_readers.end(); ++it)
	{
		//FIXME: 识别segment类型
		size += it->second->Size();
	}
	return size;
}

void ObjectReaderSnapshot::GetBucketStat(BucketStat& stat) const
{	
	for(auto it = m_readers.begin(); it != m_readers.end(); ++it)
	{
		//FIXME: 识别segment类型
		it->second->GetBucketStat(stat);
	}
}

void ObjectReaderSnapshot::GetMaxKey()
{
	if(m_readers.empty())
	{
		return;
	}
	
	auto it = m_readers.begin();
	m_max_key = it->second->MaxKey();

	for(++it; it != m_readers.end(); ++it)
	{
        const StrView& max_key = it->second->MaxKey();
		if(m_max_key < max_key)
		{
			m_max_key = max_key;
		}
	}
}

void ObjectReaderSnapshot::GetMaxObjectID()
{
	if(m_readers.empty())
	{
		return;
	}
	
	auto it = m_readers.begin();
	m_max_objid = it->second->MaxObjectID();

	for(++it; it != m_readers.end(); ++it)
	{
        objectid_t max_objid = it->second->MaxObjectID();
		if(m_max_objid < max_objid)
		{
			m_max_objid = max_objid;
		}
	}
}

}  



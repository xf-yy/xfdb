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
#include "object_reader_list.h"
#include "object_writer_list.h"
#include "iterator_impl.h"

namespace xfdb 
{

ObjectReaderList::ObjectReaderList(const BucketMetaFilePtr& meta_file, const std::map<fileid_t, ObjectReaderPtr>& new_readers) 
	: m_meta_file(meta_file), m_readers(new_readers)
{
	GetUpmostKey();
}

ObjectReaderList::~ObjectReaderList()
{
}

Status ObjectReaderList::Get(const StrView& key, objectid_t obj_id, ObjectType& type, String& value) const
{
	//逆序遍历
	for(auto it = m_readers.rbegin(); it != m_readers.rend(); ++it)
	{
		if(it->second->Get(key, obj_id, type, value) == OK)
		{
			return OK;
		}
	}
	return ERR_OBJECT_NOT_EXIST;
}

IteratorImplPtr ObjectReaderList::NewIterator()
{
	if(m_readers.size() == 1)
	{
		return m_readers.begin()->second->NewIterator();
	}
	
	std::vector<IteratorImplPtr> iters;
	iters.reserve(m_readers.size());

	for(auto it = m_readers.begin(); it != m_readers.end(); ++it)
	{
		iters.push_back(it->second->NewIterator());
	}
	return NewIteratorList(iters);
}

void ObjectReaderList::GetStat(BucketStat& stat) const
{	
	for(auto it = m_readers.begin(); it != m_readers.end(); ++it)
	{
		//FIXME: 识别segment类型
		it->second->GetStat(stat);
	}
}

void ObjectReaderList::GetUpmostKey()
{
	if(m_readers.empty())
	{
		return;
	}
	
	auto it = m_readers.begin();
	m_upmost_key = it->second->UpmostKey();

	for(++it; it != m_readers.end(); ++it)
	{
		if(m_upmost_key < it->second->UpmostKey())
		{
			m_upmost_key = it->second->UpmostKey();
		}
	}
}


}  


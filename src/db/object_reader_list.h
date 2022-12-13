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

#ifndef __xfdb_object_reader_list_h__
#define __xfdb_object_reader_list_h__

#include <algorithm>
#include <set>
#include "db_types.h"
#include "segment_file.h"
#include "bucket_metafile.h"

namespace xfdb 
{

class ObjectReaderList
{
public:
	ObjectReaderList(const BucketMetaFilePtr& meta_file, const std::map<fileid_t, ObjectReaderPtr>& new_readers);
	~ObjectReaderList();

public:	
	Status Get(const StrView& key, objectid_t obj_id, ObjectType& type, String& value) const;
	
	IteratorImplPtr NewIterator();

	/**最大key*/
	StrView UpmostKey() const
	{
		return m_upmost_key;
	}
		
	void GetStat(BucketStat& stat) const;

	inline const std::map<fileid_t, ObjectReaderPtr>& Readers() const
	{
		return m_readers;
	}
	inline const BucketMetaFilePtr& MetaFile() const
	{
		return m_meta_file;
	}
private:
	void GetUpmostKey();
	
private:
    BucketMetaFilePtr m_meta_file;
	std::map<fileid_t, ObjectReaderPtr> m_readers;
	StrView m_upmost_key;

private:
	ObjectReaderList(const ObjectReaderList&) = delete;
	ObjectReaderList& operator=(const ObjectReaderList&) = delete;

};


}  

#endif


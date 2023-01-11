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

#ifndef __xfdb_object_writer_snapshot_h__
#define __xfdb_object_writer_snapshot_h__

#include <deque>
#include <map>
#include "db_types.h"
#include "xfdb/strutil.h"
#include "object_reader.h"

namespace xfdb 
{

class ObjectWriterSnapshot : public ObjectReader
{
public:
	ObjectWriterSnapshot(ObjectWriterPtr& mem_table, ObjectWriterSnapshot* last_snapshot = nullptr);
	~ObjectWriterSnapshot()
    {}
	
public:	
	void Finish();
	
	Status Get(const StrView& key, objectid_t obj_id, ObjectType& type, std::string& value) const override;
	
	IteratorImplPtr NewIterator(objectid_t max_objid = MAX_OBJECT_ID) override;
	
	/**返回segment文件总大小*/
	uint64_t Size() const override;

	/**获取统计*/
	void GetBucketStat(BucketStat& stat) const override;

private:
    void GetMaxKey();
    
private:
	std::vector<ObjectWriterPtr> m_memwriters;

		
private:
	friend class IteratorSet;
	ObjectWriterSnapshot(const ObjectWriterSnapshot&) = delete;
	ObjectWriterSnapshot& operator=(const ObjectWriterSnapshot&) = delete;
};


}  

#endif


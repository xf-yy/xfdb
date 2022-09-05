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

#ifndef __xfdb_readwrite_memwriter_h__
#define __xfdb_readwrite_memwriter_h__

#include <map>
#include <map>
#include "types.h"
#include "table_writer.h"
#include "writeonly_mem_writer.h"

namespace xfdb
{

class ReadWriteMemWriter : public TableWriter
{
public:
	explicit ReadWriteMemWriter(BlockPool& pool, uint32_t max_object_num);
	virtual ~ReadWriteMemWriter(){}

public:	
	virtual Status Get(const StrView& key, ObjectType& type, String& value) const override;
	virtual Status Write(objectid_t start_seqid, const Object* object) override;
	virtual void Sort() override;
	
	virtual IteratorPtr NewIterator() override;

	//大于最大key的key
	virtual StrView UpmostKey()    const override;

	//小于最小key的key
	virtual StrView LowestKey() const override;

private:
	friend class ReadWriteMemWriterIterator;
	std::map<StrView, Object*> m_object_map;
	
private:
	ReadWriteMemWriter(const ReadWriteMemWriter&) = delete;
	ReadWriteMemWriter& operator=(const ReadWriteMemWriter&) = delete;
};


}  

#endif 


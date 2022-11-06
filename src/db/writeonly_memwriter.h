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

#ifndef __xfdb_writeonly_memwriter_h__
#define __xfdb_writeonly_memwriter_h__

#include "types.h"

#include "buffer.h"
#include "table_writer.h"

namespace xfdb
{

//顺序追加
class WriteOnlyMemWriter : public TableWriter
{
public:
	explicit WriteOnlyMemWriter(BlockPool& pool, uint32_t max_object_num);
	~WriteOnlyMemWriter();

public:	
	virtual Status Write(objectid_t start_seqid, const Object* object) override;
	virtual Status Write(objectid_t start_seqid, const WriteOnlyMemWriterPtr& memtable) override;
	virtual void Finish() override;
	
	virtual IteratorPtr NewIterator() override;
	
	//大于最大key的key
	virtual StrView UpmostKey() const override;

protected:
	std::vector<Object*> m_objects;

	#if _DEBUG
	bool m_finished;
	#endif

private:
	friend class WriteOnlyMemWriterIterator;
	friend class ReadWriteMemWriter;
	
	WriteOnlyMemWriter(const WriteOnlyMemWriter&) = delete;
	WriteOnlyMemWriter& operator=(const WriteOnlyMemWriter&) = delete;
};

}  

#endif 


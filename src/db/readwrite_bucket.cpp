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

#include "types.h"
#include "readwrite_bucket.h"
#include "readwrite_mem_writer.h"
#include "table_writer_snapshot.h"
#include "segment_list_file.h"
#include "notify_file.h"
#include "table_reader_snapshot.h"
#include "write_db.h"

namespace xfdb 
{

ReadWriteBucket::ReadWriteBucket(WritableEngine* engine, DBImplPtr db, const BucketInfo& info) 
	: WriteOnlyBucket(engine, db, info)
{	
}

ReadWriteBucket::~ReadWriteBucket()
{

}

TableWriterPtr ReadWriteBucket::NewTableWriter(WritableEngine* engine)
{
	assert(engine->m_conf.mode & MODE_READONLY);
	return NewReadWriteMemWriter(engine->m_pool, engine->m_conf.max_memwriter_object_num);
}

Status ReadWriteBucket::Get(const StrView& key, String& value)
{	
	m_segment_rwlock.ReadLock();
	TableWriterPtr memwriter_ptr = m_memwriter;
	TableWriterSnapshotPtr mts_ptr = m_memwriter_snapshot;
	TableReaderSnapshotPtr trs_ptr = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	ObjectType type;
	if(memwriter_ptr && memwriter_ptr->Get(key, type, value) == OK) 
	{
		return (type == SetType) ? OK : ERR_OBJECT_DELETED;
	}
	if(mts_ptr && mts_ptr->Get(key, type, value) == OK)
	{
		return (type == SetType) ? OK : ERR_OBJECT_DELETED;
	}
	if(trs_ptr && trs_ptr->Get(key, type, value) == OK) 
	{
		return (type == SetType) ? OK : ERR_OBJECT_DELETED;
	}
	return ERR_OBJECT_NOT_EXIST;
}


}  


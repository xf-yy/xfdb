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

#include "dbtypes.h"
#include "readwrite_bucket.h"
#include "readwrite_memwriter.h"
#include "table_writer_snapshot.h"
#include "bucket_metafile.h"
#include "notify_file.h"
#include "table_reader_snapshot.h"
#include "writable_db.h"

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
    return NewReadWriteMemWriter(engine->GetLargePool(), engine->GetConfig().max_memtable_objects);
}

Status ReadWriteBucket::Get(const StrView& key, String& value)
{	
	m_segment_rwlock.ReadLock();

    objectid_t curr_obj_id = m_next_object_id;
    TableWriterPtr memwriter = m_memwriter;
	TableWriterSnapshotPtr mts_ptr = m_memwriter_snapshot;
	BucketReaderSnapshot reader_snapshot = m_reader_snapshot;

	m_segment_rwlock.ReadUnlock();

	ObjectType type;
	if(memwriter && memwriter->Get(key, curr_obj_id, type, value) == OK) 
	{
    	return (type == SetType) ? OK : ERR_OBJECT_NOT_EXIST;
	}
	if(mts_ptr && mts_ptr->Get(key, curr_obj_id, type, value) == OK)
	{
		return (type == SetType) ? OK : ERR_OBJECT_NOT_EXIST;
	}
	if(reader_snapshot.readers && reader_snapshot.readers->Get(key, curr_obj_id, type, value) == OK) 
	{
		return (type == SetType) ? OK : ERR_OBJECT_NOT_EXIST;
	}
	return ERR_OBJECT_NOT_EXIST;
}

IteratorImplPtr ReadWriteBucket::NewIterator()
{
    
}

}  



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
#include "readwrite_writer.h"
#include "object_writer_list.h"
#include "bucket_metafile.h"
#include "notify_file.h"
#include "object_reader_list.h"
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

ObjectWriterPtr ReadWriteBucket::NewObjectWriter(WritableEngine* engine)
{
    return NewReadWriteWriter(engine->GetLargePool(), engine->GetConfig().max_memtable_objects);
}

Status ReadWriteBucket::Get(const StrView& key, String& value)
{	
	m_segment_rwlock.ReadLock();

    objectid_t curr_obj_id = m_next_object_id;
    ObjectWriterPtr memwriter = m_memwriter;
	ObjectWriterListPtr writer_snapshot = m_memwriter_snapshot;
	ObjectReaderListPtr reader_snapshot = m_reader_snapshot;

	m_segment_rwlock.ReadUnlock();

	ObjectType type;
	if(memwriter && memwriter->Get(key, curr_obj_id, type, value) == OK) 
	{
    	return (type == SetType) ? OK : ERR_OBJECT_NOT_EXIST;
	}
	if(writer_snapshot && writer_snapshot->Get(key, curr_obj_id, type, value) == OK)
	{
		return (type == SetType) ? OK : ERR_OBJECT_NOT_EXIST;
	}
	if(reader_snapshot && reader_snapshot->Get(key, curr_obj_id, type, value) == OK) 
	{
		return (type == SetType) ? OK : ERR_OBJECT_NOT_EXIST;
	}
	return ERR_OBJECT_NOT_EXIST;
}

Status ReadWriteBucket::NewIterator(IteratorImplPtr& iter)
{
	m_segment_rwlock.ReadLock();

    objectid_t curr_obj_id = m_next_object_id;
    ObjectWriterPtr memwriter = m_memwriter;
	ObjectWriterListPtr writer_snapshot = m_memwriter_snapshot;
	ObjectReaderListPtr reader_snapshot = m_reader_snapshot;

	m_segment_rwlock.ReadUnlock();
    
    if(memwriter)
    memwriter->NewIterator(curr_obj_id);
    if(writer_snapshot)
    writer_snapshot->NewIterator();
    if(reader_snapshot)
    reader_snapshot->NewIterator();

    //NewIteratorList();
}

}  



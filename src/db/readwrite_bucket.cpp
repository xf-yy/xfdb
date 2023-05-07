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

#include "db_types.h"
#include "readwrite_bucket.h"
#include "readwrite_objectwriter.h"
#include "object_writer_snapshot.h"
#include "bucketmeta_file.h"
#include "notify_file.h"
#include "object_reader_snapshot.h"
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
    return NewReadWriteObjectWriter(engine->GetLargePool(), engine->GetConfig().max_memtable_objects);
}

Status ReadWriteBucket::Get(const StrView& key, std::string& value)
{	
	m_segment_rwlock.ReadLock();

    objectid_t curr_obj_id = m_next_object_id;
    ObjectWriterPtr memwriter = m_memwriter;
	ObjectWriterSnapshotPtr writer_snapshot = m_memwriter_snapshot;
	ObjectReaderSnapshotPtr reader_snapshot = m_reader_snapshot;

	m_segment_rwlock.ReadUnlock();

    std::vector<ObjectReaderPtr> readers;
    readers.reserve(3);

    if(memwriter)
    {
        readers.push_back(memwriter);
    }
    if(writer_snapshot)
    {
        readers.push_back(writer_snapshot);
    }
    if(reader_snapshot)
    {
        readers.push_back(reader_snapshot);
    }

    ObjectType type;
    //FIXME: 以下与ObjectReaderSnapshot::Get相仿
    std::vector<std::string> values;
	for(size_t idx = 0; idx < readers.size(); ++idx)
	{
		if(readers[idx]->Get(key, curr_obj_id, type, value) != OK)
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
	return ERR_OBJECT_NOT_EXIST;

}

Status ReadWriteBucket::NewIterator(IteratorImplPtr& iter)
{
	m_segment_rwlock.ReadLock();

    objectid_t curr_obj_id = m_next_object_id;
    ObjectWriterPtr memwriter = m_memwriter;
	ObjectWriterSnapshotPtr writer_snapshot = m_memwriter_snapshot;
	ObjectReaderSnapshotPtr reader_snapshot = m_reader_snapshot;

	m_segment_rwlock.ReadUnlock();
    
    std::vector<IteratorImplPtr> iters;
    iters.reserve(3);

    if(memwriter)
    {
        assert(memwriter->GetObjectCount() > 0);
        IteratorImplPtr iter = memwriter->NewIterator(curr_obj_id);
        iters.push_back(iter);
    }
    if(writer_snapshot)
    {
        IteratorImplPtr iter = writer_snapshot->NewIterator();
        iters.push_back(iter);
    }
    if(reader_snapshot)
    {
        IteratorImplPtr iter = reader_snapshot->NewIterator();
        iters.push_back(iter);
    }

    iter = NewIteratorSet(iters);
    return OK;
}

}  



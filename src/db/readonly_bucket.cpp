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

#include "readonly_bucket.h"
#include "table_reader_snapshot.h"
#include "readonly_db.h"

namespace xfdb 
{

ReadOnlyBucket::ReadOnlyBucket(DBImplPtr db, const BucketInfo& info)
	: Bucket(db, info)
{
}

ReadOnlyBucket::~ReadOnlyBucket()
{
}

Status ReadOnlyBucket::Open()
{
	//读取最新的segment list
	std::vector<FileName> file_names;
	Status s = ListBucketMetaFile(m_bucket_path.c_str(), file_names);
	if(s != OK)
	{
		return s;
	}
	if(file_names.empty()) 
	{
		return OK;
	}
	return Bucket::Open(file_names.back().str);
}

Status ReadOnlyBucket::Get(const StrView& key, String& value)
{
	m_segment_rwlock.ReadLock();
	BucketReaderSnapshot reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	if(!reader_snapshot.readers)
	{
		return ERR_OBJECT_NOT_EXIST;
	}
	ObjectType type;
	Status s = reader_snapshot.readers->Get(key, type, value);
	if(s != OK)
	{
		return s;
	}
	return (type == DeleteType) ? ERR_OBJECT_NOT_EXIST : OK;
}

void ReadOnlyBucket::GetStat(BucketStat& stat) const
{
	memset(&stat, 0x00, sizeof(stat));

	m_segment_rwlock.ReadLock();
	BucketReaderSnapshot reader_snapshot = m_reader_snapshot;
	m_segment_rwlock.ReadUnlock();

	if(reader_snapshot.readers)
	{
		reader_snapshot.readers->GetStat(stat);
	}
}

}  



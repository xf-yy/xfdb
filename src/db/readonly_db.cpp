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

#include <atomic>
#include "readonly_db.h"
#include "readonly_bucket.h"
#include "table_reader_snapshot.h"
#include "bucket.h"
#include "segment_file.h"
#include "db_info_file.h"
#include "bucket_meta_file.h"
#include "lock_file.h"
#include "file.h"

namespace xfdb
{

ReadOnlyDB::ReadOnlyDB(ReadOnlyEngine* engine, const DBConfig& conf, const std::string& db_path) 
	: DBImpl(engine, conf, db_path)
{
}

ReadOnlyDB::~ReadOnlyDB()
{
}

BucketPtr ReadOnlyDB::NewBucket(const BucketInfo& bucket_info)
{
	return NewReadOnlyBucket(shared_from_this(), bucket_info);
}

Status ReadOnlyDB::Open()
{
	if(!LockFile::Exist(m_path))
	{
		return ERR_PATH_NOT_EXIST;
	}
	Status s = OpenBucket();
	if(s != OK) 
	{
		return s;
	}
	//再尝试重新加载一次，以防有所遗漏
	if(m_engine->GetConfig().auto_reload_db)
	{
		NotifyData nd(NOTIFY_UPDATE_DB_INFO, m_path, MIN_FILEID);
		((ReadOnlyEngine*)m_engine)->PostNotifyData(nd);
	}
	
	return OK;
}

void ReadOnlyDB::BeforeClose()
{
}

Status ReadOnlyDB::Get(const std::string& bucket_name, const StrView& key, String& value) const
{	
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	return bptr->Get(key, value);
}


}   



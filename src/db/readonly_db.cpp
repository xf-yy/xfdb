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
#include "object_reader_list.h"
#include "bucket.h"
#include "segment_file.h"
#include "db_infofile.h"
#include "bucket_metafile.h"
#include "lock_file.h"
#include "file.h"

namespace xfdb
{

ReadOnlyDB::ReadOnlyDB(const DBConfig& conf, const std::string& db_path) 
	: DBImpl(conf, db_path)
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
	if(!m_conf.Check())
	{
		return ERR_INVALID_CONFIG;
	}	
	if(!LockFile::Exist(m_path))
	{
		return ERR_PATH_NOT_EXIST;
	}
	Status s = OpenBucket();
	if(s != OK) 
	{
		return s;
	}
	EnginePtr engine = Engine::GetEngine();

	//再尝试重新加载一次，以防有所遗漏
	if(engine->GetConfig().auto_reload_db)
	{
		NotifyData nd(NOTIFY_UPDATE_DB_META, m_path, MIN_FILE_ID);
		((ReadOnlyEngine*)engine.get())->PostNotifyData(nd);
	}
	
	return OK;
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

Status ReadOnlyDB::NewIterator(const std::string& bucket_name, IteratorImplPtr& iter)
{
	BucketPtr bptr;
	if(!GetBucket(bucket_name, bptr))
	{
		return ERR_BUCKET_NOT_EXIST;
	}
	return bptr->NewIterator(iter);
}


}   



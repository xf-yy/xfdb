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

#ifndef __xfdb_notify_msg_h__
#define __xfdb_notify_msg_h__

#include "dbtypes.h"
#include "path.h"

namespace xfdb 
{

enum NotifyType
{
	NOTIFY_EXIT = 0,
		
	NOTIFY_UPDATE_DB_META,
	NOTIFY_UPDATE_BUCKET_META,

	NOTIFY_WRITE_DB_META = 10,
	NOTIFY_WRITE_BUCKET_META,
	NOTIFY_WRITE_SEGMENT,

	NOTIFY_FULL_MERGE,
	NOTIFY_PART_MERGE,

	NOTIFY_TRY_FLUSH,

	NOTIFY_CLEAN_DB,
};

struct NotifyData
{
	NotifyType type;
	std::string db_path;
	std::string bucket_name;
	fileid_t file_id;

	NotifyData(NotifyType type_ = NOTIFY_EXIT)
	{
		type = type_;
		file_id = INVALID_FILEID;
	}
	NotifyData(NotifyType type_, const std::string& db_path_, fileid_t fid_)
		: db_path(db_path_)
	{
		type = type_;
		file_id = fid_;
	}
	NotifyData(NotifyType type_, const std::string& db_path_, const std::string& bucket_name_, fileid_t fid_)
		: db_path(db_path_), bucket_name(bucket_name_)
	{
		type = type_;
		file_id = fid_;
	}
	
};


struct NotifyMsg
{
	NotifyType type;
	DBImplPtr db;
	BucketPtr bucket;
	fileid_t file_id;

public:
	explicit NotifyMsg(NotifyType type_ = NOTIFY_EXIT)
	{
		type = type_;
		file_id = INVALID_FILEID;
	}
	explicit NotifyMsg(const NotifyData& nd)
	{
		type = nd.type;
		file_id = nd.file_id;
	}
	
	NotifyMsg(NotifyType type_, DBImplPtr& db_)
		: db(db_)
	{
		type = type_;
		file_id = INVALID_FILEID;
	}
	NotifyMsg(NotifyType type_, DBImplPtr& db_, fileid_t fid_)
		: db(db_)
	{
		type = type_;
		file_id = fid_;
	}
	NotifyMsg(NotifyType type_, DBImplPtr& db_, BucketPtr& bucket_)
		: db(db_), bucket(bucket_)
	{
		type = type_;
		file_id = INVALID_FILEID;
	}
	NotifyMsg(NotifyType type_, DBImplPtr& db_, BucketPtr& bucket_, fileid_t fid_)
		: db(db_), bucket(bucket_)
	{
		type = type_;
		file_id = fid_;
	}
 		
};

}

#endif


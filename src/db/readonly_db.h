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

#ifndef __xfdb_readonly_db_h__
#define __xfdb_readonly_db_h__

#include "types.h"
#include "db_impl.h"
#include "readonly_engine.h"

namespace xfdb 
{

class ReadOnlyDB : public DBImpl
{
public:
	ReadOnlyDB(ReadOnlyEngine* engine, const DBConfig& conf, const std::string& db_path);
	~ReadOnlyDB();

public:		
	Status Open() override;
	Status Get(const std::string& bucket_name, const StrView& key, String& value) const override;

protected:
	BucketPtr NewBucket(const BucketInfo& bucket_info) override;
		
private:
	friend class ReadOnlyEngine;
	ReadOnlyDB(const ReadOnlyDB&) = delete;
	ReadOnlyDB& operator=(const ReadOnlyDB&) = delete;
	
};


}  

#endif


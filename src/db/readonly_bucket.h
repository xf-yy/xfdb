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

#ifndef __xfdb_readonly_bucket_h__
#define __xfdb_readonly_bucket_h__

#include <mutex>
#include "types.h"
#include "bucket.h"
#include "rwlock.h"

namespace xfdb 
{

class ReadOnlyBucket : public Bucket
{
public:
	ReadOnlyBucket(DBImplPtr db, const BucketInfo& info);
	virtual ~ReadOnlyBucket();
	
public:	
	virtual Status Open() override;

	virtual Status Get(const StrView& key, String& value) override;
	
	virtual void GetStat(BucketStat& stat) const override;
		
private:
};


}  

#endif


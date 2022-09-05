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

#ifndef __xfdb_bucket_snapshot_h__
#define __xfdb_bucket_snapshot_h__

#include <vector>
#include <map>
#include "xfdb/strutil.h"
#include "xfdb/util_types.h"
#include "xfdb/db.h"
#include "rwlock.h"
#include "types.h"

namespace xfdb 
{

class BucketSnapshot
{
public:
	BucketSnapshot(std::map<std::string, BucketPtr>& buckets);
	~BucketSnapshot(){}

public:
	void TryFlush();
	void Flush();
	void Merge(){assert(false);}
	
	inline const std::map<std::string, BucketPtr>& Buckets() const
	{
		return m_buckets;
	}
	void Buckets(std::vector<std::string>& bucket_names) const;
				
protected:
	//key: bucket_name
	std::map<std::string, BucketPtr> m_buckets;

private:
	BucketSnapshot(const BucketSnapshot&) = delete;
  	BucketSnapshot& operator=(const BucketSnapshot&) = delete;
	
};


}  

#endif


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

#include "db_impl.h"
#include "types.h"
#include "bucket.h"
#include "db_meta_file.h"
#include "bucket_snapshot.h"
#include "logger.h"


namespace xfdb 
{

BucketSnapshot::BucketSnapshot(std::map<std::string, BucketPtr>& buckets) 
	: m_buckets(buckets)
{
}

void BucketSnapshot::TryFlush()
{
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
	{
		it->second->TryFlush();
	}
}

void BucketSnapshot::Flush()
{
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
	{
		it->second->Flush();
	}
}

void BucketSnapshot::Buckets(std::vector<std::string>& bucket_names) const
{
	bucket_names.resize(m_buckets.size());
	int i = 0;
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it, ++i)
	{
		bucket_names[i] = it->first;
	}
}

#if 0
void BucketSnapshot::List(std::vector<BucketPtr>& bucket_ptrs) const
{
	bucket_ptrs.resize(m_buckets.size());
	int i = 0;
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
	{
		bucket_ptrs[i++] = it->second;
	}
}
#endif


}  



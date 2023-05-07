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
#include "db_types.h"
#include "bucket.h"
#include "dbmeta_file.h"
#include "bucket_set.h"
#include "logger.h"


namespace xfdb 
{

BucketSet::BucketSet(std::map<std::string, BucketPtr>& buckets) 
	: m_buckets(buckets)
{
}

void BucketSet::TryFlush()
{
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
	{
		it->second->TryFlush();
	}
}

void BucketSet::Flush()
{
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
	{
		it->second->Flush();
	}
}

void BucketSet::Merge()
{
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
	{
		it->second->Merge();
	}
}

void BucketSet::Clean()
{
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it)
	{
		it->second->Clean();
	}
}

void BucketSet::List(std::vector<std::string>& bucket_names) const
{
	bucket_names.resize(m_buckets.size());
	int i = 0;
	for(auto it = m_buckets.begin(); it != m_buckets.end(); ++it, ++i)
	{
		bucket_names[i] = it->first;
	}
}

#if 0
void BucketSet::List(std::vector<BucketPtr>& bucket_ptrs) const
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



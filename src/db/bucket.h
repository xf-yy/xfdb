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

#ifndef __xfdb_bucket_h__
#define __xfdb_bucket_h__

#include <vector>
#include <mutex>
#include "db_types.h"
#include "object_writer.h"
#include "rwlock.h"
#include "path.h"
#include "bucketmeta_file.h"
#include "db_impl.h"

namespace xfdb 
{

class Bucket : public std::enable_shared_from_this<Bucket>
{
public:
	Bucket(DBImplPtr& db, const BucketInfo& info);
	virtual ~Bucket()
	{
	}
	
	inline const std::string& Name() const
	{
		return m_info.name;
	}
	inline const BucketInfo& Info() const
	{
		return m_info;
	}
	
public:	
	virtual Status Open() = 0;
	
	virtual Status Get(const StrView& key, std::string& value)
	{
		return ERR_INVALID_MODE;
	}
   	virtual Status NewIterator(IteratorImplPtr& iter)
    {
		return ERR_INVALID_MODE;
    }

	virtual Status TryFlush()
	{
		return ERR_INVALID_MODE;
	}
	virtual Status Flush()
	{
		return ERR_INVALID_MODE;
	}
	virtual Status Merge()
	{
		return ERR_INVALID_MODE;
	}
	virtual Status Clean()
	{
		return ERR_INVALID_MODE;
	}
	virtual void GetStat(BucketStat& stat) const = 0;
	
public:	
	Status Open(const char* bucket_meta_filename);
    Status Backup(const std::string& db_dir);
	
protected:
	void OpenSegment(const BucketMeta& bm, const ObjectReaderSnapshot* last_snapshot, std::map<fileid_t, ObjectReaderPtr>& readers);
    void OpenSegment(const BucketMeta& bm, std::map<fileid_t, ObjectReaderPtr>& readers);

protected:
	const DBImplWptr m_db;
	const BucketInfo m_info;
	std::string m_bucket_path;

	std::mutex m_mutex;
    BucketConfig m_conf;
	
	mutable ReadWriteLock m_segment_rwlock;
	objectid_t m_next_object_id;
	fileid_t m_next_bucket_meta_fileid;
	fileid_t m_next_segment_id;

	ObjectReaderSnapshotPtr m_reader_snapshot;

private:
	friend class DBImpl;
	Bucket(const Bucket&) = delete;
  	Bucket& operator=(const Bucket&) = delete;	
};


}  // namespace xfdb

#endif


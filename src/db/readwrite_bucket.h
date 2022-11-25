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

#ifndef __xfdb_readwrite_bucket_h__
#define __xfdb_readwrite_bucket_h__

#include "dbtypes.h"
#include "readwrite_memwriter.h"
#include "table_writer_snapshot.h"
#include "table_reader_snapshot.h"
#include "bucket.h"
#include <deque>
#include <mutex>
#include <set>
#include "writable_engine.h"
#include "writeonly_bucket.h"

namespace xfdb 
{

class ReadWriteBucket : public WriteOnlyBucket
{	
public:
	ReadWriteBucket(WritableEngine* engine, DBImplPtr db, const BucketInfo& info);
	virtual ~ReadWriteBucket();
	
public:	
	virtual Status Get(const StrView& key, String& value) override;	
	virtual IteratorImplPtr NewIterator() override;
    
protected:
	virtual TableWriterPtr NewTableWriter(WritableEngine* engine);

};

}

#endif


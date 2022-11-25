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

#ifndef __xfdb_table_reader_snapshot_h__
#define __xfdb_table_reader_snapshot_h__

#include <algorithm>
#include <set>
#include "dbtypes.h"
#include "segment_file.h"
#include "bucket_metafile.h"

namespace xfdb 
{

class TableReaderSnapshot
{
public:
	TableReaderSnapshot(const std::map<fileid_t, TableReaderPtr>& new_readers);
	~TableReaderSnapshot();

public:	
	Status Get(const StrView& key, objectid_t obj_id, ObjectType& type, String& value) const;
	
	IteratorImplPtr NewIterator();

	/**最大key*/
	StrView UpmostKey() const
	{
		return m_upmost_key;
	}
		
	void GetStat(BucketStat& stat) const;

	inline const std::map<fileid_t, TableReaderPtr>& Readers() const
	{
		return m_readers;
	}

private:
	void GetUpmostKey();
	
private:
	std::map<fileid_t, TableReaderPtr> m_readers;
	StrView m_upmost_key;

private:
	TableReaderSnapshot(const TableReaderSnapshot&) = delete;
	TableReaderSnapshot& operator=(const TableReaderSnapshot&) = delete;

};


}  

#endif


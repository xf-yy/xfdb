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

#ifndef __xfdb_table_reader_h__
#define __xfdb_table_reader_h__

#include "buffer.h"
#include "xfdb/strutil.h"
#include "types.h"
#include "iterator.h"

namespace xfdb
{

class TableReader : public std::enable_shared_from_this<TableReader>
{
public:
	TableReader(){}
	virtual ~TableReader(){}

public:
	virtual Status Get(const StrView& key, ObjectType& type, String& value) const = 0;

	/**迭代器*/
	virtual IteratorPtr NewIterator() = 0;
	
	/**最大key*/
	virtual StrView UpmostKey() const = 0;
	/**最小key*/
	virtual StrView LowestKey() const = 0;
	
	/**返回segment文件总大小*/
	virtual uint64_t Size() const = 0;

	/**获取统计*/
	virtual void GetStat(BucketStat& stat) const = 0;

private:
	TableReader(const TableReader&) = delete;
	TableReader& operator=(const TableReader&) = delete;
};


}

#endif 


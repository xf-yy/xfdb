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

#ifndef __xfdb_data_block_h__
#define __xfdb_data_block_h__

#include <vector>
#include <map>
#include <list>
#include "types.h"
#include "buffer.h"
#include "xfdb/strutil.h"
#include "file.h"
#include "key_util.h"

namespace xfdb 
{

class DataBlockReaderIterator;
typedef std::shared_ptr<DataBlockReaderIterator> DataBlockReaderIteratorPtr;
#define NewDataBlockReaderIterator 	std::make_shared<DataBlockReaderIterator>

class DataBlockReader
{
public:
	DataBlockReader(const File& file, const std::string& file_path);
	~DataBlockReader();
	
public:	
	Status Read(const SegmentL0Index& L0_index);
	Status Search(const StrView& key, ObjectType& type, String& value);
	DataBlockReaderIteratorPtr NewIterator();

private:
	Status SearchGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, const StrView& key, ObjectType& type, String& value) const;
	Status SearchL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, const StrView& key, ObjectType& type, String& value) const;
	Status SearchBlock(const byte_t* block, uint32_t block_size, const SegmentL0Index& L0_index, const StrView& key, ObjectType& type, String& value) const;

	Status ParseGroup(const byte_t* group, uint32_t group_size, const L0GroupIndex& group_index, DataBlockReaderIteratorPtr& iter_ptr) const;
	Status ParseL2Group(const byte_t* group_start, uint32_t group_size, const LnGroupIndex& lngroup_index, DataBlockReaderIteratorPtr& iter_ptr) const;
	Status ParseBlock(const byte_t* block, uint32_t block_size, const SegmentL0Index& L0_index, DataBlockReaderIteratorPtr& iter_ptr) const;

private:
	const File& m_file;
	const std::string& m_file_path;

	std::string m_data;
	SegmentL0Index m_L0Index;

private:
	DataBlockReader(const DataBlockReader&) = delete;
	DataBlockReader& operator=(const DataBlockReader&) = delete;
};

typedef std::shared_ptr<DataBlockReader> DataBlockReaderPtr;
#define NewDataBlock 	std::make_shared<DataBlockReader>

class DataBlockReaderIterator 
{
public:
	DataBlockReaderIterator(DataBlockReader& block);
	~DataBlockReaderIterator()
	{}

public:
	/**移到第1个元素处*/
	inline void First()
	{
		m_idx = 0;
	}
	/**移到最后1个元素处*/
	//virtual void Last();
	
	/**向后移到一个元素*/
	void Next()
	{
		if(m_idx < m_objects.size())
		{
			++m_idx;
		}
	}
	//virtual void Prev();

	/**是否还有下一个元素*/
	inline bool Valid() const
	{
		return m_idx < m_objects.size();
	}
	
	/***/
	inline const Object& object() const
	{
		return m_objects[m_idx];
	}
	
private:
	inline void Add(Object& obj)
	{
		//FIXME: value值暂不复制
		obj.key = CloneKey(m_buf, obj.key);
		m_objects.push_back(obj);
	}	
private:
	DataBlockReader& m_block;

	WriteBuffer m_buf;
	std::vector<Object> m_objects;
	size_t m_idx;

private:
	friend class DataBlockReader;	
	DataBlockReaderIterator(const DataBlockReaderIterator&) = delete;
	DataBlockReaderIterator& operator=(const DataBlockReaderIterator&) = delete;
};

}  

#endif


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

#include "table_writer.h"

namespace xfdb
{

TableWriter::TableWriter(BlockPool& pool)
	: m_create_time(time(nullptr)), m_block_pool(pool), m_buf(m_block_pool)
{
	memset(&m_object_stat, 0x00, sizeof(m_object_stat));
}
TableWriter::~TableWriter()
{
}	

StrView TableWriter::CloneString(const StrView& str)
{
	byte_t* new_str = m_buf.Write((byte_t*)str.data, str.size);
	return StrView((char*)new_str, str.size);	
}

Object* TableWriter::CloneObject(objectid_t seqid, const Object* object)
{	
	ObjectStatItem* stat = (object->type == SetType) ? &m_object_stat.set_stat : &m_object_stat.delete_stat;
	stat->Add(object->key.size, object->value.size);
	
	byte_t* obj_buf = m_buf.Write(sizeof(Object));
	Object* new_obj = new (obj_buf)Object;
	
	new_obj->type = object->type;
	new_obj->key = CloneString(object->key);
	if(!object->value.Empty())
	{
		new_obj->value = CloneString(object->value);
	}
	new_obj->id = seqid;
	
	return new_obj;
}


}  



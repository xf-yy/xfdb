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

#include "xfdb/batch.h"
#include "writeonly_memwriter.h"
#include "engine.h"

namespace xfdb 
{

//设置指定bucket中的记录
Status ObjectBatch::Set(const std::string& bucket_name, const xfutil::StrView& key, const xfutil::StrView& value)
{
    WriteOnlyMemWriterPtr writer;

    auto it = m_data.find(bucket_name);
    if(it == m_data.end())
    {
        EnginePtr engine = Engine::GetEngine();
        //4KB内存块
        writer = NewWriteOnlyMemWriter(engine->GetSmallPool(), 1024);
    }
    else
    {
        writer = it->second;
    }
    Object obj = {SetType, key, value};

    return writer->Write(0, &obj);
}
//Append

//删除指定bucket中的记录
Status ObjectBatch::Delete(const std::string& bucket_name, const xfutil::StrView& key)
{
    WriteOnlyMemWriterPtr writer;

    auto it = m_data.find(bucket_name);
    if(it == m_data.end())
    {
        EnginePtr engine = Engine::GetEngine();
        writer = NewWriteOnlyMemWriter(engine->GetSmallPool(), 1024);
    }
    else
    {
        writer = it->second;
    }
    Object obj = {DeleteType, key};

    return writer->Write(0, &obj);    
}

//清理
void ObjectBatch::Clear()
{
    m_data.clear();
}


}  



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
#include "writeonly_objectwriter.h"
#include "engine.h"

namespace xfdb 
{

//设置指定bucket中的记录
Status ObjectBatch::Set(const std::string& bucket_name, const xfutil::StrView& key, const xfutil::StrView& value)
{
    WriteOnlyObjectWriterPtr writer;
    Status s = GetObjectWriter(bucket_name, writer);
    if(s != OK)
    {
        return s;
    }

    Object obj = {SetType, key, value};
    return writer->Write(0, &obj);
}

Status ObjectBatch::Append(const std::string& bucket_name, const xfutil::StrView& key, const xfutil::StrView& value)
{
    WriteOnlyObjectWriterPtr writer;
    Status s = GetObjectWriter(bucket_name, writer);
    if(s != OK)
    {
        return s;
    }

    Object obj = {AppendType, key, value};
    return writer->Write(0, &obj);
}

//删除指定bucket中的记录
Status ObjectBatch::Delete(const std::string& bucket_name, const xfutil::StrView& key)
{
    WriteOnlyObjectWriterPtr writer;
    Status s = GetObjectWriter(bucket_name, writer);
    if(s != OK)
    {
        return s;
    }

    Object obj = {DeleteType, key};
    return writer->Write(0, &obj);    
}

//清理
void ObjectBatch::Clear()
{
    m_writers.clear();
}

Status ObjectBatch::GetObjectWriter(const std::string& bucket_name, WriteOnlyObjectWriterPtr& writer)
{
    auto it = m_writers.find(bucket_name);
    if(it != m_writers.end())
    {
        writer = it->second;
    }
    else
    {
        EnginePtr engine = Engine::GetEngine();
        if(!engine)
        {
            return ERR_STOPPED;
        }
        writer = NewWriteOnlyObjectWriter(engine->GetSmallPool(), 1024);
        m_writers[bucket_name] = writer;
    }
    return OK;
}

}  



# ●项目介绍   
***   
    xfdb是一项自研的全新的嵌入式KV库（也称NoSQL库），采用c++11语言开发。   
    作者：xiaofan <75631302@qq.com>   
   
# ●特性   
***   
    已实现(V0.1.0)：   
        1、支持多种模式：只读，只写，读写   
        2、支持桶的创建、删除、列举、统计值   
        3、支持对象的创建、删除、读取   
        4、key、value值的最大长度为64KB   
   
    待实现：   
        1、批量写   
        2、Merge   
        3、Scan   
        4、Cache   
        5、BloomFilter   
        6、Compress   
        7、Append，Modify   
        8、WAL+Log   
        9、Backup   
        10、Windows support   
   
# ●编译方法   
***   
    注：当前版本仅支持linux环境编译，且gcc版本最低4.8.5版本，编译方法如下：   
    mkdir build   
    cd build    
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=./ ..   
    make   
    make install   
   
# ●使用样例   
***   
    只读模式：   
```  
	GlobalConfig gconf;
	gconf.mode = MODE_READONLY;
	gconf.notify_dir = "./notify";
	Status s = XfdbStart(gconf);
	if(s != OK)
	{
		printf("xfdb init failed, status: %d\n", s);
		return;
	}

	DBConfig dbconf;
	DBPtr db;
	s = DB::Open(dbconf, "test_db_path", db);
	if(s != OK)
	{
		printf("db open failed, path:%s, status: %d\n", db_path.c_str(), s);
		return;
	}
	std::string bucket("testbucket");
	std::string key("testkey");
	String value;
	const StrView key_view(key.data(), key.size());
	s = db->Get(bucket, key_view, value);
	if(s == OK)
	{
		printf("get succeed: %s\n", std::string(value.Data(), value.Size()).c_str());
	}
	else
	{
		printf("get failed: %d\n", s);
	}	
```  
    只写模式：   
```  
	GlobalConfig gconf;
	gconf.mode = MODE_WRITEONLY;
	gconf.notify_dir = "./notify";
	Status s = XfdbStart(gconf);
	if(s != OK)
	{
		printf("xfdb init failed, status: %d\n", s);
		return;
	}

	DBConfig dbconf;
	DBPtr db;
	s = DB::Open(dbconf, "test_db_path", db);
	if(s != OK)
	{
		printf("db open failed, path:%s, status: %d\n", db_path.c_str(), s);
		return;
	}
	std::string bucket("testbucket");
	std::string key("testkey");
	std::string value("testvalue");
	const StrView key_view(key.data(), key.size());
	const StrView value_view(value.data(), value.size());
	s = db->Set(bucket, key_view, value_view);
	if(s == OK)
	{
		printf("set succeed\n");
	}
	else
	{
		printf("set failed: %d\n", s);
	}
```  
   
# ●版权信息   
***   
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
   

# ●项目介绍   
***   
    xfdb是一个轻量级的嵌入式KV库（也称NoSQL库），采用c++11语言开发。   
    作者：xiaofan <75631302@qq.com>   
   
# ●特性   
***    
    已实现：  
        1、支持多种模式：只读模式，只写模式，读写模式   
        2、支持桶的操作：create、delete、backup(全量)、list、getstat等   
        3、支持Key/Value操作：put、delete、get、append、batch、iterator操作
        4、支持自动flush（暂不支持WAL）数据  
        5、支持自动merge，手工merge
        6、支持读cache
        7、支持BloomFilter
        8、后台日志输出
    实现中：  
   			NA
    待实现：   
        NA
   
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
	Status s = Start(gconf);
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
	std::string value;
	const StrView key_view(key.data(), key.size());
	s = db->Get(bucket, key_view, value);
	if(s == OK)
	{
		printf("get succeed: %s\n", value.c_str());
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
	Status s = Start(gconf);
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
   

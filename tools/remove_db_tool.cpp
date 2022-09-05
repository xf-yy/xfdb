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

#include <iostream>
#include <vector>
#include <string>
#include "xfdb/xfdb.h"

using namespace xfutil;
using namespace xfdb;

int main(int argc, char* argv[])
{
	//usage: xfdb_remove_tool <db_path>
	if(argc != 2)
	{
		printf("usage: %s <db_path>\n", argv[0]);
		return 1;
	}
	std::string db_path(argv[1]);
	
	GlobalConfig gconf;
	gconf.mode = MODE_WRITEONLY;
	gconf.notify_dir = "./notify";
	Status s = XfdbStart(gconf);
	if(s != OK)
	{
		printf("xfdb init failed, status: %d\n", s);
		return 2;
	}

	s = DB::Remove(db_path);
	if(s != OK)
	{
		printf("db remove failed, path:%s, status: %d\n", db_path.c_str(), s);
		return 2;
	}
	
	printf("db remove succeed, path:%s\n", db_path.c_str());
		
	return 0;
}


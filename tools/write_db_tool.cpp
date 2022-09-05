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

static std::vector<std::string> string_split(const std::string& str, char delim) 
{
    std::vector<std::string> elems;
    std::size_t prev_off = 0;
    std::size_t curr_off = str.find(delim);
    while (curr_off != std::string::npos) 
    {
        if (curr_off > prev_off) 
        {
            elems.push_back(str.substr(prev_off, curr_off - prev_off));
        }
        prev_off = curr_off + 1;
        curr_off = str.find(delim, prev_off);
    }
    if (prev_off != str.size()) 
    {
        elems.push_back(str.substr(prev_off));
    }
    return elems;
}

static void Usage()
{
	printf("usage:\n");
	printf("    1. set <bucket_name> <key> <value>\n");
	printf("    2. delete <bucket_name> <key>\n");
	printf("    3. get <bucket_name> <key>\n");
	printf("    4. flush <bucket_name>\n");
	printf("    5. stat <bucket_name>\n");
	printf("    6. list_bucket\n");
	printf("    7. quit\n");
	printf("    8. usage\n");
}

int main(int argc, char* argv[])
{
	//usage: xfdb_write_tool <db_path>
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

    DBConfig dbconf;
    
    DBPtr db;
	s = DB::Open(dbconf, db_path, db);
	if(s != OK)
	{
		printf("db open failed, path:%s, status: %d\n", db_path.c_str(), s);
		return 2;
	}
	Usage();
	
	//循环执行输入的命令
	for(;;)
	{
		printf(">");
		std::string input_line;
		if(!getline(std::cin, input_line))
		{
			break;
		}
		if(input_line.empty())
		{
			continue;
		}
		std::vector<std::string> strs = string_split(input_line, ' ');

		if(strs[0] == "set")
		{
			if(strs.size() < 4)
			{
				printf("invalid set command: %s\n", input_line.c_str());
			}
			else
			{
				const StrView key_view(strs[2].data(), strs[2].size());
				const StrView value_view(strs[3].data(), strs[3].size());
				s = db->Set(strs[1], key_view, value_view);
				if(s == OK)
				{
					printf("set succeed\n");
				}
				else
				{
					printf("set failed: %d\n", s);
				}
			}
		}
		else if(strs[0] == "get")
		{
			if(strs.size() < 3)
			{
				printf("invalid get command: %s\n", input_line.c_str());
			}
			else
			{
				String value;
				const StrView key_view(strs[2].data(), strs[2].size());
				s = db->Get(strs[1], key_view, value);
				if(s == OK)
				{
					printf("get succeed: %s\n", std::string(value.Data(), value.Size()).c_str());
				}
				else
				{
					printf("get failed: %d\n", s);
				}
			}
		}	
		else if(strs[0] == "delete")
		{
			if(strs.size() < 3)
			{
				printf("invalid delete command: %s\n", input_line.c_str());
			}
			else
			{
				const StrView key_view(strs[2].data(), strs[2].size());
				s = db->Delete(strs[1], key_view);
				if(s == OK)
				{
					printf("delete succeed\n");
				}
				else
				{
					printf("delete failed: %d\n", s);
				}
			}
		}		
		else if(strs[0] == "flush")
		{
			s = db->Flush(strs[1]);
			if(s == OK)
			{
				printf("flush succeed\n");
			}
			else
			{
				printf("flush failed: %d\n", s);
			}
		}
		else if(strs[0] == "stat")
		{
			if(strs.size() < 2)
			{
				printf("invalid stat command: %s\n", input_line.c_str());
			}
			else
			{
				BucketStat stat;
				s = db->GetBucketStat(strs[1], stat);
				if(s == OK)
				{
					printf("stat succeed:\n");
					printf("    memwriter num: %lu\n", stat.memwriter_stat.count);
					printf("    memwriter size: %lu\n", stat.memwriter_stat.size);
					printf("    segment num: %lu\n", stat.segment_stat.count);
					printf("    segment size: %lu\n", stat.segment_stat.size);
					printf("    set object num: %lu\n", stat.object_stat.set_stat.count);
					printf("    set object key size: %lu\n", stat.object_stat.set_stat.key_size);
					printf("    set object value size: %lu\n", stat.object_stat.set_stat.value_size);
					printf("    delete object num: %lu\n", stat.object_stat.delete_stat.count);
					printf("    delete object key size: %lu\n", stat.object_stat.delete_stat.key_size);
				}
				else
				{
					printf("stat failed: %d\n", s);
				}
			}
		}		
		else if(strs[0] == "list_bucket")
		{
			std::vector<std::string> bucket_names;
			db->ListBucket(bucket_names);
			printf("bucket count: %zd\n", bucket_names.size());
			for(size_t i = 0; i < bucket_names.size(); ++i)
			{
				printf("    %zd: %s\n", i, bucket_names[i].c_str());
			}
		}
		else if(strs[0] == "quit")
		{
			break;
		}
		else if(strs[0] == "usage")
		{
			Usage();
		}
		else
		{
			printf("invalid command: %s\n", input_line.c_str());
		}
	}

		
	return 0;
}


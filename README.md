# * 项目介绍   
***   
>xfdb是一项自研的全新的嵌入式KV库（也称NoSQL库），采用c++11语言开发。   
>作者：xiaofan <75631302@qq.com>   
   
# * 特性   
***   
>已实现(V0.1.0)：   
>>1、支持多种模式：只读，只写，读写   
>>2、支持桶的创建、删除、列举、统计值   
>>3、支持对象的创建、删除、读取   
>>4、key、value值的最大长度为64KB   
   
>待实现：   
>>1、批量写   
>>2、Merge   
>>3、Scan   
>>4、Cache   
>>5、BloomFilter   
>>6、Compress   
>>7、Append，Modify   
>>8、WAL+Log   
>>9、Backup   
>>10、Windows support   
   
# * 编译方法   
***   
>注：当前版本仅支持linux环境编译，且gcc版本最低4.8.5版本，编译方法如下：   
>>mkdir build   
>>cd build    
>>cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=./ ..   
>>make   
>>make install   
   
# * 使用样例   
***   
>只读模式：   
>>略，待补充   
>读写模式：   
>>略，待补充   
   
# * 版权信息   
***   
>Copyright (C) 2022 The xfdb Authors. All rights reserved.   
   
>Licensed under the Apache License, Version 2.0 (the "License");   
>you may not use this file except in compliance with the License.   
>You may obtain a copy of the License at   
   
>>http://www.apache.org/licenses/LICENSE-2.0   
   
>Unless required by applicable law or agreed to in writing, software   
>distributed under the License is distributed on an "AS IS" BASIS,   
>WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   
>See the License for the specific language governing permissions and   
>limitations under the License.   
   

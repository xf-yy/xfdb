cmake_minimum_required(VERSION 2.8)

project(xfdb)
set(xfdb_VERSION_MAJOR 0)
set(xfdb_VERSION_MINOR 1)
set(xfdb_VERSION_PATCH 0)

#判断编译器类型,如果是gcc编译器,则在编译选项中加入c++11支持
set(CMAKE_CXX_FLAGS "-std=c++11 -fsigned-char -D_REENTRANT -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE ${CMAKE_CXX_FLAGS}")

#默认Debug模式
#cmake -DCMAKE_BUILD_TYPE=Release ..
#cmake -DCMAKE_BUILD_TYPE=Debug ..
if(CMAKE_BUILD_TYPE AND (CMAKE_BUILD_TYPE STREQUAL "Release"))
    set(CMAKE_CXX_FLAGS_RELEASE " -Wall -O2 -DNDEBUG ")
    message("Release mode: ${CMAKE_CXX_FLAGS_RELEASE}")
else()
    SET(CMAKE_BUILD_TYPE "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG " -Wall -O0 -g -ggdb -D_DEBUG -DDEBUG")
    message("Debug mode: ${CMAKE_CXX_FLAGS_DEBUG}")
endif()

include_directories("./include")

add_subdirectory(src)
add_subdirectory(tools)



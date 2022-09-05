include_directories(/usr/local/include)
link_directories(/usr/local/lib)

add_executable(read_db_tool read_db_tool.cpp)
target_link_libraries(read_db_tool xfdb pthread)

add_executable(write_db_tool write_db_tool.cpp)
target_link_libraries(write_db_tool xfdb pthread)

add_executable(remove_db_tool remove_db_tool.cpp)
target_link_libraries(remove_db_tool xfdb pthread)

install(TARGETS write_db_tool DESTINATION sbin)
install(TARGETS read_db_tool DESTINATION sbin)
install(TARGETS remove_db_tool DESTINATION sbin)


include_directories("./db")
include_directories("./util")

aux_source_directory("./util" util_srcs)
aux_source_directory("./db" db_srcs)

add_library(${PROJECT_NAME} SHARED ${util_srcs}  ${db_srcs})
set_target_properties(${PROJECT_NAME} PROPERTIES VERSION ${xfdb_VERSION_MAJOR}.${xfdb_VERSION_MINOR} SOVERSION ${xfdb_VERSION_MAJOR})

target_link_libraries(${PROJECT_NAME} pthread)

install(TARGETS ${PROJECT_NAME} DESTINATION lib)


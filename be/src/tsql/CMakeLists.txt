# ADH-TSQL
# The Service library lists these via target_sources() in
# be/src/service/CMakeLists.txt. This file records the sources so they are not
# inserted into the upstream add_library() list.
set(TSQL_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/tsql-flags.cc
  ${CMAKE_CURRENT_LIST_DIR}/tsql-child-query.cc
  ${CMAKE_CURRENT_LIST_DIR}/tsql-executor.cc
)

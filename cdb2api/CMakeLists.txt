set(src
  cdb2api.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.pb-c.c
  ${PROJECT_SOURCE_DIR}/util/hostname_support.c
  ${PROJECT_SOURCE_DIR}/util/sbuf2.c
  ${PROJECT_SOURCE_DIR}/util/ssl_glue.c
  ${PROJECT_SOURCE_DIR}/util/ssl_support.c
  ${PROJECT_SOURCE_DIR}/util/walkback.c
)

set_source_files_properties(
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.pb-c.c
  PROPERTIES GENERATED TRUE
)

include_directories(
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROTOBUF-C_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
)

if (COMDB2_EXTRA_PLUGINS)
  include(${COMDB2_EXTRA_PLUGINS}/cdb2api/CMakeLists.txt)
endif()

# common obj files for .so/.dylib and .a
add_definitions(-DDISABLE_HOSTADDR_CACHE)
add_definitions(-DSBUF2_SERVER=0)

if (COMDB2_BBCMAKE)
  add_library(opencdb2api STATIC ${src})
  add_dependencies(opencdb2api  proto)
  target_link_libraries(opencdb2api PUBLIC protobuf-c)
  if(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    target_link_libraries(opencdb2api PUBLIC libunwind)
  endif()
  comdb2_bb_target(opencdb2api)
else()

add_library(objlib OBJECT ${src})
set_property(TARGET objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
add_dependencies(objlib proto)

add_library(cdb2api STATIC $<TARGET_OBJECTS:objlib>)
configure_file(cdb2api.pc cdb2api.pc @ONLY)
install(TARGETS cdb2api ARCHIVE DESTINATION lib)
install(FILES cdb2api.h DESTINATION include)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2api.pc
  DESTINATION lib/pkgconfig
)

if(NOT COMDB2_SKIP_CDB2API_SO)
  add_library(cdb2api_shared SHARED $<TARGET_OBJECTS:objlib>)
  set_target_properties(cdb2api_shared PROPERTIES OUTPUT_NAME cdb2api)
  if (NOT COMDB2_BUILD_STATIC)
    target_link_libraries(cdb2api_shared
      ${OPENSSL_LIBRARIES}
      ${ZLIB_LIBRARIES}
      ${PROTOBUF-C_LIBRARY}
      ${UNWIND_LIBRARY}
    )
  endif()
  install(TARGETS cdb2api_shared LIBRARY DESTINATION lib)
endif()

endif(COMDB2_BBCMAKE)

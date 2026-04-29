cmake_minimum_required(VERSION 3.20)
project(GalactikRMQ_Backend)

set(SOURCES galactik_rmq.c)
set(HEADERS galactik_rmq.h)

add_library(galactik_rmq STATIC ${SOURCES} ${HEADERS})

find_package(PkgConfig REQUIRED)
pkg_check_modules(RABBITMQ REQUIRED rabbitmq-c)

target_include_directories(galactik_rmq PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(galactik_rmq ${RABBITMQ_LIBRARIES} pthread)
target_compile_options(galactik_rmq PRIVATE -Wall -Wextra -g)

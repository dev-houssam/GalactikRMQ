cmake_minimum_required(VERSION 3.20)
project(GalactikRMQ_Frontend)

set(SOURCES main.cpp)

add_executable(galactik_rmq_frontend ${SOURCES})

find_package(PkgConfig REQUIRED)
pkg_check_modules(RABBITMQ REQUIRED rabbitmq-c)
find_package(nlohmann_json REQUIRED)

target_link_libraries(galactik_rmq_frontend
    galactik_rmq
    ${RABBITMQ_LIBRARIES}
    nlohmann_json::nlohmann_json
    pthread
)

target_include_directories(galactik_rmq_frontend PRIVATE
    ${CMAKE_SOURCE_DIR}/backend
    ${RABBITMQ_INCLUDE_DIRS}
)

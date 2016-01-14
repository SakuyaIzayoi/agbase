cmake_minimum_required(VERSION 3.0)
project(agbase)

set(agbase_VERSION_MAJOR 0)
set(agbase_VERSION_MINOR 1)

set(CMAKE_CXX_FLAGS "-std=c++0x")
set(CMAKE_CXX_COMPILER "clang++")

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

find_package(MySQL)

file(GLOB AGBASE_SRCFILES ${PROJECT_SOURCE_DIR}/src/*.cc)

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${MYSQL_INCLUDE_DIR})
include_directories(${MYSQL_INCLUDE_DIR}/private)

add_executable(agbase agbase.cc ${AGBASE_SRCFILES})

target_link_libraries(agbase ${MYSQL_LIBRARY})

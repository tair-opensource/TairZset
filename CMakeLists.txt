cmake_minimum_required (VERSION 3.0)

project(tairzset_module)

set(ROOT_DIR ${CMAKE_SOURCE_DIR})

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W -Wall -g -ggdb -std=c99 -O2 -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter")

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)

SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)  

include_directories(${ROOT_DIR}/dep)
aux_source_directory(${ROOT_DIR}/dep USRC)
add_subdirectory(src)

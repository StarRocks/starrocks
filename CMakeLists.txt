cmake_minimum_required(VERSION 3.15)
project(StarRocks)

set(CMAKE_CXX_STANDARD 11)

include_directories(
        be/
        be/src
        be/test

        gensrc/build
        gensrc/build/common
        gensrc/build/gen_cpp

        thirdparty/installed/gperftools
        thirdparty/installed/include
        thirdparty/installed/*
)


add_executable(StarRocks be/src/service/starrocks_main.cpp)
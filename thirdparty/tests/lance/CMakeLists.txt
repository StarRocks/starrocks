# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0.
cmake_minimum_required(VERSION 3.20)
project(lance_package_test LANGUAGES CXX)
find_package(StarRocksLance 0.1.0 EXACT REQUIRED CONFIG)
add_executable(lance_package_test package_test.cpp)
target_compile_features(lance_package_test PRIVATE cxx_std_17)
target_link_libraries(lance_package_test PRIVATE lance_rust)
enable_testing()
add_test(NAME lance_package_test COMMAND lance_package_test "${StarRocksLance_TEST_DATASET}")

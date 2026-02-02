# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(BASE_TEST_FILES
    cxa_throw_wrap.cpp
    bit/bit_mask_test.cpp
    concurrency/await_test.cpp
    concurrency/blocking_queue_test.cpp
    concurrency/concurrent_limiter_test.cpp
    concurrency/shared_mutex_test.cpp
    container/heap_test.cpp
    container/priority_queue_test.cpp
    crypto/md5_test.cpp
    json/easy_json-test.cpp
    path/file_util_test.cpp
    path/path_util_test.cpp
    path/path_trie_test.cpp
    simd/byte_stream_split_test.cpp
    simd/batch_run_counter_test.cpp
    simd/simd_expand_test.cpp
    simd/simd_test.cpp
    string/c_string_test.cpp
    string/faststring_test.cpp
    string/memcmp_test.cpp
    string/slice_test.cpp
    string/utf8_check_test.cpp
    utility/scoped_cleanup_test.cpp
    uuid/uuid_generator_test.cpp
    status_test.cpp
)

if (USE_AVX512)
    list(APPEND BASE_TEST_FILES simd/delta_decode_test.cpp)
endif()

set(BASE_TEST_LINK_LIBS
    Base
    Gutil
    StarRocksGen
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} -lresolv -liberty -lc -lm -ldl -rdynamic -pthread ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_library(starrocks_base_test_objs OBJECT ${BASE_TEST_FILES})
target_link_libraries(starrocks_base_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_base_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(base_test $<TARGET_OBJECTS:starrocks_base_test_objs>)
target_link_libraries(base_test ${BASE_TEST_LINK_LIBS} gtest_main)

# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

set(EXEC_FILES
        ./test_main.cpp

        ./storage/tablet_updates_test.cpp

        )
# simdjson Runtime Implement Dispatch: https://github.com/simdjson/simdjson/blob/master/doc/implementation-selection.md#runtime-cpu-detection
# Unset architecture-specific flags to avoid breaking implement runtime dispatch.

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx)
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx)
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./formats/json/binary_column_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./formats/json/numeric_column_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./formats/json/nullable_column_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
endif()

if (USE_AVX2)
    set(EXEC_FILES ${EXEC_FILES} ./column/avx_numeric_column_test.cpp)
endif()

add_executable(starrocks_test ${EXEC_FILES})

TARGET_LINK_LIBRARIES(starrocks_test ${TEST_LINK_LIBS})
SET_TARGET_PROPERTIES(starrocks_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
SET_TARGET_PROPERTIES(${PLUGIN_FILE_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}/test")

# bytes_test requires tcmalloc which conflicts with sanitizer.
# Thus we need a standalone binary without sanitizer to test it.
add_executable(bytes_test
        ./column/bytes_test.cpp
        ../src/service/mem_hook.cpp
        )

set_target_properties(bytes_test PROPERTIES
        COMPILE_OPTIONS
        "-lpthread;-std=gnu++17;-g;-fno-sanitize=address")

set_target_properties(bytes_test PROPERTIES
        LINK_OPTIONS
        "-static-libstdc++;-static-libgcc;-fno-sanitize=address")

TARGET_LINK_LIBRARIES(bytes_test
        pthread
        gtest
        tcmalloc
        )

add_executable(starrocks_column_to_arrow_test ./util/arrow/starrocks_column_to_arrow_test.cpp)
TARGET_LINK_LIBRARIES(starrocks_column_to_arrow_test ${TEST_LINK_LIBS})
SET_TARGET_PROPERTIES(starrocks_column_to_arrow_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_executable(engine_storage_migration_task_test ./storage/task/engine_storage_migration_task_test.cpp)
TARGET_LINK_LIBRARIES(engine_storage_migration_task_test ${TEST_LINK_LIBS})
SET_TARGET_PROPERTIES(engine_storage_migration_task_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

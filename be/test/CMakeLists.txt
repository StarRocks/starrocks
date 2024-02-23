set(EXEC_FILES
        ./exec/sink/connector_sink_operator_test.cpp
        ./connector_sink/hive_chunk_sink_test.cpp
        ./connector_sink/iceberg_chunk_sink_test.cpp
        ./connector_sink/file_chunk_sink_test.cpp
)

add_library(starrocks_test_objs OBJECT ${EXEC_FILES})
SET_TARGET_PROPERTIES(starrocks_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(starrocks_test test_main.cpp $<TARGET_OBJECTS:starrocks_test_objs>)
TARGET_LINK_LIBRARIES(starrocks_test ${TEST_LINK_LIBS})

# =================================================
# test cases must be compiled as a standalone binary
#ADD_BE_TEST(storage/task/engine_storage_migration_task_test)
#ADD_BE_TEST(storage/lake/compaction_task_test)

# you can simply add test case as single binary without adding `main`
# ADD_BE_TEST(exec/sorting_test)

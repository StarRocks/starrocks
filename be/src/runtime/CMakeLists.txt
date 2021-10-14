# This file is made available under Elastic License 2.0.
# This file is based on code available under the Apache license here:
#   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/CMakeLists.txt

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# add_subdirectory(bufferpool)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

set(RUNTIME_FILES
    broker_mgr.cpp
    buffer_control_block.cpp
    client_cache.cpp
    data_stream_mgr.cpp
    data_stream_sender.cpp
    datetime_value.cpp
    descriptors.cpp
    exec_env.cpp
    user_function_cache.cpp
    mem_pool.cpp
    plan_fragment_executor.cpp
    primitive_type.cpp
    raw_value.cpp
    raw_value_ir.cpp
    result_sink.cpp
    result_buffer_mgr.cpp
    result_writer.cpp
    row_batch.cpp
    runtime_state.cpp
    string_value.cpp
    thread_resource_mgr.cpp
    #  timestamp_value.cpp
    decimal_value.cpp
    decimalv2_value.cpp
    large_int_value.cpp
    tuple.cpp
    tuple_row.cpp
    vectorized_row_batch.cpp
    fragment_mgr.cpp
    dpp_sink_internal.cpp
    load_path_mgr.cpp
    types.cpp
    disk_io_mgr.cc
    disk_io_mgr_reader_context.cc
    disk_io_mgr_scan_range.cc 
    mem_tracker.cpp
    data_stream_recvr.cc
    #  export_task_mgr.cpp
    export_sink.cpp
    load_channel_mgr.cpp
    load_channel.cpp
    tablets_channel.cpp
    snapshot_loader.cpp
    query_statistics.cpp 
    message_body_sink.cpp
    stream_load/stream_load_context.cpp
    stream_load/stream_load_executor.cpp
    routine_load/data_consumer.cpp
    routine_load/data_consumer_group.cpp
    routine_load/data_consumer_pool.cpp
    routine_load/routine_load_task_executor.cpp
    small_file_mgr.cpp
    record_batch_queue.cpp
    result_queue_mgr.cpp
    memory_scratch_sink.cpp
    external_scan_context_mgr.cpp
    file_result_writer.cpp
    mysql_result_writer.cpp
    memory/system_allocator.cpp
    memory/chunk_allocator.cpp
    date_value.cpp
    timestamp_value.cpp
    vectorized/chunk_cursor.cpp
    vectorized/sorted_chunks_merger.cpp
    vectorized/time_types.cpp
    vectorized/statistic_result_writer.cpp
    hdfs/hdfs_fs_cache.cpp
    runtime_filter_worker.cpp
)

set(RUNTIME_FILES ${RUNTIME_FILES}
    mysql_table_writer.cpp
    mysql_table_sink.cpp
)

add_library(Runtime STATIC
    ${RUNTIME_FILES}
    )

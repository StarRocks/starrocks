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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

set(RUNTIME_CORE_FILES
    current_thread.cpp
    descriptors.cpp
    mem_tracker.cpp
    mem_pool.cpp
    starrocks_metrics.cpp
    memory/roaring_hook.cpp
    memory/system_allocator.cpp
    memory/mem_chunk_allocator.cpp
)

ADD_BE_LIB(RuntimeCore
    ${RUNTIME_CORE_FILES}
)

target_link_libraries(RuntimeCore PUBLIC
    ChunkCore
    ColumnCore
    TypesCore
    Common
    Base
    Gutil
    StarRocksGen
)

set(RUNTIME_FILES
    broker_mgr.cpp
    buffer_control_block.cpp
    buffer_control_result_writer.cpp
    client_cache.cpp
    local_pass_through_buffer.cpp
    lookup_stream_mgr.cpp
    data_stream_mgr.cpp
    sender_queue.cpp
    data_stream_sender.cpp
    multi_cast_data_stream_sink.cpp
    descriptors_ext.cpp
    exec_env.cpp
    exec.cpp
    global_variables.cpp
    user_function_cache.cpp
    jdbc_driver_manager.cpp
    plan_fragment_executor.cpp
    result_sink.cpp
    result_buffer_mgr.cpp
    runtime_state.cpp
    fragment_mgr.cpp
    load_path_mgr.cpp
    dummy_load_path_mgr.cpp
    agg_state_desc.cpp
    data_stream_recvr.cpp
    export_sink.cpp
    load_channel_mgr.cpp
    load_channel.cpp
    local_tablets_channel.cpp
    snapshot_loader.cpp
    query_statistics.cpp
    message_body_sink.cpp
    stream_load/transaction_mgr.cpp
    stream_load/stream_load_context.cpp
    stream_load/stream_load_executor.cpp
    stream_load/stream_load_pipe.cpp
    stream_load/time_bounded_stream_load_pipe.cpp
    batch_write/isomorphic_batch_write.cpp
    batch_write/batch_write_mgr.cpp
    batch_write/batch_write_util.cpp
    batch_write/txn_state_cache.cpp
    routine_load/data_consumer.cpp
    routine_load/data_consumer_group.cpp
    routine_load/data_consumer_pool.cpp
    routine_load/routine_load_task_executor.cpp
    small_file_mgr.cpp
    record_batch_queue.cpp
    result_queue_mgr.cpp
    memory_scratch_sink.cpp
    external_scan_context_mgr.cpp
    mysql_result_writer.cpp
    http_result_writer.cpp
    file_result_writer.cpp
    statistic_result_writer.cpp
    metadata_result_writer.cpp
    variable_result_writer.cpp
    arrow_result_writer.cpp
    chunk_cursor.cpp
    sorted_chunks_merger.cpp
    tablets_channel.cpp
    runtime_filter_worker.cpp
    global_dict/decoder.cpp
    global_dict/parser.cpp
    global_dict/miscs.cpp
    global_dict/types.cpp
    runtime_filter_cache.cpp
    lake_tablets_channel.cpp
    lake_snapshot_loader.cpp
    profile_report_worker.cpp
    schema_table_sink.cpp
    command_executor.cpp
    iceberg_table_sink.cpp
    hive_table_sink.cpp
    table_function_table_sink.cpp
    blackhole_table_sink.cpp
    dictionary_cache_sink.cpp
    type_pack.cpp
    customized_result_writer.cpp
    diagnose_daemon.cpp
    load_fail_point.cpp
    logconfig.cpp
    prof/heap_prof.cpp
)

set(RUNTIME_FILES ${RUNTIME_FILES}
    mysql_table_writer.cpp
    mysql_table_sink.cpp
)

ADD_BE_LIB(Runtime
    ${RUNTIME_FILES}
    )

target_link_libraries(Runtime PUBLIC RuntimeCore)

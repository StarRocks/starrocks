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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/compute_env")

set(COMPUTE_ENV_SORTING_FILES
    sorting/chunk_cursor.cpp
    sorting/compare_column.cpp
    sorting/data_segment.cpp
    sorting/merge.cpp
    sorting/merge_cascade.cpp
    sorting/merge_column.cpp
    sorting/merge_path.cpp
    sorting/sort_column.cpp
    sorting/sort_cursor.cpp
    sorting/sort_permute.cpp
    sorting/sorted_chunks_merger.cpp
)

set(COMPUTE_ENV_WORKGROUP_FILES
    workgroup/lock_free_scan_task_queue.cpp
    workgroup/lock_free_work_group_scan_task_queue.cpp
    workgroup/mem_tracker_manager.cpp
    workgroup/pipeline_executor_set.cpp
    workgroup/pipeline_executor_set_manager.cpp
    workgroup/priority_scan_task_queue.cpp
    workgroup/scan_executor.cpp
    workgroup/scan_task_queue_factory.cpp
    workgroup/work_group.cpp
    workgroup/work_group_manager.cpp
    workgroup/work_group_scan_task_queue.cpp
)

set(COMPUTE_ENV_SPILL_FILES
    spill/block_group.cpp
    spill/block_reader.cpp
    spill/data_stream.cpp
    spill/dir_manager.cpp
    spill/file_block_manager.cpp
    spill/hybird_block_manager.cpp
    spill/input_stream.cpp
    spill/log_block_manager.cpp
    spill/mem_table.cpp
    spill/operator_mem_resource_manager.cpp
    spill/query_spill_manager.cpp
    spill/restore_task.cpp
    spill/runtime_context.cpp
    spill/serde.cpp
    spill/spill_components.cpp
    spill/spill_metrics.cpp
    spill/spiller.cpp
    spill/spiller_factory.cpp
    spill/task_executor.cpp
)

set(COMPUTE_ENV_QUERY_CACHE_FILES
    query_cache/cache_manager.cpp
    query_cache/lane_arbiter.cpp
)

set(COMPUTE_ENV_QUERY_FILES
    query/connector_scan_mem_share_arbitrator.cpp
    query/global_late_materialization_context.cpp
)

set(COMPUTE_ENV_GLOBAL_DICT_FILES
    global_dict/fragment_dict_state.cpp
    global_dict/parser.cpp
)

set(COMPUTE_ENV_LOAD_PATH_FILES
    load_path/load_path_state_helper.cpp
    load_path/load_path_mgr.cpp
    load_path/dummy_load_path_mgr.cpp
    load_path/rejected_record_writer.cpp
)

ADD_BE_LIB(ComputeEnv
    compute_env.cpp
    data_stream/data_stream_mgr.cpp
    data_stream/data_stream_recvr.cpp
    data_stream/local_pass_through_buffer.cpp
    data_stream/sender_queue.cpp
    pipeline/driver_limiter.cpp
    pipeline/pipeline_timer_context.cpp
    profile_report_worker.cpp
    result/buffer_control_block.cpp
    result/record_batch_queue.cpp
    result/result_buffer_mgr.cpp
    result/result_queue_mgr.cpp
    ${COMPUTE_ENV_SORTING_FILES}
    ${COMPUTE_ENV_WORKGROUP_FILES}
    ${COMPUTE_ENV_SPILL_FILES}
    ${COMPUTE_ENV_QUERY_CACHE_FILES}
    ${COMPUTE_ENV_QUERY_FILES}
    ${COMPUTE_ENV_GLOBAL_DICT_FILES}
    ${COMPUTE_ENV_LOAD_PATH_FILES}
)

target_link_libraries(ComputeEnv PUBLIC
    StoragePrimitive
    ExecPrimitive
    ExprCore
    Serde
    RuntimeEnv
    RuntimeCore
    Platform
    FSCore
    IO
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

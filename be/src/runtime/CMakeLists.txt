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
    byte_buffer.cpp
    message_body_sink.cpp
    current_thread.cpp
    chunk_helper.cpp
    descriptors.cpp
    descriptors_ext.cpp
    runtime_state.cpp
    mem_tracker.cpp
    mem_pool.cpp
    runtime_filter.cpp
    runtime_filter_layout.cpp
    runtime_filter_builder.cpp
    runtime_filter_factory.cpp
    runtime_filter_cache.cpp
    bucket_aware_partition.cpp
    query_statistics.cpp
    runtime_metrics.cpp
    time_guard.cpp
    type_pack.cpp
    load_fail_point.cpp
    memory/roaring_hook.cpp
    memory/system_allocator.cpp
    memory/tracked_allocator.cpp
    memory/mem_chunk_allocator.cpp
    memory/memory_allocator.cpp
    exception_stack.cpp
    prof/heap_prof.cpp
)

ADD_BE_LIB(RuntimeCore
    ${RUNTIME_CORE_FILES}
)

target_link_libraries(RuntimeCore PRIVATE
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

set(RUNTIME_ENV_FILES
    env/diagnose_daemon.cpp
    env/global_env.cpp
    env/global_thread_pools.cpp
    env/java/java_env.cpp
)

ADD_BE_LIB(RuntimeEnv
    ${RUNTIME_ENV_FILES}
)

target_link_libraries(RuntimeEnv PUBLIC
    Platform
    RuntimeCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

set(RUNTIME_FILES
    lookup_stream_mgr.cpp
    exec_env.cpp
    jdbc_driver_manager.cpp
    pipeline_fragment_reporter.cpp
    runtime_state_helper.cpp
    stream_load/transaction_mgr.cpp
    stream_load/stream_load_executor.cpp
    routine_load/data_consumer.cpp
    routine_load/data_consumer_group.cpp
    routine_load/data_consumer_pool.cpp
    runtime_filter_merger.cpp
    runtime_filter_port.cpp
    runtime_filter_rpc.cpp
    runtime_filter_worker_context.cpp
    runtime_filter_serde.cpp
)

ADD_BE_LIB(Runtime
    ${RUNTIME_FILES}
    )

target_link_libraries(Runtime PUBLIC RuntimeEnv RuntimeCore ComputeEnv ExecPrimitive Platform Cache)
target_link_libraries(Runtime PRIVATE ExecRuntime)
target_link_libraries(Runtime PRIVATE ConnectorBuiltinRegistry)

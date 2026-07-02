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

set(RUNTIME_SRCS
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
    memory/memory_lock.cpp
    exception_stack.cpp
    prof/heap_prof.cpp
    serde/chunk_encode_context.cpp
    serde/protobuf_chunk_serde.cpp
    diagnose_daemon.cpp
    process_memory_metrics.cpp
    runtime_env.cpp
    global_thread_pools.cpp
    java/java_env.cpp
    java/java_global_ref.cpp
    java/jvm_helper.cpp
    java/jvm_class.cpp
)

ADD_BE_LIB(Runtime
    ${RUNTIME_SRCS}
)

target_link_libraries(Runtime PUBLIC
    Platform
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

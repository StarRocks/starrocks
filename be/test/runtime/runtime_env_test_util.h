// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common/config_exec_env_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_vector_index_fwd.h"

namespace starrocks::runtime_env_test {

inline void set_small_thread_pool_configs() {
    config::scanner_thread_pool_thread_num = 1;
    config::scanner_thread_pool_queue_size = 8;
    config::streaming_load_thread_pool_num_min = 0;
    config::streaming_load_thread_pool_idle_time_ms = 10;
    config::udf_thread_pool_size = 1;
    config::jvm_call_thread_pool_size = 1;
    config::automatic_partition_thread_pool_thread_num = 1;
    config::pipeline_prepare_thread_pool_thread_num = 1;
    config::pipeline_prepare_thread_pool_queue_size = 8;
    config::pipeline_sink_io_thread_pool_thread_num = 1;
    config::pipeline_sink_io_thread_pool_queue_size = 8;
    config::internal_service_query_rpc_thread_num = 1;
    config::internal_service_datacache_rpc_thread_num = 1;
    config::dictionary_cache_refresh_threadpool_size = 1;
    config::pipeline_exec_thread_pool_thread_num = 2;
    config::load_segment_thread_pool_num_max = 1;
    config::load_segment_thread_pool_queue_size = 8;
    config::put_combined_txn_log_thread_pool_num_max = 1;

    config::transaction_publish_version_worker_count = 1;
    config::pk_index_parallel_execution_threadpool_max_threads = 1;
    config::pk_index_parallel_execution_threadpool_size = 8;
    config::pk_index_memtable_flush_threadpool_max_threads = 1;
    config::pk_index_memtable_flush_threadpool_size = 8;
    config::lake_partial_update_thread_pool_max_threads = 1;
    config::lake_partial_update_thread_pool_queue_size = 8;
    config::lake_metadata_fetch_thread_count = 1;
    config::vector_index_build_max_cpu_ratio = 1.0;
    config::config_vector_index_build_concurrency = 1;
}

} // namespace starrocks::runtime_env_test

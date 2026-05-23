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

#include <cstdint>
#include <memory>

#include "common/status.h"

namespace starrocks {

class MetricRegistry;
class PriorityThreadPool;
class ThreadPool;

class GlobalThreadPools {
public:
    GlobalThreadPools();
    ~GlobalThreadPools();

    GlobalThreadPools(const GlobalThreadPools&) = delete;
    const GlobalThreadPools& operator=(const GlobalThreadPools&) = delete;

    Status init_execution_thread_pools(MetricRegistry* metrics);
    Status init_lake_thread_pools(MetricRegistry* metrics);

    void shutdown();
    void destroy();

    PriorityThreadPool* thread_pool() const { return _thread_pool.get(); }
    ThreadPool* streaming_load_thread_pool() const { return _streaming_load_thread_pool.get(); }
    ThreadPool* load_rowset_thread_pool() const { return _load_rowset_thread_pool.get(); }
    ThreadPool* load_segment_thread_pool() const { return _load_segment_thread_pool.get(); }
    ThreadPool* put_combined_txn_log_thread_pool() const { return _put_combined_txn_log_thread_pool.get(); }

    PriorityThreadPool* udf_call_pool() const { return _udf_call_pool.get(); }
    PriorityThreadPool* pipeline_prepare_pool() const { return _pipeline_prepare_pool.get(); }
    PriorityThreadPool* pipeline_sink_io_pool() const { return _pipeline_sink_io_pool.get(); }
    PriorityThreadPool* query_rpc_pool() const { return _query_rpc_pool.get(); }
    PriorityThreadPool* datacache_rpc_pool() const { return _datacache_rpc_pool.get(); }
    ThreadPool* load_rpc_pool() const { return _load_rpc_pool.get(); }
    ThreadPool* dictionary_cache_pool() const { return _dictionary_cache_pool.get(); }
    ThreadPool* automatic_partition_pool() const { return _automatic_partition_pool.get(); }

    ThreadPool* put_aggregate_metadata_thread_pool() const { return _put_aggregate_metadata_thread_pool.get(); }
    ThreadPool* lake_metadata_fetch_thread_pool() const { return _lake_metadata_fetch_thread_pool.get(); }
    ThreadPool* lake_vector_index_build_thread_pool() const { return _lake_vector_index_build_thread_pool.get(); }
    ThreadPool* pk_index_execution_thread_pool() const { return _pk_index_execution_thread_pool.get(); }
    ThreadPool* pk_index_memtable_flush_thread_pool() const { return _pk_index_memtable_flush_thread_pool.get(); }
    ThreadPool* lake_partial_update_thread_pool() const { return _lake_partial_update_thread_pool.get(); }

    int64_t max_executor_threads() const { return _max_executor_threads; }

private:
    std::unique_ptr<PriorityThreadPool> _thread_pool;
    std::unique_ptr<ThreadPool> _streaming_load_thread_pool;
    std::unique_ptr<ThreadPool> _load_rowset_thread_pool;
    std::unique_ptr<ThreadPool> _load_segment_thread_pool;
    std::unique_ptr<ThreadPool> _put_combined_txn_log_thread_pool;

    std::unique_ptr<PriorityThreadPool> _udf_call_pool;
    std::unique_ptr<PriorityThreadPool> _pipeline_prepare_pool;
    std::unique_ptr<PriorityThreadPool> _pipeline_sink_io_pool;
    std::unique_ptr<PriorityThreadPool> _query_rpc_pool;
    std::unique_ptr<PriorityThreadPool> _datacache_rpc_pool;
    std::unique_ptr<ThreadPool> _load_rpc_pool;
    std::unique_ptr<ThreadPool> _dictionary_cache_pool;
    std::unique_ptr<ThreadPool> _automatic_partition_pool;

    std::unique_ptr<ThreadPool> _put_aggregate_metadata_thread_pool;
    std::unique_ptr<ThreadPool> _lake_metadata_fetch_thread_pool;
    std::unique_ptr<ThreadPool> _lake_vector_index_build_thread_pool;
    std::unique_ptr<ThreadPool> _pk_index_execution_thread_pool;
    std::unique_ptr<ThreadPool> _pk_index_memtable_flush_thread_pool;
    std::unique_ptr<ThreadPool> _lake_partial_update_thread_pool;

    int64_t _max_executor_threads = 0;
};

} // namespace starrocks

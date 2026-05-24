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

#include "runtime/env/global_thread_pools.h"

#include <algorithm>
#include <limits>

#include "common/config_exec_env_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_vector_index_fwd.h"
#include "common/logging.h"
#include "common/system/cpu_info.h"
#include "common/thread/priority_thread_pool.hpp"
#include "common/thread/threadpool.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_metrics.h"

namespace starrocks {

GlobalThreadPools::GlobalThreadPools() = default;

GlobalThreadPools::~GlobalThreadPools() = default;

Status GlobalThreadPools::init_execution_thread_pools(MetricRegistry* metrics) {
    if (_put_combined_txn_log_thread_pool != nullptr) {
        return Status::OK();
    }

    _thread_pool = std::make_unique<PriorityThreadPool>("table_scan_io", // olap/external table scan thread pool
                                                        config::scanner_thread_pool_thread_num,
                                                        config::scanner_thread_pool_queue_size);

    // Thread pool used for streaming load to scan StreamLoadPipe. The maximum number of
    // threads and queue size are set INT32_MAX which indicate there is no limit for the
    // thread pool, and this can avoid deadlock for concurrent streaming loads. The thread
    // pool will not be full easily because fragment execution pool and http workers also
    // limit the streaming load concurrency which is controlled by fragment_pool_thread_num_max
    // and webserver_num_workers respectively. This pool will be used when
    // enable_streaming_load_thread_pool is true.
    RETURN_IF_ERROR(
            ThreadPoolBuilder("stream_load_io")
                    .set_min_threads(config::streaming_load_thread_pool_num_min)
                    .set_max_threads(INT32_MAX)
                    .set_max_queue_size(INT32_MAX)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(config::streaming_load_thread_pool_idle_time_ms))
                    .build(&_streaming_load_thread_pool));

    _udf_call_pool =
            std::make_unique<PriorityThreadPool>("udf", config::udf_thread_pool_size, config::udf_thread_pool_size);

    int automatic_partition_thread_num = config::automatic_partition_thread_pool_thread_num;
    int automatic_partition_queue_size = automatic_partition_thread_num * 10;
    RETURN_IF_ERROR(ThreadPoolBuilder("automatic_partition")
                            .set_min_threads(0)
                            .set_max_threads(automatic_partition_thread_num)
                            .set_max_queue_size(automatic_partition_queue_size)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_automatic_partition_pool));
    REGISTER_THREAD_POOL_RUNTIME_METRICS(metrics, automatic_partition, _automatic_partition_pool);

    int num_prepare_threads = config::pipeline_prepare_thread_pool_thread_num;
    if (num_prepare_threads == 0) {
        num_prepare_threads = CpuInfo::num_cores();
    } else if (num_prepare_threads < 0) {
        // -n: means n * num_cpu_cores
        num_prepare_threads = -num_prepare_threads * CpuInfo::num_cores();
    }
    _pipeline_prepare_pool = std::make_unique<PriorityThreadPool>("pip_prepare", num_prepare_threads,
                                                                  config::pipeline_prepare_thread_pool_queue_size);

    int num_sink_io_threads = config::pipeline_sink_io_thread_pool_thread_num;
    if (num_sink_io_threads <= 0) {
        num_sink_io_threads = CpuInfo::num_cores();
    }
    if (config::pipeline_sink_io_thread_pool_queue_size <= 0) {
        return Status::InvalidArgument("pipeline_sink_io_thread_pool_queue_size shoule be greater than 0");
    }
    _pipeline_sink_io_pool = std::make_unique<PriorityThreadPool>("pip_sink_io", num_sink_io_threads,
                                                                  config::pipeline_sink_io_thread_pool_queue_size);

    int query_rpc_threads = config::internal_service_query_rpc_thread_num;
    if (query_rpc_threads <= 0) {
        query_rpc_threads = CpuInfo::num_cores();
    }
    _query_rpc_pool =
            std::make_unique<PriorityThreadPool>("query_rpc", query_rpc_threads, std::numeric_limits<uint32_t>::max());

    int datacache_rpc_threads = config::internal_service_datacache_rpc_thread_num;
    if (datacache_rpc_threads <= 0) {
        datacache_rpc_threads = CpuInfo::num_cores();
    }
    _datacache_rpc_pool = std::make_unique<PriorityThreadPool>("datacache_rpc", datacache_rpc_threads,
                                                               std::numeric_limits<uint32_t>::max());

    // The _load_rpc_pool now handles routine load RPC and table function RPC.
    RETURN_IF_ERROR(ThreadPoolBuilder("load_rpc")
                            .set_min_threads(10)
                            .set_max_threads(1000)
                            .set_max_queue_size(0)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_load_rpc_pool));
    REGISTER_GAUGE_RUNTIME_METRIC(metrics, load_rpc_threadpool_size, [this]() { return _load_rpc_pool->num_threads(); })

    RETURN_IF_ERROR(ThreadPoolBuilder("dictionary_cache")
                            .set_min_threads(1)
                            .set_max_threads(config::dictionary_cache_refresh_threadpool_size)
                            .set_max_queue_size(INT32_MAX)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_dictionary_cache_pool));

    _max_executor_threads = CpuInfo::num_cores();
    if (config::pipeline_exec_thread_pool_thread_num > 0) {
        _max_executor_threads = config::pipeline_exec_thread_pool_thread_num;
    }
    _max_executor_threads = std::max<int64_t>(1, _max_executor_threads);
    LOG(INFO) << strings::Substitute("[PIPELINE] Exec thread pool: thread_num=$0", _max_executor_threads);

    RETURN_IF_ERROR(
            ThreadPoolBuilder("load_rowset_pool")
                    .set_min_threads(0)
                    .set_max_threads(config::load_segment_thread_pool_num_max)
                    .set_max_queue_size(config::load_segment_thread_pool_queue_size)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(config::streaming_load_thread_pool_idle_time_ms))
                    .build(&_load_rowset_thread_pool));

    RETURN_IF_ERROR(
            ThreadPoolBuilder("load_segment_pool")
                    .set_min_threads(0)
                    .set_max_threads(config::load_segment_thread_pool_num_max)
                    .set_max_queue_size(config::load_segment_thread_pool_queue_size)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(config::streaming_load_thread_pool_idle_time_ms))
                    .build(&_load_segment_thread_pool));

    RETURN_IF_ERROR(ThreadPoolBuilder("put_combined_txn_log_thread_pool")
                            .set_min_threads(0)
                            .set_max_threads(config::put_combined_txn_log_thread_pool_num_max)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(500))
                            .build(&_put_combined_txn_log_thread_pool));

    return Status::OK();
}

Status GlobalThreadPools::init_lake_thread_pools(MetricRegistry* metrics) {
    if (_lake_vector_index_build_thread_pool != nullptr) {
        return Status::OK();
    }

#if defined(USE_STAROS) && !defined(BE_TEST) && !defined(BUILD_FORMAT_LIB)
    int32_t max_thread_count = config::transaction_publish_version_worker_count;
    if (max_thread_count <= 0) {
        max_thread_count = CpuInfo::num_cores();
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("put_aggregate_metadata")
                            .set_min_threads(1)
                            .set_max_threads(std::max(1, max_thread_count))
                            .set_max_queue_size(std::numeric_limits<int>::max())
                            .build(&_put_aggregate_metadata_thread_pool));
    REGISTER_THREAD_POOL_RUNTIME_METRICS(metrics, put_aggregate_metadata, _put_aggregate_metadata_thread_pool);

    max_thread_count = config::pk_index_parallel_execution_threadpool_max_threads;
    if (max_thread_count <= 0) {
        max_thread_count = CpuInfo::num_cores() / 2;
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("cloud_native_pk_index_execution")
                            .set_min_threads(1)
                            .set_max_threads(std::max(1, max_thread_count))
                            .set_max_queue_size(config::pk_index_parallel_execution_threadpool_size)
                            .build(&_pk_index_execution_thread_pool));
    REGISTER_THREAD_POOL_RUNTIME_METRICS(metrics, cloud_native_pk_index_execution, _pk_index_execution_thread_pool);

    max_thread_count = config::pk_index_memtable_flush_threadpool_max_threads;
    if (max_thread_count <= 0) {
        max_thread_count = CpuInfo::num_cores() / 2;
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("cloud_native_pk_index_memtable_flush")
                            .set_min_threads(1)
                            .set_max_threads(std::max(1, max_thread_count))
                            .set_max_queue_size(config::pk_index_memtable_flush_threadpool_size)
                            .build(&_pk_index_memtable_flush_thread_pool));
    REGISTER_THREAD_POOL_RUNTIME_METRICS(metrics, cloud_native_pk_index_memtable_flush,
                                         _pk_index_memtable_flush_thread_pool);

    max_thread_count = config::lake_partial_update_thread_pool_max_threads;
    if (max_thread_count <= 0) {
        max_thread_count = CpuInfo::num_cores() / 2;
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("lake_partial_update")
                            .set_min_threads(0)
                            .set_max_threads(std::max(1, max_thread_count))
                            .set_max_queue_size(config::lake_partial_update_thread_pool_queue_size)
                            .build(&_lake_partial_update_thread_pool));
    REGISTER_THREAD_POOL_RUNTIME_METRICS(metrics, lake_partial_update, _lake_partial_update_thread_pool);
#elif defined(BE_TEST)
    RETURN_IF_ERROR(ThreadPoolBuilder("put_aggregate_metadata_pool")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .set_max_queue_size(std::numeric_limits<int>::max())
                            .build(&_put_aggregate_metadata_thread_pool));
    RETURN_IF_ERROR(ThreadPoolBuilder("cloud_native_pk_index_execution")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .set_max_queue_size(std::numeric_limits<int>::max())
                            .build(&_pk_index_execution_thread_pool));
    RETURN_IF_ERROR(ThreadPoolBuilder("cloud_native_pk_index_memtable_flush")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .set_max_queue_size(std::numeric_limits<int>::max())
                            .build(&_pk_index_memtable_flush_thread_pool));
    RETURN_IF_ERROR(ThreadPoolBuilder("lake_partial_update")
                            .set_min_threads(0)
                            .set_max_threads(4)
                            .set_max_queue_size(std::numeric_limits<int>::max())
                            .build(&_lake_partial_update_thread_pool));
#endif

    RETURN_IF_ERROR(ThreadPoolBuilder("lake_metadata_fetch")
                            .set_min_threads(0)
                            .set_max_threads(std::max(1, static_cast<int>(config::lake_metadata_fetch_thread_count)))
                            .set_max_queue_size(std::numeric_limits<int>::max())
                            .build(&_lake_metadata_fetch_thread_pool));
    REGISTER_THREAD_POOL_RUNTIME_METRICS(metrics, lake_metadata_fetch, _lake_metadata_fetch_thread_pool);

    int nproc = CpuInfo::num_cores();
    int budget = std::max(2, static_cast<int>(nproc * config::vector_index_build_max_cpu_ratio));
    int configured_omp = std::max(1, static_cast<int>(config::config_vector_index_build_concurrency));
    int effective_pool = std::max(1, budget / configured_omp);
    LOG(INFO) << "Vector index build adaptive sizing: nproc=" << nproc << " budget=" << budget
              << " pool=" << effective_pool << " omp=" << configured_omp;
    RETURN_IF_ERROR(ThreadPoolBuilder("lake_vi_build")
                            .set_min_threads(0)
                            .set_max_threads(effective_pool)
                            .set_max_queue_size(std::numeric_limits<int>::max())
                            .build(&_lake_vector_index_build_thread_pool));
    REGISTER_THREAD_POOL_RUNTIME_METRICS(metrics, lake_vi_build, _lake_vector_index_build_thread_pool);

    return Status::OK();
}

void GlobalThreadPools::shutdown() {
    if (_pipeline_sink_io_pool) _pipeline_sink_io_pool->shutdown();
    if (_put_aggregate_metadata_thread_pool) _put_aggregate_metadata_thread_pool->shutdown();
    if (_lake_metadata_fetch_thread_pool) _lake_metadata_fetch_thread_pool->shutdown();
    if (_lake_vector_index_build_thread_pool) _lake_vector_index_build_thread_pool->shutdown();
    if (_pk_index_execution_thread_pool) _pk_index_execution_thread_pool->shutdown();
    if (_pk_index_memtable_flush_thread_pool) _pk_index_memtable_flush_thread_pool->shutdown();
    if (_lake_partial_update_thread_pool) _lake_partial_update_thread_pool->shutdown();
    if (_automatic_partition_pool) _automatic_partition_pool->shutdown();
    if (_query_rpc_pool) _query_rpc_pool->shutdown();
    if (_datacache_rpc_pool) _datacache_rpc_pool->shutdown();
    if (_load_rpc_pool) _load_rpc_pool->shutdown();
    if (_thread_pool) _thread_pool->shutdown();
    if (_dictionary_cache_pool) _dictionary_cache_pool->shutdown();
    if (_udf_call_pool) _udf_call_pool->shutdown();
    if (_pipeline_prepare_pool) _pipeline_prepare_pool->shutdown();
    if (_streaming_load_thread_pool) _streaming_load_thread_pool->shutdown();
    if (_load_segment_thread_pool) _load_segment_thread_pool->shutdown();
    if (_load_rowset_thread_pool) _load_rowset_thread_pool->shutdown();
    if (_put_combined_txn_log_thread_pool) _put_combined_txn_log_thread_pool->shutdown();
}

void GlobalThreadPools::destroy() {
    _udf_call_pool.reset();
    _pipeline_prepare_pool.reset();
    _pipeline_sink_io_pool.reset();
    _query_rpc_pool.reset();
    _datacache_rpc_pool.reset();
    _load_rpc_pool.reset();
    _thread_pool.reset();
    _streaming_load_thread_pool.reset();
    _load_segment_thread_pool.reset();
    _load_rowset_thread_pool.reset();
    _put_combined_txn_log_thread_pool.reset();
    _dictionary_cache_pool.reset();
    _automatic_partition_pool.reset();
    _put_aggregate_metadata_thread_pool.reset();
    _lake_metadata_fetch_thread_pool.reset();
    _lake_vector_index_build_thread_pool.reset();
    _pk_index_execution_thread_pool.reset();
    _pk_index_memtable_flush_thread_pool.reset();
    _lake_partial_update_thread_pool.reset();
    _max_executor_threads = 0;
}

} // namespace starrocks

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

#include "compute_env/compute_env.h"

#include <algorithm>
#include <utility>

#include "base/metrics.h"
#include "common/config_exec_env_fwd.h"
#include "common/logging.h"
#include "common/system/cpu_info.h"
#include "compute_env/data_stream/data_stream_mgr.h"
#include "compute_env/dictionary_cache/dictionary_cache_manager.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_context_mgr.h"
#include "compute_env/load_path/dummy_load_path_mgr.h"
#include "compute_env/load_path/load_path_mgr.h"
#include "compute_env/pipeline/driver_limiter.h"
#include "compute_env/pipeline/pipeline_timer.h"
#include "compute_env/profile_report_worker.h"
#include "compute_env/query_cache/cache_manager.h"
#include "compute_env/result/result_buffer_mgr.h"
#include "compute_env/result/result_queue_mgr.h"
#include "compute_env/spill/dir_manager.h"
#include "compute_env/spill/global_spill_manager.h"
#include "compute_env/spill/spill_metrics.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "exec/pipeline/primitives/pipeline_metrics.h"
#include "runtime/runtime_env.h"

namespace starrocks {

ComputeEnv::ComputeEnv() = default;

ComputeEnv::~ComputeEnv() = default;

Status ComputeEnv::init(const ComputeEnvOptions& options) {
    if (options.runtime_env == nullptr) {
        return Status::InternalError("ComputeEnv RuntimeEnv is null");
    }
    if (!options.driver_queue_factory || !options.driver_executor_factory) {
        return Status::InternalError("ComputeEnv workgroup driver factories must be set");
    }
    if (_driver_limiter != nullptr || _pipeline_timer != nullptr || _workgroup_manager != nullptr) {
        return Status::InternalError("ComputeEnv has been initialized");
    }

    const int64_t max_executor_threads = options.runtime_env->max_executor_threads();
    if (max_executor_threads <= 0) {
        return Status::InternalError("RuntimeEnv execution thread pools are not initialized");
    }
    const int max_num_pipeline_drivers = max_executor_threads * config::pipeline_max_num_drivers_per_exec_thread;

    _dictionary_cache_manager = std::make_unique<DictionaryCacheManager>();
    _load_stream_mgr = std::make_unique<LoadStreamMgr>();
    _stream_context_mgr = std::make_unique<StreamContextMgr>(_load_stream_mgr.get());

    auto driver_limiter = std::make_unique<pipeline::DriverLimiter>(max_num_pipeline_drivers);
    auto pipeline_timer = std::make_unique<pipeline::PipelineTimer>();
    auto stream_mgr = std::make_unique<DataStreamMgr>(options.metrics);
    auto result_mgr = std::make_unique<ResultBufferMgr>(options.metrics);
    auto result_queue_mgr = std::make_unique<ResultQueueMgr>(options.metrics);
    _load_stream_mgr->install_metrics(options.metrics);
    RETURN_IF_ERROR(pipeline_timer->start());

    driver_limiter->init(options.metrics);
    _driver_limiter = std::move(driver_limiter);
    _pipeline_timer = std::move(pipeline_timer);
    _stream_mgr = std::move(stream_mgr);
    _result_mgr = std::move(result_mgr);
    _result_queue_mgr = std::move(result_queue_mgr);

    Status status = _init_workgroup(options, max_executor_threads);
    if (!status.ok()) {
        destroy();
        return status;
    }
    status = _start_result_mgr();
    if (!status.ok()) {
        destroy();
        return status;
    }

    status = _init_load_path(options.store_paths, options.store_paths.empty() && options.as_cn);
    if (!status.ok()) {
        destroy();
        return status;
    }

    const size_t query_cache_capacity = options.query_cache_capacity == 0
                                                ? std::max<size_t>(config::query_cache_capacity, 4L * 1024 * 1024)
                                                : options.query_cache_capacity;
    status = _init_query_cache(query_cache_capacity, options.metrics);
    if (!status.ok()) {
        destroy();
        return status;
    }

    status = _init_spill(options.store_paths, options.metrics);
    if (!status.ok()) {
        destroy();
        return status;
    }

    return Status::OK();
}

Status ComputeEnv::_init_workgroup(const ComputeEnvOptions& options, int64_t max_executor_threads) {
    const int num_io_threads = config::pipeline_scan_thread_pool_thread_num <= 0
                                       ? CpuInfo::num_cores()
                                       : config::pipeline_scan_thread_pool_thread_num;
    int connector_num_io_threads = int(config::pipeline_connector_scan_thread_num_per_cpu * CpuInfo::num_cores());
#ifdef BE_TEST
    connector_num_io_threads = std::min(connector_num_io_threads, 2);
#endif
    CHECK_GT(connector_num_io_threads, 0) << "pipeline_connector_scan_thread_num_per_cpu should greater than 0";

    if (config::hdfs_client_enable_hedged_read) {
        // Set hdfs client hedged read pool size.
        config::hdfs_client_hedged_read_threadpool_size =
                std::min(connector_num_io_threads * 2, config::hdfs_client_hedged_read_threadpool_size);
        CHECK_GT(config::hdfs_client_hedged_read_threadpool_size, 0)
                << "hdfs_client_hedged_read_threadpool_size should greater than 0";
    }

    // Disable bind cpus when cgroup has cpu quota but no cpuset.
    const bool enable_bind_cpus = config::enable_resource_group_bind_cpus &&
                                  (!CpuInfo::is_cgroup_with_cpu_quota() || CpuInfo::is_cgroup_with_cpuset());
    config::enable_resource_group_bind_cpus = enable_bind_cpus;
    workgroup::PipelineExecutorSetConfig executors_manager_opts(
            CpuInfo::num_cores(), max_executor_threads, num_io_threads, connector_num_io_threads,
            CpuInfo::get_core_ids(), enable_bind_cpus, config::enable_resource_group_cpu_borrowing,
            pipeline::PipelineExecutorMetrics::instance());
    auto workgroup_manager = std::make_unique<workgroup::WorkGroupManager>(
            std::move(executors_manager_opts), options.metrics, options.driver_queue_factory,
            options.driver_executor_factory);
    RETURN_IF_ERROR(workgroup_manager->start());
    workgroup::DefaultWorkGroupInitialization default_workgroup_init(workgroup_manager.get(), max_executor_threads);

    _workgroup_manager = std::move(workgroup_manager);
    return Status::OK();
}

Status ComputeEnv::_init_load_path(std::vector<std::string> store_paths, bool use_dummy_load_path_mgr) {
    if (_load_path_mgr != nullptr) {
        return Status::InternalError("LoadPathMgr has been initialized");
    }
    std::unique_ptr<BaseLoadPathMgr> load_path_mgr;
    if (use_dummy_load_path_mgr) {
        load_path_mgr = std::make_unique<DummyLoadPathMgr>();
    } else {
        load_path_mgr = std::make_unique<LoadPathMgr>(std::move(store_paths));
    }
    RETURN_IF_ERROR(load_path_mgr->init());
    _load_path_mgr = std::move(load_path_mgr);
    return Status::OK();
}

Status ComputeEnv::_init_spill(const std::vector<std::string>& store_paths, MetricRegistry* metrics) {
    auto spill_dir_mgr = std::make_shared<spill::DirManager>();
    RETURN_IF_ERROR(spill_dir_mgr->init(config::spill_local_storage_dir, store_paths));

    if (metrics != nullptr) {
        if (auto* spill_metrics = SpillMetrics::instance(); spill_metrics->local_disk_bytes_used() != nullptr) {
            std::weak_ptr<spill::DirManager> weak_dir_mgr = spill_dir_mgr;
            metrics->register_hook("spill_disk_bytes_used", [weak_dir_mgr, spill_metrics]() {
                auto dir_mgr = weak_dir_mgr.lock();
                if (dir_mgr == nullptr) {
                    return;
                }
                int64_t local_bytes = 0;
                for (auto& dir : dir_mgr->dirs()) {
                    local_bytes += dir->get_current_size();
                }
                spill_metrics->local_disk_bytes_used()->set_value(local_bytes);
            });
        }
    }

    _spill_dir_mgr = std::move(spill_dir_mgr);
    _global_spill_manager = std::make_shared<spill::GlobalSpillManager>();
    return Status::OK();
}

Status ComputeEnv::_init_query_cache(size_t capacity, MetricRegistry* metrics) {
    _cache_mgr = std::make_unique<query_cache::CacheManager>(capacity);
    RETURN_IF_ERROR(_cache_mgr->install_metrics(metrics));
    return Status::OK();
}

Status ComputeEnv::init_profile_report_worker(ProfileReportWorkerOptions options) {
    if (_profile_report_worker != nullptr) {
        return Status::InternalError("ProfileReportWorker has been initialized");
    }
    if (options.report_non_pipeline_fragments == nullptr || options.report_pipeline_fragments == nullptr) {
        return Status::InternalError("ProfileReportWorker report callbacks must be set");
    }
    _profile_report_worker = std::make_unique<ProfileReportWorker>(std::move(options));
    return Status::OK();
}

void ComputeEnv::stop() {
    _stop_stream_load_pipes();
    if (_stream_mgr != nullptr) {
        _stream_mgr->close();
    }
    _stop_workgroup();
    _stop_result_mgr();
}

void ComputeEnv::_stop_stream_load_pipes() {
    if (_load_stream_mgr != nullptr) {
        _load_stream_mgr->close();
    }
}

void ComputeEnv::stop_profile_report_worker() {
    if (_profile_report_worker != nullptr) {
        _profile_report_worker->close();
    }
}

void ComputeEnv::_stop_workgroup() {
    if (_workgroup_manager != nullptr) {
        _workgroup_manager->close();
    }
}

Status ComputeEnv::_start_result_mgr() {
    return _result_mgr == nullptr ? Status::OK() : _result_mgr->init();
}

void ComputeEnv::_stop_result_mgr() {
    if (_result_mgr != nullptr) {
        _result_mgr->stop();
    }
}

void ComputeEnv::_destroy_stream_context_mgr() {
    _stream_context_mgr.reset();
}

void ComputeEnv::destroy_profile_report_worker() {
    stop_profile_report_worker();
    _profile_report_worker.reset();
}

void ComputeEnv::_destroy_load_path() {
    _load_path_mgr.reset();
}

void ComputeEnv::destroy() {
    destroy_profile_report_worker();
    _destroy_load_path();
    _global_spill_manager.reset();
    _spill_dir_mgr.reset();
    if (_workgroup_manager != nullptr) {
        _workgroup_manager->close();
        _workgroup_manager->destroy();
    }
    _workgroup_manager.reset();
    _stop_result_mgr();
    _result_queue_mgr.reset();
    _result_mgr.reset();
    _destroy_stream_context_mgr();
    _stop_stream_load_pipes();
    _load_stream_mgr.reset();
    _stream_mgr.reset();
    _cache_mgr.reset();
    _driver_limiter.reset();
    _pipeline_timer.reset();
    _dictionary_cache_manager.reset();
}

} // namespace starrocks

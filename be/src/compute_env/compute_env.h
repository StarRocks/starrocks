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
#include <vector>

#include "common/status.h"
#include "compute_env/workgroup/work_group_manager.h"

namespace starrocks {

class DataStreamMgr;
class MetricRegistry;
class ProfileReportWorker;
class ResultBufferMgr;
class ResultQueueMgr;
struct ProfileReportWorkerOptions;

namespace pipeline {
class DriverLimiter;
class PipelineTimer;
} // namespace pipeline

namespace spill {
class DirManager;
class GlobalSpillManager;
} // namespace spill

struct ComputeEnvOptions {
    int max_num_pipeline_drivers = 0;
    MetricRegistry* metrics = nullptr;
};

struct ComputeEnvWorkGroupOptions {
    int64_t max_executor_threads = 0;
    MetricRegistry* metrics = nullptr;
    workgroup::WorkGroupManager::DriverQueueFactory driver_queue_factory;
    workgroup::DriverExecutorFactory driver_executor_factory;
};

class ComputeEnv {
public:
    ComputeEnv();
    ~ComputeEnv();

    ComputeEnv(const ComputeEnv&) = delete;
    ComputeEnv& operator=(const ComputeEnv&) = delete;

    Status init(const ComputeEnvOptions& options);
    Status init_workgroup(const ComputeEnvWorkGroupOptions& options);
    Status init_spill(const std::vector<std::string>& store_paths, MetricRegistry* metrics);
    Status init_profile_report_worker(ProfileReportWorkerOptions options);
    void stop();
    void stop_workgroup();
    void stop_profile_report_worker();
    Status start_result_mgr();
    void stop_result_mgr();
    void destroy_profile_report_worker();
    void destroy();

    pipeline::DriverLimiter* driver_limiter() const { return _driver_limiter.get(); }
    pipeline::PipelineTimer* pipeline_timer() const { return _pipeline_timer.get(); }
    DataStreamMgr* stream_mgr() const { return _stream_mgr.get(); }
    ResultBufferMgr* result_mgr() const { return _result_mgr.get(); }
    ResultQueueMgr* result_queue_mgr() const { return _result_queue_mgr.get(); }
    workgroup::WorkGroupManager* workgroup_manager() const { return _workgroup_manager.get(); }
    spill::DirManager* spill_dir_mgr() const { return _spill_dir_mgr.get(); }
    spill::GlobalSpillManager* global_spill_manager() const { return _global_spill_manager.get(); }
    ProfileReportWorker* profile_report_worker() const { return _profile_report_worker.get(); }

private:
    std::unique_ptr<pipeline::DriverLimiter> _driver_limiter;
    std::unique_ptr<pipeline::PipelineTimer> _pipeline_timer;
    std::unique_ptr<DataStreamMgr> _stream_mgr;
    std::unique_ptr<ResultBufferMgr> _result_mgr;
    std::unique_ptr<ResultQueueMgr> _result_queue_mgr;
    std::unique_ptr<workgroup::WorkGroupManager> _workgroup_manager;
    std::shared_ptr<spill::DirManager> _spill_dir_mgr;
    std::shared_ptr<spill::GlobalSpillManager> _global_spill_manager;
    std::unique_ptr<ProfileReportWorker> _profile_report_worker;
};

} // namespace starrocks

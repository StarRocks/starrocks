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

#include "common/status.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/cpu_util.h"

namespace starrocks::pipeline {
class PipelineExecutorMetrics;
}
namespace starrocks::workgroup {

struct PipelineExecutorSetConfig {
    PipelineExecutorSetConfig(uint32_t num_total_cores, uint32_t num_total_driver_threads,
                              uint32_t num_total_scan_threads, uint32_t num_total_connector_scan_threads,
                              CpuUtil::CpuIds total_cpuids, bool enable_bind_cpus, bool enable_cpu_borrowing,
                              pipeline::PipelineExecutorMetrics* metrics);

    std::string to_string() const;

    const uint32_t num_total_cores;
    const uint32_t num_total_driver_threads;
    const uint32_t num_total_scan_threads;
    uint32_t num_total_connector_scan_threads;

    const CpuUtil::CpuIds total_cpuids;

    const bool enable_bind_cpus;
    bool enable_cpu_borrowing;

    pipeline::PipelineExecutorMetrics* metrics;
};

class PipelineExecutorSet {
public:
    PipelineExecutorSet(const PipelineExecutorSetConfig& conf, std::string name, CpuUtil::CpuIds cpuids,
                        std::vector<CpuUtil::CpuIds> borrowed_cpuids);
    ~PipelineExecutorSet();

    Status start();
    void close();

    void change_cpus(CpuUtil::CpuIds cpuids, std::vector<CpuUtil::CpuIds> borrowed_cpuids);
    void notify_num_total_connector_scan_threads_changed() const;
    void notify_config_changed() const;

    pipeline::DriverExecutor* driver_executor() const { return _driver_executor.get(); }
    ScanExecutor* scan_executor() const { return _scan_executor.get(); }
    ScanExecutor* connector_scan_executor() const { return _connector_scan_executor.get(); }

    std::string to_string() const;

private:
    uint32_t num_driver_threads() const { return calculate_num_threads(_conf.num_total_driver_threads); }
    uint32_t num_scan_threads() const { return calculate_num_threads(_conf.num_total_scan_threads); }
    uint32_t num_connector_scan_threads() const {
        return calculate_num_threads(_conf.num_total_connector_scan_threads);
    }
    uint32_t calculate_num_threads(uint32_t num_total_threads) const;

private:
    const PipelineExecutorSetConfig& _conf;
    const std::string _name;

    CpuUtil::CpuIds _cpuids;
    std::vector<CpuUtil::CpuIds> _borrowed_cpu_ids;

    std::unique_ptr<pipeline::DriverExecutor> _driver_executor;
    std::unique_ptr<ScanExecutor> _scan_executor;
    std::unique_ptr<ScanExecutor> _connector_scan_executor;

    enum class Stage : uint8_t {
        CREATED = 0,
        STARTED = 1,
        CLOSED = 2,
    };
    Stage _stage = Stage::CREATED;
};

} // namespace starrocks::workgroup

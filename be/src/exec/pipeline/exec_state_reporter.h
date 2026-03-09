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

#include <memory>

#include "common/status.h"
#include "common/system/backend_options.h"
#include "common/thread/threadpool.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/MVMaintenance_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {
class ExecEnv;
class RuntimeProfile;

namespace pipeline {
class ExecStateReporterMetrics;

class ExecStateReporter {
public:
    explicit ExecStateReporter(const CpuUtil::CpuIds& cpuids, ExecStateReporterMetrics* metrics);
    ~ExecStateReporter();

    static std::unique_ptr<TReportExecStatusParams> create_report_exec_status_params(
            QueryContext* query_ctx, FragmentContext* fragment_ctx, RuntimeProfile* profile,
            RuntimeProfile* load_channel_profile, const Status& status, bool done);

    static Status report_exec_status(const TReportExecStatusParams& params, ExecEnv* exec_env,
                                     const TNetworkAddress& fe_addr);

    void submit(std::function<void()>&& report_task, bool priority = false);

    void bind_cpus(const CpuUtil::CpuIds& cpuids) const;

    Status update_max_threads(int max_threads);
    Status update_priority_max_threads(int max_threads);

    // STREAM MV
    static TMVMaintenanceTasks create_report_epoch_params(const QueryContext* query_ctx,
                                                          const std::vector<FragmentContext*>& fragment_ctxs);

    static Status report_epoch(const TMVMaintenanceTasks& params, ExecEnv* exec_env, const TNetworkAddress& fe_addr);

public:
    // Accessors exposed only for unit tests (via the friend declaration above).
    // Normal code paths must not call these methods.
    int TEST_pool_max_threads() const { return _thread_pool->max_threads(); }
    int TEST_priority_pool_max_threads() const { return _priority_thread_pool->max_threads(); }

private:
    ExecStateReporterMetrics* _metrics;
    std::unique_ptr<ThreadPool> _thread_pool;
    std::unique_ptr<ThreadPool> _priority_thread_pool;
};
} // namespace pipeline
} // namespace starrocks

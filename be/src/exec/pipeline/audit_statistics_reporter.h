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

#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/threadpool.h"

namespace starrocks::pipeline {
class AuditStatisticsReporter {
public:
    AuditStatisticsReporter();

    static Status report_audit_statistics(const TReportAuditStatisticsParams& params, ExecEnv* exec_env,
                                          const TNetworkAddress& fe_addr);

    [[nodiscard]] Status submit(std::function<void()>&& report_task);

private:
    std::unique_ptr<ThreadPool> _thread_pool;
};
} // namespace starrocks::pipeline

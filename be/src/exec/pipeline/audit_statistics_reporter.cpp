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

#include "exec/pipeline/audit_statistics_reporter.h"

#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "gen_cpp/FrontendService_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::pipeline {

AuditStatisticsReporter::AuditStatisticsReporter() {
    auto status = ThreadPoolBuilder("audit_report")
                          .set_min_threads(1)
                          .set_max_threads(2)
                          .set_max_queue_size(1000)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                          .build(&_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for ExecStateReport: error=" << status.to_string();
    }
}

// including the final status when execution finishes.
Status AuditStatisticsReporter::report_audit_statistics(const TReportAuditStatisticsParams& params, ExecEnv* exec_env,
                                                        const TNetworkAddress& fe_addr) {
    TReportAuditStatisticsResult res;
    Status rpc_status;
    rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
            fe_addr.hostname, fe_addr.port,
            [&res, &params](FrontendServiceConnection& client) { client->reportAuditStatistics(res, params); },
            config::thrift_rpc_timeout_ms);
    if (rpc_status.ok()) {
        rpc_status = Status(res.status);
    }
    return rpc_status;
}

Status AuditStatisticsReporter::submit(std::function<void()>&& report_task) {
    return _thread_pool->submit_func(std::move(report_task));
}
} // namespace starrocks::pipeline

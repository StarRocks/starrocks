// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "agent/report_task.h"

#include "agent/utils.h"
#include "runtime/exec_env.h"

namespace starrocks {

AgentStatus report_task(const TReportRequest& request, TMasterResult* result) {
    MasterServerClient client(ExecEnv::GetInstance()->frontend_client_cache());
    return client.report(request, result);
}

} // namespace starrocks

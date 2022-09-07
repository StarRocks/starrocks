// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "agent/task_worker_pool.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"

namespace starrocks {

void run_drop_tablet_task(std::shared_ptr<DropTabletAgentTaskRequest> agent_task_req);
void run_create_tablet_task(std::shared_ptr<CreateTabletAgentTaskRequest> agent_task_req, ExecEnv* exec_env);

} // namespace starrocks

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

#include "finish_task.h"

#include "agent/status.h"
#include "agent/utils.h"
#include "common/logging.h"
#include "runtime/exec_env.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

const uint32_t TASK_FINISH_MAX_RETRY = 3;
const uint32_t ALTER_FINISH_TASK_MAX_RETRY = 10;

void finish_task(const TFinishTaskRequest& finish_task_request) {
    // Return result to FE
    TMasterResult result;
    uint32_t try_time = 0;
    int32_t sleep_seconds = 1;
    int32_t max_retry_times = TASK_FINISH_MAX_RETRY;

    MasterServerClient client(ExecEnv::GetInstance()->frontend_client_cache());

    while (try_time < max_retry_times) {
        StarRocksMetrics::instance()->finish_task_requests_total.increment(1);
        AgentStatus client_status = client.finish_task(finish_task_request, &result);

        if (client_status == STARROCKS_SUCCESS) {
            // This means FE alter thread pool is full, all alter finish request to FE is meaningless
            // so that we will sleep && retry 10 times
            if (result.status.status_code == TStatusCode::TOO_MANY_TASKS &&
                finish_task_request.task_type == TTaskType::ALTER) {
                max_retry_times = ALTER_FINISH_TASK_MAX_RETRY;
                sleep_seconds = sleep_seconds * 2;
            } else {
                break;
            }
        }
        try_time += 1;
        StarRocksMetrics::instance()->finish_task_requests_failed.increment(1);
        LOG(WARNING) << "finish task failed retry: " << try_time << "/" << TASK_FINISH_MAX_RETRY
                     << "client_status: " << client_status << " status_code: " << result.status.status_code;

        sleep(sleep_seconds);
    }
}

void finish_req(const TFinishRequest& finish_request) {
    // Return result to FE
    TMasterResult result;
    uint32_t try_time = 0;
    int32_t sleep_seconds = 1;
    int32_t max_retry_times = TASK_FINISH_MAX_RETRY;

    MasterServerClient client(&g_frontend_service_client_cache);

    while (try_time < max_retry_times) {
        AgentStatus client_status = client.finish_req(finish_request, &result);

        if (client_status == STARROCKS_SUCCESS) {
            // This means FE alter thread pool is full, all alter finish request to FE is meaningless
            // so that we will sleep && retry 10 times
            if (result.status.status_code == TStatusCode::TOO_MANY_TASKS &&
                finish_request.task_type == TTaskType::ALTER) {
                max_retry_times = ALTER_FINISH_TASK_MAX_RETRY;
                sleep_seconds = sleep_seconds * 2;
            } else {
                break;
            }
        }
        try_time += 1;
        LOG(WARNING) << "finish failed retry: " << try_time << "/" << TASK_FINISH_MAX_RETRY
                     << "client_status: " << client_status << " status_code: " << result.status.status_code;

        sleep(sleep_seconds);
    }
}

} // namespace starrocks

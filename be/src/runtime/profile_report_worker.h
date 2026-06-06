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

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "base/hash/hash_std.hpp"
#include "base/uid_util.h"
#include "common/status.h"
#include "common/thread/thread.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/profile_report_task.h"

namespace starrocks {

class FragmentMgr;

class TUniqueId;

namespace pipeline {
class QueryContextManager;
}

struct NonPipelineReportTask {
    NonPipelineReportTask(int64_t last_report_time, TQueryType::type task_type)
            : last_report_time(last_report_time), task_type(task_type) {}

    int64_t last_report_time;
    TQueryType::type task_type;
};

struct PipelineReportTask {
    PipelineReportTask(int64_t last_report_time, TQueryType::type task_type)
            : last_report_time(last_report_time), task_type(task_type) {}

    int64_t last_report_time;
    TQueryType::type task_type;
};

class ProfileReportWorker {
public:
    ProfileReportWorker(FragmentMgr* fragment_mgr, pipeline::QueryContextManager* query_context_manager);
    ~ProfileReportWorker() = default;
    void execute();
    void close();
    Status register_non_pipeline_load(const TUniqueId& fragment_instance_id);
    void unregister_non_pipeline_load(const TUniqueId& fragment_instance_id);

    Status register_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id);
    void unregister_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id);

private:
    void _start_report_profile();
    void _unregister_pipeline_loads(const std::vector<PipeLineReportTaskKey>& tasks);
    void _unregister_non_pipeline_loads(const std::vector<TUniqueId>& fragment_instance_ids);

    std::unordered_map<PipeLineReportTaskKey, PipelineReportTask, PipeLineReportTaskKeyHasher,
                       PipeLineReportTaskKeyEqual>
            _pipeline_report_tasks;
    std::mutex _pipeline_report_mutex;

    std::unordered_map<TUniqueId, NonPipelineReportTask> _non_pipeline_report_tasks;
    std::mutex _non_pipeline_report_mutex;

    FragmentMgr* _fragment_mgr;
    pipeline::QueryContextManager* _query_context_manager;
    std::atomic<bool> _stop{false};
    std::thread _thread;
};

} // namespace starrocks

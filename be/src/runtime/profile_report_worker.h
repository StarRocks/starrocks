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
#include <thread>

#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "util/thread.h"

namespace starrocks {

class TUniqueId;

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

struct PipeLineReportTaskKey {
    PipeLineReportTaskKey(const TUniqueId& query_id, const TUniqueId& fragment_instance_id)
            : query_id(query_id), fragment_instance_id(fragment_instance_id) {}

    TUniqueId query_id;
    TUniqueId fragment_instance_id;
};

struct PipeLineReportTaskKeyEqual {
    bool operator()(const PipeLineReportTaskKey& k1, const PipeLineReportTaskKey& k2) const {
        return k1.query_id == k2.query_id && k1.fragment_instance_id == k2.fragment_instance_id;
    }
};

struct PipeLineReportTaskKeyHasher {
    std::size_t operator()(const PipeLineReportTaskKey& key) const {
        return (((std::hash<int64_t>()(key.query_id.lo) + (std::hash<int64_t>()(key.query_id.hi) >> 4)) +
                 (std::hash<int64_t>()(key.fragment_instance_id.hi) >> 8)) +
                (std::hash<int64_t>()(key.fragment_instance_id.hi) >> 12));
    }
};

class ProfileReportWorker {
public:
    ProfileReportWorker(ExecEnv* env);
    ~ProfileReportWorker();
    void execute();
    Status register_non_pipeline_load(const TUniqueId& fragment_instance_id);
    void unregister_non_pipeline_load(const TUniqueId& fragment_instance_id);

    Status register_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id);
    void unregister_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id);

private:
    void _start_report_profile();

    std::unordered_map<PipeLineReportTaskKey, PipelineReportTask, PipeLineReportTaskKeyHasher,
                       PipeLineReportTaskKeyEqual>
            _pipeline_report_tasks;
    std::mutex _pipeline_report_mutex;

    std::unordered_map<TUniqueId, NonPipelineReportTask> _non_pipeline_report_tasks;
    std::mutex _non_pipeline_report_mutex;

    std::thread _thread;

    std::atomic<bool> _stop;
};

} // namespace starrocks

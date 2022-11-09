// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
    Status unregister_non_pipeline_load(const TUniqueId& fragment_instance_id);

    Status register_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id);
    Status unregister_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id);

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

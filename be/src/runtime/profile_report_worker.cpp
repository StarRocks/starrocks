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

#include "runtime/profile_report_worker.h"

#include "exec/pipeline/query_context.h"
#include "runtime/fragment_mgr.h"
#include "util/misc.h"

namespace starrocks {

Status ProfileReportWorker::register_non_pipeline_load(const TUniqueId& fragment_instance_id) {
    std::lock_guard lg(_non_pipeline_report_mutex);
    if (_non_pipeline_report_tasks.find(fragment_instance_id) != _non_pipeline_report_tasks.end()) {
        std::stringstream msg;
        msg << "Fragment instance " << print_id(fragment_instance_id) << " has been registered";
        LOG(WARNING) << msg.str();
        return Status::InternalError(msg.str());
    }

    VLOG(3) << "register_non_pipeline_load fragment_instance_id=" << print_id(fragment_instance_id);
    _non_pipeline_report_tasks.emplace(fragment_instance_id, NonPipelineReportTask(UnixMillis(), TQueryType::LOAD));
    return Status::OK();
}

void ProfileReportWorker::unregister_non_pipeline_load(const TUniqueId& fragment_instance_id) {
    VLOG(3) << "unregister_non_pipeline_load fragment_instance_id=" << print_id(fragment_instance_id);
    std::lock_guard lg(_non_pipeline_report_mutex);
    _non_pipeline_report_tasks.erase(fragment_instance_id);
}

Status ProfileReportWorker::register_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id) {
    std::lock_guard lg(_pipeline_report_mutex);
    PipeLineReportTaskKey key(query_id, fragment_instance_id);
    if (_pipeline_report_tasks.find(key) != _pipeline_report_tasks.end()) {
        std::stringstream msg;
        msg << "Query id " << print_id(query_id) << ", Fragment instance " << print_id(fragment_instance_id)
            << " has been registered";
        LOG(WARNING) << msg.str();
        return Status::InternalError(msg.str());
    }
    VLOG(3) << "register_pipeline_load query_id=" << print_id(query_id)
            << ", fragment_instance_id=" << print_id(fragment_instance_id);
    _pipeline_report_tasks.emplace(std::move(key), PipelineReportTask(UnixMillis(), TQueryType::LOAD));
    return Status::OK();
}

void ProfileReportWorker::unregister_pipeline_load(const TUniqueId& query_id, const TUniqueId& fragment_instance_id) {
    VLOG(3) << "unregister_pipeline_load query_id=" << print_id(query_id)
            << ", fragment_instance_id=" << print_id(fragment_instance_id);
    std::lock_guard lg(_pipeline_report_mutex);
    _pipeline_report_tasks.erase(PipeLineReportTaskKey(query_id, fragment_instance_id));
}

void ProfileReportWorker::_start_report_profile() {
    int64_t cur_ms = UnixMillis();

    // report non pipeline load task
    std::vector<TUniqueId> non_pipeline_need_report_fragment_ids;
    {
        std::lock_guard lg(_non_pipeline_report_mutex);

        for (const auto& iter : _non_pipeline_report_tasks) {
            if (iter.second.task_type != TQueryType::LOAD) {
                continue;
            }
            const int64_t last_report_ms = iter.second.last_report_time;
            if (cur_ms - last_report_ms >= config::profile_report_interval * 1000) {
                non_pipeline_need_report_fragment_ids.push_back(iter.first);
            }
        }
    }

    FragmentMgr* fragment_mgr = ExecEnv::GetInstance()->fragment_mgr();
    DCHECK(fragment_mgr != nullptr);
    fragment_mgr->report_fragments(non_pipeline_need_report_fragment_ids);

    // report pipeline load task
    std::vector<PipeLineReportTaskKey> pipeline_need_report_query_fragment_ids;
    {
        std::lock_guard lg(_pipeline_report_mutex);

        for (const auto& iter : _pipeline_report_tasks) {
            if (iter.second.task_type != TQueryType::LOAD) {
                continue;
            }
            const int64_t last_report_ms = iter.second.last_report_time;
            if (cur_ms - last_report_ms >= config::profile_report_interval * 1000) {
                pipeline_need_report_query_fragment_ids.push_back(iter.first);
            }
        }
    }

    pipeline::QueryContextManager* query_context_manager = ExecEnv::GetInstance()->query_context_mgr();
    DCHECK(query_context_manager != nullptr);
    query_context_manager->report_fragments(pipeline_need_report_query_fragment_ids);
}

void ProfileReportWorker::execute() {
    LOG(INFO) << "ProfileReportWorker start working.";

    int32_t interval = config::profile_report_interval;

    while (!_stop.load(std::memory_order_consume)) {
        _start_report_profile();

        if (interval <= 0) {
            LOG(WARNING) << "profile_report_interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        nap_sleep(interval, [this] { return _stop.load(std::memory_order_consume); });
    }
    LOG(INFO) << "ProfileReportWorker going to exit.";
}

ProfileReportWorker::ProfileReportWorker(ExecEnv* env) : _thread([this] { execute(); }), _stop(false) {
    Thread::set_thread_name(_thread, "profile_report");
}

void ProfileReportWorker::close() {
    _stop.store(true, std::memory_order_release);
    _thread.join();
}

} // namespace starrocks

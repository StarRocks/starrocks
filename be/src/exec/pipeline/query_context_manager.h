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
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "base/hash/hash_std.hpp"
#include "base/metrics.h"
#include "base/statusor.h"
#include "base/uid_util.h"
#include "compute_env/profile_report_task.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/primitives/query_lifecycle.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks::pipeline {

// TODO: use brpc::TimerThread refactor QueryContext
class QueryContextManager : public QueryLifecycle {
public:
    QueryContextManager(size_t log2_num_slots);
    ~QueryContextManager();
    Status init(MetricRegistry* metrics = nullptr);
    StatusOr<QueryContext*> get_or_register(const TUniqueId& query_id, bool return_error_if_not_exist = false);
    QueryContextPtr get(const TUniqueId& query_id, bool need_prepared = false);
    void count_down_fragments(QueryContext* query_ctx);
    void on_fragment_finished(FragmentContextPtr fragment_ctx) override;
    size_t size();
    bool remove(const TUniqueId& query_id);
    // used for graceful exit
    void clear();

    std::vector<starrocks::PipeLineReportTaskKey> report_fragments(
            const std::vector<starrocks::PipeLineReportTaskKey>& pipeline_need_report_query_fragment_ids);

    void report_fragments_with_same_host(
            const std::vector<std::shared_ptr<FragmentContext>>& need_report_fragment_context,
            std::vector<bool>& reported, const TNetworkAddress& last_coord_addr,
            std::vector<TReportExecStatusParams>& report_exec_status_params_vector,
            std::vector<int32_t>& cur_batch_report_indexes,
            std::vector<starrocks::PipeLineReportTaskKey>& tasks_to_unregister);

    void collect_query_statistics(const PCollectQueryStatisticsRequest* request,
                                  PCollectQueryStatisticsResult* response);
    void for_each_active_ctx(const std::function<void(QueryContextPtr)>& func);

private:
    static void _clean_func(QueryContextManager* manager);
    void _clean_query_contexts();
    void _stop_clean_func() { _stop.store(true); }
    bool _is_stopped() { return _stop; }
    size_t _slot_idx(const TUniqueId& query_id);
    void _clean_slot_unlocked(size_t i, std::vector<QueryContextPtr>& del);

private:
    const size_t _num_slots;
    const size_t _slot_mask;
    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _context_maps;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _second_chance_maps;

    std::atomic<bool> _stop{false};
    std::shared_ptr<std::thread> _clean_thread;
    MetricRegistry* _metrics = nullptr;

    inline static const char* _metric_name = "pip_query_ctx_cnt";
    std::unique_ptr<UIntGauge> _query_ctx_cnt;
};

} // namespace starrocks::pipeline

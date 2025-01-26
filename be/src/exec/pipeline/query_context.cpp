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

#include "exec/pipeline/query_context.h"

#include <memory>
#include <vector>

#include "agent/master_info.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/spill/query_spill_manager.h"
#include "exec/workgroup/work_group.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_filter_cache.h"
#include "util/thread.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::pipeline {

QueryContext::QueryContext()
        : _fragment_mgr(new FragmentContextManager()),
          _total_fragments(0),
          _num_fragments(0),
          _num_active_fragments(0),
          _wg_running_query_token_ptr(nullptr) {
    _sub_plan_query_statistics_recvr = std::make_shared<QueryStatisticsRecvr>();
    _stream_epoch_manager = std::make_shared<StreamEpochManager>();
    _lifetime_sw.start();
}

QueryContext::~QueryContext() noexcept {
    // When destruct FragmentContextManager, we use query-level MemTracker. since when PipelineDriver executor
    // release QueryContext when it finishes the last driver of the query, the current instance-level MemTracker will
    // be freed before it is adopted to account memory usage of MemChunkAllocator. In destructor of FragmentContextManager,
    // the per-instance RuntimeStates that contain instance-level MemTracker is freed one by one, if there are
    // remaining other RuntimeStates after the current RuntimeState is freed, MemChunkAllocator uses the MemTracker of the
    // current RuntimeState to release Operators, OperatorFactories in the remaining RuntimeStates will trigger
    // segmentation fault.
    if (_mem_tracker != nullptr) {
        LOG(INFO) << fmt::format(
                "finished query_id:{} context life time:{} cpu costs:{} peak memusage:{} scan_bytes:{} spilled "
                "bytes:{}",
                print_id(query_id()), lifetime(), cpu_cost(), mem_cost_bytes(), get_scan_bytes(), get_spill_bytes());
    }

    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker.get());
        _fragment_mgr.reset();
    }

    // Accounting memory usage during QueryContext's destruction should not use query-level MemTracker, but its released
    // in the mid of QueryContext destruction, so use process-level memory tracker
    if (_exec_env != nullptr) {
        if (_is_runtime_filter_coordinator) {
            _exec_env->runtime_filter_worker()->close_query(_query_id);
        }
        _exec_env->runtime_filter_cache()->remove(_query_id);
    }

    // Make sure all bytes are released back to parent trackers.
    if (_connector_scan_mem_tracker != nullptr) {
        _connector_scan_mem_tracker->release_without_root();
    }
}

void QueryContext::count_down_fragments() {
    size_t old = _num_active_fragments.fetch_sub(1);
    DCHECK_GE(old, 1);
    bool all_fragments_finished = old == 1;
    if (!all_fragments_finished) {
        return;
    }

    // Acquire the pointer to avoid be released when removing query
    auto query_trace = shared_query_trace();
    ExecEnv::GetInstance()->query_context_mgr()->remove(_query_id);
    // @TODO(silverbullet233): if necessary, remove the dump from the execution thread
    // considering that this feature is generally used for debugging,
    // I think it should not have a big impact now
    if (query_trace != nullptr) {
        (void)query_trace->dump();
    }
}

FragmentContextManager* QueryContext::fragment_mgr() {
    return _fragment_mgr.get();
}

void QueryContext::cancel(const Status& status) {
    _is_cancelled = true;
    _fragment_mgr->cancel(status);
}

void QueryContext::init_mem_tracker(int64_t query_mem_limit, MemTracker* parent, int64_t big_query_mem_limit,
                                    std::optional<double> spill_mem_reserve_ratio, workgroup::WorkGroup* wg,
                                    RuntimeState* runtime_state, int connector_scan_node_number) {
    std::call_once(_init_mem_tracker_once, [=]() {
        _profile = std::make_shared<RuntimeProfile>("Query" + print_id(_query_id));
        auto* mem_tracker_counter =
                ADD_COUNTER_SKIP_MERGE(_profile.get(), "MemoryLimit", TUnit::BYTES, TCounterMergeType::SKIP_ALL);
        mem_tracker_counter->set(query_mem_limit);
        size_t lowest_limit = parent->lowest_limit();
        size_t tracker_reserve_limit = -1;

        if (spill_mem_reserve_ratio.has_value()) {
            tracker_reserve_limit = lowest_limit * spill_mem_reserve_ratio.value();
        }

        if (wg != nullptr && big_query_mem_limit > 0 &&
            (query_mem_limit <= 0 || big_query_mem_limit < query_mem_limit)) {
            std::string label = "Group=" + wg->name() + ", " + _profile->name();
            _mem_tracker = std::make_shared<MemTracker>(MemTracker::RESOURCE_GROUP_BIG_QUERY, big_query_mem_limit,
                                                        std::move(label), parent);
            _mem_tracker->set_reserve_limit(tracker_reserve_limit);
        } else {
            _mem_tracker = std::make_shared<MemTracker>(MemTracker::QUERY, query_mem_limit, _profile->name(), parent);
            _mem_tracker->set_reserve_limit(tracker_reserve_limit);
        }

        _static_query_mem_limit = lowest_limit;
        if (query_mem_limit > 0) {
            _static_query_mem_limit = std::min(query_mem_limit, _static_query_mem_limit);
        }
        if (big_query_mem_limit > 0) {
            _static_query_mem_limit = std::min(big_query_mem_limit, _static_query_mem_limit);
        }
        _connector_scan_operator_mem_share_arbitrator = _object_pool.add(
                new ConnectorScanOperatorMemShareArbitrator(_static_query_mem_limit, connector_scan_node_number));

        {
            MemTracker* connector_scan_parent = GlobalEnv::GetInstance()->connector_scan_pool_mem_tracker();
            if (wg != nullptr) {
                connector_scan_parent = wg->connector_scan_mem_tracker();
            }
            double connector_scan_use_query_mem_ratio = config::connector_scan_use_query_mem_ratio;
            if (runtime_state != nullptr && runtime_state->query_options().__isset.connector_scan_use_query_mem_ratio) {
                connector_scan_use_query_mem_ratio = runtime_state->query_options().connector_scan_use_query_mem_ratio;
            }
            _connector_scan_mem_tracker = std::make_shared<MemTracker>(
                    MemTracker::QUERY, _static_query_mem_limit * connector_scan_use_query_mem_ratio,
                    _profile->name() + "/connector_scan", connector_scan_parent);
        }
    });
}

Status QueryContext::init_spill_manager(const TQueryOptions& query_options) {
    Status st;
    std::call_once(_init_spill_manager_once, [this, &st, &query_options]() {
        _spill_manager = std::make_unique<spill::QuerySpillManager>(_query_id);
        st = _spill_manager->init_block_manager(query_options);
    });
    return st;
}

Status QueryContext::init_query_once(workgroup::WorkGroup* wg, bool enable_group_level_query_queue) {
    Status st = Status::OK();
    if (wg != nullptr) {
        std::call_once(_init_query_once, [this, &st, wg, enable_group_level_query_queue]() {
            this->init_query_begin_time();
            auto maybe_token = wg->acquire_running_query_token(enable_group_level_query_queue);
            if (maybe_token.ok()) {
                _wg_running_query_token_ptr = std::move(maybe_token.value());
                _wg_running_query_token_atomic_ptr = _wg_running_query_token_ptr.get();
            } else {
                st = maybe_token.status();
            }
        });
    }

    return st;
}

void QueryContext::release_workgroup_token_once() {
    auto* old = _wg_running_query_token_atomic_ptr.load();
    if (old != nullptr && _wg_running_query_token_atomic_ptr.compare_exchange_strong(old, nullptr)) {
        // The release_workgroup_token_once function is called by FragmentContext::cancel
        // to detach the QueryContext from the workgroup.
        // When the workgroup undergoes a configuration change, the old version of the workgroup is released,
        // and a new version is created. The old workgroup will only be physically destroyed once no
        // QueryContext is attached to it.
        // However, the MemTracker of the old workgroup outlives the workgroup itself because
        // it is accessed during the destruction of the QueryContext through its MemTracker
        // (the workgroup's MemTracker serves as the parent of the QueryContext's MemTracker).
        // To prevent the MemTracker from being released prematurely, it must be explicitly retained
        // to ensure it remains valid until it is no longer needed.
        _wg_mem_tracker = _wg_running_query_token_ptr->get_wg()->grab_mem_tracker();
        _wg_running_query_token_ptr.reset();
    }
}
void QueryContext::set_query_trace(std::shared_ptr<starrocks::debug::QueryTrace> query_trace) {
    std::call_once(_query_trace_init_flag, [this, &query_trace]() { _query_trace = std::move(query_trace); });
}

std::shared_ptr<QueryStatisticsRecvr> QueryContext::maintained_query_recv() {
    return _sub_plan_query_statistics_recvr;
}

std::shared_ptr<QueryStatistics> QueryContext::intermediate_query_statistic() {
    auto query_statistic = std::make_shared<QueryStatistics>();
    // Not transmit delta if it's the final sink
    if (_is_final_sink) {
        return nullptr;
    }

    query_statistic->add_cpu_costs(_delta_cpu_cost_ns.exchange(0));
    query_statistic->add_mem_costs(mem_cost_bytes());
    {
        std::lock_guard l(_scan_stats_lock);
        for (const auto& [table_id, scan_stats] : _scan_stats) {
            QueryStatisticsItemPB stats_item;
            stats_item.set_table_id(table_id);
            stats_item.set_scan_rows(scan_stats->delta_scan_rows_num.exchange(0));
            stats_item.set_scan_bytes(scan_stats->delta_scan_bytes.exchange(0));
            query_statistic->add_stats_item(stats_item);
        }
    }
    for (const auto& [node_id, exec_stats] : _node_exec_stats) {
        query_statistic->add_exec_stats_item(
                node_id, exec_stats->push_rows.exchange(0), exec_stats->pull_rows.exchange(0),
                exec_stats->pred_filter_rows.exchange(0), exec_stats->index_filter_rows.exchange(0),
                exec_stats->rf_filter_rows.exchange(0));
    }
    _sub_plan_query_statistics_recvr->aggregate(query_statistic.get());
    return query_statistic;
}

std::shared_ptr<QueryStatistics> QueryContext::final_query_statistic() {
    DCHECK(_is_final_sink) << "must be final sink";
    auto res = std::make_shared<QueryStatistics>();
    res->add_cpu_costs(cpu_cost());
    res->add_mem_costs(mem_cost_bytes());
    res->add_spill_bytes(get_spill_bytes());

    {
        std::lock_guard l(_scan_stats_lock);
        for (const auto& [table_id, scan_stats] : _scan_stats) {
            QueryStatisticsItemPB stats_item;
            stats_item.set_table_id(table_id);
            stats_item.set_scan_rows(scan_stats->total_scan_rows_num);
            stats_item.set_scan_bytes(scan_stats->total_scan_bytes);
            res->add_stats_item(stats_item);
        }
    }

    for (const auto& [node_id, exec_stats] : _node_exec_stats) {
        res->add_exec_stats_item(node_id, exec_stats->push_rows, exec_stats->pull_rows, exec_stats->pred_filter_rows,
                                 exec_stats->index_filter_rows, exec_stats->rf_filter_rows);
    }

    _sub_plan_query_statistics_recvr->aggregate(res.get());
    return res;
}

void QueryContext::update_scan_stats(int64_t table_id, int64_t scan_rows_num, int64_t scan_bytes) {
    ScanStats* stats = nullptr;
    {
        std::lock_guard l(_scan_stats_lock);
        auto iter = _scan_stats.find(table_id);
        if (iter == _scan_stats.end()) {
            _scan_stats.insert({table_id, std::make_shared<ScanStats>()});
            iter = _scan_stats.find(table_id);
        }
        stats = iter->second.get();
    }

    stats->total_scan_rows_num += scan_rows_num;
    stats->delta_scan_rows_num += scan_rows_num;
    stats->total_scan_bytes += scan_bytes;
    stats->delta_scan_bytes += scan_bytes;
}

void QueryContext::init_node_exec_stats(const std::vector<int32_t>& exec_stats_node_ids) {
    std::call_once(_node_exec_stats_init_flag, [this, &exec_stats_node_ids]() {
        for (int32_t node_id : exec_stats_node_ids) {
            auto node_exec_stats = std::make_shared<NodeExecStats>();
            _node_exec_stats[node_id] = node_exec_stats;
        }
    });
}

QueryContextManager::QueryContextManager(size_t log2_num_slots)
        : _num_slots(1 << log2_num_slots),
          _slot_mask(_num_slots - 1),
          _mutexes(_num_slots),
          _context_maps(_num_slots),
          _second_chance_maps(_num_slots) {}

Status QueryContextManager::init() {
    // regist query context metrics
    auto metrics = StarRocksMetrics::instance()->metrics();
    _query_ctx_cnt = std::make_unique<UIntGauge>(MetricUnit::NOUNIT);
    metrics->register_metric(_metric_name, _query_ctx_cnt.get());
    metrics->register_hook(_metric_name, [this]() { _query_ctx_cnt->set_value(this->size()); });

    try {
        _clean_thread = std::make_shared<std::thread>(_clean_func, this);
        Thread::set_thread_name(*_clean_thread.get(), "query_ctx_clr");
        return Status::OK();
    } catch (...) {
        return Status::InternalError("Fail to create clean_thread of QueryContextManager");
    }
}
void QueryContextManager::_clean_slot_unlocked(size_t i, std::vector<QueryContextPtr>& del) {
    auto& sc_map = _second_chance_maps[i];
    auto sc_it = sc_map.begin();
    while (sc_it != sc_map.end()) {
        if (sc_it->second->has_no_active_instances() && sc_it->second->is_delivery_expired()) {
            del.emplace_back(std::move(sc_it->second));
            sc_it = sc_map.erase(sc_it);
        } else {
            ++sc_it;
        }
    }
}
void QueryContextManager::_clean_query_contexts() {
    for (auto i = 0; i < _num_slots; ++i) {
        auto& mutex = _mutexes[i];
        std::vector<QueryContextPtr> del_list;
        std::unique_lock write_lock(mutex);
        _clean_slot_unlocked(i, del_list);
    }
}

void QueryContextManager::_clean_func(QueryContextManager* manager) {
    while (!manager->_is_stopped()) {
        manager->_clean_query_contexts();
        std::this_thread::sleep_for(milliseconds(100));
    }
}

size_t QueryContextManager::_slot_idx(const TUniqueId& query_id) {
    return HashUtil::hash(&query_id.hi, sizeof(query_id.hi), 0) & _slot_mask;
}

QueryContextManager::~QueryContextManager() {
    // unregist metrics
    auto metrics = StarRocksMetrics::instance()->metrics();
    metrics->deregister_hook(_metric_name);
    _query_ctx_cnt.reset();

    if (_clean_thread) {
        this->_stop_clean_func();
        _clean_thread->join();
        clear();
    }
}

#define RETURN_NULL_IF_CTX_CANCELLED(query_ctx) \
    if (query_ctx->is_cancelled()) {            \
        return nullptr;                         \
    }                                           \
    query_ctx->increment_num_fragments();       \
    if (query_ctx->is_cancelled()) {            \
        query_ctx->rollback_inc_fragments();    \
        return nullptr;                         \
    }

QueryContext* QueryContextManager::get_or_register(const TUniqueId& query_id) {
    size_t i = _slot_idx(query_id);
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];

    {
        std::shared_lock<std::shared_mutex> read_lock(mutex);
        // lookup query context in context_map
        auto it = context_map.find(query_id);
        if (it != context_map.end()) {
            RETURN_NULL_IF_CTX_CANCELLED(it->second);
            return it->second.get();
        }
    }
    {
        std::unique_lock<std::shared_mutex> write_lock(mutex);
        // lookup query context in context_map at first
        auto it = context_map.find(query_id);
        auto sc_it = sc_map.find(query_id);
        if (it != context_map.end()) {
            RETURN_NULL_IF_CTX_CANCELLED(it->second);
            return it->second.get();
        } else {
            // lookup query context for the second chance in sc_map
            if (sc_it != sc_map.end()) {
                auto ctx = std::move(sc_it->second);
                sc_map.erase(sc_it);
                RETURN_NULL_IF_CTX_CANCELLED(ctx);
                auto* raw_ctx_ptr = ctx.get();
                context_map.emplace(query_id, std::move(ctx));
                return raw_ctx_ptr;
            }
        }

        // finally, find no query contexts, so create a new one
        auto&& ctx = std::make_shared<QueryContext>();
        auto* ctx_raw_ptr = ctx.get();
        ctx_raw_ptr->set_query_id(query_id);
        ctx_raw_ptr->increment_num_fragments();
        context_map.emplace(query_id, std::move(ctx));
        return ctx_raw_ptr;
    }
}

QueryContextPtr QueryContextManager::get(const TUniqueId& query_id, bool need_prepared) {
    size_t i = _slot_idx(query_id);
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];
    std::shared_lock<std::shared_mutex> read_lock(mutex);
    // lookup query context in context_map for the first chance
    auto it = context_map.find(query_id);
    auto check_if_prepared = [need_prepared](auto it) -> QueryContextPtr {
        if (need_prepared) {
            if (it->second->is_prepared()) {
                return it->second;
            } else {
                return nullptr;
            }
        } else {
            return it->second;
        }
    };
    if (it != context_map.end()) {
        return check_if_prepared(it);
    } else {
        // lookup query context in context_map for the second chance
        auto sc_it = sc_map.find(query_id);
        if (sc_it != sc_map.end()) {
            return check_if_prepared(sc_it);
        } else {
            return nullptr;
        }
    }
}

size_t QueryContextManager::size() {
    size_t sz = 0;
    for (int i = 0; i < _mutexes.size(); ++i) {
        std::shared_lock<std::shared_mutex> read_lock(_mutexes[i]);
        sz += _context_maps[i].size();
        sz += _second_chance_maps[i].size();
    }
    return sz;
}

bool QueryContextManager::remove(const TUniqueId& query_id) {
    size_t i = _slot_idx(query_id);
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];

    // retain the query_ctx reference to avoid call destructors while holding a lock
    // we should define them before hold the write lock
    QueryContextPtr query_ctx;
    std::vector<QueryContextPtr> del_list;

    std::unique_lock<std::shared_mutex> write_lock(mutex);
    _clean_slot_unlocked(i, del_list);
    // return directly if query_ctx is absent
    auto it = context_map.find(query_id);
    if (it == context_map.end()) {
        return false;
    }

    // the query context is really dead, so just cleanup
    if (it->second->is_dead()) {
        query_ctx = std::move(it->second);
        context_map.erase(it);
        return true;
    } else if (it->second->has_no_active_instances()) {
        // although all of active fragments of the query context terminates, but some fragments maybe comes too late
        // in the future, so extend the lifetime of query context and wait for some time till fragments on wire have
        // vanished
        auto ctx = std::move(it->second);
        ctx->extend_delivery_lifetime();
        context_map.erase(it);
        sc_map.emplace(query_id, std::move(ctx));
        return false;
    }
    return false;
}

void QueryContextManager::clear() {
    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(_mutexes.size());
    for (auto& _mutexe : _mutexes) {
        locks.emplace_back(_mutexe);
    }
    _second_chance_maps.clear();
    _context_maps.clear();
}

void QueryContextManager::report_fragments_with_same_host(
        const std::vector<std::shared_ptr<FragmentContext>>& need_report_fragment_context, std::vector<bool>& reported,
        const TNetworkAddress& last_coord_addr, std::vector<TReportExecStatusParams>& report_exec_status_params_vector,
        std::vector<int32_t>& cur_batch_report_indexes) {
    for (int i = 0; i < need_report_fragment_context.size(); i++) {
        if (reported[i] == false) {
            FragmentContext* fragment_ctx = need_report_fragment_context[i].get();

            if (fragment_ctx->all_execution_groups_finished()) {
                reported[i] = true;
                continue;
            }

            Status fragment_ctx_status = fragment_ctx->final_status();
            if (!fragment_ctx_status.ok()) {
                reported[i] = true;
                starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_pipeline_load(
                        fragment_ctx->query_id(), fragment_ctx->fragment_instance_id());
                continue;
            }

            auto fe_addr = fragment_ctx->fe_addr();
            auto fragment_id = fragment_ctx->fragment_instance_id();
            auto* runtime_state = fragment_ctx->runtime_state();
            DCHECK(runtime_state != nullptr);

            if (fe_addr == last_coord_addr) {
                TReportExecStatusParams params;

                params.protocol_version = FrontendServiceVersion::V1;
                params.__set_query_id(fragment_ctx->query_id());
                params.__set_backend_num(runtime_state->be_number());
                params.__set_fragment_instance_id(fragment_id);
                fragment_ctx_status.set_t_status(&params);
                params.__set_done(false);

                if (runtime_state->query_options().query_type == TQueryType::LOAD) {
                    runtime_state->update_report_load_status(&params);
                    params.__set_load_type(runtime_state->query_options().load_job_type);
                }

                auto backend_id = get_backend_id();
                if (backend_id.has_value()) {
                    params.__set_backend_id(backend_id.value());
                }

                report_exec_status_params_vector.emplace_back(std::move(params));
                cur_batch_report_indexes.push_back(i);
                reported[i] = true;
            }
        }
    }
}

void QueryContextManager::collect_query_statistics(const PCollectQueryStatisticsRequest* request,
                                                   PCollectQueryStatisticsResult* response) {
    for (int i = 0; i < request->query_ids_size(); i++) {
        const PUniqueId& p_query_id = request->query_ids(i);
        TUniqueId id;
        id.__set_hi(p_query_id.hi());
        id.__set_lo(p_query_id.lo());
        if (auto query_ctx = get(id, true); query_ctx != nullptr) {
            int64_t cpu_cost = query_ctx->cpu_cost();
            int64_t scan_rows = query_ctx->cur_scan_rows_num();
            int64_t scan_bytes = query_ctx->get_scan_bytes();
            int64_t mem_usage_bytes = query_ctx->current_mem_usage_bytes();
            auto query_statistics = response->add_query_statistics();
            auto query_id = query_statistics->mutable_query_id();
            query_id->set_hi(p_query_id.hi());
            query_id->set_lo(p_query_id.lo());
            query_statistics->set_cpu_cost_ns(cpu_cost);
            query_statistics->set_scan_rows(scan_rows);
            query_statistics->set_scan_bytes(scan_bytes);
            query_statistics->set_mem_usage_bytes(mem_usage_bytes);
            query_statistics->set_spill_bytes(query_ctx->get_spill_bytes());
        }
    }
}

void QueryContextManager::report_fragments(
        const std::vector<PipeLineReportTaskKey>& pipeline_need_report_query_fragment_ids) {
    std::vector<std::shared_ptr<FragmentContext>> need_report_fragment_context;
    std::vector<std::shared_ptr<QueryContext>> need_report_query_ctx;

    std::vector<PipeLineReportTaskKey> fragment_context_non_exist;

    for (const auto& key : pipeline_need_report_query_fragment_ids) {
        TUniqueId query_id = key.query_id;
        TUniqueId fragment_instance_id = key.fragment_instance_id;
        auto query_ctx = get(query_id);
        if (!query_ctx) {
            fragment_context_non_exist.push_back(key);
            continue;
        }
        need_report_query_ctx.push_back(query_ctx);
        auto fragment_ctx = query_ctx->fragment_mgr()->get(fragment_instance_id);
        if (!fragment_ctx) {
            fragment_context_non_exist.push_back(key);
            continue;
        }
        need_report_fragment_context.push_back(fragment_ctx);
    }

    std::vector<bool> reported(need_report_fragment_context.size(), false);
    for (int i = 0; i < need_report_fragment_context.size(); i++) {
        if (reported[i] == false) {
            reported[i] = true;

            FragmentContext* fragment_ctx = need_report_fragment_context[i].get();

            if (fragment_ctx->all_execution_groups_finished()) {
                continue;
            }

            Status fragment_ctx_status = fragment_ctx->final_status();
            if (!fragment_ctx_status.ok()) {
                starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_pipeline_load(
                        fragment_ctx->query_id(), fragment_ctx->fragment_instance_id());
                continue;
            }

            auto fe_addr = fragment_ctx->fe_addr();
            auto fragment_id = fragment_ctx->fragment_instance_id();
            auto* runtime_state = fragment_ctx->runtime_state();
            DCHECK(runtime_state != nullptr);

            std::vector<TReportExecStatusParams> report_exec_status_params_vector;

            TReportExecStatusParams params;

            params.protocol_version = FrontendServiceVersion::V1;
            params.__set_query_id(fragment_ctx->query_id());
            params.__set_backend_num(runtime_state->be_number());
            params.__set_fragment_instance_id(fragment_id);
            fragment_ctx_status.set_t_status(&params);
            params.__set_done(false);

            if (runtime_state->query_options().query_type == TQueryType::LOAD) {
                runtime_state->update_report_load_status(&params);
                params.__set_load_type(runtime_state->query_options().load_job_type);
            }

            auto backend_id = get_backend_id();
            if (backend_id.has_value()) {
                params.__set_backend_id(backend_id.value());
            }

            report_exec_status_params_vector.push_back(params);

            std::vector<int32_t> cur_batch_report_indexes;
            cur_batch_report_indexes.push_back(i);

            report_fragments_with_same_host(need_report_fragment_context, reported, fe_addr,
                                            report_exec_status_params_vector, cur_batch_report_indexes);

            TBatchReportExecStatusParams report_batch;
            report_batch.__set_params_list(report_exec_status_params_vector);

            TBatchReportExecStatusResult res;
            Status rpc_status;

            VLOG_ROW << "debug: reportExecStatus params is " << apache::thrift::ThriftDebugString(params).c_str();
            rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                    fe_addr,
                    [&res, &report_batch](FrontendServiceConnection& client) {
                        client->batchReportExecStatus(res, report_batch);
                    },
                    config::thrift_rpc_timeout_ms);
            if (!rpc_status.ok()) {
                LOG(WARNING) << "thrift rpc error:" << rpc_status;
                continue;
            }

            const std::vector<TStatus>& status_list = res.status_list;
            for (int j = 0; j < status_list.size(); j++) {
                Status rpc_status = Status(status_list[j]);
                if (!rpc_status.ok()) {
                    int32_t index = cur_batch_report_indexes[j];
                    FragmentContext* fragment_ctx = need_report_fragment_context[index].get();
                    fragment_ctx->cancel(rpc_status);
                }
            }
        }
    }

    for (const auto& key : fragment_context_non_exist) {
        starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_pipeline_load(key.query_id,
                                                                                             key.fragment_instance_id);
    }
}

void QueryContextManager::for_each_active_ctx(const std::function<void(QueryContextPtr)>& func) {
    for (auto i = 0; i < _num_slots; ++i) {
        auto& mutex = _mutexes[i];
        std::vector<QueryContextPtr> del_list;
        std::unique_lock write_lock(mutex);
        auto& contexts = _context_maps[i];
        for (auto& [_, context] : contexts) {
            func(context);
        }
    }
}

} // namespace starrocks::pipeline

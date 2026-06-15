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
#include <utility>
#include <vector>

#include "common/config_exec_flow_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "compute_env/compute_env.h"
#include "compute_env/query/connector_scan_mem_share_arbitrator.h"
#include "compute_env/query/global_late_materialization_context.h"
#include "compute_env/spill/query_spill_manager.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_manager.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_query_lifecycle.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

const RuntimeServices* runtime_services(const QueryExecutionServices* query_execution_services) {
    return query_execution_services != nullptr ? query_execution_services->runtime : nullptr;
}

} // namespace

QueryContext::QueryContext()
        : _fragment_mgr(new FragmentContextManager()),

          _num_fragments(0),
          _num_active_fragments(0),
          _wg_running_query_token_ptr(nullptr) {
    _sub_plan_query_statistics_recvr = std::make_shared<QueryStatisticsRecvr>();
    _query_runtime_state.set_object_pool(&_object_pool);
    _lifetime_sw.start();
}

QueryContextPtr QueryContext::create() {
    auto query_ctx = std::make_shared<QueryContext>();
    query_ctx->_fragment_mgr = std::make_unique<FragmentContextManager>(query_ctx);
    return query_ctx;
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
        if (lifetime() > config::big_query_sec * 1000 * 1000 * 1000) {
            int64_t read_local = get_read_local_cnt();
            int64_t read_total = read_local + get_read_remote_cnt();
            double cache_hit_ratio = read_total > 0 ? (((double)read_local / read_total) * 100) : 100;
            LOG(INFO) << fmt::format(
                    "finished query_id:{} context life time:{} cpu costs:{} peak memusage:{} scan_bytes:{} spilled "
                    "bytes:{} cache_hit_ratio:{:.1f}%",
                    print_id(query_id()), lifetime(), _query_runtime_state.cpu_cost(), mem_cost_bytes(),
                    _query_runtime_state.get_scan_bytes(), get_spill_bytes(), cache_hit_ratio);
        }
    }

    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker.get());
        _fragment_mgr.reset();
    }

    // Accounting memory usage during QueryContext's destruction should not use query-level MemTracker, but its released
    // in the mid of QueryContext destruction, so use process-level memory tracker
    if (auto* services = runtime_services(_query_execution_services); services != nullptr) {
        if (_is_runtime_filter_coordinator && services->runtime_filter_query_lifecycle != nullptr) {
            services->runtime_filter_query_lifecycle->close_query(query_id());
        }
        services->runtime_filter_cache->remove(query_id());
    }

    // Make sure all bytes are released back to parent trackers.
    if (_connector_scan_mem_tracker != nullptr) {
        _connector_scan_mem_tracker->release_without_root();
    }
}

bool QueryContext::decrement_num_active_fragments() {
    size_t old = _num_active_fragments.fetch_sub(1);
    DCHECK_GE(old, 1);
    return old == 1;
}

void QueryContext::count_down_fragment() {
    if (!decrement_num_active_fragments()) {
        return;
    }

    if (auto* lifecycle = _query_lifecycle.load(); lifecycle != nullptr) {
        // Keep manager-owned query contexts alive if removal erases the last shared_ptr.
        [[maybe_unused]] auto self = weak_from_this().lock();
        auto id = query_id();
        lifecycle->on_query_releasable(id);
    }
}

FragmentContextManager* QueryContext::fragment_mgr() {
    return _fragment_mgr.get();
}

void QueryContext::attach_to_runtime_state(RuntimeState* state) {
    DCHECK(state != nullptr);
    auto lifetime = weak_from_this();
    DCHECK(!lifetime.expired());
    state->set_query_ctx(this, &query_runtime_state(), object_pool());
    state->set_query_ctx_lifetime(std::move(lifetime));
}

void QueryContext::cancel(const Status& status, bool cancelled_by_fe) {
    _is_cancelled = true;
    if (cancelled_by_fe) {
        // only update when confirm cancelled from fe
        set_cancelled_by_fe();
    }
    if (_cancelled_status.load() != nullptr) {
        return;
    }
    Status* old_status = nullptr;
    if (_cancelled_status.compare_exchange_strong(old_status, &_s_status)) {
        _s_status = status;
        if (!status.ok()) {
            release_workgroup_token_once();
        }
    }
    _fragment_mgr->cancel(status);
}

void QueryContext::init_mem_tracker(int64_t query_mem_limit, MemTracker* parent, int64_t big_query_mem_limit,
                                    std::optional<double> spill_mem_reserve_ratio, workgroup::WorkGroup* wg,
                                    RuntimeState* runtime_state, int connector_scan_node_number,
                                    int64_t connector_scan_default_data_source_mem_bytes) {
    std::call_once(_init_mem_tracker_once, [=]() {
        _profile = std::make_shared<RuntimeProfile>("Query" + print_id(query_id()));
        auto* mem_tracker_counter =
                ADD_COUNTER_SKIP_MERGE(_profile.get(), "MemoryLimit", TUnit::BYTES, TCounterMergeType::SKIP_ALL);
        COUNTER_SET(mem_tracker_counter, query_mem_limit);
        size_t lowest_limit = parent->lowest_limit();
        size_t tracker_reserve_limit = -1;

        if (spill_mem_reserve_ratio.has_value()) {
            tracker_reserve_limit = lowest_limit * spill_mem_reserve_ratio.value();
        }

        if (wg != nullptr && big_query_mem_limit > 0 &&
            (query_mem_limit <= 0 || big_query_mem_limit < query_mem_limit)) {
            std::string label = "Group=" + wg->name() + ", " + _profile->name();
            _mem_tracker = std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP_BIG_QUERY, big_query_mem_limit,
                                                        std::move(label), parent);
            _mem_tracker->set_reserve_limit(tracker_reserve_limit);
        } else {
            _mem_tracker =
                    std::make_shared<MemTracker>(MemTrackerType::QUERY, query_mem_limit, _profile->name(), parent);
            _mem_tracker->set_reserve_limit(tracker_reserve_limit);
        }
        _query_runtime_state.set_query_mem_tracker(_mem_tracker.get());

        _static_query_mem_limit = lowest_limit;
        if (query_mem_limit > 0) {
            _static_query_mem_limit = std::min(query_mem_limit, _static_query_mem_limit);
        }
        if (big_query_mem_limit > 0) {
            _static_query_mem_limit = std::min(big_query_mem_limit, _static_query_mem_limit);
        }
        _connector_scan_operator_mem_share_arbitrator = _object_pool.add(new ConnectorScanOperatorMemShareArbitrator(
                _static_query_mem_limit, connector_scan_node_number, connector_scan_default_data_source_mem_bytes));
        if (runtime_state != nullptr && runtime_state->enable_global_late_materialization()) {
            _global_late_materialization_ctx_mgr = _object_pool.add(new GlobalLateMaterilizationContextMgr());
        }
        _query_runtime_state.set_static_query_mem_limit(_static_query_mem_limit);
        _query_runtime_state.set_connector_scan_operator_mem_share_arbitrator(
                _connector_scan_operator_mem_share_arbitrator);
        _query_runtime_state.set_global_late_materialization_ctx_mgr(_global_late_materialization_ctx_mgr);

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
                    MemTrackerType::QUERY, _static_query_mem_limit * connector_scan_use_query_mem_ratio,
                    _profile->name() + "/connector_scan", connector_scan_parent);
            _query_runtime_state.set_connector_scan_mem_tracker(_connector_scan_mem_tracker.get());
        }
    });
}

Status QueryContext::init_spill_manager(const TQueryOptions& query_options) {
    Status st;
    std::call_once(_init_spill_manager_once, [this, &st, &query_options]() {
        auto* services = runtime_services(_query_execution_services);
        auto* compute_env = ExecEnv::GetInstance()->compute_env();
        auto* g_spill_manager = services != nullptr ? services->global_spill_manager : nullptr;
        if (g_spill_manager == nullptr && compute_env != nullptr) {
            g_spill_manager = compute_env->global_spill_manager();
        }
        auto* spill_dir_mgr = services != nullptr ? services->spill_dir_mgr : nullptr;
        if (spill_dir_mgr == nullptr && compute_env != nullptr) {
            spill_dir_mgr = compute_env->spill_dir_mgr();
        }
        _spill_manager = std::make_unique<spill::QuerySpillManager>(query_id(), g_spill_manager, spill_dir_mgr);
        _query_runtime_state.set_query_spill_manager(_spill_manager.get());
        st = _spill_manager->init_block_manager(query_options);
    });
    return st;
}

Status QueryContext::init_query_once(workgroup::WorkGroup* wg, bool enable_group_level_query_queue) {
    Status st = Status::OK();
    if (wg != nullptr) {
        std::call_once(_init_query_once, [this, &st, wg, enable_group_level_query_queue]() {
            _query_runtime_state.init_query_begin_time();
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
        // The release_workgroup_token_once function is called by cancel_fragment_context
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
std::shared_ptr<QueryStatisticsRecvr> QueryContext::maintained_query_recv() {
    return _sub_plan_query_statistics_recvr;
}

std::shared_ptr<QueryStatistics> QueryContext::intermediate_query_statistic(int64_t delta_transmitted_bytes) {
    auto query_statistic = std::make_shared<QueryStatistics>();
    // Not transmit delta if it's the final sink
    if (_is_final_sink) {
        return nullptr;
    }

    query_statistic->add_cpu_costs(_query_runtime_state.consume_delta_cpu_cost());
    query_statistic->add_mem_costs(mem_cost_bytes());
    query_statistic->add_transmitted_bytes(delta_transmitted_bytes);
    _query_runtime_state.consume_delta_scan_stats(query_statistic.get());
    for (const auto& [node_id, exec_stats] : _query_runtime_state.node_exec_stats()) {
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
    res->add_cpu_costs(_query_runtime_state.cpu_cost());
    res->add_mem_costs(mem_cost_bytes());
    res->add_spill_bytes(get_spill_bytes());
    res->add_read_stats(get_read_local_cnt(), get_read_remote_cnt());
    res->add_transmitted_bytes(get_transmitted_bytes());

    _query_runtime_state.add_total_scan_stats(res.get());

    for (const auto& [node_id, exec_stats] : _query_runtime_state.node_exec_stats()) {
        res->add_exec_stats_item(node_id, exec_stats->push_rows, exec_stats->pull_rows, exec_stats->pred_filter_rows,
                                 exec_stats->index_filter_rows, exec_stats->rf_filter_rows);
    }

    _sub_plan_query_statistics_recvr->aggregate(res.get());
    return res;
}

std::shared_ptr<QueryStatistics> QueryContext::snapshot_query_statistic() {
    auto res = std::make_shared<QueryStatistics>();
    res->add_cpu_costs(_query_runtime_state.cpu_cost());
    if (_mem_tracker != nullptr) {
        res->add_mem_costs(mem_cost_bytes());
    }
    res->add_spill_bytes(get_spill_bytes());
    res->add_read_stats(get_read_local_cnt(), get_read_remote_cnt());
    res->add_transmitted_bytes(get_transmitted_bytes());

    _query_runtime_state.add_total_scan_stats(res.get());

    for (const auto& [node_id, exec_stats] : _query_runtime_state.node_exec_stats()) {
        res->add_exec_stats_item(node_id, exec_stats->push_rows, exec_stats->pull_rows, exec_stats->pred_filter_rows,
                                 exec_stats->index_filter_rows, exec_stats->rf_filter_rows);
    }

    _sub_plan_query_statistics_recvr->aggregate(res.get());
    return res;
}

} // namespace starrocks::pipeline

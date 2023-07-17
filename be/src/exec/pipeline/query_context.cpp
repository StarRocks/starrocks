// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "exec/pipeline/query_context.h"

#include <memory>

#include "exec/pipeline/fragment_context.h"
#include "exec/workgroup/work_group.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_filter_cache.h"
#include "util/thread.h"

namespace starrocks::pipeline {
QueryContext::QueryContext()
        : _fragment_mgr(new FragmentContextManager()),
          _total_fragments(0),
          _num_fragments(0),
          _num_active_fragments(0),
          _wg_running_query_token_ptr(nullptr) {
    _sub_plan_query_statistics_recvr = std::make_shared<QueryStatisticsRecvr>();
}

QueryContext::~QueryContext() {
    // When destruct FragmentContextManager, we use query-level MemTracker. since when PipelineDriver executor
    // release QueryContext when it finishes the last driver of the query, the current instance-level MemTracker will
    // be freed before it is adopted to account memory usage of ChunkAllocator. In destructor of FragmentContextManager,
    // the per-instance RuntimeStates that contain instance-level MemTracker is freed one by one, if there are
    // remaining other RuntimeStates after the current RuntimeState is freed, ChunkAllocator uses the MemTracker of the
    // current RuntimeState to release Operators, OperatorFactories in the remaining RuntimeStates will trigger
    // segmentation fault.
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
}

FragmentContextManager* QueryContext::fragment_mgr() {
    return _fragment_mgr.get();
}

void QueryContext::cancel(const Status& status) {
    _fragment_mgr->cancel(status);
}

int64_t QueryContext::compute_query_mem_limit(int64_t parent_mem_limit, int64_t per_instance_mem_limit,
                                              size_t pipeline_dop, int64_t option_query_mem_limit) {
    // no mem_limit
    if (per_instance_mem_limit <= 0 && option_query_mem_limit <= 0) {
        return -1;
    }

    int64_t mem_limit;
    if (option_query_mem_limit > 0) {
        mem_limit = option_query_mem_limit;
    } else {
        mem_limit = per_instance_mem_limit;
        // query's mem_limit = per-instance mem_limit * num_instances * pipeline_dop
        static constexpr int64_t MEM_LIMIT_MAX = std::numeric_limits<int64_t>::max();
        if (MEM_LIMIT_MAX / total_fragments() / pipeline_dop > mem_limit) {
            mem_limit *= static_cast<int64_t>(total_fragments()) * pipeline_dop;
        } else {
            mem_limit = MEM_LIMIT_MAX;
        }
    }

    // query's mem_limit never exceeds its parent's limit if it exists
    return parent_mem_limit == -1 ? mem_limit : std::min(parent_mem_limit, mem_limit);
}

void QueryContext::init_mem_tracker(int64_t query_mem_limit, MemTracker* parent, int64_t big_query_mem_limit,
                                    workgroup::WorkGroup* wg) {
    std::call_once(_init_mem_tracker_once, [=]() {
        _profile = std::make_shared<RuntimeProfile>("Query" + print_id(_query_id));
        auto* mem_tracker_counter = ADD_COUNTER_SKIP_MERGE(_profile.get(), "MemoryLimit", TUnit::BYTES);
        mem_tracker_counter->set(query_mem_limit);
        _mem_tracker = std::make_shared<MemTracker>(MemTracker::QUERY, query_mem_limit, _profile->name(), parent);
        if (wg != nullptr && big_query_mem_limit > 0 && big_query_mem_limit < query_mem_limit) {
            std::string label = "Group=" + wg->name() + ", " + _profile->name();
            _mem_tracker = std::make_shared<MemTracker>(MemTracker::RESOURCE_GROUP_BIG_QUERY, big_query_mem_limit,
                                                        std::move(label), parent);
        } else {
            _mem_tracker = std::make_shared<MemTracker>(MemTracker::QUERY, query_mem_limit, _profile->name(), parent);
        }
    });
}

Status QueryContext::init_query_once(workgroup::WorkGroup* wg) {
    Status st = Status::OK();
    if (wg != nullptr) {
        std::call_once(_init_query_once, [this, &st, wg]() {
            this->init_query_begin_time();
            auto maybe_token = wg->acquire_running_query_token();
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
    // Not transmit delta if it's the result sink node
    if (_is_result_sink) {
        return query_statistic;
    }
    query_statistic->add_scan_stats(_delta_scan_rows_num.exchange(0), _delta_scan_bytes.exchange(0));
    query_statistic->add_cpu_costs(_delta_cpu_cost_ns.exchange(0));
    query_statistic->add_mem_costs(mem_cost_bytes());
    _sub_plan_query_statistics_recvr->aggregate(query_statistic.get());
    return query_statistic;
}

std::shared_ptr<QueryStatistics> QueryContext::final_query_statistic() {
    DCHECK(_is_result_sink) << "must be the result sink";
    auto res = std::make_shared<QueryStatistics>();
    res->add_scan_stats(_total_scan_rows_num, _total_scan_bytes);
    res->add_cpu_costs(_total_cpu_cost_ns);
    res->add_mem_costs(mem_cost_bytes());

    _sub_plan_query_statistics_recvr->aggregate(res.get());
    return res;
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
void QueryContextManager::_clean_slot_unlocked(size_t i) {
    auto& sc_map = _second_chance_maps[i];
    auto sc_it = sc_map.begin();
    while (sc_it != sc_map.end()) {
        if (sc_it->second->has_no_active_instances() && sc_it->second->is_delivery_expired()) {
            sc_it = sc_map.erase(sc_it);
        } else {
            ++sc_it;
        }
    }
}
void QueryContextManager::_clean_query_contexts() {
    for (auto i = 0; i < _num_slots; ++i) {
        auto& mutex = _mutexes[i];
        std::unique_lock write_lock(mutex);
        _clean_slot_unlocked(i);
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
            it->second->increment_num_fragments();
            return it->second.get();
        }
    }
    {
        std::unique_lock<std::shared_mutex> write_lock(mutex);
        // lookup query context in context_map at first
        auto it = context_map.find(query_id);
        auto sc_it = sc_map.find(query_id);
        if (it != context_map.end()) {
            it->second->increment_num_fragments();
            return it->second.get();
        } else {
            // lookup query context for the second chance in sc_map
            if (sc_it != sc_map.end()) {
                auto ctx = std::move(sc_it->second);
                ctx->increment_num_fragments();
                sc_map.erase(sc_it);
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

QueryContextPtr QueryContextManager::get(const TUniqueId& query_id) {
    size_t i = _slot_idx(query_id);
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];
    std::shared_lock<std::shared_mutex> read_lock(mutex);
    // lookup query context in context_map for the first chance
    auto it = context_map.find(query_id);
    if (it != context_map.end()) {
        return it->second;
    } else {
        // lookup query context in context_map for the second chance
        auto sc_it = sc_map.find(query_id);
        if (sc_it != sc_map.end()) {
            return sc_it->second;
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

    std::unique_lock<std::shared_mutex> write_lock(mutex);
    _clean_slot_unlocked(i);
    // return directly if query_ctx is absent
    auto it = context_map.find(query_id);
    if (it == context_map.end()) {
        return false;
    }

    // the query context is really dead, so just cleanup
    if (it->second->is_dead()) {
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
    for (int i = 0; i < _mutexes.size(); ++i) {
        locks.emplace_back(_mutexes[i]);
    }
    _second_chance_maps.clear();
    _context_maps.clear();
}

} // namespace starrocks::pipeline

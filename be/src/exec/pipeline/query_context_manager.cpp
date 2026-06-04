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

#include "exec/pipeline/query_context_manager.h"

#include <memory>
#include <vector>

#include "base/hash/hash.h"
#include "common/config_rpc_client_fwd.h"
#include "common/system/master_info.h"
#include "common/thread/thread.h"
#include "common/util/thrift_client_cache.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_manager.h"
#include "exec/pipeline/query_context.h"
#include "platform/thrift_rpc_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"

namespace starrocks::pipeline {

QueryContextManager::QueryContextManager(size_t log2_num_slots)
        : _num_slots(1 << log2_num_slots),
          _slot_mask(_num_slots - 1),
          _mutexes(_num_slots),
          _context_maps(_num_slots),
          _second_chance_maps(_num_slots) {}

Status QueryContextManager::init(MetricRegistry* metrics) {
    _metrics = metrics;
    // regist query context metrics
    if (_metrics != nullptr) {
        _query_ctx_cnt = std::make_unique<UIntGauge>(MetricUnit::NOUNIT);
        _metrics->register_metric(_metric_name, _query_ctx_cnt.get());
        _metrics->register_hook(_metric_name, [this]() { _query_ctx_cnt->set_value(this->size()); });
    }

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
    if (_metrics != nullptr) {
        _metrics->deregister_hook(_metric_name);
    }
    _query_ctx_cnt.reset();

    if (_clean_thread) {
        this->_stop_clean_func();
        _clean_thread->join();
        clear();
    }
}

#define RETURN_CANCELLED_STATUS_IF_CTX_CANCELLED(query_ctx) \
    if (query_ctx->is_cancelled()) {                        \
        return query_ctx->get_cancelled_status();           \
    }                                                       \
    query_ctx->increment_num_fragments();                   \
    if (query_ctx->is_cancelled()) {                        \
        query_ctx->rollback_inc_fragments();                \
        return query_ctx->get_cancelled_status();           \
    }

StatusOr<QueryContext*> QueryContextManager::get_or_register(const TUniqueId& query_id,
                                                             bool return_error_if_not_exist) {
    size_t i = _slot_idx(query_id);
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];

    {
        std::shared_lock<std::shared_mutex> read_lock(mutex);
        // lookup query context in context_map
        auto it = context_map.find(query_id);
        if (it != context_map.end()) {
            RETURN_CANCELLED_STATUS_IF_CTX_CANCELLED(it->second);
            return it->second.get();
        }
    }
    {
        std::unique_lock<std::shared_mutex> write_lock(mutex);
        // lookup query context in context_map at first
        auto it = context_map.find(query_id);
        auto sc_it = sc_map.find(query_id);
        if (it != context_map.end()) {
            RETURN_CANCELLED_STATUS_IF_CTX_CANCELLED(it->second);
            return it->second.get();
        } else {
            // lookup query context for the second chance in sc_map
            if (sc_it != sc_map.end()) {
                auto ctx = std::move(sc_it->second);
                auto* raw_ctx_ptr = ctx.get();
                sc_map.erase(sc_it);
                auto cancel_status = [ctx]() -> Status {
                    RETURN_CANCELLED_STATUS_IF_CTX_CANCELLED(ctx);
                    return Status::OK();
                }();
                // If there are still active fragments, we cannot directly remove the query context
                // because the operator is still executing.
                // We need to wait until the fragment execution is complete,
                // then call QueryContextManager::remove to safely remove this query context.
                if (cancel_status.ok() || !ctx->has_no_active_instances()) {
                    context_map.emplace(query_id, std::move(ctx));
                }
                RETURN_IF_ERROR(cancel_status);
                return raw_ctx_ptr;
            }
        }

        if (return_error_if_not_exist) {
            return Status::Cancelled("Query terminates prematurely");
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
                    RuntimeStateHelper::update_report_load_status(runtime_state, &params);
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
    std::vector<std::shared_ptr<QueryContext>> need_report_query_ctx;
    std::vector<std::shared_ptr<FragmentContext>> need_report_fragment_context;

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
                RuntimeStateHelper::update_report_load_status(runtime_state, &params);
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

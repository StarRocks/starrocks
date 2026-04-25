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

#include "exec/pipeline/operator.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "base/failpoint/fail_point.h"
#include "common/logging.h"
#include "common/runtime_profile.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/query_context.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

const int32_t Operator::s_pseudo_plan_node_id_for_final_sink = -1;

Operator::Operator(OperatorFactory* factory, int32_t id, std::string name, int32_t plan_node_id, bool is_subordinate,
                   int32_t driver_sequence, OperatorRuntimeAccess* runtime_access)
        : _factory(factory),
          _runtime_access(runtime_access != nullptr ? runtime_access : factory),
          _id(id),
          _name(std::move(name)),
          _plan_node_id(plan_node_id),
          _is_subordinate(is_subordinate),
          _driver_sequence(driver_sequence),
          _runtime_filter_probe_sequence(driver_sequence) {
    std::string upper_name(_name);
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    std::string profile_name = strings::Substitute("$0 (plan_node_id=$1)", upper_name, _plan_node_id);
    // some pipeline may have multiple limit operators with same plan_node_id, so add operator id to profile name
    if (upper_name == "LIMIT") {
        profile_name += " (operator id=" + std::to_string(id) + ")";
    }
    _runtime_profile = std::make_shared<RuntimeProfile>(profile_name);
    _runtime_profile->set_metadata(_id);

    _common_metrics = std::make_shared<RuntimeProfile>("CommonMetrics");
    _runtime_profile->add_child(_common_metrics.get(), true, nullptr);

    _unique_metrics = std::make_shared<RuntimeProfile>("UniqueMetrics");
    _runtime_profile->add_child(_unique_metrics.get(), true, nullptr);
    if (!_is_subordinate && _plan_node_id == s_pseudo_plan_node_id_for_final_sink) {
        _common_metrics->add_info_string("IsFinalSink");
    }
    if (_is_subordinate) {
        _common_metrics->add_info_string("IsSubordinate");
    }
    if (is_combinatorial_operator()) {
        _common_metrics->add_info_string("IsCombinatorial");
    }
}

Status Operator::prepare(RuntimeState* state) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    return Status::OK();
}

Status Operator::prepare_local_state(RuntimeState* state) {
    _mem_tracker = std::make_shared<MemTracker>();

    _total_timer = ADD_TIMER(_common_metrics, "OperatorTotalTime");
    _push_timer = ADD_TIMER(_common_metrics, "PushTotalTime");
    _pull_timer = ADD_TIMER(_common_metrics, "PullTotalTime");
    _finishing_timer = ADD_TIMER_WITH_THRESHOLD(_common_metrics, "SetFinishingTime", 1_ms);
    _finished_timer = ADD_TIMER_WITH_THRESHOLD(_common_metrics, "SetFinishedTime", 1_ms);
    _close_timer = ADD_TIMER_WITH_THRESHOLD(_common_metrics, "CloseTime", 1_ms);
    _local_prepare_timer = ADD_TIMER_WITH_THRESHOLD(_common_metrics, "LocalPrepareTime", 1_ms);
    if (_global_prepare_time_ns > 1) {
        _global_prepare_timer = ADD_TIMER_WITH_THRESHOLD(_common_metrics, "GlobalPrepareTime", 1_ms);
        COUNTER_SET(_global_prepare_timer, _global_prepare_time_ns);
    }
    _push_chunk_num_counter = ADD_COUNTER(_common_metrics, "PushChunkNum", TUnit::UNIT);
    _push_row_num_counter = ADD_COUNTER(_common_metrics, "PushRowNum", TUnit::UNIT);
    _pull_chunk_num_counter = ADD_COUNTER(_common_metrics, "PullChunkNum", TUnit::UNIT);
    _pull_row_num_counter = ADD_COUNTER(_common_metrics, "PullRowNum", TUnit::UNIT);
    _pull_chunk_bytes_counter = ADD_COUNTER(_common_metrics, "OutputChunkBytes", TUnit::BYTES);

    for_each_child_operator([&](Operator* child) {
        child->_common_metrics->add_info_string("IsSubordinate");
        child->_common_metrics->add_info_string("IsChild");
    });

    return Status::OK();
}

void Operator::set_prepare_time(int64_t cost_ns) {
    _global_prepare_time_ns = cost_ns;
}

void Operator::set_local_prepare_time(int64_t cost_ns) {
    if (_local_prepare_timer != nullptr) {
        COUNTER_SET(_local_prepare_timer, cost_ns);
    }
}

void Operator::set_precondition_ready(RuntimeState* state) {
    _runtime_in_filters.clear();
    _runtime_access->bind_runtime_in_filters(state, _driver_sequence, &_runtime_in_filters);
    VLOG_QUERY << "plan_node_id:" << _plan_node_id << " sequence:" << _driver_sequence
               << " local in runtime filter num:" << _runtime_in_filters.size() << " op:" << this->get_raw_name();
}

void Operator::close(RuntimeState* state) {
    if (auto* rf_bloom_filters = _runtime_access->get_runtime_bloom_filters()) {
        _init_rf_counters(false);
        COUNTER_SET(_runtime_in_filter_num_counter, (int64_t)runtime_in_filters().size());
        COUNTER_SET(_runtime_bloom_filter_num_counter, (int64_t)rf_bloom_filters->size());

        if (!rf_bloom_filters->descriptors().empty()) {
            std::string rf_desc = "";
            for (const auto& [filter_id, desc] : rf_bloom_filters->descriptors()) {
                rf_desc += "<" + std::to_string(filter_id) + ": ";
                if (desc != nullptr && desc->runtime_filter(0) != nullptr) {
                    rf_desc += desc->runtime_filter(0)->debug_string();
                } else {
                    rf_desc += "NULL";
                }
                rf_desc += "> ";
            }
            _common_metrics->add_info_string("RuntimeFilterDesc", rf_desc);
        }
    }

    // Pipeline do not need the built in total time counter
    // Reset here to discard assignments from Analytor, Aggregator, etc.
    COUNTER_SET(_runtime_profile->total_time_counter(), (int64_t)0L);
    COUNTER_SET(_common_metrics->total_time_counter(), (int64_t)0L);
    COUNTER_SET(_unique_metrics->total_time_counter(), (int64_t)0L);
}

std::vector<ExprContext*>& Operator::runtime_in_filters() {
    return _runtime_in_filters;
}

int64_t Operator::global_rf_wait_timeout_ns() const {
    const auto* global_rf_collector = _runtime_access->get_runtime_bloom_filters();
    if (global_rf_collector == nullptr) {
        return 0;
    }

    return 1000'000L * global_rf_collector->wait_timeout_ms();
}

Status Operator::eval_conjuncts_and_in_filters(const std::vector<ExprContext*>& conjuncts, Chunk* chunk,
                                               FilterPtr* filter, bool apply_filter) {
    if (UNLIKELY(!_conjuncts_and_in_filters_is_cached)) {
        _cached_conjuncts_and_in_filters.insert(_cached_conjuncts_and_in_filters.end(), conjuncts.begin(),
                                                conjuncts.end());
        auto& in_filters = runtime_in_filters();
        _cached_conjuncts_and_in_filters.insert(_cached_conjuncts_and_in_filters.end(), in_filters.begin(),
                                                in_filters.end());
        _conjuncts_and_in_filters_is_cached = true;
    }
    if (_cached_conjuncts_and_in_filters.empty()) {
        return Status::OK();
    }
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }
    _init_conjuct_counters();
    {
        SCOPED_TIMER(_conjuncts_timer);
        auto before = chunk->num_rows();
        COUNTER_UPDATE(_conjuncts_input_counter, before);
        RETURN_IF_ERROR(starrocks::ChunkPredicateEvaluator::eval_conjuncts(_cached_conjuncts_and_in_filters, chunk,
                                                                           filter, apply_filter));
        auto after = chunk->num_rows();
        COUNTER_UPDATE(_conjuncts_output_counter, after);
    }

    return Status::OK();
}

Status Operator::eval_no_eq_join_runtime_in_filters(Chunk* chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }
    _init_conjuct_counters();
    {
        SCOPED_TIMER(_conjuncts_timer);
        auto& in_filters = runtime_in_filters();
        std::vector<ExprContext*> selected_vector;
        for (ExprContext* in_filter : in_filters) {
            if (in_filter->build_from_only_in_filter()) {
                selected_vector.push_back(in_filter);
            }
        }
        size_t before = chunk->num_rows();
        COUNTER_UPDATE(_conjuncts_input_counter, before);
        RETURN_IF_ERROR(starrocks::ChunkPredicateEvaluator::eval_conjuncts(selected_vector, chunk, nullptr));
        size_t after = chunk->num_rows();
        COUNTER_UPDATE(_conjuncts_output_counter, after);
    }

    return Status::OK();
}

Status Operator::eval_conjuncts(const std::vector<ExprContext*>& conjuncts, Chunk* chunk, FilterPtr* filter) {
    if (conjuncts.empty()) {
        return Status::OK();
    }
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }
    _init_conjuct_counters();
    {
        SCOPED_TIMER(_conjuncts_timer);
        size_t before = chunk->num_rows();
        COUNTER_UPDATE(_conjuncts_input_counter, before);
        RETURN_IF_ERROR(starrocks::ChunkPredicateEvaluator::eval_conjuncts(conjuncts, chunk, filter));
        size_t after = chunk->num_rows();
        COUNTER_UPDATE(_conjuncts_output_counter, after);
    }

    return Status::OK();
}

void Operator::eval_runtime_bloom_filters(Chunk* chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return;
    }

    if (auto* bloom_filters = _runtime_access->get_runtime_bloom_filters()) {
        _init_rf_counters(true);
        bloom_filters->evaluate(chunk, _bloom_filter_eval_context);
    }

    ChunkPredicateEvaluator::eval_filter_null_values(chunk, _runtime_access->get_filter_null_value_columns());
}

void Operator::_init_rf_counters(bool init_bloom) {
    if (_runtime_in_filter_num_counter == nullptr) {
        _runtime_in_filter_num_counter =
                ADD_COUNTER_SKIP_MERGE(_common_metrics, "RuntimeInFilterNum", TUnit::UNIT, TCounterMergeType::SKIP_ALL);
        _runtime_bloom_filter_num_counter =
                ADD_COUNTER_SKIP_MERGE(_common_metrics, "RuntimeFilterNum", TUnit::UNIT, TCounterMergeType::SKIP_ALL);
    }
    if (init_bloom && _bloom_filter_eval_context.join_runtime_filter_timer == nullptr) {
        _bloom_filter_eval_context.join_runtime_filter_timer = ADD_TIMER(_common_metrics, "JoinRuntimeFilterTime");
        _bloom_filter_eval_context.join_runtime_filter_hash_timer =
                ADD_TIMER(_common_metrics, "JoinRuntimeFilterHashTime");
        _bloom_filter_eval_context.join_runtime_filter_input_counter =
                ADD_COUNTER(_common_metrics, "JoinRuntimeFilterInputRows", TUnit::UNIT);
        _bloom_filter_eval_context.join_runtime_filter_output_counter =
                ADD_COUNTER(_common_metrics, "JoinRuntimeFilterOutputRows", TUnit::UNIT);
        _bloom_filter_eval_context.join_runtime_filter_eval_counter =
                ADD_COUNTER(_common_metrics, "JoinRuntimeFilterEvaluate", TUnit::UNIT);
        _bloom_filter_eval_context.driver_sequence = _runtime_filter_probe_sequence;
    }
}

void Operator::_init_conjuct_counters() {
    if (_conjuncts_timer == nullptr) {
        _conjuncts_timer = ADD_TIMER(_common_metrics, "ConjunctsTime");
        _conjuncts_input_counter = ADD_COUNTER(_common_metrics, "ConjunctsInputRows", TUnit::UNIT);
        _conjuncts_output_counter = ADD_COUNTER(_common_metrics, "ConjunctsOutputRows", TUnit::UNIT);
    }
}

void Operator::update_exec_stats(RuntimeState* state) {
    auto ctx = state->query_ctx();
    if (!_is_subordinate && ctx != nullptr && ctx->need_record_exec_stats(_plan_node_id)) {
        ctx->update_push_rows_stats(_plan_node_id, COUNTER_VALUE(_push_row_num_counter));
        ctx->update_pull_rows_stats(_plan_node_id, COUNTER_VALUE(_pull_row_num_counter));
        if (_conjuncts_input_counter != nullptr && _conjuncts_output_counter != nullptr) {
            ctx->update_pred_filter_stats(
                    _plan_node_id, COUNTER_VALUE(_conjuncts_input_counter) - COUNTER_VALUE(_conjuncts_output_counter));
        }
        if (_bloom_filter_eval_context.join_runtime_filter_input_counter != nullptr &&
            _bloom_filter_eval_context.join_runtime_filter_output_counter != nullptr) {
            int64_t input_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_input_counter);
            int64_t output_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_output_counter);
            ctx->update_rf_filter_stats(_plan_node_id, input_rows - output_rows);
        }
    }
}

} // namespace starrocks::pipeline

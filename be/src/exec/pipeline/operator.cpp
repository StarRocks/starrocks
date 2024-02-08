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
#include <utility>

#include "exec/exec_node.h"
#include "exec/pipeline/query_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"
#include "util/failpoint/fail_point.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

const int32_t Operator::s_pseudo_plan_node_id_for_final_sink = -1;

Operator::Operator(OperatorFactory* factory, int32_t id, std::string name, int32_t plan_node_id, bool is_subordinate,
                   int32_t driver_sequence)
        : _factory(factory),
          _id(id),
          _name(std::move(name)),
          _plan_node_id(plan_node_id),
          _is_subordinate(is_subordinate),
          _driver_sequence(driver_sequence) {
    std::string upper_name(_name);
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    std::string profile_name = strings::Substitute("$0 (plan_node_id=$1)", upper_name, _plan_node_id);
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
    _mem_tracker = state->query_ctx()->operator_mem_tracker(_plan_node_id);
    _total_timer = ADD_TIMER(_common_metrics, "OperatorTotalTime");
    _push_timer = ADD_TIMER(_common_metrics, "PushTotalTime");
    _pull_timer = ADD_TIMER(_common_metrics, "PullTotalTime");
    _finishing_timer = ADD_TIMER(_common_metrics, "SetFinishingTime");
    _finished_timer = ADD_TIMER(_common_metrics, "SetFinishedTime");
    _close_timer = ADD_TIMER(_common_metrics, "CloseTime");
    _prepare_timer = ADD_TIMER(_common_metrics, "PrepareTime");

    _push_chunk_num_counter = ADD_COUNTER(_common_metrics, "PushChunkNum", TUnit::UNIT);
    _push_row_num_counter = ADD_COUNTER(_common_metrics, "PushRowNum", TUnit::UNIT);
    _pull_chunk_num_counter = ADD_COUNTER(_common_metrics, "PullChunkNum", TUnit::UNIT);
    _pull_row_num_counter = ADD_COUNTER(_common_metrics, "PullRowNum", TUnit::UNIT);
    if (state->query_ctx() && state->query_ctx()->spill_manager()) {
        _mem_resource_manager.prepare(this, state->query_ctx()->spill_manager());
    }
    for_each_child_operator([&](Operator* child) {
        child->_common_metrics->add_info_string("IsSubordinate");
        child->_common_metrics->add_info_string("IsChild");
    });
    return Status::OK();
}

void Operator::set_prepare_time(int64_t cost_ns) {
    _prepare_timer->set(cost_ns);
}

void Operator::set_precondition_ready(RuntimeState* state) {
    _factory->prepare_runtime_in_filters(state);
}

const LocalRFWaitingSet& Operator::rf_waiting_set() const {
    DCHECK(_factory != nullptr);
    return _factory->rf_waiting_set();
}

RuntimeFilterHub* Operator::runtime_filter_hub() {
    return _factory->runtime_filter_hub();
}

void Operator::close(RuntimeState* state) {
    _mem_resource_manager.close();
    if (auto* rf_bloom_filters = runtime_bloom_filters()) {
        _init_rf_counters(false);
        _runtime_in_filter_num_counter->set((int64_t)runtime_in_filters().size());
        _runtime_bloom_filter_num_counter->set((int64_t)rf_bloom_filters->size());
    }

    if (!_is_subordinate) {
        _common_metrics
                ->add_counter("OperatorPeakMemoryUsage", TUnit::BYTES,
                              RuntimeProfile::Counter::create_strategy(TCounterAggregateType::AVG,
                                                                       TCounterMergeType::SKIP_FIRST_MERGE))
                ->set(_mem_tracker->peak_consumption());
        _common_metrics
                ->add_counter("OperatorAllocatedMemoryUsage", TUnit::BYTES,
                              RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM,
                                                                       TCounterMergeType::SKIP_FIRST_MERGE))
                ->set(_mem_tracker->allocation());
        _common_metrics
                ->add_counter("OperatorDeallocatedMemoryUsage", TUnit::BYTES,
                              RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM,
                                                                       TCounterMergeType::SKIP_FIRST_MERGE))
                ->set(_mem_tracker->deallocation());
    }

    // Pipeline do not need the built in total time counter
    // Reset here to discard assignments from Analytor, Aggregator, etc.
    _runtime_profile->total_time_counter()->set(0L);
    _common_metrics->total_time_counter()->set(0L);
    _unique_metrics->total_time_counter()->set(0L);
}

std::vector<ExprContext*>& Operator::runtime_in_filters() {
    return _factory->get_runtime_in_filters();
}

RuntimeFilterProbeCollector* Operator::runtime_bloom_filters() {
    return _factory->get_runtime_bloom_filters();
}
const RuntimeFilterProbeCollector* Operator::runtime_bloom_filters() const {
    return _factory->get_runtime_bloom_filters();
}

int64_t Operator::global_rf_wait_timeout_ns() const {
    const auto* global_rf_collector = runtime_bloom_filters();
    if (global_rf_collector == nullptr) {
        return 0;
    }

    return 1000'000L * global_rf_collector->wait_timeout_ms();
}

const std::vector<SlotId>& Operator::filter_null_value_columns() const {
    return _factory->get_filter_null_value_columns();
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
        _conjuncts_input_counter->update(before);
        RETURN_IF_ERROR(
                starrocks::ExecNode::eval_conjuncts(_cached_conjuncts_and_in_filters, chunk, filter, apply_filter));
        auto after = chunk->num_rows();
        _conjuncts_output_counter->update(after);
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
        _conjuncts_input_counter->update(before);
        RETURN_IF_ERROR(starrocks::ExecNode::eval_conjuncts(conjuncts, chunk, filter));
        size_t after = chunk->num_rows();
        _conjuncts_output_counter->update(after);
    }

    return Status::OK();
}

void Operator::eval_runtime_bloom_filters(Chunk* chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return;
    }

    if (auto* bloom_filters = runtime_bloom_filters()) {
        _init_rf_counters(true);
        bloom_filters->evaluate(chunk, _bloom_filter_eval_context);
    }

    ExecNode::eval_filter_null_values(chunk, filter_null_value_columns());
}

RuntimeState* Operator::runtime_state() const {
    return _factory->runtime_state();
}

void Operator::_init_rf_counters(bool init_bloom) {
    if (_runtime_in_filter_num_counter == nullptr) {
        _runtime_in_filter_num_counter =
                ADD_COUNTER_SKIP_MERGE(_common_metrics, "RuntimeInFilterNum", TUnit::UNIT, TCounterMergeType::SKIP_ALL);
        _runtime_bloom_filter_num_counter = ADD_COUNTER_SKIP_MERGE(_common_metrics, "RuntimeBloomFilterNum",
                                                                   TUnit::UNIT, TCounterMergeType::SKIP_ALL);
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
    }
}

void Operator::_init_conjuct_counters() {
    if (_conjuncts_timer == nullptr) {
        _conjuncts_timer = ADD_TIMER(_common_metrics, "ConjunctsTime");
        _conjuncts_input_counter = ADD_COUNTER(_common_metrics, "ConjunctsInputRows", TUnit::UNIT);
        _conjuncts_output_counter = ADD_COUNTER(_common_metrics, "ConjunctsOutputRows", TUnit::UNIT);
    }
}

OperatorFactory::OperatorFactory(int32_t id, std::string name, int32_t plan_node_id)
        : _id(id), _name(std::move(name)), _plan_node_id(plan_node_id) {
    std::string upper_name(_name);
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    _runtime_profile =
            std::make_shared<RuntimeProfile>(strings::Substitute("$0_factory (id=$1)", upper_name, _plan_node_id));
    _runtime_profile->set_metadata(_id);
}

Status OperatorFactory::prepare(RuntimeState* state) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    _state = state;
    if (_runtime_filter_collector) {
        // TODO(hcf) no proper profile for rf_filter_collector attached to
        RETURN_IF_ERROR(_runtime_filter_collector->prepare(state, _row_desc, _runtime_profile.get()));
        auto& descriptors = _runtime_filter_collector->get_rf_probe_collector()->descriptors();
        for (auto& [filter_id, desc] : descriptors) {
            if (desc->is_local() || desc->runtime_filter() != nullptr) {
                continue;
            }
            auto grf = state->exec_env()->runtime_filter_cache()->get(state->query_id(), filter_id);
            ExecEnv::GetInstance()->add_rf_event({_state->query_id(), filter_id, BackendOptions::get_localhost(),
                                                  strings::Substitute("INSTALL_GRF_TO_OPERATOR(op_id=$0, success=$1",
                                                                      this->_plan_node_id, grf != nullptr)});

            if (grf == nullptr) {
                continue;
            }

            desc->set_shared_runtime_filter(grf);
        }
    }
    return Status::OK();
}

/// OperatorFactory.
void OperatorFactory::close(RuntimeState* state) {
    if (_runtime_filter_collector) {
        _runtime_filter_collector->close(state);
    }
}

void OperatorFactory::_prepare_runtime_in_filters(RuntimeState* state) {
    auto holders = _runtime_filter_hub->gather_holders(_rf_waiting_set);
    for (auto& holder : holders) {
        DCHECK(holder->is_ready());
        auto* collector = holder->get_collector();

        collector->rewrite_in_filters(_tuple_slot_mappings);

        auto&& in_filters = collector->get_in_filters_bounded_by_tuple_ids(_tuple_ids);
        for (auto* filter : in_filters) {
            filter->prepare(state);
            filter->open(state);
            _runtime_in_filters.push_back(filter);
        }
    }
}

bool OperatorFactory::has_runtime_filters() const {
    // Check runtime in-filters.
    if (!_rf_waiting_set.empty()) {
        return true;
    }

    // Check runtime bloom-filters.
    if (_runtime_filter_collector == nullptr) {
        return false;
    }
    auto* global_rf_collector = _runtime_filter_collector->get_rf_probe_collector();
    return global_rf_collector != nullptr && !global_rf_collector->descriptors().empty();
}

bool OperatorFactory::has_topn_filter() const {
    if (_runtime_filter_collector == nullptr) {
        return false;
    }
    auto* global_rf_collector = _runtime_filter_collector->get_rf_probe_collector();
    return global_rf_collector != nullptr && global_rf_collector->has_topn_filter();
}

} // namespace starrocks::pipeline

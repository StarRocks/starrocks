// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/operator.h"

#include <algorithm>

#include "exec/exec_node.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {
Operator::Operator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id)
        : _factory(factory), _id(id), _name(name), _plan_node_id(plan_node_id) {
    std::string upper_name(_name);
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    _runtime_profile = std::make_shared<RuntimeProfile>(strings::Substitute("$0 (id=$1)", upper_name, _plan_node_id));
    _runtime_profile->set_metadata(_id);
    _mem_tracker = std::make_unique<MemTracker>(_runtime_profile.get(), -1, _runtime_profile->name(), nullptr);
}

Status Operator::prepare(RuntimeState* state) {
    _push_timer = ADD_TIMER(_runtime_profile, "PushTotalTime");
    _pull_timer = ADD_TIMER(_runtime_profile, "PullTotalTime");

    _push_chunk_num_counter = ADD_COUNTER(_runtime_profile, "PushChunkNum", TUnit::UNIT);
    _push_row_num_counter = ADD_COUNTER(_runtime_profile, "PushRowNum", TUnit::UNIT);
    _pull_chunk_num_counter = ADD_COUNTER(_runtime_profile, "PullChunkNum", TUnit::UNIT);
    _pull_row_num_counter = ADD_COUNTER(_runtime_profile, "PullRowNum", TUnit::UNIT);
    _runtime_in_filter_num_counter = ADD_COUNTER(_runtime_profile, "RuntimeInFilterNum", TUnit::UNIT);
    _runtime_bloom_filter_num_counter = ADD_COUNTER(_runtime_profile, "RuntimeBloomFilterNum", TUnit::UNIT);
    _conjuncts_timer = ADD_TIMER(_runtime_profile, "JoinRuntimeFilterTime");
    _conjuncts_input_counter = ADD_COUNTER(_runtime_profile, "ConjunctsInputRows", TUnit::UNIT);
    _conjuncts_output_counter = ADD_COUNTER(_runtime_profile, "ConjunctsOutputRows", TUnit::UNIT);
    _conjuncts_eval_counter = ADD_COUNTER(_runtime_profile, "ConjunctsEvaluate", TUnit::UNIT);
    _bloom_filter_eval_context.prepare(_runtime_profile.get());
    return Status::OK();
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

Status Operator::close(RuntimeState* state) {
    if (auto* rf_bloom_filters = runtime_bloom_filters()) {
        _runtime_in_filter_num_counter->set((int64_t)runtime_in_filters().size());
        _runtime_bloom_filter_num_counter->set((int64_t)rf_bloom_filters->size());
    }
    return Status::OK();
}

std::vector<ExprContext*>& Operator::runtime_in_filters() {
    return _factory->get_runtime_in_filters();
}

RuntimeFilterProbeCollector* Operator::runtime_bloom_filters() {
    return _factory->get_runtime_bloom_filters();
}

void Operator::eval_conjuncts_and_in_filters(const std::vector<ExprContext*>& conjuncts, vectorized::Chunk* chunk) {
    if (UNLIKELY(!_conjuncts_and_in_filters_is_cached)) {
        _cached_conjuncts_and_in_filters.insert(_cached_conjuncts_and_in_filters.end(), conjuncts.begin(),
                                                conjuncts.end());
        auto& in_filters = runtime_in_filters();
        _cached_conjuncts_and_in_filters.insert(_cached_conjuncts_and_in_filters.end(), in_filters.begin(),
                                                in_filters.end());
        _conjuncts_and_in_filters_is_cached = true;
    }
    if (chunk == nullptr || chunk->is_empty()) {
        return;
    }
    {
        SCOPED_TIMER(_conjuncts_timer);
        auto before = chunk->num_rows();
        _conjuncts_input_counter->update(before);
        starrocks::ExecNode::eval_conjuncts(_cached_conjuncts_and_in_filters, chunk);
        auto after = chunk->num_rows();
        _conjuncts_output_counter->update(after);
        _conjuncts_eval_counter->update(before - after);
    }
}

void Operator::eval_runtime_bloom_filters(vectorized::Chunk* chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return;
    }
    if (auto* bloom_filters = runtime_bloom_filters()) {
        bloom_filters->evaluate(chunk, _bloom_filter_eval_context);
    }
}

OperatorFactory::OperatorFactory(int32_t id, const std::string& name, int32_t plan_node_id)
        : _id(id), _name(name), _plan_node_id(plan_node_id) {
    std::string upper_name(_name);
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    _runtime_profile =
            std::make_shared<RuntimeProfile>(strings::Substitute("$0_factory (id=$1)", upper_name, _plan_node_id));
    _runtime_profile->set_metadata(_id);
}

Status OperatorFactory::prepare(RuntimeState* state) {
    if (_runtime_filter_collector) {
        RETURN_IF_ERROR(_runtime_filter_collector->prepare(state, _row_desc, _runtime_profile.get()));
    }
    return Status::OK();
}

void OperatorFactory::close(RuntimeState* state) {
    if (_runtime_filter_collector) {
        _runtime_filter_collector->close(state);
    }
}

} // namespace starrocks::pipeline

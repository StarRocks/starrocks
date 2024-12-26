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

#include "exec/query_cache/conjugate_operator.h"

#include <utility>

namespace starrocks::query_cache {

static inline std::string base_name_of_conjugate_op(const std::string& s) {
    std::string lc;
    lc.resize(s.size());
    std::transform(s.begin(), s.end(), lc.begin(), [](char c) { return (char)std::tolower(c); });
    const char* source = "_source";
    const char* sink = "_sink";
    if (memcmp(lc.data() + lc.size() - 7, source, 7) == 0) {
        lc.resize(lc.size() - 7);
    } else if (memcmp(lc.data() + lc.size() - 5, sink, 5) == 0) {
        lc.resize(lc.size() - 5);
    }
    return lc;
}

ConjugateOperator::ConjugateOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence,
                                     pipeline::OperatorPtr sink_op, pipeline::OperatorPtr source_op)
        : pipeline::Operator(factory, factory->id(), factory->get_raw_name(), factory->plan_node_id(), true,
                             driver_sequence),
          _sink_op(std::move(sink_op)),
          _source_op(std::move(source_op)) {}

Status ConjugateOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_source_op->prepare(state));
    RETURN_IF_ERROR(_sink_op->prepare(state));
    _source_op->set_runtime_filter_probe_sequence(_runtime_filter_probe_sequence);
    _sink_op->set_runtime_filter_probe_sequence(_runtime_filter_probe_sequence);
    return Status::OK();
}

void ConjugateOperator::close(RuntimeState* state) {
    _sink_op->close(state);
    _source_op->close(state);
    Operator::close(state);
}

bool ConjugateOperator::has_output() const {
    return _source_op->has_output();
}

bool ConjugateOperator::need_input() const {
    return _sink_op->need_input();
}

bool ConjugateOperator::is_finished() const {
    return _source_op->is_finished();
}

Status ConjugateOperator::set_finishing(RuntimeState* state) {
    return _sink_op->set_finishing(state);
}

Status ConjugateOperator::set_finished(RuntimeState* state) {
    auto sink_status = _sink_op->set_finished(state);
    auto source_status = _source_op->set_finished(state);
    if (sink_status.ok()) {
        return source_status;
    } else {
        return sink_status;
    }
}

Status ConjugateOperator::set_cancelled(RuntimeState* state) {
    auto sink_status = _sink_op->set_cancelled(state);
    auto source_status = _source_op->set_cancelled(state);
    if (sink_status.ok()) {
        return source_status;
    } else {
        return sink_status;
    }
}

const pipeline::LocalRFWaitingSet& ConjugateOperator::rf_waiting_set() const {
    return _source_op->rf_waiting_set();
}

RuntimeFilterProbeCollector* ConjugateOperator::runtime_bloom_filters() {
    return _source_op->runtime_bloom_filters();
}

const RuntimeFilterProbeCollector* ConjugateOperator::runtime_bloom_filters() const {
    return _source_op->runtime_bloom_filters();
}

void ConjugateOperator::set_precondition_ready(RuntimeState* state) {
    _sink_op->set_precondition_ready(state);
    _source_op->set_precondition_ready(state);
}

StatusOr<ChunkPtr> ConjugateOperator::pull_chunk(RuntimeState* state) {
    return _source_op->pull_chunk(state);
}

Status ConjugateOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _sink_op->push_chunk(state, chunk);
}

Status ConjugateOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    RETURN_IF_ERROR(_sink_op->reset_state(state, refill_chunks));
    RETURN_IF_ERROR(_source_op->reset_state(state, {}));
    return Status::OK();
}

ConjugateOperatorFactory::ConjugateOperatorFactory(pipeline::OpFactoryPtr sink_op_factory,
                                                   const pipeline::OpFactoryPtr& source_op_factory)
        : pipeline::OperatorFactory(
                  source_op_factory->id(),
                  strings::Substitute("conjugate_$0", base_name_of_conjugate_op(source_op_factory->get_raw_name())),
                  source_op_factory->plan_node_id()),
          _sink_op_factory(std::move(sink_op_factory)),
          _source_op_factory(source_op_factory) {}

Status ConjugateOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(pipeline::OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_source_op_factory->prepare(state));
    RETURN_IF_ERROR(_sink_op_factory->prepare(state));
    return Status::OK();
}

void ConjugateOperatorFactory::close(RuntimeState* state) {
    _sink_op_factory->close(state);
    _source_op_factory->close(state);
    pipeline::OperatorFactory::close(state);
}

pipeline::OperatorPtr ConjugateOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto sink_op = _sink_op_factory->create(degree_of_parallelism, driver_sequence);
    auto source_op = _source_op_factory->create(degree_of_parallelism, driver_sequence);
    return std::make_shared<ConjugateOperator>(this, driver_sequence, sink_op, source_op);
}

} // namespace starrocks::query_cache

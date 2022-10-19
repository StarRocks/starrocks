// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/aggregate/sorted_aggregate_streaming_sink_operator.h"

#include <string>

#include "exec/vectorized/sorted_streaming_aggregator.h"
#include "runtime/current_thread.h"

namespace starrocks::pipeline {
SortedAggregateStreamingSinkOperator::SortedAggregateStreamingSinkOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
        std::shared_ptr<SortedStreamingAggregator> aggregator)
        : Operator(factory, id, "sorted_aggregate_streaming_sink", plan_node_id, driver_sequence),
          _aggregator(std::move(aggregator)) {
    _aggregator->ref();
}

Status SortedAggregateStreamingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    return _aggregator->open(state);
}

void SortedAggregateStreamingSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

bool SortedAggregateStreamingSinkOperator::is_finished() const {
    return _is_finished || _aggregator->is_finished();
}

Status SortedAggregateStreamingSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> SortedAggregateStreamingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status SortedAggregateStreamingSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();
    if (chunk_size > 0) {
        std::string log = "sorted_id_" + std::to_string(_driver_sequence) + ":";
        for (size_t i = 0; i < chunk_size; ++i) {
            log += chunk->debug_row(i) + "\n";
        }
        LOG(WARNING) << print_id(tls_thread_status.query_id()) << log;
    }
    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    RETURN_IF_ERROR(_aggregator->evaluate_exprs(chunk.get()));
    _aggregator->streaming_compute_agg_state(chunk_size);
    return Status::OK();
}

OperatorPtr SortedAggregateStreamingSinkOperatorFactory::create(int32_t degree_of_parallelism,
                                                                int32_t driver_sequence) {
    return std::make_shared<SortedAggregateStreamingSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                  _aggregator_factory->get_or_create(driver_sequence));
}

} // namespace starrocks::pipeline
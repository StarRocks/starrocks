// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_blocking_source_operator.h"

#include "exec/exec_node.h"

namespace starrocks::pipeline {

bool AggregateBlockingSourceOperator::has_output() const {
    return _aggregator->is_sink_complete() && !_aggregator->is_ht_eos();
}

bool AggregateBlockingSourceOperator::is_finished() const {
    return _aggregator->is_sink_complete() && _aggregator->is_ht_eos();
}

void AggregateBlockingSourceOperator::finish(RuntimeState* state) {
    _is_finished = true;
}

Status AggregateBlockingSourceOperator::close(RuntimeState* state) {
    // _aggregator is shared by sink operator and source operator
    // we must only close it at source operator
    RETURN_IF_ERROR(_aggregator->close(state));
    return SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> AggregateBlockingSourceOperator::pull_chunk(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    int32_t chunk_size = config::vector_chunk_size;
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();

    if (_aggregator->is_none_group_by_exprs()) {
        SCOPED_TIMER(_aggregator->get_results_timer());
        _aggregator->convert_to_chunk_no_groupby(&chunk);
    } else {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                     \
    else if (_aggregator->hash_map_variant().type == vectorized::HashMapVariant::Type::NAME)                      \
            _aggregator->convert_hash_map_to_chunk<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, chunk_size, &chunk);
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    }

    // TODO(hcf) force annotation
    // eval_join_runtime_filters(chunk.get());

    // For having
    size_t old_size = chunk->num_rows();

    ExecNode::eval_conjuncts(_aggregator->conjunct_ctxs(), chunk.get());
    _aggregator->update_num_rows_returned(-(old_size - chunk->num_rows()));

    DCHECK_CHUNK(chunk);

    return std::move(chunk);
}
} // namespace starrocks::pipeline

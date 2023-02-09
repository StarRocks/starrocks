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

#include "exec/aggregate/distinct_streaming_node.h"

#include <variant>

#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_streaming_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks {

Status DistinctStreamingNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBaseNode::prepare(state));
    _aggregator->set_aggr_phase(AggrPhase1);
    return Status::OK();
}

Status DistinctStreamingNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_aggregator->open(state));
    RETURN_IF_ERROR(_children[0]->open(state));
    return Status::OK();
}

Status DistinctStreamingNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    *eos = false;

    if (_aggregator->is_ht_eos()) {
        COUNTER_SET(_aggregator->rows_returned_counter(), _aggregator->num_rows_returned());
        COUNTER_SET(_aggregator->pass_through_row_count(), _aggregator->num_pass_through_rows());
        *eos = true;
        return Status::OK();
    }

    // TODO: merge small chunks to large chunk for optimization
    while (!_child_eos) {
        ChunkPtr input_chunk;
        RETURN_IF_ERROR(_children[0]->get_next(state, &input_chunk, &_child_eos));
        if (!_child_eos) {
            if (input_chunk->is_empty()) {
                continue;
            }

            size_t input_chunk_size = input_chunk->num_rows();
            _aggregator->update_num_input_rows(input_chunk_size);
            COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());
            RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(input_chunk.get()));

            if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
                // force execute streaming
                SCOPED_TIMER(_aggregator->streaming_timer());
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(input_chunk.get(), chunk));
                break;
            } else if (_aggregator->streaming_preaggregation_mode() ==
                       TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
                RETURN_IF_ERROR(state->check_mem_limit("AggrNode"));
                SCOPED_TIMER(_aggregator->agg_compute_timer());
                TRY_CATCH_ALLOC_SCOPE_START()

                _aggregator->build_hash_set(input_chunk_size);

                COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

                _aggregator->try_convert_to_two_level_set();

                TRY_CATCH_ALLOC_SCOPE_END()
                continue;
            } else {
                // TODO: calc the real capacity of hashtable, will add one interface in the class of habletable
                size_t real_capacity =
                        _aggregator->hash_set_variant().capacity() - _aggregator->hash_set_variant().capacity() / 8;
                size_t remain_size = real_capacity - _aggregator->hash_set_variant().size();
                bool ht_needs_expansion = remain_size < input_chunk_size;
                if (!ht_needs_expansion ||
                    _aggregator->should_expand_preagg_hash_tables(_children[0]->rows_returned(), input_chunk_size,
                                                                  _aggregator->mem_pool()->total_allocated_bytes(),
                                                                  _aggregator->hash_set_variant().size())) {
                    RETURN_IF_ERROR(state->check_mem_limit("AggrNode"));
                    // hash table is not full or allow expand the hash table according reduction rate
                    SCOPED_TIMER(_aggregator->agg_compute_timer());

                    TRY_CATCH_ALLOC_SCOPE_START()
                    _aggregator->build_hash_set(input_chunk_size);

                    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

                    _aggregator->try_convert_to_two_level_set();
                    TRY_CATCH_ALLOC_SCOPE_END()

                    continue;
                } else {
                    {
                        SCOPED_TIMER(_aggregator->agg_compute_timer());
                        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set_with_selection(input_chunk_size));
                    }

                    {
                        SCOPED_TIMER(_aggregator->streaming_timer());
                        size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection());
                        if (zero_count == 0) {
                            RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(input_chunk.get(), chunk));
                        } else if (zero_count != _aggregator->streaming_selection().size()) {
                            RETURN_IF_ERROR(
                                    _aggregator->output_chunk_by_streaming_with_selection(input_chunk.get(), chunk));
                        }
                    }

                    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
                    if ((*chunk) != nullptr && (*chunk)->num_rows() > 0) {
                        break;
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    eval_join_runtime_filters(chunk->get());

    if (_child_eos) {
        if (!_aggregator->is_ht_eos() && _aggregator->hash_set_variant().size() > 0) {
            _output_chunk_from_hash_set(chunk);
            *eos = false;
            _aggregator->process_limit(chunk);

            DCHECK_CHUNK(*chunk);
            return Status::OK();
        } else if (_aggregator->hash_set_variant().size() == 0) {
            COUNTER_SET(_aggregator->rows_returned_counter(), _aggregator->num_rows_returned());
            COUNTER_SET(_aggregator->pass_through_row_count(), _aggregator->num_pass_through_rows());
            *eos = true;
            return Status::OK();
        }
    }

    _aggregator->process_limit(chunk);
    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void DistinctStreamingNode::_output_chunk_from_hash_set(ChunkPtr* chunk) {
    if (!_aggregator->it_hash().has_value()) {
        _aggregator->hash_set_variant().visit(
                [&](auto& hash_set_with_key) { _aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    }

    _aggregator->convert_hash_set_to_chunk(runtime_state()->chunk_size(), chunk);
}

pipeline::OpFactories DistinctStreamingNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);

    auto should_cache = context->should_interpolate_cache_operator(ops_with_sink[0], id());
    auto* upstream_source_op = context->source_operator(ops_with_sink);
    auto operators_generator = [this, should_cache, upstream_source_op, context](bool post_cache) {
        // shared by sink operator factory and source operator factory
        AggregatorFactoryPtr aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);
        AggrMode aggr_mode =
                should_cache ? (post_cache ? AM_STREAMING_POST_CACHE : AM_STREAMING_PRE_CACHE) : AM_DEFAULT;
        aggregator_factory->set_aggr_mode(aggr_mode);
        auto sink_operator = std::make_shared<AggregateDistinctStreamingSinkOperatorFactory>(
                context->next_operator_id(), id(), aggregator_factory);
        auto source_operator = std::make_shared<AggregateDistinctStreamingSourceOperatorFactory>(
                context->next_operator_id(), id(), aggregator_factory);
        context->inherit_upstream_source_properties(source_operator.get(), upstream_source_op);
        return std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>(sink_operator, source_operator);
    };
    auto [agg_sink_op, agg_source_op] = operators_generator(true);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(agg_sink_op.get(), context, rc_rf_probe_collector);
    ops_with_sink.emplace_back(std::move(agg_sink_op));

    OpFactories ops_with_source;
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(agg_source_op.get(), context, rc_rf_probe_collector);
    ops_with_source.push_back(std::move(agg_source_op));

    if (should_cache) {
        ops_with_source = context->interpolate_cache_operator(ops_with_sink, ops_with_source, operators_generator);
    }
    context->add_pipeline(ops_with_sink);
    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return ops_with_source;
}

} // namespace starrocks

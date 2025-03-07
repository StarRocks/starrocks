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

#include "exec/topn_node.h"

#include <memory>
#include <type_traits>

#include "exec/chunks_sorter.h"
#include "exec/chunks_sorter_full_sort.h"
#include "exec/chunks_sorter_heap_sort.h"
#include "exec/chunks_sorter_topn.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/sort/local_merge_sort_source_operator.h"
#include "exec/pipeline/sort/local_parallel_merge_sort_source_operator.h"
#include "exec/pipeline/sort/local_partition_topn_context.h"
#include "exec/pipeline/sort/local_partition_topn_sink.h"
#include "exec/pipeline/sort/local_partition_topn_source.h"
#include "exec/pipeline/sort/partition_sort_sink_operator.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/pipeline/sort/spillable_partition_sort_sink_operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/pipeline/spill_process_channel.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"

namespace starrocks {

TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _tnode(tnode) {
    _sort_keys = tnode.sort_node.__isset.sql_sort_keys ? tnode.sort_node.sql_sort_keys : "NONE";
    _offset = tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0;
    _materialized_tuple_desc = nullptr;
    _sort_timer = nullptr;
}

TopNNode::~TopNNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status TopNNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.sort_node.sort_info, _pool, state));
    // create analytic_partition_exprs for pipeline execution engine to speedup AnalyticNode evaluation.
    if (tnode.sort_node.__isset.analytic_partition_exprs) {
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.sort_node.analytic_partition_exprs,
                                                &_analytic_partition_exprs, state));
        RETURN_IF_ERROR(Expr::prepare(_analytic_partition_exprs, runtime_state()));
        RETURN_IF_ERROR(Expr::open(_analytic_partition_exprs, runtime_state()));
        for (auto& expr : _analytic_partition_exprs) {
            auto& type_desc = expr->root()->type();
            if (!type_desc.support_groupby()) {
                return Status::NotSupported(
                        fmt::format("partition by type {} is not supported", type_desc.debug_string()));
            }
        }
    }

    // create analytic_partition_exprs for pipeline execution engine to speedup AnalyticNode evaluation.
    if (tnode.sort_node.__isset.partition_exprs) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(_pool, tnode.sort_node.partition_exprs, &_local_partition_exprs, state));
    }

    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _is_null_first = tnode.sort_node.sort_info.nulls_first;
    bool has_outer_join_child = tnode.sort_node.__isset.has_outer_join_child && tnode.sort_node.has_outer_join_child;
    if (!_sort_exec_exprs.sort_tuple_slot_expr_ctxs().empty()) {
        size_t size = _sort_exec_exprs.sort_tuple_slot_expr_ctxs().size();
        _order_by_types.resize(size);
        for (size_t i = 0; i < size; ++i) {
            const TExprNode& expr_node = tnode.sort_node.sort_info.sort_tuple_slot_exprs[i].nodes[0];
            _order_by_types[i].type_desc = TypeDescriptor::from_thrift(expr_node.type);
            _order_by_types[i].is_nullable = expr_node.is_nullable || has_outer_join_child;
        }
    }
    if (tnode.sort_node.__isset.build_runtime_filters) {
        for (const auto& desc : tnode.sort_node.build_runtime_filters) {
            auto* rf_desc = _pool->add(new RuntimeFilterBuildDescriptor());
            RETURN_IF_ERROR(rf_desc->init(_pool, desc, state));
            _build_runtime_filters.emplace_back(rf_desc);
        }
    }

    DCHECK_EQ(_conjuncts.size(), 0) << "TopNNode should never have predicates to evaluate.";
    _abort_on_default_limit_exceeded = tnode.sort_node.is_default_limit;
    _materialized_tuple_desc = _row_descriptor.tuple_descriptors()[0];
    DCHECK(_materialized_tuple_desc != nullptr);

    bool all_slot_ref = true;
    std::unordered_set<SlotId> early_materialized_slots;
    for (ExprContext* expr_ctx : _sort_exec_exprs.lhs_ordering_expr_ctxs()) {
        auto* expr = expr_ctx->root();
        if (expr->is_slotref()) {
            early_materialized_slots.insert(down_cast<ColumnRef*>(expr)->slot_id());
        } else {
            all_slot_ref = false;
        }
    }

    // In lazy materialization of cascading merging, an extra ordinal column of type FixedLengthColumn<uint32_t>(in
    // very rare cases, data skew is drastic, maybe FixedLengthColumn<uint64_t>) is added to participate permutation
    // in cascading merging phase. so only if the cost of permuting ordinal column is less that the permuting
    // no-group-by output columns, then cascading can benefit from lazy materialization. for an example:
    // select c0, c1, c2 from t group by c1,c2
    // if c0 is bigint, the byte width of the element of c0 is 8, permuting c0 costs more than permuting ordinal column,
    // but if c0 is tinyint, the byte width of the element of c0 is 1, obviously permuting ordinal column costs more.
    // The permutation cost is proportion of the total size of bytes of the elements of non-group-by columns.
    int materialized_cost = 0;
    for (auto* slot : _materialized_tuple_desc->slots()) {
        if (early_materialized_slots.count(slot->id())) {
            continue;
        }
        // nullable column always contribute 1 byte to materialized cost.
        materialized_cost += slot->is_nullable();
        if (slot->type().is_string_type()) {
            // Slice is 16 bytes
            materialized_cost += 16;
        } else {
            materialized_cost += std::max<int>(1, slot->type().get_slot_size());
        }
    }
    // The ordinal column is almost always FixedLengthColumn<uint32_t>, so cost is sizeof(uint32_t) + 4(margin)
    static constexpr auto ORDINAL_SORT_COST = 8;
    auto late_materialization = _tnode.sort_node.__isset.late_materialization && _tnode.sort_node.late_materialization;
    if (late_materialization && all_slot_ref && materialized_cost > ORDINAL_SORT_COST) {
        _early_materialized_slots.insert(_early_materialized_slots.begin(), early_materialized_slots.begin(),
                                         early_materialized_slots.end());
    }

    _runtime_profile->add_info_string("SortKeys", _sort_keys);
    _runtime_profile->add_info_string("SortType", tnode.sort_node.use_top_n ? "TopN" : "All");
    return Status::OK();
}

Status TopNNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor));

    _abort_on_default_limit_exceeded = _abort_on_default_limit_exceeded && state->abort_on_default_limit_exceeded();

    _sort_timer = ADD_TIMER(runtime_profile(), "ChunksSorter");
    return Status::OK();
}

Status TopNNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Top n, before open."));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));

    // sort all input chunk in turn, keep top N rows.
    ExecNode* data_source = child(0);
    RETURN_IF_ERROR(data_source->open(state));
    Status status = _consume_chunks(state, data_source);
    data_source->close(state);

    _mem_tracker->set(_chunks_sorter->mem_usage());

    return status;
}

Status TopNNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Top n, before moving result to chunk."));

    if (_chunks_sorter == nullptr) {
        *eos = true;
        *chunk = nullptr;
        return Status::OK();
    }

    {
        SCOPED_TIMER(_sort_timer);
        RETURN_IF_ERROR(_chunks_sorter->get_next(chunk, eos));
    }
    if (*eos) {
        _chunks_sorter = nullptr;
    } else {
        _num_rows_returned += (*chunk)->num_rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    if (_limit > 0 && reached_limit()) {
        _chunks_sorter = nullptr;
    }

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void TopNNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    _chunks_sorter = nullptr;

    _sort_exec_exprs.close(state);
    ExecNode::close(state);
}

Status TopNNode::_consume_chunks(RuntimeState* state, ExecNode* child) {
    ScopedTimer<MonotonicStopWatch> timer(_sort_timer);
    if (_limit > 0) {
        // ChunksSorterHeapSort has higher performance when sorting fewer elements,
        // after testing we think 1024 is a good threshold
        if (_limit <= ChunksSorter::USE_HEAP_SORTER_LIMIT_SZ) {
            _chunks_sorter = std::make_unique<ChunksSorterHeapSort>(state, &(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
                                                                    &_is_asc_order, &_is_null_first, _sort_keys,
                                                                    _offset, _limit);
        } else {
            _chunks_sorter = std::make_unique<ChunksSorterTopn>(
                    state, &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first, _sort_keys,
                    _offset, _limit, TTopNType::ROW_NUMBER, ChunksSorterTopn::kDefaultMaxBufferRows,
                    ChunksSorterTopn::kDefaultMaxBufferBytes, ChunksSorterTopn::max_buffered_chunks(_limit));
        }

    } else {
        _chunks_sorter = std::make_unique<ChunksSorterFullSort>(state, &(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
                                                                &_is_asc_order, &_is_null_first, _sort_keys, 1024000,
                                                                16 * 1024 * 1024, _early_materialized_slots);
    }

    bool eos = false;
    _chunks_sorter->setup_runtime(state, runtime_profile(), runtime_state()->instance_mem_tracker());
    do {
        RETURN_IF_CANCELLED(state);
        ChunkPtr chunk;
        timer.stop();
        RETURN_IF_ERROR(child->get_next(state, &chunk, &eos));
        if (_abort_on_default_limit_exceeded && _limit > 0 && child->rows_returned() > _limit) {
            return Status::InternalError("DEFAULT_ORDER_BY_LIMIT has been exceeded.");
        }
        timer.start();
        if (chunk != nullptr && chunk->num_rows() > 0) {
            auto materialize_chunk = ChunksSorter::materialize_chunk_before_sort(chunk.get(), _materialized_tuple_desc,
                                                                                 _sort_exec_exprs, _order_by_types);
            RETURN_IF_ERROR(materialize_chunk);
            TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_chunks_sorter->update(state, materialize_chunk.value())));
        }
    } while (!eos);

    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_chunks_sorter->done(state)));
    return Status::OK();
}

template <class ContextFactory, class SinkFactory, class SourceFactory>
std::vector<std::shared_ptr<pipeline::OperatorFactory>> TopNNode::_decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context, bool is_partition_topn, bool is_partition_skewed, bool need_merge,
        bool enable_parallel_merge) {
    using namespace pipeline;

    OpFactories ops_sink_with_sort = _children[0]->decompose_to_pipeline(context);
    ops_sink_with_sort = context->maybe_interpolate_grouped_exchange(_id, ops_sink_with_sort);

    int64_t partition_limit = _limit;

    if (is_partition_topn) {
        partition_limit = _tnode.sort_node.partition_limit;
        if (_tnode.sort_node.__isset.pre_agg_insert_local_shuffle && _tnode.sort_node.pre_agg_insert_local_shuffle) {
            ops_sink_with_sort = context->maybe_interpolate_local_shuffle_exchange(
                    runtime_state(), id(), ops_sink_with_sort, _local_partition_exprs);
        }
    } else if (need_merge) {
        if (enable_parallel_merge) {
            ops_sink_with_sort = context->maybe_interpolate_local_passthrough_exchange(
                    runtime_state(), id(), ops_sink_with_sort, context->degree_of_parallelism(), is_partition_skewed);
        }
    } else {
        // prepend local shuffle to PartitionSortSinkOperator
        ops_sink_with_sort = context->maybe_interpolate_local_shuffle_exchange(
                runtime_state(), id(), ops_sink_with_sort, _analytic_partition_exprs);
    }

    // define a runtime filter holder
    context->fragment_context()->runtime_filter_hub()->add_holder(_id);

    auto degree_of_parallelism = context->source_operator(ops_sink_with_sort)->degree_of_parallelism();

    // spill components
    // TODO: avoid create spill channel when when disable spill

    auto workgroup = context->fragment_context()->workgroup();
    auto spill_channel_factory = std::make_shared<SpillProcessChannelFactory>(degree_of_parallelism);

    // spill process operator
    if (runtime_state()->enable_spill() && runtime_state()->enable_sort_spill() && _limit < 0 && !is_partition_topn) {
        context->interpolate_spill_process(id(), spill_channel_factory, degree_of_parallelism);
    }

    bool has_outer_join_child = _tnode.sort_node.__isset.has_outer_join_child && _tnode.sort_node.has_outer_join_child;

    // create context factory
    bool enable_pre_agg = _tnode.sort_node.__isset.pre_agg_exprs && (!_tnode.sort_node.pre_agg_exprs.empty());
    auto context_factory = std::make_shared<ContextFactory>(
            runtime_state(), _tnode.sort_node.topn_type, need_merge, _sort_exec_exprs.lhs_ordering_expr_ctxs(),
            _is_asc_order, _is_null_first, _tnode.sort_node.partition_exprs, enable_pre_agg,
            _tnode.sort_node.pre_agg_exprs, _tnode.sort_node.pre_agg_output_slot_id, _offset, partition_limit,
            _sort_keys, _order_by_types, has_outer_join_child, _build_runtime_filters);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));

    OperatorFactoryPtr sink_operator;

    int64_t max_buffered_rows = ChunksSorterFullSort::kDefaultMaxBufferRows;
    int64_t max_buffered_bytes = ChunksSorterFullSort::kDefaultMaxBufferBytes;
    if (_tnode.sort_node.__isset.max_buffered_bytes) {
        max_buffered_bytes = _tnode.sort_node.max_buffered_bytes;
    }
    if (_tnode.sort_node.__isset.max_buffered_rows) {
        max_buffered_rows = _tnode.sort_node.max_buffered_rows;
    }

    sink_operator = std::make_shared<SinkFactory>(
            context->next_operator_id(), id(), context_factory, _sort_exec_exprs, _is_asc_order, _is_null_first,
            _sort_keys, _offset, _limit, _tnode.sort_node.topn_type, _order_by_types, _materialized_tuple_desc,
            child(0)->row_desc(), _row_descriptor, _analytic_partition_exprs, max_buffered_rows, max_buffered_bytes,
            _early_materialized_slots, spill_channel_factory);

    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(sink_operator.get(), context, rc_rf_probe_collector);

    OpFactories operators_source_with_sort;
    SourceOperatorFactoryPtr source_operator;

    source_operator = std::make_shared<SourceFactory>(context->next_operator_id(), id(), context_factory);
    if constexpr (std::is_same_v<LocalParallelMergeSortSourceOperatorFactory, SourceFactory>) {
        down_cast<LocalParallelMergeSortSourceOperatorFactory*>(source_operator.get())
                ->set_tuple_desc(_materialized_tuple_desc);
        down_cast<LocalParallelMergeSortSourceOperatorFactory*>(source_operator.get())->set_is_gathered(need_merge);
        if (_tnode.sort_node.__isset.parallel_merge_late_materialize_mode) {
            down_cast<LocalParallelMergeSortSourceOperatorFactory*>(source_operator.get())
                    ->set_materialized_mode(_tnode.sort_node.parallel_merge_late_materialize_mode);
        }
    }

    ops_sink_with_sort.emplace_back(std::move(sink_operator));
    context->add_pipeline(ops_sink_with_sort);

    auto* upstream_source_op = context->source_operator(ops_sink_with_sort);
    context->inherit_upstream_source_properties(source_operator.get(), upstream_source_op);
    if (need_merge && !is_partition_topn) {
        if (!enable_parallel_merge) {
            source_operator->set_degree_of_parallelism(1);
        }
        source_operator->set_could_local_shuffle(true);
    }
    operators_source_with_sort.emplace_back(std::move(source_operator));
    if (need_merge && !is_partition_topn) {
        if (enable_parallel_merge) {
            // This particular source will be executed in a concurrent way, and finally we need to gather them into one
            // stream to satisfied the ordering property
            operators_source_with_sort = context->maybe_interpolate_local_passthrough_exchange(
                    runtime_state(), id(), operators_source_with_sort);
        }
    }

    return operators_source_with_sort;
}

pipeline::OpFactories TopNNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    // is_partition_topn is needed for a special optimization on the case of ranking window function with limit or predicate(rk < 100)
    bool is_partition_topn = _tnode.sort_node.__isset.partition_exprs && !_tnode.sort_node.partition_exprs.empty();
    bool is_rank_topn_type = _tnode.sort_node.__isset.topn_type && _tnode.sort_node.topn_type != TTopNType::ROW_NUMBER;
    // need_merge = true means gather is needed for multiple streams of data
    // need_merge = false means gather is no longer needed
    bool is_partition_skewed =
            _tnode.sort_node.__isset.analytic_partition_skewed && _tnode.sort_node.analytic_partition_skewed;
    bool need_merge = _analytic_partition_exprs.empty() || is_partition_skewed;
    bool enable_parallel_merge = _tnode.sort_node.__isset.enable_parallel_merge &&
                                 _tnode.sort_node.enable_parallel_merge &&
                                 !_sort_exec_exprs.lhs_ordering_expr_ctxs().empty();

    OpFactories operators_source_with_sort;

    if (is_partition_topn) {
        operators_source_with_sort =
                _decompose_to_pipeline<LocalPartitionTopnContextFactory, LocalPartitionTopnSinkOperatorFactory,
                                       LocalPartitionTopnSourceOperatorFactory>(
                        context, is_partition_topn, is_partition_skewed, need_merge, enable_parallel_merge);
    } else {
        if (runtime_state()->enable_spill() && runtime_state()->enable_sort_spill() && _limit < 0) {
            if (enable_parallel_merge) {
                operators_source_with_sort =
                        _decompose_to_pipeline<SortContextFactory, SpillablePartitionSortSinkOperatorFactory,
                                               LocalParallelMergeSortSourceOperatorFactory>(
                                context, is_partition_topn, is_partition_skewed, need_merge, enable_parallel_merge);
            } else {
                operators_source_with_sort =
                        _decompose_to_pipeline<SortContextFactory, SpillablePartitionSortSinkOperatorFactory,
                                               LocalMergeSortSourceOperatorFactory>(
                                context, is_partition_topn, is_partition_skewed, need_merge, enable_parallel_merge);
            }
        } else {
            if (enable_parallel_merge) {
                operators_source_with_sort =
                        _decompose_to_pipeline<SortContextFactory, PartitionSortSinkOperatorFactory,
                                               LocalParallelMergeSortSourceOperatorFactory>(
                                context, is_partition_topn, is_partition_skewed, need_merge, enable_parallel_merge);
            } else {
                operators_source_with_sort =
                        _decompose_to_pipeline<SortContextFactory, PartitionSortSinkOperatorFactory,
                                               LocalMergeSortSourceOperatorFactory>(
                                context, is_partition_topn, is_partition_skewed, need_merge, enable_parallel_merge);
            }
        }
    }

    // Do not add LimitOperator if topn has partition columns or its type is not ROW_NUMBER
    if (!is_partition_topn && !is_rank_topn_type && limit() != -1) {
        operators_source_with_sort.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    return operators_source_with_sort;
}

} // namespace starrocks

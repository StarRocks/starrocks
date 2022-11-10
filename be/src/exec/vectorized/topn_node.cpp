// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/topn_node.h"

#include <memory>

#include "column/column_helper.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/sort/local_merge_sort_source_operator.h"
#include "exec/pipeline/sort/partition_sort_sink_operator.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/vectorized/chunk_sorter_heapsorter.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"

namespace starrocks::vectorized {

TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
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

    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    // create analytic_partition_exprs for pipeline execution engine to speedup AnalyticNode evaluation.
    if (tnode.sort_node.__isset.analytic_partition_exprs) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(_pool, tnode.sort_node.analytic_partition_exprs, &_analytic_partition_exprs));
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

    DCHECK_EQ(_conjuncts.size(), 0) << "TopNNode should never have predicates to evaluate.";
    _abort_on_default_limit_exceeded = tnode.sort_node.is_default_limit;
    _materialized_tuple_desc = _row_descriptor.tuple_descriptors()[0];
    DCHECK(_materialized_tuple_desc != nullptr);

    if (tnode.sort_node.__isset.sql_sort_keys) {
        _runtime_profile->add_info_string("SortKeys", tnode.sort_node.sql_sort_keys);
    }
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
        _chunks_sorter->get_next(chunk, eos);
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

Status TopNNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _chunks_sorter = nullptr;

    _sort_exec_exprs.close(state);
    return ExecNode::close(state);
}

Status TopNNode::_consume_chunks(RuntimeState* state, ExecNode* child) {
    static const uint SIZE_OF_CHUNK_FOR_TOPN = 3000;
    static const uint SIZE_OF_CHUNK_FOR_FULL_SORT = 5000;

    ScopedTimer<MonotonicStopWatch> timer(_sort_timer);
    if (_limit > 0) {
        // HeapChunkSorter has higher performance when sorting fewer elements,
        // after testing we think 1024 is a good threshold
        // if (_limit <= ChunksSorter::USE_HEAP_SORTER_LIMIT_SZ) {
        //     _chunks_sorter = std::make_unique<HeapChunkSorter>(state, &(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
        //                                                        &_is_asc_order, &_is_null_first, _offset, _limit,
        //                                                        SIZE_OF_CHUNK_FOR_TOPN);
        // } else {
        _chunks_sorter =
                std::make_unique<ChunksSorterTopn>(state, &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order,
                                                   &_is_null_first, _offset, _limit, SIZE_OF_CHUNK_FOR_TOPN);
        // }

    } else {
        _chunks_sorter =
                std::make_unique<ChunksSorterFullSort>(state, &(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
                                                       &_is_asc_order, &_is_null_first, SIZE_OF_CHUNK_FOR_FULL_SORT);
    }

    bool eos = false;
    _chunks_sorter->setup_runtime(runtime_profile(), "ChunksSorter");
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
            ChunkPtr materialize_chunk = ChunksSorter::materialize_chunk_before_sort(
                    chunk.get(), _materialized_tuple_desc, _sort_exec_exprs, _order_by_types);
            TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_chunks_sorter->update(state, materialize_chunk)));
        }
    } while (!eos);

    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_chunks_sorter->done(state)));
    return Status::OK();
}

pipeline::OpFactories TopNNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories operators_sink_with_sort = _children[0]->decompose_to_pipeline(context);
    bool is_merging = _analytic_partition_exprs.empty();

    if (!is_merging) {
        // prepend local shuffle to PartitionSortSinkOperator
        operators_sink_with_sort = context->maybe_interpolate_local_shuffle_exchange(
                runtime_state(), operators_sink_with_sort, _analytic_partition_exprs);
    }

    auto degree_of_parallelism =
            down_cast<SourceOperatorFactory*>(operators_sink_with_sort[0].get())->degree_of_parallelism();
    auto sort_context_factory = std::make_shared<SortContextFactory>(
            runtime_state(), is_merging, _limit, degree_of_parallelism, _is_asc_order, _is_null_first);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    auto partition_sort_sink_operator = std::make_shared<PartitionSortSinkOperatorFactory>(
            context->next_operator_id(), id(), sort_context_factory, _sort_exec_exprs, _is_asc_order, _is_null_first,
            _offset, _limit, _order_by_types, _materialized_tuple_desc, child(0)->row_desc(), _row_descriptor,
            _analytic_partition_exprs);
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(partition_sort_sink_operator.get(), context, rc_rf_probe_collector);

    OpFactories operators_source_with_sort;
    auto local_merge_sort_source_operator = std::make_shared<LocalMergeSortSourceOperatorFactory>(
            context->next_operator_id(), id(), sort_context_factory);
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(local_merge_sort_source_operator.get(), context, rc_rf_probe_collector);

    operators_sink_with_sort.emplace_back(std::move(partition_sort_sink_operator));
    context->add_pipeline(operators_sink_with_sort);
    if (is_merging) {
        // local_merge_sort_source_operator's instance count must be 1
        local_merge_sort_source_operator->set_degree_of_parallelism(1);
    } else {
        // Each PartitionSortSinkOperator has an independent LocalMergeSortSinkOperator respectively
        local_merge_sort_source_operator->set_degree_of_parallelism(degree_of_parallelism);
    }
    operators_source_with_sort.emplace_back(std::move(local_merge_sort_source_operator));

    return operators_source_with_sort;
}

} // namespace starrocks::vectorized

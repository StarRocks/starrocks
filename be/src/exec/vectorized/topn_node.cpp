// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/topn_node.h"

#include <memory>

#include "column/column_helper.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/sort/sort_sink_operator.h"
#include "exec/pipeline/sort/sort_source_operator.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {

TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
    _offset = tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0;
    _materialized_tuple_desc = nullptr;
    _sort_timer = nullptr;
}

TopNNode::~TopNNode() = default;

Status TopNNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
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

Status TopNNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("get_next for RowBatch is not supported");
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
        _chunks_sorter =
                std::make_unique<ChunksSorterTopn>(&(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order,
                                                   &_is_null_first, _offset, _limit, SIZE_OF_CHUNK_FOR_TOPN);
    } else {
        _chunks_sorter =
                std::make_unique<ChunksSorterFullSort>(&(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order,
                                                       &_is_null_first, SIZE_OF_CHUNK_FOR_FULL_SORT);
    }

    bool eos = false;
    _chunks_sorter->setup_runtime(runtime_profile(), "ChunksSorter");
    do {
        RETURN_IF_ERROR(state->check_mem_limit("Sort"));
        RETURN_IF_CANCELLED(state);
        ChunkPtr chunk;
        timer.stop();
        RETURN_IF_ERROR(child->get_next(state, &chunk, &eos));
        if (_abort_on_default_limit_exceeded && _limit > 0 && child->rows_returned() > _limit) {
            return Status::InternalError("DEFAULT_ORDER_BY_LIMIT has been exceeded.");
        }
        timer.start();
        if (chunk != nullptr && chunk->num_rows() > 0) {
            ChunkPtr materialize_chunk = _materialize_chunk_before_sort(chunk.get());
            RETURN_IF_ERROR(_chunks_sorter->update(state, materialize_chunk));
        }
    } while (!eos);
    RETURN_IF_ERROR(_chunks_sorter->done(state));
    return Status::OK();
}

ChunkPtr TopNNode::_materialize_chunk_before_sort(Chunk* chunk) {
    ChunkPtr materialize_chunk = std::make_shared<Chunk>();

    // materialize all sorting columns: replace old columns with evaluated columns
    const size_t row_num = chunk->num_rows();
    const auto& slots_in_row_descriptor = _materialized_tuple_desc->slots();
    const auto& slots_in_sort_exprs = _sort_exec_exprs.sort_tuple_slot_expr_ctxs();

    DCHECK_EQ(slots_in_row_descriptor.size(), slots_in_sort_exprs.size());

    for (size_t i = 0; i < slots_in_sort_exprs.size(); ++i) {
        ExprContext* expr_ctx = slots_in_sort_exprs[i];
        ColumnPtr col = expr_ctx->evaluate(chunk);
        if (col->is_constant()) {
            if (col->is_nullable()) {
                // Constant null column doesn't have original column data type information,
                // so replace it by a nullable column of original data type filled with all NULLs.
                ColumnPtr new_col = ColumnHelper::create_column(_order_by_types[i].type_desc, true);
                new_col->append_nulls(row_num);
                materialize_chunk->append_column(new_col, slots_in_row_descriptor[i]->id());
            } else {
                // Case 1: an expression may generate a constant column which will be reused by
                // another call of evaluate(). We clone its data column to resize it as same as
                // the size of the chunk, so that Chunk::num_rows() can return the right number
                // if this ConstColumn is the first column of the chunk.
                // Case 2: an expression may generate a constant column for one Chunk, but a
                // non-constant one for another Chunk, we replace them all by non-constant columns.
                auto* const_col = down_cast<ConstColumn*>(col.get());
                const auto& data_col = const_col->data_column();
                auto new_col = data_col->clone_empty();
                new_col->append(*data_col, 0, 1);
                new_col->assign(row_num, 0);
                if (_order_by_types[i].is_nullable) {
                    ColumnPtr null_col =
                            NullableColumn::create(ColumnPtr(new_col.release()), NullColumn::create(row_num, 0));
                    materialize_chunk->append_column(null_col, slots_in_row_descriptor[i]->id());
                } else {
                    materialize_chunk->append_column(ColumnPtr(new_col.release()), slots_in_row_descriptor[i]->id());
                }
            }
        } else {
            // When get a non-null column, but it should be nullable, we wrap it with a NullableColumn.
            if (!col->is_nullable() && _order_by_types[i].is_nullable) {
                col = NullableColumn::create(col, NullColumn::create(col->size(), 0));
            }
            materialize_chunk->append_column(col, slots_in_row_descriptor[i]->id());
        }
    }

    return materialize_chunk;
}

pipeline::OpFactories TopNNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    // step 0: construct pipeline end with sort operator.
    // get operators before sort operator
    OpFactories operators_sink_with_sort = _children[0]->decompose_to_pipeline(context);
    operators_sink_with_sort = context->maybe_interpolate_local_passthrough_exchange(operators_sink_with_sort);

    static const uint SIZE_OF_CHUNK_FOR_TOPN = 3000;
    static const uint SIZE_OF_CHUNK_FOR_FULL_SORT = 5000;
    std::shared_ptr<ChunksSorter> chunks_sorter;
    if (_limit > 0) {
        chunks_sorter = std::make_unique<vectorized::ChunksSorterTopn>(&(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
                                                                       &_is_asc_order, &_is_null_first, _offset, _limit,
                                                                       SIZE_OF_CHUNK_FOR_TOPN);
    } else {
        chunks_sorter = std::make_unique<vectorized::ChunksSorterFullSort>(&(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
                                                                           &_is_asc_order, &_is_null_first,
                                                                           SIZE_OF_CHUNK_FOR_FULL_SORT);
    }

    // add SortSinkOperator to this pipeline
    auto sort_sink_operator = std::make_shared<SortSinkOperatorFactory>(
            context->next_operator_id(), id(), chunks_sorter, _sort_exec_exprs, _order_by_types,
            _materialized_tuple_desc, child(0)->row_desc(), _row_descriptor);
    operators_sink_with_sort.emplace_back(std::move(sort_sink_operator));
    context->add_pipeline(operators_sink_with_sort);

    OpFactories operators_source_with_sort;
    auto sort_source_operator =
            std::make_shared<SortSourceOperatorFactory>(context->next_operator_id(), id(), std::move(chunks_sorter));
    // SourceSourceOperator's instance count must be 1
    sort_source_operator->set_degree_of_parallelism(1);
    operators_source_with_sort.emplace_back(std::move(sort_source_operator));

    // return to the following pipeline
    return operators_source_with_sort;
}

} // namespace starrocks::vectorized

// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/except_node.h"

#include <memory>

#include "column/column_helper.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

ExceptNode::ExceptNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _tuple_id(tnode.except_node.tuple_id), _tuple_desc(nullptr) {}

Status ExceptNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    DCHECK_GE(_children.size(), 2);

    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& result_texpr_lists = tnode.except_node.result_expr_lists;
    for (auto& texprs : result_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, texprs, &ctxs));
        _child_expr_lists.push_back(ctxs);
    }
    return Status::OK();
}

Status ExceptNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    DCHECK(_tuple_desc != nullptr);
    _build_pool = std::make_unique<MemPool>(mem_tracker());

    _build_set_timer = ADD_TIMER(runtime_profile(), "BuildSetTime");
    _erase_duplicate_row_timer = ADD_TIMER(runtime_profile(), "EraseDuplicateRowTime");
    _get_result_timer = ADD_TIMER(runtime_profile(), "GetResultTime");

    for (size_t i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(Expr::prepare(_child_expr_lists[i], state, child(i)->row_desc(), expr_mem_tracker()));
        DCHECK_EQ(_child_expr_lists[i].size(), _tuple_desc->slots().size());
    }

    size_t size_column_type = _tuple_desc->slots().size();
    _types.resize(size_column_type);
    for (int i = 0; i < size_column_type; ++i) {
        _types[i].result_type = _tuple_desc->slots()[i]->type();
        _types[i].is_constant = _child_expr_lists[0][i]->root()->is_constant();
    }

    return Status::OK();
}

// step 1:
// Build hashset(_hash_set) for leftmost _child_expr of child(0).
//
// step 2:
// for every other children of B(1~N),
// erase the rows of child at hashset(_hash_set) througth set deleted of key.
//
// step 3:
// for all undeleted keys in hashset(_hash_set),
// construct columns as chunk as result to parent node.
Status ExceptNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    // open result expr lists.
    for (const vector<ExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(Expr::open(exprs, state));
    }

    // initial build hash table used for remove duplicted
    _hash_set = std::make_unique<HashSerializeSet>();

    ChunkPtr chunk = nullptr;
    RETURN_IF_ERROR(child(0)->open(state));
    bool eos = false;

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->get_next(state, &chunk, &eos));
    if (!eos) {
        ScopedTimer<MonotonicStopWatch> build_timer(_build_set_timer);
        std::vector<ExceptColumnTypes>* types = &_types;
        RETURN_IF_ERROR(_hash_set->build_set(
                state, chunk, _child_expr_lists[0], _build_pool.get(),
                [=](const ColumnPtr& column, int i) -> void { (*types)[i].is_nullable = column->is_nullable(); }));
        while (true) {
            RETURN_IF_CANCELLED(state);
            build_timer.stop();
            RETURN_IF_ERROR(child(0)->get_next(state, &chunk, &eos));
            build_timer.start();
            if (eos || chunk == nullptr) {
                break;
            } else if (chunk->num_rows() == 0) {
                continue;
            } else {
                RETURN_IF_ERROR(_hash_set->build_set(state, chunk, _child_expr_lists[0], _build_pool.get(),
                                                     [](const ColumnPtr& column, int i) -> void {}));
            }
        }
    }

    // if a table is empty, the result must be empty
    if (_hash_set->hash_set->size() == 0) {
        _hash_set_iterator = _hash_set->begin();
        return Status::OK();
    }

    for (int i = 1; i < _children.size(); ++i) {
        RETURN_IF_ERROR(child(i)->open(state));
        eos = false;
        while (true) {
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(child(i)->get_next(state, &chunk, &eos));
            if (eos || chunk == nullptr) {
                break;
            } else if (chunk->num_rows() == 0) {
                continue;
            } else {
                SCOPED_TIMER(_erase_duplicate_row_timer);
                RETURN_IF_ERROR(_hash_set->erase_duplicate_row(state, chunk->num_rows(), chunk, _child_expr_lists[i]));
            }
        }
        // TODO: optimize, when hash set has no values, direct return
    }

    _hash_set_iterator = _hash_set->begin();
    return Status::OK();
}

Status ExceptNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("get_next for row_batch is not supported");
}

Status ExceptNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    *eos = false;

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    int32_t read_index = 0;
    _hash_set->_results.resize(config::vector_chunk_size);
    while (_hash_set_iterator != _hash_set->end() && read_index < config::vector_chunk_size) {
        if (!_hash_set_iterator->deleted) {
            _hash_set->_results[read_index] = _hash_set_iterator->slice;
            ++read_index;
        }
        ++_hash_set_iterator;
    }
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    if (read_index > 0) {
        Columns result_columns(_types.size());
        for (size_t i = 0; i < _types.size(); ++i) {
            result_columns[i] = // default NullableColumn
                    ColumnHelper::create_column(_types[i].result_type, _types[i].is_nullable);
            result_columns[i]->reserve(config::vector_chunk_size);
        }

        {
            SCOPED_TIMER(_get_result_timer);
            _hash_set->insert_keys_to_columns(_hash_set->_results, result_columns, read_index);
        }

        for (size_t i = 0; i < result_columns.size(); i++) {
            result_chunk->append_column(std::move(result_columns[i]), _tuple_desc->slots()[i]->id());
        }

        _num_rows_returned += read_index;
        if (reached_limit()) {
            int64_t num_rows_over = _num_rows_returned - _limit;
            result_chunk->set_num_rows(read_index - num_rows_over);
            COUNTER_SET(_rows_returned_counter, _limit);
        }
        *eos = false;
    } else {
        *eos = true;
    }

    DCHECK_LE(result_chunk->num_rows(), config::vector_chunk_size);
    *chunk = std::move(result_chunk);

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

Status ExceptNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    for (auto& exprs : _child_expr_lists) {
        Expr::close(exprs, state);
    }

    if (_build_pool != nullptr) {
        _build_pool->free_all();
    }

    return ExecNode::close(state);
}

} // namespace starrocks::vectorized

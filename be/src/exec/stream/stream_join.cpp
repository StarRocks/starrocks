// This file is made available under Elastic License 2.0.

#include "exec/stream/stream_join.h"

#include "exec/exec_node.h"
#include "runtime/primitive_type.h"
#include "storage/chunk_helper.h"

namespace starrocks {

// ==========================  StreamJoinNode ==========================
Status StreamJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.stream_join_node);

    _join_op = tnode.stream_join_node.join_op;
    if (tnode.stream_join_node.__isset.sql_join_predicates) {
        _sql_join_conjuncts = tnode.stream_join_node.sql_join_predicates;
    }
    auto& eq_join_conjuncts = tnode.stream_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        ExprContext* left = nullptr;
        ExprContext* right = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.left, &left));
        _probe_expr_ctxs.push_back(left);
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.right, &right));
        _build_expr_ctxs.push_back(right);
    }

    RETURN_IF_ERROR(
            Expr::create_expr_trees(_pool, tnode.stream_join_node.other_join_conjuncts, &_other_join_conjunct_ctxs));

    return Status::OK();
}

pipeline::OpFactories StreamJoinNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    OpFactories left_ops = _children[0]->decompose_to_pipeline(context);
    // OpFactories right_ops = _children[1]->decompose_to_pipeline(context);

    // Left side
    auto left_factory = std::make_shared<StreamJoinOperatorFactory>(
            context->next_operator_id(), id(), _row_descriptor, child(0)->row_desc(), child(1)->row_desc(),
            _sql_join_conjuncts, _join_op, _probe_expr_ctxs, _build_expr_ctxs, _other_join_conjunct_ctxs);
    left_ops.emplace_back(std::move(left_factory));

    return left_ops;
}

// ==========================  StreamJoinOperator Setup ==========================
Status StreamJoinOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void StreamJoinOperator::close(RuntimeState* state) {
    Operator::close(state);
}

// ==========================  StreamJoinOperator ControlFlow ==========================
bool StreamJoinOperator::is_finished() const {
    return _output;
}
bool StreamJoinOperator::has_output() const {
    return !_output;
}
bool StreamJoinOperator::need_input() const {
    return !_output;
}
Status StreamJoinOperator::set_finishing(RuntimeState* state) {
    return Status::OK();
}
Status StreamJoinOperator::set_finished(RuntimeState* state) {
    return Status::OK();
}

// ==========================  StreamJoinOperator DataFlow ==========================
StatusOr<vectorized::ChunkPtr> StreamJoinOperator::pull_chunk(RuntimeState* state) {
    if (_output) {
        return Status::RuntimeError("no output");
    }
    _output = true;

    // TODO: implement the join algorithm
    // mock a default value as output
    ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    for (size_t i = 0; i < _col_types.size(); i++) {
        SlotDescriptor* slot = _col_types[i];
        bool nullable = _col_types[i]->is_nullable();
        ColumnPtr new_col = vectorized::ColumnHelper::create_column(slot->type(), nullable);
        switch (slot->type().type) {
        case TYPE_INT: {
            new_col->append_datum(9527);
            break;
        }
        case TYPE_VARCHAR: {
            new_col->append_datum("starrocks");
            break;
        }
        default: {
            new_col->append_default();
            break;
        }
        }
        chunk->append_column(new_col, slot->id());
    }
    
    for (size_t i = 0; i < chunk->num_rows(); i++) {
        VLOG(2) << "StreamJoin: mock output: " << chunk->debug_row(i);
    }

    return chunk;
}

Status StreamJoinOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return Status::OK();
}

pipeline::OperatorPtr StreamJoinOperatorFactory::create(int32_t dop, int32_t driver_seq) {
    return std::make_shared<StreamJoinOperator>(this, _id, _plan_node_id, driver_seq, _join_op, _col_types,
                                                _probe_column_count, _build_column_count);
}

// ==========================  StreamJoinOperator Factory ==========================
Status StreamJoinOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_probe_eq_exprs, state));
    RETURN_IF_ERROR(Expr::prepare(_build_eq_exprs, state));
    RETURN_IF_ERROR(Expr::prepare(_other_join_conjunct_exprs, state));
    _init_row_desc();

    return Status::OK();
}

void StreamJoinOperatorFactory::close(RuntimeState* state) {
    Expr::close(_probe_eq_exprs, state);
    Expr::close(_build_eq_exprs, state);
    Expr::close(_other_join_conjunct_exprs, state);

    OperatorFactory::close(state);
}

void StreamJoinOperatorFactory::_init_row_desc() {
    for (auto& tuple_desc : _left_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _probe_column_count++;
        }
    }
    for (auto& tuple_desc : _right_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _build_column_count++;
        }
    }
}

} // namespace starrocks
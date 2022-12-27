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

#include "exec/stream/stream_join_node.h"

namespace starrocks {

Status StreamJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.stream_join_node);

    _join_op = tnode.stream_join_node.join_op;
    if (_join_op != TJoinOp::INNER_JOIN) {
        return Status::InternalError("StreamJoin only supports InnerJoin for now.");
    }

    if (tnode.stream_join_node.__isset.sql_join_predicates) {
        _sql_join_conjuncts = tnode.stream_join_node.sql_join_predicates;
    }
    auto& eq_join_conjuncts = tnode.stream_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        ExprContext* left = nullptr;
        ExprContext* right = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.left, &left, state));
        _probe_expr_ctxs.push_back(left);
        auto* left_expr = left->root();
        assert(left_expr->is_slotref());

        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.right, &right, state));
        _build_expr_ctxs.push_back(right);
        // auto* right_expr = right->root();
        // assert(right_expr->is_slotref());
        // _join_key_descs.emplace_back(pipeline::LookupJoinKeyDesc{&left_expr->type(),
        //                                                          down_cast<vectorized::ColumnRef*>(left_expr),
        //                                                          down_cast<vectorized::ColumnRef*>(right_expr)});
    }

    // other conjuncts.
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.stream_join_node.other_join_conjuncts,
                                            &_other_join_conjunct_ctxs, state));

    // TODO: support output columns later.
    if (tnode.stream_join_node.__isset.output_columns) {
        _output_slots.insert(tnode.stream_join_node.output_columns.begin(),
                             tnode.stream_join_node.output_columns.end());
    }

    return Status::OK();
}

pipeline::OpFactories StreamJoinNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    OpFactories left_ops = _children[0]->decompose_to_pipeline(context);
    OpFactories right_ops = _children[1]->decompose_to_pipeline(context);
    _left_row_desc = child(0)->row_desc();
    _right_row_desc = child(1)->row_desc();
    // assert right_op must be index_seek operator.
    assert(right_ops.size() >= 1);

    // if (typeid(*(right_ops[0])) != typeid(pipeline::LookupJoinSeekOperatorFactory)) {
    //     throw std::runtime_error("Only support right table as lookup-join table");
    // }

    // auto* right_source = down_cast<SourceOperatorFactory*>(right_ops[0].get());
    // auto* left_source = down_cast<SourceOperatorFactory*>(left_ops[0].get());

    // pipeline::LookupJoinContextParams params(left_source->degree_of_parallelism(),
    //                                          right_source->degree_of_parallelism(), _join_key_descs, _left_row_desc,
    //                                          _right_row_desc, _other_join_conjunct_ctxs);
    // _lookup_join_context = std::make_shared<pipeline::LookupJoinContext>(std::move(params));
    // // Left side
    // auto left_factory = std::make_shared<pipeline::LookupJoinProbeOperatorFactory>(context->next_operator_id(), id(),
    //                                                                                _lookup_join_context);
    // left_ops.emplace_back(std::move(left_factory));
    // context->add_pipeline(left_ops);

    // // TODO: Convert OlapTableScan to IndexSeek here.
    // auto index_seek_factory = down_cast<pipeline::LookupJoinSeekOperatorFactory*>(right_source);
    // index_seek_factory->with_lookup_join_context(_lookup_join_context);
    return right_ops;
}

} // namespace starrocks

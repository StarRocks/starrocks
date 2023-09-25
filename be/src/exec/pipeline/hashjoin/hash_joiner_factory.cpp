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

#include "exec/pipeline/hashjoin/hash_joiner_factory.h"

#include <unordered_map>
#include <utility>

#include "common/global_types.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"

namespace starrocks::pipeline {

Status HashJoinerFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._other_join_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._other_join_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._conjunct_ctxs, state));

    //TODO: if enable range join optimize
    // check could range optimize
    //

    // now we only support equal join key column is slot ref
    for (auto expr_ctx : _param._build_expr_ctxs) {
        if (!expr_ctx->root()->is_slotref()) {
            return Status::OK();
        }
    }
    //
    if (!state->enable_range_join()) {
        return Status::OK();
    }

    //
    std::unordered_map<SlotId, std::pair<ExprContext*, ExprContext*>> range_exprs;
    for (ExprContext* expr_ctx : _param._other_join_conjunct_ctxs) {
        auto root = expr_ctx->root();
        const auto& children = root->children();

        if (root->node_type() != TExprNodeType::BINARY_PRED) {
            continue;
        }
        if (children.size() != 2) {
            continue;
        }
        if (!root->get_child(0)->is_slotref() && !root->get_child(1)->is_slotref()) {
            continue;
        }
        ColumnRef* ref = root->get_child(0)->get_column_ref();
        auto slot_id = ref->slot_id();
        if (root->op() == TExprOpcode::GE) {
            range_exprs[slot_id].first = expr_ctx;
            continue;
        }
        if (root->op() == TExprOpcode::LE) {
            range_exprs[slot_id].second = expr_ctx;
            continue;
        }
    }
    //
    if (!range_exprs.empty()) {
        for (auto& [slot_id, range_expr] : range_exprs) {
            if (range_expr.first != nullptr && range_expr.second != nullptr) {
                _param._range_conjunct_ctxs.push_back(range_expr.first);
                _param._range_conjunct_ctxs.push_back(range_expr.second);
                break;
            }
        }
    }

    return Status::OK();
}

void HashJoinerFactory::close(RuntimeState* state) {
    Expr::close(_param._conjunct_ctxs, state);
    Expr::close(_param._other_join_conjunct_ctxs, state);
    Expr::close(_param._range_conjunct_ctxs, state);
    Expr::close(_param._probe_expr_ctxs, state);
    Expr::close(_param._build_expr_ctxs, state);
}

HashJoinerPtr HashJoinerFactory::create_builder(int32_t builder_dop, int32_t builder_driver_seq) {
    _builder_dop = builder_dop;
    return _create_joiner(_builder_map, builder_driver_seq);
}

HashJoinerPtr HashJoinerFactory::create_prober(int32_t prober_dop, int32_t prober_driver_seq) {
    _prober_dop = prober_dop;
    DCHECK_GT(_builder_dop, 0);
    DCHECK_GE(_prober_dop, _builder_dop);
    // There is one-to-one correspondence between the prober and builder,
    // so the prober can use builder directly.
    if (_prober_dop == _builder_dop) {
        return get_builder(prober_dop, prober_driver_seq);
    }
    return _create_joiner(_prober_map, prober_driver_seq);
}

HashJoinerPtr HashJoinerFactory::get_builder(int32_t prober_dop, int32_t prober_driver_seq) {
    _prober_dop = prober_dop;
    return _builder_map[prober_driver_seq % _builder_dop];
}

HashJoinerPtr HashJoinerFactory::_create_joiner(HashJoinerMap& joiner_map, int32_t driver_sequence) {
    if (auto it = joiner_map.find(driver_sequence); it != joiner_map.end()) {
        return it->second;
    }

    auto joiner = std::make_shared<HashJoiner>(_param);
    joiner_map.emplace(driver_sequence, joiner);
    return joiner;
}

} // namespace starrocks::pipeline

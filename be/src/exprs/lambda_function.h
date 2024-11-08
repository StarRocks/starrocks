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

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "glog/logging.h"
#include "gutil/casts.h"

namespace starrocks {

// The children of lambda function include 3 parts: lambda expr, lambda arguments and optimal common sub expressions,
// the layout may be:
// lambda_expr, arg_1, arg_2..., sub_expr_slot_id_1, sub_expr_slot_id_2, ... sub_expr_1, sub_expr_2, ...
// taking  (x,y) -> x*2 + x*2 + y as example, 3 parts are listed below:
//      lambda expr      | lambda argument | common sub expression |
// slot[1] + slot[1] + y |    x, y         |   slot[1], x * 2      |
// Note the common sub expressions should be evaluated in order before the lambda expr.

class LambdaFunction final : public Expr {
public:
    LambdaFunction(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new LambdaFunction(*this)); }

    Status prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) override;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    int get_slot_ids(std::vector<SlotId>* slot_ids) const override;

    int get_captured_slot_ids(std::vector<SlotId>* slot_ids) const {
        DCHECK(_is_prepared);
        slot_ids->insert(slot_ids->end(), _captured_slot_ids.begin(), _captured_slot_ids.end());
        return _captured_slot_ids.size();
    }

    int get_lambda_arguments_ids(std::vector<SlotId>* ids) const {
        DCHECK(_is_prepared);
        ids->assign(_arguments_ids.begin(), _arguments_ids.end());
        return _arguments_ids.size();
    }

    const std::vector<SlotId>& get_lambda_arguments_ids() const { return _arguments_ids; }
    const std::vector<SlotId>& get_common_sub_expr_ids() const { return _common_sub_expr_ids; }

    bool is_lambda_function() const override { return true; }
    bool is_lambda_expr_independent() const { return _is_lambda_expr_independent; }

    Expr* get_lambda_expr() const { return _children[0]; }
    std::string debug_string() const override;

    struct ExtractContext {
        std::unordered_set<SlotId> lambda_arguments;
        // slot id of common sub expr inside lambda expr
        std::unordered_set<SlotId> common_sub_expr_ids;
        SlotId next_slot_id;
        std::map<SlotId, Expr*> outer_common_exprs;
    };

    // Extract the outer common expression in lambda expr.
    // Outer common expr is an expression that does not depend on lambda arguments at all. Such expressions can be calculated independently.
    // NOTE: Calling this interface may rewrite Lambda expr, and all outer common expr will be replaced with ColumnRef expr.
    // Functions using lambda expressions can extract common expressions first and calculate them separately, so as to optimize.

    // take `array_map(x->any_match(array_map(x->x < 10, arr1)), arr1)` as an example,
    // `any_match(array_map(x->x<10, arr1))` is an outer common expr. it will create 2 column ref exprs to replace them.
    // 1. slot 1 -> array_map(x->x<10, arr1)
    // 2. slot 2 -> any_match(slot 1, arr1)
    Status extract_outer_common_exprs(RuntimeState* state, ExprContext* expr_ctx, ExtractContext* ctx);

private:
    Status collect_lambda_argument_ids();
    Status collect_capture_slot_ids();
    Status collect_common_sub_exprs();
    Status extract_outer_common_exprs(RuntimeState* state, ExprContext* expr_ctx, Expr* expr, ExtractContext* ctx);

    std::vector<SlotId> _captured_slot_ids;
    std::vector<SlotId> _arguments_ids;
    std::vector<SlotId> _common_sub_expr_ids;
    std::vector<Expr*> _common_sub_expr;

    int _common_sub_expr_num;
    bool _is_prepared = false;
    bool _is_lambda_expr_independent = false;
};
} // namespace starrocks

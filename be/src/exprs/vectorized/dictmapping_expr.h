// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <mutex>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "glog/logging.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {
// DictMappingExpr.
// The original expression will be rewritten as a dictionary mapping function in the global field optimization.
// child(0) was input lowcardinality dictionary column (input was ID type).
// child(1) was origin expr (input was string type).
//
// in Global Dictionary Optimization. The process of constructing a dictionary mapping requires
// a new dictionary to be constructed using the origin global dictionary columns as input columns.
// So BE needs to know the original expressions.
class DictMappingExpr final : public Expr {
public:
    DictMappingExpr(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new DictMappingExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    template <class Rewrite>
    Status rewrite(Rewrite&& rewriter) {
        std::call_once(*_rewrite_once_flag, [&]() {
            DCHECK(dict_func_expr == nullptr);
            auto rewrite_result = rewriter();
            _rewrite_status = rewrite_result.status();
            if (_rewrite_status.ok()) {
                dict_func_expr = rewrite_result.value();
                DCHECK(dict_func_expr != nullptr);
            }
        });
        return _rewrite_status;
    }

    SlotId slot_id() {
        DCHECK(dict_func_expr == nullptr);
        DCHECK_EQ(children().size(), 2);
        return down_cast<const ColumnRef*>(get_child(0))->slot_id();
    }

    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        DCHECK(dict_func_expr == nullptr);
        return get_child(1)->get_slot_ids(slot_ids);
    }

private:
    std::shared_ptr<std::once_flag> _rewrite_once_flag = std::make_shared<std::once_flag>();
    Status _rewrite_status;
    // used for dictionary expression calculation.
    // the input columns are dictionary columns
    Expr* dict_func_expr = nullptr;
};
} // namespace starrocks::vectorized

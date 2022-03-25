// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "glog/logging.h"

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

    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override { return get_child(1)->evaluate(context, ptr); }

    SlotId slot_id() {
        DCHECK_EQ(children().size(), 2);
        return down_cast<const ColumnRef*>(get_child(0))->slot_id();
    }
    int get_slot_ids(std::vector<SlotId>* slot_ids) const override { return get_child(1)->get_slot_ids(slot_ids); }
};
} // namespace starrocks::vectorized
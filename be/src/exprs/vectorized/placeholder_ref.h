// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {
namespace vectorized {
// place holder for function call. representing an input column for function call.
// now it was only used in global dictionary optimization
class PlaceHolderRef final : public Expr {
public:
    PlaceHolderRef(const TExprNode& node);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override;
    bool is_constant() const override { return false; }
    Expr* clone(ObjectPool* pool) const override { return pool->add(new PlaceHolderRef(*this)); }
    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        slot_ids->emplace_back(_column_id);
        return 1;
    }

private:
    SlotId _column_id;
};
} // namespace vectorized
} // namespace starrocks

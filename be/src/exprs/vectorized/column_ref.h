// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {
namespace vectorized {

class ColumnRef final : public Expr {
public:
    ColumnRef(const TExprNode& node);

    ColumnRef(const SlotDescriptor* desc);

    // only used for UT
    ColumnRef(const TypeDescriptor& type, SlotId slot = -1);

    SlotId slot_id() const { return _column_id; }

    TupleId tuple_id() const { return _tuple_id; }

    void set_slot_id(SlotId slot_id) { _column_id = slot_id; }

    void set_tuple_id(TupleId tuple_id) { _tuple_id = tuple_id; }

    // FixMe(kks): currenly, join runtime filter need this method
    // we should find a way remove this method
    bool is_bound(const std::vector<TupleId>& tuple_ids) const override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ColumnRef(*this)); }

    bool is_constant() const override { return false; }

    int get_slot_ids(std::vector<SlotId>* slot_ids) const override;

    std::string debug_string() const override;

    // vector query engine
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override;

    static vectorized::ColumnPtr& get_column(Expr* expr, vectorized::Chunk* chunk);

private:
    // FixMe(kks): currenly, join runtime filter depend on _tuple_id.
    // we should find a way remove _tuple_id from ColumnRef
    SlotId _column_id;

    TupleId _tuple_id = 0; // used for desc this slot from
};

} // namespace vectorized
} // namespace starrocks

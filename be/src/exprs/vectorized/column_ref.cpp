// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/column_ref.h"

namespace starrocks::vectorized {

ColumnRef::ColumnRef(const TExprNode& node)
        : Expr(node, true), _column_id(node.slot_ref.slot_id), _tuple_id(node.slot_ref.tuple_id) {}

ColumnRef::ColumnRef(const SlotDescriptor* desc) : Expr(desc->type(), true), _column_id(desc->id()) {}

int ColumnRef::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    slot_ids->push_back(_column_id);
    return 1;
}

bool ColumnRef::is_bound(const std::vector<TupleId>& tuple_ids) const {
    for (int tuple_id : tuple_ids) {
        if (_tuple_id == tuple_id) {
            return true;
        }
    }

    return false;
}

std::string ColumnRef::debug_string() const {
    std::stringstream out;
    out << "ColumnRef (column_id=" << _column_id << ", type=" << this->type().debug_string() << ")";
    return out.str();
}

ColumnPtr ColumnRef::evaluate(ExprContext* context, Chunk* ptr) {
    return get_column(this, ptr);
}

} // namespace starrocks::vectorized

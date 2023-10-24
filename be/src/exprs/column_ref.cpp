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

#include "exprs/column_ref.h"

#include "column/chunk.h"
#include "exprs/expr.h"

namespace starrocks {

ColumnRef::ColumnRef(const TExprNode& node)
        : Expr(node, true), _column_id(node.slot_ref.slot_id), _tuple_id(node.slot_ref.tuple_id) {}

ColumnRef::ColumnRef(const SlotDescriptor* desc) : Expr(desc->type(), true), _column_id(desc->id()) {}

ColumnRef::ColumnRef(const TypeDescriptor& type, SlotId slot) : Expr(type, true), _column_id(slot) {}

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

StatusOr<ColumnPtr> ColumnRef::evaluate_checked(ExprContext* context, Chunk* ptr) {
    return get_column(this, ptr);
}

ColumnPtr& ColumnRef::get_column(Expr* expr, Chunk* chunk) {
    auto* ref = down_cast<ColumnRef*>(expr);
    ColumnPtr& column = (chunk)->get_column_by_slot_id(ref->slot_id());
    return column;
}

} // namespace starrocks

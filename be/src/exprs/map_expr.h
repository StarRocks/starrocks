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

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {

// map's key column and value column come from expression of children[0], children[1]
class MapExpr final : public Expr {
public:
    explicit MapExpr(const TExprNode& node) : Expr(node) {}

    MapExpr(const MapExpr&) = default;
    MapExpr(MapExpr&&) = default;

    // a naive way to predicate whether there will be duplicated keys
    bool maybe_duplicated_keys() { return !_children[0]->is_slotref(); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MapExpr(*this)); }
};

class MapExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace starrocks

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

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {

// map_apply(lambda function, map), like map_apply((k,v)->(k+1,length(v)), map<int,char>)

class MapApplyExpr final : public Expr {
public:
    MapApplyExpr(const TExprNode& node);

    // for tests
    explicit MapApplyExpr(TypeDescriptor type);

    Status prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MapApplyExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    bool _maybe_duplicated_keys;
    std::vector<SlotId> _arguments_ids;
};
} // namespace starrocks

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
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "glog/logging.h"

namespace starrocks {

// array_map(lambda function, array0, array1...)

class ArrayMapExpr final : public Expr {
public:
    ArrayMapExpr(const TExprNode& node);

    // for tests
    explicit ArrayMapExpr(TypeDescriptor type);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ArrayMapExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;
};
} // namespace starrocks

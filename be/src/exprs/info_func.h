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

class VectorizedInfoFunc final : public Expr {
public:
    VectorizedInfoFunc(const TExprNode& node);

    ~VectorizedInfoFunc() override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedInfoFunc(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    std::string debug_string() const override;

private:
    // @IMPORTANT: BinaryColumnPtr's build_slice will cause multi-thread(OLAP_SCANNER) crash
    ColumnPtr _value;
};
} // namespace starrocks

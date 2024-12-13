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

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "storage/table_reader.h"

namespace starrocks {

class DictQueryExpr final : public Expr {
public:
    DictQueryExpr(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new DictQueryExpr(*this)); }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;
    Status prepare(RuntimeState* state, ExprContext* context) override;
    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;
    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

private:
    RuntimeState* _runtime_state;
    TDictQueryExpr _dict_query_expr;

    Schema _key_schema;
    Schema _value_schema;
    std::vector<SlotId> _key_slot_ids;
    SlotId _value_slot_id;
    std::shared_ptr<TableReader> _table_reader;
};
} // namespace starrocks

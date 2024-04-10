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
#include "storage/dictionary_cache_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class DictionaryGetExpr final : public Expr {
public:
    DictionaryGetExpr(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new DictionaryGetExpr(*this)); }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;
    Status prepare(RuntimeState* state, ExprContext* context) override;

    int64_t get_dict_id() { return _dictionary_get_expr.dict_id; }
    int64_t get_txn_id() { return _dictionary_get_expr.txn_id; }

private:
    RuntimeState* _runtime_state;
    TDictionaryGetExpr _dictionary_get_expr;

    SchemaPtr _schema = nullptr;
    DictionaryCachePtr _dictionary = nullptr;

    ChunkPtr _key_chunk = nullptr;
    ChunkPtr _value_chunk = nullptr;
    ColumnPtr _struct_column = nullptr;
};

} // namespace starrocks
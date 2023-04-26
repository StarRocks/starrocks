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

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {
// place holder for function call. representing an input column for function call.
// now it was only used in global dictionary optimization
class PlaceHolderRef final : public Expr {
public:
    PlaceHolderRef(const TExprNode& node);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;
    bool is_constant() const override { return false; }
    Expr* clone(ObjectPool* pool) const override { return pool->add(new PlaceHolderRef(*this)); }
    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        slot_ids->emplace_back(_column_id);
        return 1;
    }

private:
    SlotId _column_id;
};
} // namespace starrocks

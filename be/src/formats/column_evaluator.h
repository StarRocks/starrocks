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
#include <vector>

#include "column/column.h"
#include "common/global_types.h"
#include "common/statusor.h"
#include "gen_cpp/Exprs_types.h"
#include "types/type_descriptor.h"

namespace starrocks {

class Chunk;
class ExprContext;
class RuntimeState;

// a convenience class to abstract away complexities of handling expr and its context
// take chunk as input, and output column
class ColumnEvaluator {
public:
    static Status init(const std::vector<std::unique_ptr<ColumnEvaluator>>& source);

    static std::vector<std::unique_ptr<ColumnEvaluator>> clone(
            const std::vector<std::unique_ptr<ColumnEvaluator>>& source);

    static std::vector<TypeDescriptor> types(const std::vector<std::unique_ptr<ColumnEvaluator>>& source);

    virtual ~ColumnEvaluator() = default;

    // idempotent
    virtual Status init() = 0;

    virtual std::unique_ptr<ColumnEvaluator> clone() const = 0;

    // requires inited
    virtual TypeDescriptor type() const = 0;

    // requires inited
    virtual StatusOr<ColumnPtr> evaluate(Chunk* chunk) = 0;
};

class ColumnExprEvaluator : public ColumnEvaluator {
public:
    static std::vector<std::unique_ptr<ColumnEvaluator>> from_exprs(const std::vector<TExpr>& exprs,
                                                                    RuntimeState* state);

    ColumnExprEvaluator(const TExpr& expr, RuntimeState* state);

    ~ColumnExprEvaluator() override;

    Status init() override;

    std::unique_ptr<ColumnEvaluator> clone() const override;

    TypeDescriptor type() const override;

    StatusOr<ColumnPtr> evaluate(Chunk* chunk) override;

private:
    TExpr _expr;
    ExprContext* _expr_ctx = nullptr;
    RuntimeState* _state;
};

// used for UT, since it is too hard to mock TExpr :(
class ColumnSlotIdEvaluator : public ColumnEvaluator {
public:
    static std::vector<std::unique_ptr<ColumnEvaluator>> from_types(const std::vector<TypeDescriptor>& types);

    ColumnSlotIdEvaluator(SlotId slot_id, TypeDescriptor type) : _slot_id(slot_id), _type(type) {}

    ~ColumnSlotIdEvaluator() override = default;

    Status init() override;

    std::unique_ptr<ColumnEvaluator> clone() const override;

    TypeDescriptor type() const override { return _type; }

    StatusOr<ColumnPtr> evaluate(Chunk* chunk) override;

private:
    SlotId _slot_id;
    TypeDescriptor _type;
};

} // namespace starrocks

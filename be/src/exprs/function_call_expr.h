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
#include "exprs/builtin_functions.h"
#include "exprs/expr.h"

namespace starrocks {

class BloomFilter;

class VectorizedFunctionCallExpr final : public Expr {
public:
    explicit VectorizedFunctionCallExpr(const TExprNode& node);

    ~VectorizedFunctionCallExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedFunctionCallExpr(*this)); }

    const FunctionDescriptor* get_function_desc() { return _fn_desc; }

    bool support_ngram_bloom_filter(ExprContext* context) const override;
    bool ngram_bloom_filter(ExprContext* context, const BloomFilter* bf,
                            const NgramBloomFilterReaderOptions& reader_options) const override;

protected:
    [[nodiscard]] Status prepare(RuntimeState* state, ExprContext* context) override;

    [[nodiscard]] Status open(RuntimeState* state, ExprContext* context,
                              FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    bool is_constant() const override;

    [[nodiscard]] StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    bool split_normal_string_to_ngram(FunctionContext* fn_ctx, const NgramBloomFilterReaderOptions& reader_options,
                                      NgramBloomFilterState* ngram_state, const std::string& func_name) const;

    bool split_like_string_to_ngram(FunctionContext* fn_ctx, const NgramBloomFilterReaderOptions& reader_options,
                                    std::vector<Slice>& ngram_set) const;

    const FunctionDescriptor* _fn_desc{nullptr};

    bool _is_returning_random_value = false;
};

} // namespace starrocks

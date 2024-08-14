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
#include "exprs/agg_state_function.h"
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
    static bool split_normal_string_to_ngram(const Slice& needle, FunctionContext* fn_ctx,
                                             const NgramBloomFilterReaderOptions& reader_options,
                                             std::vector<std::string>& ngram_set, const std::string& func_name);

    static bool split_like_string_to_ngram(const Slice& needle, const NgramBloomFilterReaderOptions& reader_options,
                                           std::vector<std::string>& ngram_set);

protected:
    Status prepare(RuntimeState* state, ExprContext* context) override;

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    bool is_constant() const override;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    const FunctionDescriptor* _get_function_by_fid(const TFunction& fn);
    const FunctionDescriptor* _get_function(const TFunction& fn, const std::vector<TypeDescriptor>& arg_types,
                                            const TypeDescriptor& result_type, std::vector<bool> arg_nullables);

    const FunctionDescriptor* _fn_desc{nullptr};

    bool _is_returning_random_value = false;

    // only set when it's a agg state combinator function to track its lifecycle be with the expr
    std::shared_ptr<AggStateFunction> _agg_state_func = nullptr;
    // only set when it's a agg state combinator function to track its lifecycle be with the expr
    std::shared_ptr<FunctionDescriptor> _agg_func_desc = nullptr;
};

} // namespace starrocks

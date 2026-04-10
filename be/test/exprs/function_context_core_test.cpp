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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/column_helper.h"
#include "common/ngram_bloom_filter_state.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"

namespace starrocks {

TEST(FunctionContextCoreTest, CreateTestContextArgAccess) {
    std::vector<FunctionContext::TypeDesc> arg_types = {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR)};
    auto ctx = std::unique_ptr<FunctionContext>(
            FunctionContext::create_test_context(std::move(arg_types), TypeDescriptor(TYPE_BIGINT)));

    EXPECT_EQ(2, ctx->get_num_args());
    ASSERT_NE(nullptr, ctx->get_arg_type(0));
    EXPECT_EQ(TYPE_INT, ctx->get_arg_type(0)->type);
    ASSERT_NE(nullptr, ctx->get_arg_type(1));
    EXPECT_EQ(TYPE_VARCHAR, ctx->get_arg_type(1)->type);
    EXPECT_EQ(nullptr, ctx->get_arg_type(-1));
    EXPECT_EQ(nullptr, ctx->get_arg_type(2));
    EXPECT_EQ(TYPE_BIGINT, ctx->get_return_type().type);
}

TEST(FunctionContextCoreTest, FunctionStateScopeSetGet) {
    FunctionContext ctx;
    int thread_local_state = 11;
    int fragment_local_state = 22;

    ctx.set_function_state(FunctionContext::THREAD_LOCAL, &thread_local_state);
    ctx.set_function_state(FunctionContext::FRAGMENT_LOCAL, &fragment_local_state);

    EXPECT_EQ(&thread_local_state, ctx.get_function_state(FunctionContext::THREAD_LOCAL));
    EXPECT_EQ(&fragment_local_state, ctx.get_function_state(FunctionContext::FRAGMENT_LOCAL));
}

TEST(FunctionContextCoreTest, CloneKeepsConstantsAndFragmentState) {
    MemPool mem_pool;
    std::vector<FunctionContext::TypeDesc> arg_types = {TypeDescriptor(TYPE_INT)};
    auto ctx = std::unique_ptr<FunctionContext>(
            FunctionContext::create_context(nullptr, &mem_pool, TypeDescriptor(TYPE_INT), arg_types));

    int fragment_local_state = 123;
    ctx->set_function_state(FunctionContext::FRAGMENT_LOCAL, &fragment_local_state);

    Columns constant_columns;
    constant_columns.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(42, 1));
    ctx->set_constant_columns(std::move(constant_columns));

    MemPool clone_pool;
    auto clone_ctx = std::unique_ptr<FunctionContext>(ctx->clone(&clone_pool));

    EXPECT_EQ(&fragment_local_state, clone_ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    EXPECT_TRUE(clone_ctx->is_constant_column(0));
    EXPECT_EQ(1, clone_ctx->get_num_constant_columns());
    EXPECT_EQ(42, ColumnHelper::get_const_value<TYPE_INT>(clone_ctx->get_constant_column(0)));
}

TEST(FunctionContextCoreTest, ErrorMessageSticky) {
    FunctionContext ctx;
    EXPECT_FALSE(ctx.has_error());

    ctx.set_error("first error");
    EXPECT_TRUE(ctx.has_error());
    ASSERT_NE(nullptr, ctx.error_msg());
    EXPECT_STREQ("first error", ctx.error_msg());

    ctx.set_error("second error");
    ASSERT_NE(nullptr, ctx.error_msg());
    EXPECT_STREQ("first error", ctx.error_msg());
}

TEST(FunctionContextCoreTest, NgramStateHolderSetGet) {
    FunctionContext ctx;
    EXPECT_EQ(nullptr, ctx.get_ngram_state().get());

    auto state = std::make_unique<NgramBloomFilterState>();
    state->initialized = true;
    state->index_useful = true;
    state->ngram_set.emplace_back("abc");
    ctx.get_ngram_state() = std::move(state);

    ASSERT_NE(nullptr, ctx.get_ngram_state().get());
    EXPECT_TRUE(ctx.get_ngram_state()->initialized);
    EXPECT_TRUE(ctx.get_ngram_state()->index_useful);
    ASSERT_EQ(1, ctx.get_ngram_state()->ngram_set.size());
    EXPECT_EQ("abc", ctx.get_ngram_state()->ngram_set[0]);
}

} // namespace starrocks

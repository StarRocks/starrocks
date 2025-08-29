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

#include <algorithm>
#include <cmath>
#include <memory>

#include "exprs/agg/base_aggregate_test.h"
#include "exprs/agg/combinator/agg_state_combine.h"
#include "exprs/agg/combinator/agg_state_merge.h"
#include "exprs/agg/combinator/agg_state_union.h"
#include "exprs/agg/combinator/agg_state_utils.h"
#include "exprs/agg/combinator/state_function.h"
#include "exprs/agg/combinator/state_merge_function.h"
#include "exprs/agg/combinator/state_union_function.h"
#include "runtime/types.h"
#include "types/logical_type.h"

namespace starrocks {

class AggStateFunctionsTest : public testing::Test {
public:
    AggStateFunctionsTest() = default;

    void SetUp() override {
        _allocator = std::make_unique<CountingAllocatorWithHook>();
        tls_agg_state_allocator = _allocator.get();
    }
    void TearDown() override {
        tls_agg_state_allocator = nullptr;
        _allocator.reset();
    }

private:
    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

// Test helper functions for StateUnionFunction
template <typename T>
ColumnPtr create_agg_state_column(const AggregateFunction* func, FunctionContext* ctx, const ColumnPtr& input_column,
                                  bool is_nullable, const TypeDescriptor& intermediate_type) {
    VLOG_ROW << "create_agg_state_column: " << func->get_name() << " is_nullable: " << is_nullable;
    auto result_column = ColumnHelper::create_column(intermediate_type, is_nullable);
    auto state = ManagedAggrState::create(ctx, func);

    const Column* row_column = input_column.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state->state());
    func->serialize_to_column(ctx, state->state(), result_column.get());

    return result_column;
}

std::string get_func_name(const std::string& func_name) {
    if (func_name.rfind("nullable ", 0) == 0) {
        return func_name.substr(9);
    }
    return func_name;
}

template <typename T>
void test_agg_state_union_basic(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                                const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                                const TypeDescriptor& intermediate_type, bool is_result_nullable = false) {
    VLOG_ROW << "test_agg_state_union_basic: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create AggStateUnion
    AggStateUnion union_func(agg_state_desc, func);

    // Create test data - create aggregate state columns that contain serialized intermediate states
    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();

    // Convert input data to aggregate state columns (intermediate format)
    ColumnPtr agg_state_column1 =
            create_agg_state_column<T>(func, ctx, input_column1, is_result_nullable, intermediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, input_column2, is_result_nullable, intermediate_type);

    // Test union operation by creating a state and merging the aggregate state columns
    auto state = ManagedAggrState::create(ctx, &union_func);

    // Update state with first aggregate state column
    for (size_t i = 0; i < agg_state_column1->size(); ++i) {
        union_func.merge(ctx, agg_state_column1.get(), state->state(), i);
    }

    // Update state with second aggregate state column
    for (size_t i = 0; i < agg_state_column2->size(); ++i) {
        union_func.merge(ctx, agg_state_column2.get(), state->state(), i);
    }

    // Serialize result
    ColumnPtr result_column = ColumnHelper::create_column(intermediate_type, is_result_nullable);
    union_func.finalize_to_column(ctx, state->state(), result_column.get());

    // Verify result
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_agg_state_union_nullable(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                                   const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                                   const TypeDescriptor& intermediate_type, bool is_result_nullable = true) {
    VLOG_ROW << "test_agg_state_union_nullable: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create AggStateUnion
    AggStateUnion union_func(agg_state_desc, func);

    // Create test data with nullable columns
    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();

    // Make columns nullable
    auto nullable_column1 = ColumnHelper::cast_to_nullable_column(input_column1);
    auto nullable_column2 = ColumnHelper::cast_to_nullable_column(input_column2);

    // Convert input data to aggregate state columns (intermediate format)
    ColumnPtr agg_state_column1 =
            create_agg_state_column<T>(func, ctx, nullable_column1, is_result_nullable, intermediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, nullable_column2, is_result_nullable, intermediate_type);

    // Test union operation by creating a state and merging the aggregate state columns
    auto state = ManagedAggrState::create(ctx, &union_func);

    // Update state with first aggregate state column
    for (size_t i = 0; i < agg_state_column1->size(); ++i) {
        union_func.merge(ctx, agg_state_column1.get(), state->state(), i);
    }

    // Update state with second aggregate state column
    for (size_t i = 0; i < agg_state_column2->size(); ++i) {
        union_func.merge(ctx, agg_state_column2.get(), state->state(), i);
    }

    // Serialize result
    ColumnPtr result_column = ColumnHelper::create_column(intermediate_type, is_result_nullable);
    union_func.finalize_to_column(ctx, state->state(), result_column.get());

    // Verify result
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_agg_state_union_empty_column(FunctionContext* ctx, const AggregateFunction* func,
                                       const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                       const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                       bool is_result_nullable = false) {
    VLOG_ROW << "test_agg_state_union_empty_column: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create AggStateUnion
    AggStateUnion union_func(agg_state_desc, func);

    // Create empty columns
    auto empty_column1 =
            ColumnHelper::create_column(TypeDescriptor(std::is_same_v<T, int32_t> ? TYPE_INT : TYPE_BIGINT), false);
    auto empty_column2 =
            ColumnHelper::create_column(TypeDescriptor(std::is_same_v<T, int32_t> ? TYPE_INT : TYPE_BIGINT), false);

    // Test union operation by creating a state and merging the aggregate state columns
    auto state = ManagedAggrState::create(ctx, &union_func);

    // Update state with first aggregate state column
    for (size_t i = 0; i < empty_column1->size(); ++i) {
        union_func.merge(ctx, empty_column1.get(), state->state(), i);
    }

    // Update state with second aggregate state column
    for (size_t i = 0; i < empty_column2->size(); ++i) {
        union_func.merge(ctx, empty_column2.get(), state->state(), i);
    }

    // Serialize result
    ColumnPtr result_column = ColumnHelper::create_column(intermediate_type, is_result_nullable);
    union_func.finalize_to_column(ctx, state->state(), result_column.get());

    ASSERT_NE(result_column, nullptr);
}

template <typename T>
void test_agg_state_union_invalid_cases(FunctionContext* ctx, const std::string& func_name,
                                        const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type) {
    VLOG_ROW << "test_agg_state_union_invalid_cases: " << func_name;

    // Test with invalid function name
    AggStateDesc invalid_desc("invalid_func", ret_type, arg_types, false, 1);
    auto invalid_func = AggStateDesc::get_agg_state_func(&invalid_desc);
    ASSERT_EQ(invalid_func, nullptr);
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_sum) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc, true);
        test_agg_state_union_nullable<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                               true);
        test_agg_state_union_empty_column<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                                   true);
        test_agg_state_union_invalid_cases<int32_t>(ctx, "sum", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_count) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
        test_agg_state_union_nullable<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                               false);
        test_agg_state_union_empty_column<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc,
                                                   return_type_desc, false);
        test_agg_state_union_invalid_cases<int32_t>(ctx, "count", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_max) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc, true);
        test_agg_state_union_nullable<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                               true);
        test_agg_state_union_empty_column<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                                   true);
        test_agg_state_union_invalid_cases<int32_t>(ctx, "max", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_min) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc, true);
        test_agg_state_union_nullable<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                               true);
        test_agg_state_union_empty_column<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                                   true);
        test_agg_state_union_invalid_cases<int32_t>(ctx, "min", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_avg) {
    auto return_type = TYPE_DOUBLE;
    auto arg_type = TYPE_INT;
    auto intermediate_type = TYPE_VARBINARY;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    TypeDescriptor intermediate_type_desc = TypeDescriptor(intermediate_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc, intermediate_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc, intermediate_type_desc,
                                            true);
        test_agg_state_union_nullable<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                               intermediate_type_desc, true);
        test_agg_state_union_empty_column<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                   intermediate_type_desc, true);
        test_agg_state_union_invalid_cases<int32_t>(ctx, "avg", arg_type_descs, return_type_desc);
    }
}

template <typename T>
void test_agg_state_merge_basic(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                                const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                                const TypeDescriptor& intermediate_type, bool is_result_nullable = false) {
    VLOG_ROW << "test_agg_state_merge_basic: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create AggStateMerge
    AggStateMerge merge_func(agg_state_desc, func);

    // Create test data
    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();

    // Convert input data to aggregate state columns (intermediate format)
    ColumnPtr agg_state_column1 =
            create_agg_state_column<T>(func, ctx, input_column1, is_result_nullable, intermediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, input_column2, is_result_nullable, intermediate_type);

    // Test merge operation by creating a state and merging the aggregate state columns
    auto state = ManagedAggrState::create(ctx, &merge_func);

    // Update state with first aggregate state column
    for (size_t i = 0; i < agg_state_column1->size(); ++i) {
        const Column* columns[] = {agg_state_column1.get()};
        merge_func.update(ctx, columns, state->state(), i);
    }

    // Update state with second aggregate state column
    for (size_t i = 0; i < agg_state_column2->size(); ++i) {
        const Column* columns[] = {agg_state_column2.get()};
        merge_func.update(ctx, columns, state->state(), i);
    }

    // Serialize result
    ColumnPtr result_column = ColumnHelper::create_column(ret_type, is_result_nullable);
    merge_func.finalize_to_column(ctx, state->state(), result_column.get());

    // Verify result
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_agg_state_merge_nullable(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                                   const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                                   const TypeDescriptor& intermediate_type, bool is_result_nullable = true) {
    VLOG_ROW << "test_agg_state_merge_nullable: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create AggStateMerge
    AggStateMerge merge_func(agg_state_desc, func);

    // Create test data with nullable columns
    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();

    // Make columns nullable
    auto nullable_column1 = ColumnHelper::cast_to_nullable_column(input_column1);
    auto nullable_column2 = ColumnHelper::cast_to_nullable_column(input_column2);

    // Convert input data to aggregate state columns (intermediate format)
    ColumnPtr agg_state_column1 =
            create_agg_state_column<T>(func, ctx, nullable_column1, is_result_nullable, intermediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, nullable_column2, is_result_nullable, intermediate_type);

    // Test merge operation by creating a state and merging the aggregate state columns
    auto state = ManagedAggrState::create(ctx, &merge_func);

    // Update state with first aggregate state column
    for (size_t i = 0; i < agg_state_column1->size(); ++i) {
        merge_func.merge(ctx, agg_state_column1.get(), state->state(), i);
    }

    // Update state with second aggregate state column
    for (size_t i = 0; i < agg_state_column2->size(); ++i) {
        merge_func.merge(ctx, agg_state_column2.get(), state->state(), i);
    }

    // Serialize result
    ColumnPtr result_column = ColumnHelper::create_column(ret_type, is_result_nullable);
    merge_func.finalize_to_column(ctx, state->state(), result_column.get());

    // Verify result
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_agg_state_merge_empty_column(FunctionContext* ctx, const AggregateFunction* func,
                                       const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                       const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                       bool is_result_nullable = false) {
    VLOG_ROW << "test_agg_state_merge_empty_column: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create AggStateMerge
    AggStateMerge merge_func(agg_state_desc, func);

    // Create empty columns
    auto empty_column1 =
            ColumnHelper::create_column(TypeDescriptor(std::is_same_v<T, int32_t> ? TYPE_INT : TYPE_BIGINT), false);
    auto empty_column2 =
            ColumnHelper::create_column(TypeDescriptor(std::is_same_v<T, int32_t> ? TYPE_INT : TYPE_BIGINT), false);

    // Convert input data to aggregate state columns (intermediate format)
    ColumnPtr agg_state_column1 =
            create_agg_state_column<T>(func, ctx, empty_column1, is_result_nullable, intermediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, empty_column2, is_result_nullable, intermediate_type);

    // Test merge operation by creating a state and merging the aggregate state columns
    auto state = ManagedAggrState::create(ctx, &merge_func);

    // Update state with first aggregate state column
    for (size_t i = 0; i < agg_state_column1->size(); ++i) {
        merge_func.merge(ctx, agg_state_column1.get(), state->state(), i);
    }

    // Update state with second aggregate state column
    for (size_t i = 0; i < agg_state_column2->size(); ++i) {
        merge_func.merge(ctx, agg_state_column2.get(), state->state(), i);
    }

    // Serialize result
    ColumnPtr result_column = ColumnHelper::create_column(ret_type, is_result_nullable);
    merge_func.finalize_to_column(ctx, state->state(), result_column.get());

    // Verify result
    ASSERT_NE(result_column, nullptr);
}

template <typename T>
void test_agg_state_merge_invalid_cases(FunctionContext* ctx, const std::string& func_name,
                                        const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type) {
    VLOG_ROW << "test_agg_state_merge_invalid_cases: " << func_name;

    // Test with invalid function name
    AggStateDesc invalid_desc("invalid_func", ret_type, arg_types, false, 1);
    auto invalid_func = AggStateDesc::get_agg_state_func(&invalid_desc);
    ASSERT_EQ(invalid_func, nullptr);
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_min) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc, true);
        test_agg_state_merge_nullable<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                               true);
        test_agg_state_merge_empty_column<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                                   true);
        test_agg_state_merge_invalid_cases<int32_t>(ctx, "min", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_max) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc, true);
        test_agg_state_merge_nullable<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                               true);
        test_agg_state_merge_empty_column<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                                   true);
        test_agg_state_merge_invalid_cases<int32_t>(ctx, "max", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_avg) {
    auto return_type = TYPE_DOUBLE;
    auto arg_type = TYPE_INT;
    auto intermediate_type = TYPE_VARBINARY;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    TypeDescriptor intermediate_type_desc = TypeDescriptor(intermediate_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc, intermediate_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc, intermediate_type_desc,
                                            true);
        test_agg_state_merge_nullable<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                               intermediate_type_desc, true);
        test_agg_state_merge_empty_column<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                   intermediate_type_desc, true);
        test_agg_state_merge_invalid_cases<int32_t>(ctx, "avg", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_sum) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc, true);
        test_agg_state_merge_nullable<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                               true);
        test_agg_state_merge_empty_column<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                                   true);
        test_agg_state_merge_invalid_cases<int64_t>(ctx, "sum", arg_type_descs, return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_count) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int64_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
    }
    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_basic<int64_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                            false);
        test_agg_state_merge_nullable<int64_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                               false);
        test_agg_state_merge_empty_column<int64_t>(ctx, func, "count", arg_type_descs, return_type_desc,
                                                   return_type_desc, false);
        test_agg_state_merge_invalid_cases<int64_t>(ctx, "count", arg_type_descs, return_type_desc);
    }
}

template <typename T>
void test_state_function_basic(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                               const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                               const TypeDescriptor& intermediate_type, bool is_result_nullable = false) {
    VLOG_ROW << "test_state_function_basic: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create arg nullables
    std::vector<bool> arg_nullables(arg_types.size(), is_result_nullable);

    // Create StateFunction
    StateFunction state_func(agg_state_desc, intermediate_type, arg_nullables);

    // Create test data
    ColumnPtr input_column = gen_input_column1<T>();

    // Test execute
    Columns input_columns = {input_column};
    auto result = state_func.execute(ctx, input_columns);
    ASSERT_TRUE(result.ok());

    // Verify result
    ColumnPtr result_column = result.value();
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_state_function_nullable(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                                  const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                                  const TypeDescriptor& intermediate_type, bool is_result_nullable = true) {
    VLOG_ROW << "test_state_function_nullable: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create arg nullables
    std::vector<bool> arg_nullables(arg_types.size(), is_result_nullable);

    // Create StateFunction
    StateFunction state_func(agg_state_desc, intermediate_type, arg_nullables);

    // Create test data with nullable columns
    ColumnPtr input_column = gen_input_column1<T>();

    // Make column nullable
    auto nullable_column = ColumnHelper::cast_to_nullable_column(input_column);

    // Test execute with nullable column
    Columns input_columns = {nullable_column};
    auto result = state_func.execute(ctx, input_columns);
    ASSERT_TRUE(result.ok());

    // Verify result
    ColumnPtr result_column = result.value();
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_state_function_empty_column(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                                      const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                                      const TypeDescriptor& intermediate_type, bool is_result_nullable = false) {
    VLOG_ROW << "test_state_function_empty_column: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create arg nullables
    std::vector<bool> arg_nullables(arg_types.size(), is_result_nullable);

    // Create StateFunction
    StateFunction state_func(agg_state_desc, intermediate_type, arg_nullables);

    // Create empty column
    auto empty_column =
            ColumnHelper::create_column(TypeDescriptor(std::is_same_v<T, int32_t> ? TYPE_INT : TYPE_BIGINT), false);

    // Test execute with empty column
    Columns input_columns = {empty_column};
    auto result = state_func.execute(ctx, input_columns);
    ASSERT_TRUE(result.ok());

    // Verify result
    ColumnPtr result_column = result.value();
    ASSERT_NE(result_column, nullptr);
    //ASSERT_EQ(result_column->size(), 0);
}

TEST_F(AggStateFunctionsTest, test_state_function_sum) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_state_function_basic<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_state_function_basic<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc, true);
        test_state_function_nullable<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_state_function_empty_column<int64_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                                  true);
    }
}

TEST_F(AggStateFunctionsTest, test_state_function_count) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_state_function_basic<int64_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                           false);
    }
}

TEST_F(AggStateFunctionsTest, test_state_function_min) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_state_function_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_state_function_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc, true);
        test_state_function_nullable<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_state_function_empty_column<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                                  true);
    }
}

TEST_F(AggStateFunctionsTest, test_state_function_avg) {
    auto return_type = TYPE_DOUBLE;
    auto arg_type = TYPE_INT;
    auto intermediate_type = TYPE_VARBINARY;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    TypeDescriptor intermediate_type_desc = TypeDescriptor(intermediate_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_state_function_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc, intermediate_type_desc,
                                           false);
    }
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_state_function_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc, intermediate_type_desc,
                                           true);
        test_state_function_nullable<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                              intermediate_type_desc, true);
        test_state_function_empty_column<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                  intermediate_type_desc, true);
    }
}

template <typename T>
void test_agg_state_union_function_basic(FunctionContext* ctx, const AggregateFunction* func,
                                         const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                         const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                         bool is_result_nullable = false) {
    VLOG_ROW << "test_agg_state_union_function_basic: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create test data
    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();

    // Create agg state columns
    ColumnPtr agg_state_column1 =
            create_agg_state_column<T>(func, ctx, input_column1, is_result_nullable, intermediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, input_column2, is_result_nullable, intermediate_type);

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create StateUnionFunction
    std::vector<bool> arg_nullables = {false, false};
    StateUnionFunction union_func(agg_state_desc, intermediate_type, arg_nullables);

    // Test prepare
    Status status = union_func.prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Prepare failed: " << status.message();

    // Test execute
    Columns columns = {agg_state_column1, agg_state_column2};
    auto result = union_func.execute(ctx, columns);
    ASSERT_TRUE(result.ok()) << "Execute failed: " << result.status().message();

    // Verify result
    ASSERT_EQ(result.value()->size(), agg_state_column1->size());

    // Test close
    status = union_func.close(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Close failed: " << status.message();
}

template <typename T>
void test_agg_state_union_function_nullable(FunctionContext* ctx, const AggregateFunction* func,
                                            const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                            const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                            bool is_result_nullable = true) {
    VLOG_ROW << "test_agg_state_union_function_nullable: " << func_name
             << " is_result_nullable: " << is_result_nullable;

    // Create test data with nullable columns
    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();

    // Make columns nullable
    auto nullable_column1 = ColumnHelper::cast_to_nullable_column(input_column1);
    auto nullable_column2 = ColumnHelper::cast_to_nullable_column(input_column2);

    // Create agg state columns
    ColumnPtr agg_state_column1 =
            create_agg_state_column<T>(func, ctx, nullable_column1, is_result_nullable, intermediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, nullable_column2, is_result_nullable, intermediate_type);

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create StateUnionFunction with nullable arguments
    std::vector<bool> arg_nullables = {true, true};
    StateUnionFunction union_func(agg_state_desc, intermediate_type, arg_nullables);

    // Test prepare
    Status status = union_func.prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Prepare failed: " << status.message();

    // Test execute
    Columns columns = {agg_state_column1, agg_state_column2};
    auto result = union_func.execute(ctx, columns);
    ASSERT_TRUE(result.ok()) << "Execute failed: " << result.status().message();

    // Verify result
    ASSERT_EQ(result.value()->size(), agg_state_column1->size());

    // Test close
    status = union_func.close(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Close failed: " << status.message();
}

// Test cases for StateUnionFunction
TEST_F(AggStateFunctionsTest, test_agg_state_union_function_sum) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_basic<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                     return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                        return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_count) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_basic<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc,
                                                     return_type_desc, false);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_max) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                     return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                        return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_min) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                     return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                        return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_avg) {
    auto return_type = TYPE_DOUBLE;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    TypeDescriptor intermediate_type_desc = TypeDescriptor(TYPE_VARBINARY);
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                     intermediate_type_desc, false);
    }
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                        intermediate_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_string) {
    auto return_type = TYPE_VARCHAR;
    auto arg_type = TYPE_VARCHAR;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("any_value", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_basic<Slice>(ctx, func, "any_value", arg_type_descs, return_type_desc,
                                                   return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("any_value", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<Slice>(ctx, func, "any_value", arg_type_descs, return_type_desc,
                                                      return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_decimal) {
    auto return_type = TYPE_DECIMALV2;
    auto arg_type = TYPE_DECIMALV2;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_union_function_basic<DecimalV2Value>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                            return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<DecimalV2Value>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                               return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_timestamp) {
    auto return_type = TYPE_DATETIME;
    auto arg_type = TYPE_DATETIME;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_union_function_basic<TimestampValue>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                            return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<TimestampValue>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                               return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_union_function_date) {
    auto return_type = TYPE_DATE;
    auto arg_type = TYPE_DATE;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_union_function_basic<DateValue>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                       return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_union_function_nullable<DateValue>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                          return_type_desc, true);
    }
}

// Test helper functions for StateMergeFunction
template <typename T>
void test_agg_state_merge_function_basic(FunctionContext* ctx, const AggregateFunction* func,
                                         const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                         const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                         bool is_nullable = false) {
    VLOG_ROW << "test_agg_state_merge_function_basic: " << func_name << " is_nullable: " << is_nullable;

    // Create test data
    ColumnPtr input_column = gen_input_column1<T>();

    // Create agg state column
    ColumnPtr agg_state_column = create_agg_state_column<T>(func, ctx, input_column, is_nullable, intermediate_type);

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_nullable, 1);

    // Create StateMergeFunction
    std::vector<bool> arg_nullables = {is_nullable};
    StateMergeFunction merge_func(agg_state_desc, intermediate_type, arg_nullables);

    // Test prepare
    Status status = merge_func.prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Prepare failed: " << status.message();

    // Test execute
    Columns columns = {agg_state_column};
    auto result = merge_func.execute(ctx, columns);
    ASSERT_TRUE(result.ok()) << "Execute failed: " << result.status().message();

    // Verify result
    ASSERT_EQ(result.value()->size(), agg_state_column->size());

    // Test close
    status = merge_func.close(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Close failed: " << status.message();
}

template <typename T>
void test_agg_state_merge_function_nullable(FunctionContext* ctx, const AggregateFunction* func,
                                            const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                            const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                            bool is_nullable = true) {
    VLOG_ROW << "test_agg_state_merge_function_nullable: " << func_name << " is_nullable: " << is_nullable;

    // Create test data with nullable columns
    ColumnPtr input_column = gen_input_column1<T>();

    // Make column nullable
    auto nullable_column = ColumnHelper::cast_to_nullable_column(input_column);

    // Create agg state column
    ColumnPtr agg_state_column = create_agg_state_column<T>(func, ctx, nullable_column, is_nullable, intermediate_type);

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_nullable, 1);

    // Create StateMergeFunction with nullable arguments
    std::vector<bool> arg_nullables = {is_nullable};
    StateMergeFunction merge_func(agg_state_desc, intermediate_type, arg_nullables);

    // Test prepare
    Status status = merge_func.prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Prepare failed: " << status.message();

    // Test execute
    Columns columns = {agg_state_column};
    auto result = merge_func.execute(ctx, columns);
    ASSERT_TRUE(result.ok()) << "Execute failed: " << result.status().message();

    // Verify result
    ASSERT_EQ(result.value()->size(), agg_state_column->size());

    // Test close
    status = merge_func.close(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok()) << "Close failed: " << status.message();
}

// Test cases for StateMergeFunction
TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_sum) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                     return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                        return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_count) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc,
                                                     return_type_desc, false);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_max) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                     return_type_desc);
    }
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                        return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_min) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                     return_type_desc);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                        return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_avg) {
    auto return_type = TYPE_DOUBLE;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    TypeDescriptor intermediate_type_desc = TypeDescriptor(TYPE_VARBINARY);
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                     intermediate_type_desc);
    }
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                        intermediate_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_string) {
    auto return_type = TYPE_VARCHAR;
    auto arg_type = TYPE_VARCHAR;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("any_value", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<Slice>(ctx, func, "any_value", arg_type_descs, return_type_desc,
                                                   return_type_desc);
    }
    {
        auto func = get_aggregate_function("any_value", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<Slice>(ctx, func, "any_value", arg_type_descs, return_type_desc,
                                                      return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_decimal) {
    auto return_type = TYPE_DECIMALV2;
    auto arg_type = TYPE_DECIMALV2;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<DecimalV2Value>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                            return_type_desc);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<DecimalV2Value>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                               return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_timestamp) {
    auto return_type = TYPE_DATETIME;
    auto arg_type = TYPE_DATETIME;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_basic<TimestampValue>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                            return_type_desc);
    }
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<TimestampValue>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                               return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_date) {
    auto return_type = TYPE_DATE;
    auto arg_type = TYPE_DATE;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);

        test_agg_state_merge_function_basic<DateValue>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                       return_type_desc);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_merge_function_nullable<DateValue>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                          return_type_desc);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_merge_function_memory_allocation) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    std::vector<TypeDescriptor> arg_type_descs = {TypeDescriptor(arg_type)};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();
    auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
    ASSERT_NE(func, nullptr);

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func->get_name(), return_type_desc, arg_type_descs, true, 1);

    // Create StateMergeFunction
    TypeDescriptor intermediate_type = return_type_desc;
    std::vector<bool> arg_nullables = {false};
    StateMergeFunction merge_func(agg_state_desc, intermediate_type, arg_nullables);

    // Test with large dataset
    auto large_input_column = gen_input_column1<int32_t>();
    ColumnPtr agg_state_column =
            create_agg_state_column<int32_t>(func, ctx, large_input_column, false, intermediate_type);

    // Test prepare
    Status status = merge_func.prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(status.ok());

    // Test execute with large dataset
    Columns columns = {agg_state_column};
    auto result = merge_func.execute(ctx, columns);
    ASSERT_TRUE(result.ok());
}

// Test helper functions for AggStateCombine
template <typename T>
void test_agg_state_combine_basic(FunctionContext* ctx, const AggregateFunction* base_func,
                                  const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                  const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                  bool is_nullable = false) {
    VLOG_ROW << "test_agg_state_combine_basic: " << func_name << " is_nullable: " << is_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_nullable, 1);

    // Create AggStateCombine
    AggStateCombine combine_func(agg_state_desc, base_func);

    // Test basic properties
    ASSERT_EQ(combine_func.get_name(), "agg_state_combine");
    ASSERT_EQ(combine_func.size(), base_func->size());
    ASSERT_EQ(combine_func.alignof_size(), base_func->alignof_size());
    ASSERT_EQ(combine_func.is_pod_state(), base_func->is_pod_state());
    ASSERT_EQ(combine_func.get_agg_state_desc()->get_func_name(), func_name);

    // Create test data
    ColumnPtr input_column = gen_input_column1<T>();
    if (is_nullable) {
        input_column = ColumnHelper::cast_to_nullable_column(input_column);
    }

    // Test with AggStateCombine
    auto combine_state = ManagedAggrState::create(ctx, &combine_func);
    const Column* row_column = input_column.get();
    combine_func.update_batch_single_state(ctx, row_column->size(), &row_column, combine_state->state());

    // Get result from combine function (should be serialized state)
    auto combine_result_column = ColumnHelper::create_column(intermediate_type, is_nullable);
    combine_func.finalize_to_column(ctx, combine_state->state(), combine_result_column.get());

    // Test with base function for comparison
    auto base_state = ManagedAggrState::create(ctx, base_func);
    base_func->update_batch_single_state(ctx, row_column->size(), &row_column, base_state->state());

    // Get serialized state from base function
    auto base_serialized_column = ColumnHelper::create_column(intermediate_type, is_nullable);
    base_func->serialize_to_column(ctx, base_state->state(), base_serialized_column.get());

    // Get final result from base function
    auto base_final_column = ColumnHelper::create_column(ret_type, is_nullable);
    base_func->finalize_to_column(ctx, base_state->state(), base_final_column.get());

    // Verify that AggStateCombine's finalize_to_column returns the same as base function's serialize_to_column
    ASSERT_EQ(combine_result_column->size(), base_serialized_column->size());

    // The key difference: AggStateCombine should return serialized state, not final result
    if (!is_nullable && combine_result_column->size() > 0 && base_serialized_column->size() > 0) {
        ASSERT_TRUE(combine_result_column->equals(0, *base_serialized_column, 0));
    }
}

template <typename T>
void test_agg_state_combine_merge(FunctionContext* ctx, const AggregateFunction* base_func,
                                  const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                  const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                  bool is_nullable = false) {
    VLOG_ROW << "test_agg_state_combine_merge: " << func_name << " is_nullable: " << is_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_nullable, 1);

    // Create AggStateCombine
    AggStateCombine combine_func(agg_state_desc, base_func);

    // Create test data
    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();
    if (is_nullable) {
        input_column1 = ColumnHelper::cast_to_nullable_column(input_column1);
        input_column2 = ColumnHelper::cast_to_nullable_column(input_column2);
    }

    // Create two separate states
    auto state1 = ManagedAggrState::create(ctx, &combine_func);
    auto state2 = ManagedAggrState::create(ctx, &combine_func);

    // Update first state
    const Column* row_column1 = input_column1.get();
    combine_func.update_batch_single_state(ctx, row_column1->size(), &row_column1, state1->state());

    // Update second state
    const Column* row_column2 = input_column2.get();
    combine_func.update_batch_single_state(ctx, row_column2->size(), &row_column2, state2->state());

    // Serialize first state for merging
    auto serialize_column = ColumnHelper::create_column(intermediate_type, is_nullable);
    combine_func.serialize_to_column(ctx, state1->state(), serialize_column.get());

    // Merge first state into second state
    combine_func.merge(ctx, serialize_column.get(), state2->state(), 0);

    // Get final result
    auto result_column = ColumnHelper::create_column(intermediate_type, is_nullable);
    combine_func.finalize_to_column(ctx, state2->state(), result_column.get());

    // Verify merge worked (result should contain combined data)
    ASSERT_EQ(result_column->size(), 1);
}

template <typename T>
void test_agg_state_combine_convert_format(FunctionContext* ctx, const AggregateFunction* base_func,
                                           const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                           const TypeDescriptor& ret_type, const TypeDescriptor& intermediate_type,
                                           bool is_nullable = false) {
    VLOG_ROW << "test_agg_state_combine_convert_format: " << func_name << " is_nullable: " << is_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_nullable, 1);

    // Create AggStateCombine
    AggStateCombine combine_func(agg_state_desc, base_func);

    // Create test input column
    ColumnPtr input_column = gen_input_column1<T>();
    if (is_nullable) {
        input_column = ColumnHelper::cast_to_nullable_column(input_column);
    }

    // Test convert_to_serialize_format
    Columns input_columns = {input_column};
    ColumnPtr output_column;
    combine_func.convert_to_serialize_format(ctx, input_columns, input_column->size(), &output_column);

    // Should return the same column (pass-through behavior)
    ASSERT_EQ(output_column.get(), input_column.get());
}

template <typename T>
void test_agg_state_combine_error_cases(FunctionContext* ctx, const std::string& func_name,
                                        const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type) {
    VLOG_ROW << "test_agg_state_combine_error_cases: " << func_name;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, false, 1);

    // Test with null function (should trigger DCHECK in debug builds)
    // In release builds, this will likely crash, so we don't test it
    // AggStateCombine null_combine_func(agg_state_desc, nullptr);

    // Test convert_to_serialize_format with wrong number of columns
    auto base_func = get_aggregate_function(func_name, {TYPE_INT}, TYPE_BIGINT, false);
    ASSERT_NE(base_func, nullptr);

    AggStateCombine combine_func(agg_state_desc, base_func);

    ColumnPtr input_column1 = gen_input_column1<T>();
    ColumnPtr input_column2 = gen_input_column2<T>();

    // Test with wrong number of columns (should DCHECK fail)
    Columns wrong_columns = {input_column1, input_column2}; // Should be 1 column
    ColumnPtr output_column;

    // This should trigger DCHECK_EQ(1, srcs.size()) in debug builds
    // In release builds it might process incorrectly
    // combine_func.convert_to_serialize_format(ctx, wrong_columns, input_column1->size(), &output_column);
}

// Test cases for AggStateCombine
TEST_F(AggStateFunctionsTest, test_agg_state_combine_sum) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_merge<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                       return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("sum", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_agg_state_combine_merge<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "sum", arg_type_descs, return_type_desc,
                                                       return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_combine_count) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_BIGINT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_merge<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc,
                                                       return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("count", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_merge<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "count", arg_type_descs, return_type_desc,
                                                       return_type_desc, false);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_combine_avg) {
    auto return_type = TYPE_DOUBLE;
    auto arg_type = TYPE_INT;
    auto intermediate_type = TYPE_VARBINARY;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    TypeDescriptor intermediate_type_desc = TypeDescriptor(intermediate_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        // For avg, the immediate type might be different from return type (contains count and sum)
        // Use the function's return type as immediate type for simplicity
        test_agg_state_combine_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                              intermediate_type_desc, false);
        test_agg_state_combine_merge<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                              intermediate_type_desc, false);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                       intermediate_type_desc, false);
    }
    {
        auto func = get_aggregate_function("avg", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                              intermediate_type_desc, true);
        test_agg_state_combine_merge<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                              intermediate_type_desc, true);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "avg", arg_type_descs, return_type_desc,
                                                       intermediate_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_combine_max) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_merge<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                       return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("max", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_agg_state_combine_merge<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "max", arg_type_descs, return_type_desc,
                                                       return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_combine_min) {
    auto return_type = TYPE_INT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, false);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_merge<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                              false);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                       return_type_desc, false);
    }
    {
        auto func = get_aggregate_function("min", {arg_type}, return_type, true);
        ASSERT_NE(func, nullptr);
        test_agg_state_combine_basic<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_agg_state_combine_merge<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc, return_type_desc,
                                              true);
        test_agg_state_combine_convert_format<int32_t>(ctx, func, "min", arg_type_descs, return_type_desc,
                                                       return_type_desc, true);
    }
}

TEST_F(AggStateFunctionsTest, test_agg_state_combine_error_cases) {
    auto return_type = TYPE_BIGINT;
    auto arg_type = TYPE_INT;
    TypeDescriptor return_type_desc = TypeDescriptor(return_type);
    TypeDescriptor arg_type_desc = TypeDescriptor(arg_type);
    std::vector<TypeDescriptor> arg_type_descs = {arg_type_desc};
    auto utils = std::make_unique<FunctionUtils>(nullptr, return_type_desc, arg_type_descs);
    auto ctx = utils->get_fn_ctx();

    test_agg_state_combine_error_cases<int32_t>(ctx, "sum", arg_type_descs, return_type_desc);
}

} // namespace starrocks

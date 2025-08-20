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
#include "exprs/agg/combinator/agg_state_merge.h"
#include "exprs/agg/combinator/agg_state_union.h"
#include "exprs/agg/combinator/agg_state_utils.h"
#include "exprs/agg/combinator/state_function.h"
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

// Test helper functions for AggStateUnionFunction
template <typename T>
ColumnPtr create_agg_state_column(const AggregateFunction* func, FunctionContext* ctx, const ColumnPtr& input_column,
                                  bool is_nullable, const TypeDescriptor& immediate_type) {
    VLOG_ROW << "create_agg_state_column: " << func->get_name() << " is_nullable: " << is_nullable;
    auto result_column = ColumnHelper::create_column(immediate_type, is_nullable);
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
                                const TypeDescriptor& immediate_type, bool is_result_nullable = false) {
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
            create_agg_state_column<T>(func, ctx, input_column1, is_result_nullable, immediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, input_column2, is_result_nullable, immediate_type);

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
    ColumnPtr result_column = ColumnHelper::create_column(immediate_type, is_result_nullable);
    union_func.finalize_to_column(ctx, state->state(), result_column.get());

    // Verify result
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_agg_state_union_nullable(FunctionContext* ctx, const AggregateFunction* func, const std::string& func_name,
                                   const std::vector<TypeDescriptor>& arg_types, const TypeDescriptor& ret_type,
                                   const TypeDescriptor& immediate_type, bool is_result_nullable = true) {
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
            create_agg_state_column<T>(func, ctx, nullable_column1, is_result_nullable, immediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, nullable_column2, is_result_nullable, immediate_type);

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
    ColumnPtr result_column = ColumnHelper::create_column(immediate_type, is_result_nullable);
    union_func.finalize_to_column(ctx, state->state(), result_column.get());

    // Verify result
    ASSERT_NE(result_column, nullptr);
    ASSERT_EQ(result_column->is_nullable(), is_result_nullable);
}

template <typename T>
void test_agg_state_union_empty_column(FunctionContext* ctx, const AggregateFunction* func,
                                       const std::string& func_name, const std::vector<TypeDescriptor>& arg_types,
                                       const TypeDescriptor& ret_type, const TypeDescriptor& immediate_type,
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
    ColumnPtr result_column = ColumnHelper::create_column(immediate_type, is_result_nullable);
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
                                const TypeDescriptor& immediate_type, bool is_result_nullable = false) {
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
            create_agg_state_column<T>(func, ctx, input_column1, is_result_nullable, immediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, input_column2, is_result_nullable, immediate_type);

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
                                   const TypeDescriptor& immediate_type, bool is_result_nullable = true) {
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
            create_agg_state_column<T>(func, ctx, nullable_column1, is_result_nullable, immediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, nullable_column2, is_result_nullable, immediate_type);

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
                                       const TypeDescriptor& ret_type, const TypeDescriptor& immediate_type,
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
            create_agg_state_column<T>(func, ctx, empty_column1, is_result_nullable, immediate_type);
    ColumnPtr agg_state_column2 =
            create_agg_state_column<T>(func, ctx, empty_column2, is_result_nullable, immediate_type);

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
                               const TypeDescriptor& immediate_type, bool is_result_nullable = false) {
    VLOG_ROW << "test_state_function_basic: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create arg nullables
    std::vector<bool> arg_nullables(arg_types.size(), is_result_nullable);

    // Create StateFunction
    StateFunction state_func(agg_state_desc, immediate_type, arg_nullables);

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
                                  const TypeDescriptor& immediate_type, bool is_result_nullable = true) {
    VLOG_ROW << "test_state_function_nullable: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create arg nullables
    std::vector<bool> arg_nullables(arg_types.size(), is_result_nullable);

    // Create StateFunction
    StateFunction state_func(agg_state_desc, immediate_type, arg_nullables);

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
                                      const TypeDescriptor& immediate_type, bool is_result_nullable = false) {
    VLOG_ROW << "test_state_function_empty_column: " << func_name << " is_result_nullable: " << is_result_nullable;

    // Create AggStateDesc
    AggStateDesc agg_state_desc(func_name, ret_type, arg_types, is_result_nullable, 1);

    // Create arg nullables
    std::vector<bool> arg_nullables(arg_types.size(), is_result_nullable);

    // Create StateFunction
    StateFunction state_func(agg_state_desc, immediate_type, arg_nullables);

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

} // namespace starrocks

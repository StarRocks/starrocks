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

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <memory>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/group_concat.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/agg/sum.h"
#include "exprs/arithmetic_operation.h"
#include "exprs/function_context.h"
#include "gen_cpp/Data_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/time_types.h"
#include "testutil/function_utils.h"
#include "types/bitmap_value.h"
#include "util/slice.h"
#include "util/thrift_util.h"
#include "util/unaligned_access.h"

namespace starrocks {
// adaptor to TypeDescriptor
struct UTRawType {
    LogicalType type;
    int precision;
    int scale;
    operator TypeDescriptor() {
        TypeDescriptor type_desc;
        type_desc.type = type;
        type_desc.precision = precision;
        type_desc.scale = scale;
        return type_desc;
    }
};

class ManagedAggrState {
public:
    ~ManagedAggrState() { _func->destroy(_ctx, _state); }
    static std::unique_ptr<ManagedAggrState> create(FunctionContext* ctx, const AggregateFunction* func) {
        return std::make_unique<ManagedAggrState>(ctx, func);
    }
    AggDataPtr state() { return _state; }

private:
    ManagedAggrState(FunctionContext* ctx, const AggregateFunction* func) : _ctx(ctx), _func(func) {
        _state = _mem_pool.allocate_aligned(func->size(), func->alignof_size());
        _func->create(_ctx, _state);
    }
    FunctionContext* _ctx;
    const AggregateFunction* _func;
    MemPool _mem_pool;
    AggDataPtr _state;
};

template <typename T>
inline ColumnPtr gen_input_column1() {
    using DataColumn = typename ColumnTraits<T>::ColumnType;
    auto column = DataColumn::create();
    for (int i = 0; i < 1024; i++) {
        column->append(i);
    }
    column->append(100);
    column->append(200);
    return column;
}

template <LogicalType LT>
ColumnPtr gen_input_decimal_column1(const FunctionContext::TypeDesc* type_desc) {
    auto column = RunTimeColumnType<LT>::create(type_desc->precision, type_desc->scale);
    for (int i = 0; i < 1024; i++) {
        column->append(i);
    }
    column->append(100);
    column->append(200);
    return column;
}

template <typename T>
inline ColumnPtr gen_input_column2() {
    using DataColumn = typename ColumnTraits<T>::ColumnType;
    auto column = DataColumn::create();
    for (int i = 2000; i < 3000; i++) {
        column->append(i);
    }
    return column;
}

template <>
inline ColumnPtr gen_input_column1<Slice>() {
    auto column = BinaryColumn::create();
    std::vector<Slice> strings{{"ddd"}, {"ddd"}, {"eeeee"}, {"ff"}, {"ff"}, {"ddd"}};
    column->append_strings(strings.data(), strings.size());
    return column;
}

template <>
inline ColumnPtr gen_input_column2<Slice>() {
    auto column2 = BinaryColumn::create();
    std::vector<Slice> strings2{{"kkk"}, {"k"}, {"kk"}, {"kkk"}};
    column2->append_strings(strings2.data(), strings2.size());
    return column2;
}

template <>
inline ColumnPtr gen_input_column1<DecimalV2Value>() {
    auto column = DecimalColumn::create();
    column->append(DecimalV2Value(1));
    column->append(DecimalV2Value(2));
    column->append(DecimalV2Value(3));
    return column;
}

template <>
inline ColumnPtr gen_input_column2<DecimalV2Value>() {
    auto column2 = DecimalColumn::create();
    column2->append(DecimalV2Value(7));
    column2->append(DecimalV2Value(8));
    column2->append(DecimalV2Value(3));
    return column2;
}

template <>
inline ColumnPtr gen_input_column1<TimestampValue>() {
    auto column = TimestampColumn::create();
    for (int j = 0; j < 20; ++j) {
        column->append(TimestampValue::create(2000 + j, 1, 1, 0, 30, 30));
    }
    column->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    column->append(TimestampValue::create(2001, 1, 1, 0, 30, 30));
    return column;
}

template <>
inline ColumnPtr gen_input_column2<TimestampValue>() {
    auto column = TimestampColumn::create();
    for (int j = 0; j < 20; ++j) {
        column->append(TimestampValue::create(1000 + j, 1, 1, 0, 30, 30));
    }
    column->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    column->append(TimestampValue::create(1000, 1, 1, 0, 30, 30));
    return column;
}

template <>
inline ColumnPtr gen_input_column1<DateValue>() {
    auto column = DateColumn::create();
    for (int j = 0; j < 20; ++j) {
        column->append(DateValue::create(2000 + j, 1, 1));
    }
    column->append(DateValue::create(2000, 1, 1));
    column->append(DateValue::create(2001, 1, 1));
    return column;
}

template <>
inline ColumnPtr gen_input_column2<DateValue>() {
    auto column = DateColumn::create();
    for (int j = 0; j < 20; ++j) {
        column->append(DateValue::create(1000 + j, 1, 1));
    }
    column->append(DateValue::create(2000, 1, 1));
    column->append(DateValue::create(1000, 1, 1));
    return column;
}

template <LogicalType LT>
ColumnPtr gen_input_decimal_column2(const FunctionContext::TypeDesc* type_desc) {
    auto column = RunTimeColumnType<LT>::create(type_desc->precision, type_desc->scale);
    for (int i = 2000; i < 3000; i++) {
        column->append(i);
    }
    return column;
}

template <typename T, typename TResult>
void test_agg_function(FunctionContext* ctx, const AggregateFunction* func, TResult update_result1,
                       TResult update_result2, TResult merge_result) {
    int64_t mem_usage = 0;
    ctx->set_mem_usage_counter(&mem_usage);
    using ResultColumn = typename ColumnTraits<TResult>::ColumnType;
    using ResultColumnPtr = typename ColumnTraits<TResult>::ColumnType::Ptr;
    ResultColumnPtr result_column = ResultColumn::create();

    // update input column 1
    auto aggr_state = ManagedAggrState::create(ctx, func);
    ColumnPtr column;
    column = gen_input_column1<T>();
    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, aggr_state->state());
    func->finalize_to_column(ctx, aggr_state->state(), result_column.get());
    ASSERT_EQ(update_result1, result_column->get_data()[0]);

    // update input column 2
    auto aggr_state2 = ManagedAggrState::create(ctx, func);
    ColumnPtr column2;
    column2 = gen_input_column2<T>();
    row_column = column2.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, aggr_state2->state());
    func->finalize_to_column(ctx, aggr_state2->state(), result_column.get());
    ASSERT_EQ(update_result2, result_column->get_data()[1]);

    // merge column 1 and column 2
    ColumnPtr serde_column = BinaryColumn::create();
    std::string func_name = func->get_name();
    if (func_name == "count" || func_name == "sum" || func_name == "maxmin") {
        serde_column = ResultColumn::create();
    }

    func->serialize_to_column(ctx, aggr_state->state(), serde_column.get());
    func->merge(ctx, serde_column.get(), aggr_state2->state(), 0);
    func->finalize_to_column(ctx, aggr_state2->state(), result_column.get());
    ASSERT_EQ(merge_result, result_column->get_data()[2]);
}

template <LogicalType LT, typename TResult = RunTimeCppType<TYPE_DECIMAL128>, typename = DecimalLTGuard<LT>>
void test_decimal_agg_function(FunctionContext* ctx, const AggregateFunction* func, TResult update_result1,
                               TResult update_result2, TResult merge_result) {
    using ResultColumn = RunTimeColumnType<TYPE_DECIMAL128>;
    using ResultColumnPtr = typename RunTimeColumnType<TYPE_DECIMAL128>::Ptr;
    const auto& result_type = ctx->get_return_type();
    ResultColumnPtr result_column = ResultColumn::create(result_type.precision, result_type.scale);

    // update input column 1
    auto aggr_state = ManagedAggrState::create(ctx, func);
    ColumnPtr column = gen_input_decimal_column1<LT>(ctx->get_arg_type(0));
    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, aggr_state->state());
    func->finalize_to_column(ctx, aggr_state->state(), result_column.get());
    ASSERT_EQ(update_result1, result_column->get_data()[0]);

    // update input column 2
    auto aggr_state2 = ManagedAggrState::create(ctx, func);
    ColumnPtr column2 = gen_input_decimal_column2<LT>(ctx->get_arg_type(0));
    row_column = column2.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, aggr_state2->state());
    func->finalize_to_column(ctx, aggr_state2->state(), result_column.get());
    ASSERT_EQ(update_result2, result_column->get_data()[1]);

    // merge column 1 and column 2
    ColumnPtr serde_column = BinaryColumn::create();
    std::string func_name = func->get_name();
    if (func_name == "count" || func_name == "sum" || func_name == "decimal_sum" || func_name == "maxmin") {
        serde_column = ResultColumn::create(result_type.precision, result_type.scale);
    }

    func->serialize_to_column(ctx, aggr_state->state(), serde_column.get());
    func->merge(ctx, serde_column.get(), aggr_state2->state(), 0);
    func->finalize_to_column(ctx, aggr_state2->state(), result_column.get());
    ASSERT_EQ(merge_result, result_column->get_data()[2]);
}

template <typename T, typename TResult>
void test_agg_variance_function(FunctionContext* ctx, const AggregateFunction* func, TResult update_result1,
                                TResult update_result2, TResult merge_result) {
    using ResultColumn = typename ColumnTraits<TResult>::ColumnType;
    using ResultColumnPtr = typename ColumnTraits<TResult>::ColumnType::Ptr;
    ResultColumnPtr result_column = ResultColumn::create();

    auto state = ManagedAggrState::create(ctx, func);
    // update input column 1
    ColumnPtr column = gen_input_column1<T>();
    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state->state());
    func->finalize_to_column(ctx, state->state(), result_column.get());
    ASSERT_EQ(update_result1, result_column->get_data()[0]);

    // update input column 2
    auto state2 = ManagedAggrState::create(ctx, func);
    ColumnPtr column2 = gen_input_column2<T>();
    row_column = column2.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state2->state());
    func->finalize_to_column(ctx, state2->state(), result_column.get());
    ASSERT_EQ(update_result2, result_column->get_data()[1]);

    // merge column 1 and column 2
    ColumnPtr serde_column = BinaryColumn::create();
    func->serialize_to_column(ctx, state->state(), serde_column.get());
    func->merge(ctx, serde_column.get(), state2->state(), 0);
    func->finalize_to_column(ctx, state2->state(), result_column.get());
    ASSERT_TRUE(std::abs(merge_result - result_column->get_data()[2]) < 1e-8);
}

} // namespace starrocks

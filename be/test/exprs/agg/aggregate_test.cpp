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

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/agg/sum.h"
#include "exprs/anyval_util.h"
#include "exprs/arithmetic_operation.h"
#include "exprs/function_context.h"
#include "gen_cpp/Data_types.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/time_types.h"
#include "testutil/function_utils.h"
#include "types/bitmap_value.h"
#include "util/slice.h"
#include "util/thrift_util.h"
#include "util/unaligned_access.h"

namespace starrocks {

class AggregateTest : public testing::Test {
public:
    AggregateTest() = default;

    void SetUp() override {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() override { delete utils; }

private:
    FunctionUtils* utils{};
    FunctionContext* ctx{};
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
ColumnPtr gen_input_column1() {
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
ColumnPtr gen_input_column2() {
    using DataColumn = typename ColumnTraits<T>::ColumnType;
    auto column = DataColumn::create();
    for (int i = 2000; i < 3000; i++) {
        column->append(i);
    }
    return column;
}

template <>
ColumnPtr gen_input_column1<Slice>() {
    auto column = BinaryColumn::create();
    std::vector<Slice> strings{{"ddd"}, {"ddd"}, {"eeeee"}, {"ff"}, {"ff"}, {"ddd"}};
    column->append_strings(strings);
    return column;
}

template <>
ColumnPtr gen_input_column2<Slice>() {
    auto column2 = BinaryColumn::create();
    std::vector<Slice> strings2{{"kkk"}, {"k"}, {"kk"}, {"kkk"}};
    column2->append_strings(strings2);
    return column2;
}

template <>
ColumnPtr gen_input_column1<DecimalV2Value>() {
    auto column = DecimalColumn::create();
    column->append(DecimalV2Value(1));
    column->append(DecimalV2Value(2));
    column->append(DecimalV2Value(3));
    return column;
}

template <>
ColumnPtr gen_input_column2<DecimalV2Value>() {
    auto column2 = DecimalColumn::create();
    column2->append(DecimalV2Value(7));
    column2->append(DecimalV2Value(8));
    column2->append(DecimalV2Value(3));
    return column2;
}

template <>
ColumnPtr gen_input_column1<TimestampValue>() {
    auto column = TimestampColumn::create();
    for (int j = 0; j < 20; ++j) {
        column->append(TimestampValue::create(2000 + j, 1, 1, 0, 30, 30));
    }
    column->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    column->append(TimestampValue::create(2001, 1, 1, 0, 30, 30));
    return column;
}

template <>
ColumnPtr gen_input_column2<TimestampValue>() {
    auto column = TimestampColumn::create();
    for (int j = 0; j < 20; ++j) {
        column->append(TimestampValue::create(1000 + j, 1, 1, 0, 30, 30));
    }
    column->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    column->append(TimestampValue::create(1000, 1, 1, 0, 30, 30));
    return column;
}

template <>
ColumnPtr gen_input_column1<DateValue>() {
    auto column = DateColumn::create();
    for (int j = 0; j < 20; ++j) {
        column->append(DateValue::create(2000 + j, 1, 1));
    }
    column->append(DateValue::create(2000, 1, 1));
    column->append(DateValue::create(2001, 1, 1));
    return column;
}

template <>
ColumnPtr gen_input_column2<DateValue>() {
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
    using ResultColumn = typename ColumnTraits<TResult>::ColumnType;
    auto result_column = ResultColumn::create();

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
    const auto& result_type = ctx->get_return_type();
    auto result_column = ResultColumn::create(result_type.precision, result_type.scale);

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
    auto result_column = ResultColumn::create();

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

TEST_F(AggregateTest, test_count) {
    const AggregateFunction* func = get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, false);
    test_agg_function<int16_t, int64_t>(ctx, func, 1026, 1000, 2026);
    test_agg_function<int32_t, int64_t>(ctx, func, 1026, 1000, 2026);
    test_agg_function<int64_t, int64_t>(ctx, func, 1026, 1000, 2026);
    test_agg_function<int128_t, int64_t>(ctx, func, 1026, 1000, 2026);
    test_agg_function<float, int64_t>(ctx, func, 1026, 1000, 2026);
    test_agg_function<double, int64_t>(ctx, func, 1026, 1000, 2026);
    test_agg_function<DecimalV2Value, int64_t>(ctx, func, 3, 3, 6);
    test_agg_function<Slice, int64_t>(ctx, func, 6, 4, 10);
    test_agg_function<TimestampValue, int64_t>(ctx, func, 22, 22, 44);
    test_agg_function<DateValue, int64_t>(ctx, func, 22, 22, 44);
}

TEST_F(AggregateTest, test_sum) {
    const AggregateFunction* func = get_aggregate_function("sum", TYPE_SMALLINT, TYPE_BIGINT, false);
    test_agg_function<int16_t, int64_t>(ctx, func, 524076, 2499500, 3023576);

    func = get_aggregate_function("sum", TYPE_INT, TYPE_BIGINT, false);
    test_agg_function<int32_t, int64_t>(ctx, func, 524076, 2499500, 3023576);

    func = get_aggregate_function("sum", TYPE_BIGINT, TYPE_BIGINT, false);
    test_agg_function<int64_t, int64_t>(ctx, func, 524076, 2499500, 3023576);

    func = get_aggregate_function("sum", TYPE_LARGEINT, TYPE_LARGEINT, false);
    test_agg_function<int128_t, int128_t>(ctx, func, 524076, 2499500, 3023576);

    func = get_aggregate_function("sum", TYPE_FLOAT, TYPE_DOUBLE, false);
    test_agg_function<float, double>(ctx, func, 524076, 2499500, 3023576);

    func = get_aggregate_function("sum", TYPE_DOUBLE, TYPE_DOUBLE, false);
    test_agg_function<double, double>(ctx, func, 524076, 2499500, 3023576);

    func = get_aggregate_function("sum", TYPE_DECIMALV2, TYPE_DECIMALV2, false);
    test_agg_function<DecimalV2Value, DecimalV2Value>(ctx, func, DecimalV2Value{6}, DecimalV2Value{18},
                                                      DecimalV2Value{24});
}

TEST_F(AggregateTest, test_decimal_sum) {
    {
        const auto* func = get_aggregate_function("decimal_sum", TYPE_DECIMAL32, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL32, .precision = 9, .scale = 9}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 9});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        test_decimal_agg_function<TYPE_DECIMAL32>(ctx, func, 524076, 2499500, 3023576);
    }
    {
        const auto* func = get_aggregate_function("decimal_sum", TYPE_DECIMAL64, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL64, .precision = 9, .scale = 3}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 3});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        test_decimal_agg_function<TYPE_DECIMAL64>(ctx, func, 524076, 2499500, 3023576);
    }
    {
        const auto* func = get_aggregate_function("decimal_sum", TYPE_DECIMAL128, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 15}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 15});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        test_decimal_agg_function<TYPE_DECIMAL128>(ctx, func, 524076, 2499500, 3023576);
    }
}

TEST_F(AggregateTest, test_avg) {
    const AggregateFunction* func = get_aggregate_function("avg", TYPE_SMALLINT, TYPE_DOUBLE, false);
    test_agg_function<int16_t, double>(ctx, func, 524076 / 1026.0, 2499500 / 1000.0, 3023576 / 2026.0);

    func = get_aggregate_function("avg", TYPE_INT, TYPE_DOUBLE, false);
    test_agg_function<int32_t, double>(ctx, func, 524076 / 1026.0, 2499500 / 1000.0, 3023576 / 2026.0);

    func = get_aggregate_function("avg", TYPE_BIGINT, TYPE_DOUBLE, false);
    test_agg_function<int64_t, double>(ctx, func, 524076 / 1026.0, 2499500 / 1000.0, 3023576 / 2026.0);

    func = get_aggregate_function("avg", TYPE_LARGEINT, TYPE_DOUBLE, false);
    test_agg_function<int128_t, double>(ctx, func, 524076 / 1026.0, 2499500 / 1000.0, 3023576 / 2026.0);

    func = get_aggregate_function("avg", TYPE_FLOAT, TYPE_DOUBLE, false);
    test_agg_function<float, double>(ctx, func, 524076 / 1026.0, 2499500 / 1000.0, 3023576 / 2026.0);

    func = get_aggregate_function("avg", TYPE_DOUBLE, TYPE_DOUBLE, false);
    test_agg_function<double, double>(ctx, func, 524076 / 1026.0, 2499500 / 1000.0, 3023576 / 2026.0);

    func = get_aggregate_function("avg", TYPE_DECIMALV2, TYPE_DECIMALV2, false);
    test_agg_function<DecimalV2Value, DecimalV2Value>(ctx, func, DecimalV2Value{2}, DecimalV2Value{6},
                                                      DecimalV2Value{4});

    func = get_aggregate_function("avg", TYPE_DATETIME, TYPE_DATETIME, false);
    test_agg_function<TimestampValue, TimestampValue>(ctx, func, TimestampValue::create(2008, 9, 6, 10, 19, 35),
                                                      TimestampValue::create(1054, 2, 2, 20, 8, 41),
                                                      TimestampValue::create(1531, 5, 22, 15, 14, 9));

    func = get_aggregate_function("avg", TYPE_DATE, TYPE_DATE, false);
    test_agg_function<DateValue, DateValue>(ctx, func, DateValue{2454716}, DateValue{2106058}, DateValue{2280387});
}

TEST_F(AggregateTest, test_decimal_avg) {
    {
        const auto* func = get_aggregate_function("decimal_avg", TYPE_DECIMAL32, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL32, .precision = 9, .scale = 9}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 12});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        auto update_result1 = decimal_div_integer<int128_t>(524076, 1026, 9);
        auto update_result2 = decimal_div_integer<int128_t>(2499500, 1000, 9);
        auto merge_result = decimal_div_integer<int128_t>(3023576, 2026, 9);
        test_decimal_agg_function<TYPE_DECIMAL32>(ctx, func, update_result1, update_result2, merge_result);
    }
    {
        const auto* func = get_aggregate_function("decimal_avg", TYPE_DECIMAL64, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL64, .precision = 9, .scale = 3}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 9});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        auto update_result1 = decimal_div_integer<int128_t>(524076, 1026, 3);
        auto update_result2 = decimal_div_integer<int128_t>(2499500, 1000, 3);
        auto merge_result = decimal_div_integer<int128_t>(3023576, 2026, 3);
        test_decimal_agg_function<TYPE_DECIMAL64>(ctx, func, update_result1, update_result2, merge_result);
    }
    {
        const auto* func = get_aggregate_function("decimal_avg", TYPE_DECIMAL128, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 15}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 15});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        auto update_result1 = decimal_div_integer<int128_t>(524076, 1026, 15);
        auto update_result2 = decimal_div_integer<int128_t>(2499500, 1000, 15);
        auto merge_result = decimal_div_integer<int128_t>(3023576, 2026, 15);
        test_decimal_agg_function<TYPE_DECIMAL128>(ctx, func, update_result1, update_result2, merge_result);
    }
}

TEST_F(AggregateTest, test_variance) {
    double variance_result0;
    {
        double count = 1026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        variance_result0 = variance_sum / count;
    }

    double variance_result1;
    {
        double count = 1000;
        double avg_value = (4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result1 = variance_sum / count;
    }

    double variance_result2;
    {
        double count = 2026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200 + 4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result2 = variance_sum / count;
    }

    {
        const AggregateFunction* func = get_aggregate_function("variance", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }

    {
        const AggregateFunction* func = get_aggregate_function("variance_pop", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }

    {
        const AggregateFunction* func = get_aggregate_function("var_pop", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }
}

TEST_F(AggregateTest, test_variance_samp) {
    double variance_result0;
    {
        double count = 1026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        variance_result0 = variance_sum / (count - 1);
    }

    double variance_result1;
    {
        double count = 1000;
        double avg_value = (4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result1 = variance_sum / (count - 1);
    }

    double variance_result2;
    {
        double count = 2026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200 + 4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result2 = variance_sum / (count - 1);
    }

    {
        const AggregateFunction* func = get_aggregate_function("variance_samp", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }

    {
        const AggregateFunction* func = get_aggregate_function("var_samp", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }
}

TEST_F(AggregateTest, test_stddev) {
    double variance_result0;
    {
        double count = 1026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        variance_result0 = sqrt(variance_sum / count);
    }

    double variance_result1;
    {
        double count = 1000;
        double avg_value = (4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result1 = sqrt(variance_sum / count);
    }

    double variance_result2;
    {
        double count = 2026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200 + 4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result2 = sqrt(variance_sum / count);
    }

    {
        const AggregateFunction* func = get_aggregate_function("stddev", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }

    {
        const AggregateFunction* func = get_aggregate_function("stddev_pop", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }

    {
        const auto* func = get_aggregate_function("std", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }
}

TEST_F(AggregateTest, test_stddev_samp) {
    double variance_result0;
    {
        double count = 1026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        variance_result0 = sqrt(variance_sum / (count - 1));
    }

    double variance_result1;
    {
        double count = 1000;
        double avg_value = (4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result1 = sqrt(variance_sum / (count - 1));
    }

    double variance_result2;
    {
        double count = 2026;
        double avg_value = (1023 * 1024 / 2 + 100 + 200 + 4999 * 1000 / 2) / count;
        double variance_sum = 0;
        for (int i = 0; i < 1024; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_sum += (100 - avg_value) * (100 - avg_value);
        variance_sum += (200 - avg_value) * (200 - avg_value);
        for (int i = 2000; i < 3000; ++i) {
            variance_sum += (i - avg_value) * (i - avg_value);
        }
        variance_result2 = sqrt(variance_sum / (count - 1));
    }

    {
        const AggregateFunction* func = get_aggregate_function("stddev_samp", TYPE_SMALLINT, TYPE_DOUBLE, false);
        test_agg_variance_function<int16_t, double>(ctx, func, variance_result0, variance_result1, variance_result2);
    }
}

TEST_F(AggregateTest, test_maxby) {
    const AggregateFunction* func = get_aggregate_function("max_by", TYPE_VARCHAR, TYPE_INT, false);
    auto result_column = Int32Column::create();
    auto aggr_state = ManagedAggrState::create(ctx, func);
    auto int_column = Int32Column::create();
    for (int i = 0; i < 10; i++) {
        int_column->append(i);
    }
    auto varchar_column = BinaryColumn::create();
    std::vector<Slice> strings{{"aaa"}, {"ddd"}, {"zzzz"}, {"ff"}, {"ff"}, {"ddd"}, {"ddd"}, {"ddd"}, {"ddd"}, {""}};
    varchar_column->append_strings(strings);
    Columns columns;
    columns.emplace_back(int_column);
    columns.emplace_back(varchar_column);
    std::vector<const Column*> raw_columns;
    raw_columns.resize(columns.size());
    for (int i = 0; i < columns.size(); ++i) {
        raw_columns[i] = columns[i].get();
    }
    func->update_batch_single_state(ctx, int_column->size(), raw_columns.data(), aggr_state->state());
    func->finalize_to_column(ctx, aggr_state->state(), result_column.get());
    ASSERT_EQ(2, result_column->get_data()[0]);

    //test nullable column
    func = get_aggregate_function("max_by", TYPE_DECIMALV2, TYPE_DOUBLE, false);
    aggr_state = ManagedAggrState::create(ctx, func);
    auto data_column1 = DoubleColumn::create();
    auto null_column1 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column1->append(i + 0.11);
        null_column1->append(i % 13 ? false : true);
    }
    auto doubleColumn = NullableColumn::create(std::move(data_column1), std::move(null_column1));
    auto data_column2 = DecimalColumn::create();
    auto null_column2 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column2->append(DecimalV2Value(i));
        null_column2->append(i % 11 ? false : true);
    }
    auto decimalColumn = NullableColumn::create(std::move(data_column2), std::move(null_column2));
    auto data_column3 = DoubleColumn::create();
    auto null_column3 = NullColumn::create();
    auto nullable_result_column = NullableColumn::create(std::move(data_column3), std::move(null_column3));
    Columns nullColumns;
    nullColumns.emplace_back(doubleColumn);
    nullColumns.emplace_back(decimalColumn);
    std::vector<const Column*> raw_nullColumns;
    raw_nullColumns.resize(nullColumns.size());
    for (int i = 0; i < nullColumns.size(); ++i) {
        raw_nullColumns[i] = nullColumns[i].get();
    }
    func->update_batch_single_state(ctx, doubleColumn->size(), raw_nullColumns.data(), aggr_state->state());
    func->finalize_to_column(ctx, aggr_state->state(), nullable_result_column.get());
    ASSERT_EQ(98.11, nullable_result_column->data_column()->get(0).get<double>());
}

TEST_F(AggregateTest, test_minby) {
    const AggregateFunction* func = get_aggregate_function("min_by", TYPE_VARCHAR, TYPE_INT, false);
    auto result_column = Int32Column::create();
    auto aggr_state = ManagedAggrState::create(ctx, func);
    auto int_column = Int32Column::create();
    for (int i = 0; i < 10; i++) {
        int_column->append(i);
    }
    auto varchar_column = BinaryColumn::create();
    std::vector<Slice> strings{{"ccc"}, {"aaa"}, {"ddd"}, {"zzzz"}, {"ff"}, {"ff"}, {"ddd"}, {"ddd"}, {"ddd"}, {"ddd"}};
    varchar_column->append_strings(strings);
    Columns columns;
    columns.emplace_back(int_column);
    columns.emplace_back(varchar_column);
    std::vector<const Column*> raw_columns;
    raw_columns.resize(columns.size());
    for (int i = 0; i < columns.size(); ++i) {
        raw_columns[i] = columns[i].get();
    }
    func->update_batch_single_state(ctx, int_column->size(), raw_columns.data(), aggr_state->state());
    func->finalize_to_column(ctx, aggr_state->state(), result_column.get());
    ASSERT_EQ(1, result_column->get_data()[0]);

    //test nullable column
    func = get_aggregate_function("min_by", TYPE_DECIMALV2, TYPE_DOUBLE, false);
    aggr_state = ManagedAggrState::create(ctx, func);
    auto data_column1 = DoubleColumn::create();
    auto null_column1 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column1->append(i + 0.11);
        null_column1->append(i % 13 ? false : true);
    }
    auto doubleColumn = NullableColumn::create(std::move(data_column1), std::move(null_column1));
    auto data_column2 = DecimalColumn::create();
    auto null_column2 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column2->append(DecimalV2Value(i));
        null_column2->append(i % 11 ? false : true);
    }
    auto decimalColumn = NullableColumn::create(std::move(data_column2), std::move(null_column2));
    auto data_column3 = DoubleColumn::create();
    auto null_column3 = NullColumn::create();
    auto nullable_result_column = NullableColumn::create(std::move(data_column3), std::move(null_column3));
    Columns nullColumns;
    nullColumns.emplace_back(doubleColumn);
    nullColumns.emplace_back(decimalColumn);
    std::vector<const Column*> raw_nullColumns;
    raw_nullColumns.resize(nullColumns.size());
    for (int i = 0; i < nullColumns.size(); ++i) {
        raw_nullColumns[i] = nullColumns[i].get();
    }
    func->update_batch_single_state(ctx, doubleColumn->size(), raw_nullColumns.data(), aggr_state->state());
    func->finalize_to_column(ctx, aggr_state->state(), nullable_result_column.get());
    ASSERT_EQ(1.11, nullable_result_column->data_column()->get(0).get<double>());
}

TEST_F(AggregateTest, test_maxby_with_nullable_aggregator) {
    const AggregateFunction* func = get_aggregate_function("max_by", TYPE_VARCHAR, TYPE_INT, true);
    auto* ctx_with_args0 = FunctionContext::create_test_context(
            {FunctionContext::TypeDesc{.type = TYPE_INT}, FunctionContext::TypeDesc{.type = TYPE_VARCHAR}},
            FunctionContext::TypeDesc{.type = TYPE_INT});
    std::unique_ptr<FunctionContext> gc_ctx0(ctx_with_args0);

    auto result_column = Int32Column::create();
    auto aggr_state = ManagedAggrState::create(ctx_with_args0, func);
    auto int_column = Int32Column::create();
    for (int i = 0; i < 10; i++) {
        int_column->append(i);
    }
    auto varchar_column = BinaryColumn::create();
    std::vector<Slice> strings{{"aaa"}, {"ddd"}, {"zzzz"}, {"ff"}, {"ff"}, {"ddd"}, {"ddd"}, {"ddd"}, {"ddd"}, {""}};
    varchar_column->append_strings(strings);
    Columns columns;
    columns.emplace_back(int_column);
    columns.emplace_back(varchar_column);
    std::vector<const Column*> raw_columns;
    raw_columns.resize(columns.size());
    for (int i = 0; i < columns.size(); ++i) {
        raw_columns[i] = columns[i].get();
    }
    func->update_batch_single_state(ctx_with_args0, int_column->size(), raw_columns.data(), aggr_state->state());
    func->finalize_to_column(ctx_with_args0, aggr_state->state(), result_column.get());
    ASSERT_EQ(2, result_column->get_data()[0]);

    //test nullable column
    func = get_aggregate_function("max_by", TYPE_DECIMALV2, TYPE_DOUBLE, true);
    auto* ctx_with_args1 = FunctionContext::create_test_context(
            {FunctionContext::TypeDesc{.type = TYPE_DOUBLE},
             FunctionContext::TypeDesc{.type = TYPE_DECIMALV2, .precision = 38, .scale = 9}},
            FunctionContext::TypeDesc{.type = TYPE_DOUBLE});
    std::unique_ptr<FunctionContext> gc_ctx1(ctx_with_args1);

    aggr_state = ManagedAggrState::create(ctx_with_args1, func);
    auto data_column1 = DoubleColumn::create();
    auto null_column1 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column1->append(i + 0.11);
        null_column1->append(i % 13 ? false : true);
    }
    auto doubleColumn = NullableColumn::create(std::move(data_column1), std::move(null_column1));
    auto data_column2 = DecimalColumn::create();
    auto null_column2 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column2->append(DecimalV2Value(i));
        null_column2->append(i % 11 ? false : true);
    }
    auto decimalColumn = NullableColumn::create(std::move(data_column2), std::move(null_column2));
    auto data_column3 = DoubleColumn::create();
    auto null_column3 = NullColumn::create();
    auto nullable_result_column = NullableColumn::create(std::move(data_column3), std::move(null_column3));
    Columns nullColumns;
    nullColumns.emplace_back(doubleColumn);
    nullColumns.emplace_back(decimalColumn);
    std::vector<const Column*> raw_nullColumns;
    raw_nullColumns.resize(nullColumns.size());
    for (int i = 0; i < nullColumns.size(); ++i) {
        raw_nullColumns[i] = nullColumns[i].get();
    }
    func->update_batch_single_state(ctx_with_args1, doubleColumn->size(), raw_nullColumns.data(), aggr_state->state());
    func->finalize_to_column(ctx_with_args1, aggr_state->state(), nullable_result_column.get());
    ASSERT_EQ(98.11, nullable_result_column->data_column()->get(0).get<double>());
}

TEST_F(AggregateTest, test_minby_with_nullable_aggregator) {
    const AggregateFunction* func = get_aggregate_function("min_by", TYPE_VARCHAR, TYPE_INT, true);
    auto* ctx_with_args0 = FunctionContext::create_test_context(
            {FunctionContext::TypeDesc{.type = TYPE_INT}, FunctionContext::TypeDesc{.type = TYPE_VARCHAR}},
            FunctionContext::TypeDesc{.type = TYPE_INT});
    std::unique_ptr<FunctionContext> gc_ctx0(ctx_with_args0);

    auto result_column = Int32Column::create();
    auto aggr_state = ManagedAggrState::create(ctx_with_args0, func);
    auto int_column = Int32Column::create();
    for (int i = 0; i < 10; i++) {
        int_column->append(i);
    }
    auto varchar_column = BinaryColumn::create();
    std::vector<Slice> strings{{"xxx"}, {"aaa"}, {"ddd"}, {"zzzz"}, {"ff"}, {"ff"}, {"ddd"}, {"ddd"}, {"ddd"}, {"ddd"}};
    varchar_column->append_strings(strings);
    Columns columns;
    columns.emplace_back(int_column);
    columns.emplace_back(varchar_column);
    std::vector<const Column*> raw_columns;
    raw_columns.resize(columns.size());
    for (int i = 0; i < columns.size(); ++i) {
        raw_columns[i] = columns[i].get();
    }
    func->update_batch_single_state(ctx_with_args0, int_column->size(), raw_columns.data(), aggr_state->state());
    func->finalize_to_column(ctx_with_args0, aggr_state->state(), result_column.get());
    ASSERT_EQ(1, result_column->get_data()[0]);

    //test nullable column
    func = get_aggregate_function("min_by", TYPE_DECIMALV2, TYPE_DOUBLE, true);
    auto* ctx_with_args1 = FunctionContext::create_test_context(
            {FunctionContext::TypeDesc{.type = TYPE_DOUBLE},
             FunctionContext::TypeDesc{.type = TYPE_DECIMALV2, .precision = 38, .scale = 9}},
            FunctionContext::TypeDesc{.type = TYPE_DOUBLE});
    std::unique_ptr<FunctionContext> gc_ctx1(ctx_with_args1);

    aggr_state = ManagedAggrState::create(ctx_with_args1, func);
    auto data_column1 = DoubleColumn::create();
    auto null_column1 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column1->append(i + 0.11);
        null_column1->append(i % 13 ? false : true);
    }
    auto doubleColumn = NullableColumn::create(std::move(data_column1), std::move(null_column1));
    auto data_column2 = DecimalColumn::create();
    auto null_column2 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column2->append(DecimalV2Value(i));
        null_column2->append(i % 11 ? false : true);
    }
    auto decimalColumn = NullableColumn::create(std::move(data_column2), std::move(null_column2));
    auto data_column3 = DoubleColumn::create();
    auto null_column3 = NullColumn::create();
    auto nullable_result_column = NullableColumn::create(std::move(data_column3), std::move(null_column3));
    Columns nullColumns;
    nullColumns.emplace_back(doubleColumn);
    nullColumns.emplace_back(decimalColumn);
    std::vector<const Column*> raw_nullColumns;
    raw_nullColumns.resize(nullColumns.size());
    for (int i = 0; i < nullColumns.size(); ++i) {
        raw_nullColumns[i] = nullColumns[i].get();
    }
    func->update_batch_single_state(ctx_with_args1, doubleColumn->size(), raw_nullColumns.data(), aggr_state->state());
    func->finalize_to_column(ctx_with_args1, aggr_state->state(), nullable_result_column.get());
    ASSERT_EQ(1.11, nullable_result_column->data_column()->get(0).get<double>());
}

TEST_F(AggregateTest, test_max) {
    const AggregateFunction* func = get_aggregate_function("max", TYPE_SMALLINT, TYPE_SMALLINT, false);
    test_agg_function<int16_t, int16_t>(ctx, func, 1023, 2999, 2999);

    func = get_aggregate_function("max", TYPE_INT, TYPE_INT, false);
    test_agg_function<int32_t, int32_t>(ctx, func, 1023, 2999, 2999);

    func = get_aggregate_function("max", TYPE_BIGINT, TYPE_BIGINT, false);
    test_agg_function<int64_t, int64_t>(ctx, func, 1023, 2999, 2999);

    func = get_aggregate_function("max", TYPE_LARGEINT, TYPE_LARGEINT, false);
    test_agg_function<int128_t, int128_t>(ctx, func, 1023, 2999, 2999);

    func = get_aggregate_function("max", TYPE_FLOAT, TYPE_FLOAT, false);
    test_agg_function<float, float>(ctx, func, 1023, 2999, 2999);

    func = get_aggregate_function("max", TYPE_DOUBLE, TYPE_DOUBLE, false);
    test_agg_function<double, double>(ctx, func, 1023, 2999, 2999);

    func = get_aggregate_function("max", TYPE_VARCHAR, TYPE_VARCHAR, false);
    test_agg_function<Slice, Slice>(ctx, func, {"ff"}, {"kkk"}, {"kkk"});

    func = get_aggregate_function("max", TYPE_DECIMALV2, TYPE_DECIMALV2, false);
    test_agg_function<DecimalV2Value, DecimalV2Value>(ctx, func, DecimalV2Value{3}, DecimalV2Value{8},
                                                      DecimalV2Value{8});

    func = get_aggregate_function("max", TYPE_DATETIME, TYPE_DATETIME, false);
    test_agg_function<TimestampValue, TimestampValue>(ctx, func, TimestampValue::create(2019, 1, 1, 0, 30, 30),
                                                      TimestampValue::create(2000, 1, 1, 0, 30, 30),
                                                      TimestampValue::create(2019, 1, 1, 0, 30, 30));

    func = get_aggregate_function("max", TYPE_DATE, TYPE_DATE, false);
    test_agg_function<DateValue, DateValue>(ctx, func, DateValue::create(2019, 1, 1), DateValue::create(2000, 1, 1),
                                            DateValue::create(2019, 1, 1));
} // namespace starrocks

TEST_F(AggregateTest, test_min) {
    const AggregateFunction* func = get_aggregate_function("min", TYPE_SMALLINT, TYPE_SMALLINT, false);
    test_agg_function<int16_t, int16_t>(ctx, func, 0, 2000, 0);

    func = get_aggregate_function("min", TYPE_INT, TYPE_INT, false);
    test_agg_function<int32_t, int32_t>(ctx, func, 0, 2000, 0);

    func = get_aggregate_function("min", TYPE_BIGINT, TYPE_BIGINT, false);
    test_agg_function<int64_t, int64_t>(ctx, func, 0, 2000, 0);

    func = get_aggregate_function("min", TYPE_LARGEINT, TYPE_LARGEINT, false);
    test_agg_function<int128_t, int128_t>(ctx, func, 0, 2000, 0);

    func = get_aggregate_function("min", TYPE_FLOAT, TYPE_FLOAT, false);
    test_agg_function<float, float>(ctx, func, 0, 2000, 0);

    func = get_aggregate_function("min", TYPE_DOUBLE, TYPE_DOUBLE, false);
    test_agg_function<double, double>(ctx, func, 0, 2000, 0);

    func = get_aggregate_function("min", TYPE_VARCHAR, TYPE_VARCHAR, false);
    test_agg_function<Slice, Slice>(ctx, func, {"ddd"}, {"k"}, {"ddd"});

    func = get_aggregate_function("min", TYPE_DECIMALV2, TYPE_DECIMALV2, false);
    test_agg_function<DecimalV2Value, DecimalV2Value>(ctx, func, DecimalV2Value{1}, DecimalV2Value{3},
                                                      DecimalV2Value{1});

    func = get_aggregate_function("min", TYPE_DATETIME, TYPE_DATETIME, false);
    test_agg_function<TimestampValue, TimestampValue>(ctx, func, TimestampValue::create(2000, 1, 1, 0, 30, 30),
                                                      TimestampValue::create(1000, 1, 1, 0, 30, 30),
                                                      TimestampValue::create(1000, 1, 1, 0, 30, 30));

    func = get_aggregate_function("min", TYPE_DATE, TYPE_DATE, false);
    test_agg_function<DateValue, DateValue>(ctx, func, DateValue::create(2000, 1, 1), DateValue::create(1000, 1, 1),
                                            DateValue::create(1000, 1, 1));
}

//TEST_F(AggregateTest, test_bitmap_count) {
//    const AggregateFunction* func = get_aggregate_function("bitmap_union_int", TYPE_SMALLINT, TYPE_BIGINT, false);
//    test_agg_function<int16_t, int64_t>(ctx, func, 1024, 1000, 2024);
//
//    func = get_aggregate_function("bitmap_union_int", TYPE_INT, TYPE_BIGINT, false);
//    test_agg_function<int32_t, int64_t>(ctx, func, 1024, 1000, 2024);
//
//    func = get_aggregate_function("bitmap_union_int", TYPE_BIGINT, TYPE_BIGINT, false);
//    test_agg_function<int64_t, int64_t>(ctx, func, 1024, 1000, 2024);
//}

TEST_F(AggregateTest, test_count_distinct) {
    const AggregateFunction* func = get_aggregate_function("multi_distinct_count", TYPE_SMALLINT, TYPE_BIGINT, false);
    test_agg_function<int16_t, int64_t>(ctx, func, 1024, 1000, 2024);

    func = get_aggregate_function("multi_distinct_count", TYPE_INT, TYPE_BIGINT, false);
    test_agg_function<int32_t, int64_t>(ctx, func, 1024, 1000, 2024);

    func = get_aggregate_function("multi_distinct_count", TYPE_BIGINT, TYPE_BIGINT, false);
    test_agg_function<int64_t, int64_t>(ctx, func, 1024, 1000, 2024);

    func = get_aggregate_function("multi_distinct_count", TYPE_LARGEINT, TYPE_BIGINT, false);
    test_agg_function<int128_t, int64_t>(ctx, func, 1024, 1000, 2024);

    func = get_aggregate_function("multi_distinct_count", TYPE_FLOAT, TYPE_BIGINT, false);
    test_agg_function<float, int64_t>(ctx, func, 1024, 1000, 2024);

    func = get_aggregate_function("multi_distinct_count", TYPE_DOUBLE, TYPE_BIGINT, false);
    test_agg_function<double, int64_t>(ctx, func, 1024, 1000, 2024);

    func = get_aggregate_function("multi_distinct_count", TYPE_VARCHAR, TYPE_BIGINT, false);
    test_agg_function<Slice, int64_t>(ctx, func, 3, 3, 6);

    func = get_aggregate_function("multi_distinct_count", TYPE_DECIMALV2, TYPE_BIGINT, false);
    test_agg_function<DecimalV2Value, int64_t>(ctx, func, 3, 3, 5);

    func = get_aggregate_function("multi_distinct_count", TYPE_DATETIME, TYPE_BIGINT, false);
    test_agg_function<TimestampValue, int64_t>(ctx, func, 20, 21, 40);

    func = get_aggregate_function("multi_distinct_count", TYPE_DATE, TYPE_BIGINT, false);
    test_agg_function<DateValue, int64_t>(ctx, func, 20, 21, 40);
}

TEST_F(AggregateTest, test_sum_distinct) {
    const AggregateFunction* func = get_aggregate_function("multi_distinct_sum", TYPE_SMALLINT, TYPE_BIGINT, false);
    test_agg_function<int16_t, int64_t>(ctx, func, 523776, 2499500, 3023276);

    func = get_aggregate_function("multi_distinct_sum", TYPE_INT, TYPE_BIGINT, false);
    test_agg_function<int32_t, int64_t>(ctx, func, 523776, 2499500, 3023276);

    func = get_aggregate_function("multi_distinct_sum", TYPE_BIGINT, TYPE_BIGINT, false);
    test_agg_function<int64_t, int64_t>(ctx, func, 523776, 2499500, 3023276);

    func = get_aggregate_function("multi_distinct_sum", TYPE_LARGEINT, TYPE_LARGEINT, false);
    test_agg_function<int128_t, int128_t>(ctx, func, 523776, 2499500, 3023276);

    func = get_aggregate_function("multi_distinct_sum", TYPE_FLOAT, TYPE_DOUBLE, false);
    test_agg_function<float, double>(ctx, func, 523776, 2499500, 3023276);

    func = get_aggregate_function("multi_distinct_sum", TYPE_DOUBLE, TYPE_DOUBLE, false);
    test_agg_function<double, double>(ctx, func, 523776, 2499500, 3023276);

    func = get_aggregate_function("multi_distinct_sum", TYPE_DECIMALV2, TYPE_DECIMALV2, false);
    test_agg_function<DecimalV2Value, DecimalV2Value>(ctx, func, DecimalV2Value(6), DecimalV2Value(18),
                                                      DecimalV2Value(21));
}

TEST_F(AggregateTest, test_decimal_multi_distinct_sum) {
    {
        const auto* func = get_aggregate_function("decimal_multi_distinct_sum", TYPE_DECIMAL32, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL32, .precision = 9, .scale = 9}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 9});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        test_decimal_agg_function<TYPE_DECIMAL32>(ctx, func, 523776, 2499500, 3023276);
    }
    {
        const auto* func = get_aggregate_function("decimal_multi_distinct_sum", TYPE_DECIMAL64, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL64, .precision = 9, .scale = 3}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 3});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        test_decimal_agg_function<TYPE_DECIMAL64>(ctx, func, 523776, 2499500, 3023276);
    }
    {
        const auto* func = get_aggregate_function("decimal_multi_distinct_sum", TYPE_DECIMAL128, TYPE_DECIMAL128, false,
                                                  TFunctionBinaryType::BUILTIN, 3);
        auto* ctx = FunctionContext::create_test_context(
                {FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 15}},
                FunctionContext::TypeDesc{.type = TYPE_DECIMAL128, .precision = 38, .scale = 15});
        std::unique_ptr<FunctionContext> gc_ctx(ctx);
        test_decimal_agg_function<TYPE_DECIMAL128>(ctx, func, 523776, 2499500, 3023276);
    }
}

TEST_F(AggregateTest, test_window_funnel) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_BIGINT)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATETIME)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_INT)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_ARRAY))};

    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_INT));
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* func = get_aggregate_function("window_funnel", TYPE_DATETIME, TYPE_INT, false);
    ColumnBuilder<TYPE_BOOLEAN> builder(config::vector_chunk_size);
    // Type.BIGINT, Type.DATETIME, Type.INT, Type.ARRAY_BOOLEAN
    builder.append(true);
    builder.append(true);
    builder.append(true);
    builder.append(true);
    builder.append(true);
    builder.append(true);
    auto data_col = NullableColumn::create(builder.build(false), NullColumn::create(6, 0));

    auto offsets = UInt32Column::create();
    offsets->append(2);                                    // [true, true]
    offsets->append(4);                                    // [true, true]
    offsets->append(6);                                    // [true, true]
    auto column4 = ArrayColumn::create(data_col, offsets); // array_column, 4th column

    auto column1 = Int64Column::create(); // first column, but there use const.
    column1->append(1800);
    // column1->append(1800);

    auto column2 = TimestampColumn::create();
    column2->append(TimestampValue::create(2022, 6, 10, 12, 30, 30)); // 2nd column.

    auto const_column1 = ColumnHelper::create_const_column<TYPE_BIGINT>(1800, 1);
    auto column3 = ColumnHelper::create_const_column<TYPE_INT>(2, 1);
    Columns const_columns;
    const_columns.emplace_back(const_column1); // first column
    const_columns.emplace_back(column3);
    const_columns.emplace_back(column3); // 3rd const column
    local_ctx->set_constant_columns(const_columns);

    std::vector<const Column*> raw_column; // to column list.
    raw_column.resize(4);
    raw_column[0] = column1.get();
    raw_column[1] = column2.get();
    raw_column[2] = column3.get();
    raw_column[3] = column4.get();

    using ResultColumn = typename ColumnTraits<int32_t>::ColumnType;
    auto result_column = ResultColumn::create();
    auto state = ManagedAggrState::create(ctx, func);
    func->update(local_ctx.get(), raw_column.data(), state->state(), 0);
    func->finalize_to_column(local_ctx.get(), state->state(), result_column.get());
    // args: (1800, timestamp, 0, [true, true])
    // only have one row, but condition is all true, so result is 2.
    ASSERT_EQ(2, result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_dict_merge) {
    const AggregateFunction* func = get_aggregate_function("dict_merge", TYPE_ARRAY, TYPE_VARCHAR, false);
    ColumnBuilder<TYPE_VARCHAR> builder(config::vector_chunk_size);
    builder.append(Slice("key1"));
    builder.append(Slice("key2"));
    builder.append(Slice("starrocks-1"));
    builder.append(Slice("starrocks-starrocks"));
    builder.append(Slice("starrocks-starrocks"));
    auto data_col = NullableColumn::create(builder.build(false), NullColumn::create(5, 0));

    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(0);
    offsets->append(2);
    offsets->append(5);
    // []
    // [key1, key2]
    // [sr-1, sr-2, sr-3]
    auto col = ArrayColumn::create(data_col, offsets);
    const Column* column = col.get();
    auto state = ManagedAggrState::create(ctx, func);
    func->update_batch_single_state(ctx, col->size(), &column, state->state());

    auto res = BinaryColumn::create();
    func->finalize_to_column(ctx, state->state(), res.get());

    ASSERT_EQ(res->size(), 1);
    auto slice = res->get_slice(0);
    std::map<int, std::string> datas;
    auto dict = from_json_string<TGlobalDict>(std::string(slice.data, slice.size));
    int sz = dict.ids.size();
    for (int i = 0; i < sz; ++i) {
        datas.emplace(dict.ids[i], dict.strings[i]);
    }
    ASSERT_EQ(dict.ids.size(), dict.strings.size());

    std::set<std::string> origin_data;
    std::set<int> ids;
    auto binary_column = down_cast<BinaryColumn*>(data_col->data_column().get());
    for (int i = 0; i < binary_column->size(); ++i) {
        auto slice = binary_column->get_slice(i);
        origin_data.emplace(slice.data, slice.size);
    }

    for (const auto& [k, v] : datas) {
        ASSERT_TRUE(origin_data.count(v) != 0);
        origin_data.erase(v);
    }

    ASSERT_TRUE(origin_data.empty());
}

TEST_F(AggregateTest, test_sum_nullable) {
    using NullableSumInt64 = NullableAggregateFunctionState<SumAggregateState<int64_t>, false>;
    const AggregateFunction* sum_null = get_aggregate_function("sum", TYPE_INT, TYPE_BIGINT, true);
    auto state = ManagedAggrState::create(ctx, sum_null);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column->append(i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();

    // test update
    sum_null->update_batch_single_state(ctx, column->size(), &row_column, state->state());
    auto* null_state = (NullableSumInt64*)state->state();
    int64_t result = *reinterpret_cast<const int64_t*>(null_state->nested_state());
    ASSERT_EQ(2450, result);

    // test serialize
    auto serde_column2 = NullableColumn::create(Int64Column::create(), NullColumn::create());
    sum_null->serialize_to_column(ctx, state->state(), serde_column2.get());

    // test merge
    auto state2 = ManagedAggrState::create(ctx, sum_null);

    auto data_column2 = Int32Column::create();
    auto null_column2 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column2->append(i);
        null_column2->append(i % 2 ? 0 : 1);
    }
    auto column2 = NullableColumn::create(std::move(data_column2), std::move(null_column2));
    const Column* row_column2 = column2.get();

    sum_null->update_batch_single_state(ctx, column2->size(), &row_column2, state2->state());

    sum_null->merge(ctx, serde_column2.get(), state2->state(), 0);

    auto result_column = NullableColumn::create(Int64Column::create(), NullColumn::create());
    sum_null->finalize_to_column(ctx, state2->state(), result_column.get());

    const Column& result_data_column = result_column->data_column_ref();
    const auto& result_data = static_cast<const Int64Column&>(result_data_column);
    ASSERT_EQ(4950, result_data.get_data()[0]);
}

TEST_F(AggregateTest, test_count_nullable) {
    const AggregateFunction* func = get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, true);
    auto state = ManagedAggrState::create(ctx, func);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();

    for (int i = 0; i < 1024; i++) {
        data_column->append(i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));

    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, column->size(), &row_column, state->state());

    int64_t result = *reinterpret_cast<int64_t*>(state->state());
    ASSERT_EQ(512, result);
}

TEST_F(AggregateTest, test_bitmap_nullable) {
    const AggregateFunction* bitmap_null = get_aggregate_function("bitmap_union_int", TYPE_INT, TYPE_BIGINT, true);
    auto state = ManagedAggrState::create(ctx, bitmap_null);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();

    for (int i = 0; i < 100; i++) {
        data_column->append(i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();

    // test update
    bitmap_null->update_batch_single_state(ctx, column->size(), &row_column, state->state());

    auto result_column = NullableColumn::create(Int64Column::create(), NullColumn::create());
    bitmap_null->finalize_to_column(ctx, state->state(), result_column.get());

    const Column& result_data_column = result_column->data_column_ref();
    const auto& result_data = static_cast<const Int64Column&>(result_data_column);
    ASSERT_EQ(false, result_column->is_null(0));
    ASSERT_EQ(50, result_data.get_data()[0]);
}

TEST_F(AggregateTest, test_group_concat) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("group_concat", TYPE_VARCHAR, TYPE_VARCHAR, false);
    auto state = ManagedAggrState::create(ctx, group_concat_function);

    auto data_column = BinaryColumn::create();

    for (int i = 0; i < 6; i++) {
        std::string val("starrocks");
        val.append(std::to_string(i));
        data_column->append(val);
    }

    const Column* row_column = data_column.get();

    // test update
    group_concat_function->update_batch_single_state(ctx, data_column->size(), &row_column, state->state());

    auto result_column = BinaryColumn::create();
    group_concat_function->finalize_to_column(ctx, state->state(), result_column.get());

    ASSERT_EQ("starrocks0, starrocks1, starrocks2, starrocks3, starrocks4, starrocks5", result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_group_concat_const_seperator) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_VARCHAR)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_VARCHAR))};

    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_VARCHAR));
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* group_concat_function =
            get_aggregate_function("group_concat", TYPE_VARCHAR, TYPE_VARCHAR, false);
    auto state = ManagedAggrState::create(ctx, group_concat_function);

    auto data_column = BinaryColumn::create();

    data_column->append("abc");
    data_column->append("bcd");
    data_column->append("cde");
    data_column->append("def");
    data_column->append("efg");
    data_column->append("fgh");
    data_column->append("ghi");
    data_column->append("hij");
    data_column->append("ijk");

    auto separator_column = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);

    std::vector<const Column*> raw_columns;
    raw_columns.resize(2);
    raw_columns[0] = data_column.get();
    raw_columns[1] = separator_column.get();

    Columns const_columns;
    const_columns.emplace_back(data_column);
    const_columns.emplace_back(separator_column);
    local_ctx->set_constant_columns(const_columns);

    // test update
    group_concat_function->update_batch_single_state(local_ctx.get(), data_column->size(), raw_columns.data(),
                                                     state->state());

    auto result_column = BinaryColumn::create();
    group_concat_function->finalize_to_column(local_ctx.get(), state->state(), result_column.get());

    ASSERT_EQ("abcbcdcdedefefgfghghihijijk", result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_percentile_cont) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DOUBLE)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DOUBLE))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DOUBLE));
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* func = get_aggregate_function("percentile_cont", TYPE_DOUBLE, TYPE_DOUBLE, false);

    // update input column 1
    auto state1 = ManagedAggrState::create(ctx, func);

    auto data_column1 = DoubleColumn::create();
    auto const_colunm1 = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.1, 1);
    data_column1->append(3.0);
    data_column1->append(3.0);

    std::vector<const Column*> raw_columns1;
    raw_columns1.resize(2);
    raw_columns1[0] = data_column1.get();
    raw_columns1[1] = const_colunm1.get();

    func->update_batch_single_state(local_ctx.get(), data_column1->size(), raw_columns1.data(), state1->state());

    // update input column 2
    auto state2 = ManagedAggrState::create(ctx, func);

    auto data_column2 = DoubleColumn::create();
    auto const_colunm2 = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.1, 1);
    data_column2->append(6.0);

    std::vector<const Column*> raw_columns2;
    raw_columns2.resize(2);
    raw_columns2[0] = data_column2.get();
    raw_columns2[1] = const_colunm2.get();

    func->update_batch_single_state(local_ctx.get(), data_column2->size(), raw_columns2.data(), state2->state());

    // merge column 1 and column 2
    ColumnPtr serde_column = BinaryColumn::create();
    auto result_column = DoubleColumn::create();
    func->serialize_to_column(local_ctx.get(), state1->state(), serde_column.get());
    func->merge(local_ctx.get(), serde_column.get(), state2->state(), 0);
    func->finalize_to_column(local_ctx.get(), state2->state(), result_column.get());

    ASSERT_EQ(3, result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_percentile_disc) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DOUBLE)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DOUBLE))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DOUBLE));
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* func = get_aggregate_function("percentile_disc", TYPE_DOUBLE, TYPE_DOUBLE, false);

    // update input column 1
    auto state1 = ManagedAggrState::create(ctx, func);

    auto data_column1 = DoubleColumn::create();
    auto const_colunm1 = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.1, 1);
    data_column1->append(3.0);
    data_column1->append(4.0);

    std::vector<const Column*> raw_columns1;
    raw_columns1.resize(2);
    raw_columns1[0] = data_column1.get();
    raw_columns1[1] = const_colunm1.get();

    func->update_batch_single_state(local_ctx.get(), data_column1->size(), raw_columns1.data(), state1->state());

    // update input column 2
    auto state2 = ManagedAggrState::create(ctx, func);

    auto data_column2 = DoubleColumn::create();
    auto const_colunm2 = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.1, 1);
    data_column2->append(6.0);

    std::vector<const Column*> raw_columns2;
    raw_columns2.resize(2);
    raw_columns2[0] = data_column2.get();
    raw_columns2[1] = const_colunm2.get();

    func->update_batch_single_state(local_ctx.get(), data_column2->size(), raw_columns2.data(), state2->state());

    // merge column 1 and column 2
    ColumnPtr serde_column = BinaryColumn::create();
    auto result_column = DoubleColumn::create();
    func->serialize_to_column(local_ctx.get(), state1->state(), serde_column.get());
    func->merge(local_ctx.get(), serde_column.get(), state2->state(), 0);
    func->finalize_to_column(local_ctx.get(), state2->state(), result_column.get());

    // [3,4,6], rate = 0.1 -> 4
    ASSERT_EQ(4, result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_intersect_count) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("intersect_count", TYPE_INT, TYPE_BIGINT, false);
    auto state = ManagedAggrState::create(ctx, group_concat_function);

    auto data_column = BitmapColumn::create();
    auto int_column = Int32Column::create();
    auto int_const1 = ColumnHelper::create_const_column<TYPE_INT>(1, 1);
    auto int_const2 = ColumnHelper::create_const_column<TYPE_INT>(2, 1);

    BitmapValue b1;
    b1.add(1);
    data_column->append(&b1);
    int_column->append(1);

    BitmapValue b2;
    b2.add(2);
    b2.add(1);
    data_column->append(&b2);
    int_column->append(2);

    BitmapValue b3;
    b3.add(1);
    b3.add(3);
    data_column->append(&b3);
    int_column->append(3);

    Columns columns;
    columns.emplace_back(data_column);
    columns.emplace_back(int_column);
    columns.emplace_back(int_const1);
    columns.emplace_back(int_const2);

    std::vector<const Column*> raw_columns;
    raw_columns.resize(columns.size());
    for (int i = 0; i < columns.size(); ++i) {
        raw_columns[i] = columns[i].get();
    }

    Columns const_columns;
    const_columns.emplace_back(nullptr);
    const_columns.emplace_back(nullptr);
    const_columns.emplace_back(int_const1);
    const_columns.emplace_back(int_const2);
    ctx->set_constant_columns(const_columns);

    // test update
    group_concat_function->update_batch_single_state(ctx, data_column->size(), raw_columns.data(), state->state());

    auto result_column = Int64Column::create();
    group_concat_function->finalize_to_column(ctx, state->state(), result_column.get());

    ASSERT_EQ(1, result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_bitmap_intersect) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("bitmap_intersect", TYPE_OBJECT, TYPE_OBJECT, false);
    auto state = ManagedAggrState::create(ctx, group_concat_function);

    auto data_column = BitmapColumn::create();

    BitmapValue b1;
    b1.add(1);
    data_column->append(&b1);

    BitmapValue b2;
    b2.add(2);
    b2.add(1);
    data_column->append(&b2);

    BitmapValue b3;
    b3.add(1);
    b3.add(3);
    data_column->append(&b3);

    const Column* row_column = data_column.get();

    // test update
    group_concat_function->update_batch_single_state(ctx, data_column->size(), &row_column, state->state());

    auto result_column = BitmapColumn::create();
    group_concat_function->finalize_to_column(ctx, state->state(), result_column.get());

    ASSERT_EQ("1", result_column->get_pool()[0].to_string());
}

template <typename T>
ColumnPtr gen_histogram_column() {
    using DataColumn = typename ColumnTraits<T>::ColumnType;
    auto column = DataColumn::create();
    for (int i = 0; i < 1024; i++) {
        if (i == 100) {
            column->append(100);
        }
        if (i == 200) {
            column->append(200);
        }
        column->append(i);
    }
    return column;
}

TEST_F(AggregateTest, test_histogram) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_BIGINT)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_INT)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DOUBLE)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_INT))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_VARCHAR));
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* histogram_function = get_aggregate_function("histogram", TYPE_BIGINT, TYPE_VARCHAR, true);
    auto state = ManagedAggrState::create(ctx, histogram_function);

    auto data_column = gen_histogram_column<int64_t>();
    auto const1 = ColumnHelper::create_const_column<TYPE_INT>(10, data_column->size());
    auto const2 = ColumnHelper::create_const_column<TYPE_DOUBLE>(1, data_column->size());
    auto const3 = ColumnHelper::create_const_column<TYPE_INT>(2, data_column->size());

    Columns const_columns;
    const_columns.emplace_back(data_column);
    const_columns.emplace_back(const1); // first column
    const_columns.emplace_back(const2);
    const_columns.emplace_back(const3); // 3rd const column
    local_ctx->set_constant_columns(const_columns);

    std::vector<const Column*> raw_columns;
    raw_columns.resize(4);
    raw_columns[0] = data_column.get();
    raw_columns[1] = const1.get();
    raw_columns[2] = const2.get();
    raw_columns[3] = const3.get();
    for (int i = 0; i < data_column->size(); ++i) {
        histogram_function->update(local_ctx.get(), raw_columns.data(), state->state(), i);
    }

    auto result_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    histogram_function->finalize_to_column(local_ctx.get(), state->state(), result_column.get());
    ASSERT_EQ(
            "['[[\"0\",\"100\",\"102\",\"2\"],[\"101\",\"201\",\"204\",\"1\"],[\"202\",\"303\",\"306\",\"1\"],[\"304\","
            "\"405\",\"408\",\"1\"],[\"406\",\"507\",\"510\",\"1\"],[\"508\",\"609\",\"612\",\"1\"],[\"610\",\"711\","
            "\"714\",\"1\"],[\"712\",\"813\",\"816\",\"1\"],[\"814\",\"915\",\"918\",\"1\"],[\"916\",\"1017\",\"1020\","
            "\"1\"],[\"1018\",\"1023\",\"1026\",\"1\"]]']",
            result_column->debug_string());
}

TEST_F(AggregateTest, test_bitmap_intersect_nullable) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("bitmap_intersect", TYPE_OBJECT, TYPE_OBJECT, true);
    auto state = ManagedAggrState::create(ctx, group_concat_function);

    auto data_column = BitmapColumn::create();
    auto null_column = NullColumn::create();

    BitmapValue b1;
    b1.add(1);
    data_column->append(&b1);
    null_column->append(false);

    BitmapValue b2;
    b2.add(2);
    b2.add(1);
    data_column->append(&b2);
    null_column->append(false);

    BitmapValue b3;
    b3.add(1);
    b3.add(3);
    data_column->append(&b3);
    null_column->append(false);

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();

    // test update
    group_concat_function->update_batch_single_state(ctx, column->size(), &row_column, state->state());

    auto result_column = NullableColumn::create(BitmapColumn::create(), NullColumn::create());
    group_concat_function->finalize_to_column(ctx, state->state(), result_column.get());

    const Column& result_data_column = result_column->data_column_ref();
    const auto& result_data = static_cast<const BitmapColumn&>(result_data_column);

    ASSERT_EQ("1", result_data.get_pool()[0].to_string());
}

template <typename T, typename TResult>
void test_non_deterministic_agg_function(FunctionContext* ctx, const AggregateFunction* func) {
    using ResultColumn = typename ColumnTraits<TResult>::ColumnType;
    using ExpeactedResultColumnType = typename ColumnTraits<T>::ColumnType;
    auto state = ManagedAggrState::create(ctx, func);
    // update input column 1
    auto result_column1 = ResultColumn::create();
    ColumnPtr column = gen_input_column1<T>();
    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state->state());
    func->finalize_to_column(ctx, state->state(), result_column1.get());

    auto expected_column1 = down_cast<const ExpeactedResultColumnType&>(row_column[0]);
    ASSERT_EQ(expected_column1.get_data()[0], result_column1->get_data()[0]);

    // update input column 2
    auto result_column2 = ResultColumn::create();
    auto state2 = ManagedAggrState::create(ctx, func);
    ColumnPtr column2 = gen_input_column2<T>();
    row_column = column2.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state2->state());
    func->finalize_to_column(ctx, state2->state(), result_column2.get());

    auto expected_column2 = down_cast<const ExpeactedResultColumnType&>(row_column[0]);
    ASSERT_EQ(expected_column2.get_data()[0], result_column2->get_data()[0]);

    // merge column 1 and column 2
    auto final_result_column = ResultColumn::create();
    func->serialize_to_column(ctx, state->state(), final_result_column.get());
    func->merge(ctx, final_result_column.get(), state2->state(), 0);
    func->finalize_to_column(ctx, state2->state(), final_result_column.get());

    ASSERT_EQ(final_result_column->get_data()[0], result_column1->get_data()[0]);
}

TEST_F(AggregateTest, test_any_value) {
    const AggregateFunction* func = get_aggregate_function("any_value", TYPE_SMALLINT, TYPE_SMALLINT, false);
    test_non_deterministic_agg_function<int16_t, int16_t>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_INT, TYPE_INT, false);
    test_non_deterministic_agg_function<int32_t, int32_t>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_BIGINT, TYPE_BIGINT, false);
    test_non_deterministic_agg_function<int64_t, int64_t>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_LARGEINT, TYPE_LARGEINT, false);
    test_non_deterministic_agg_function<int128_t, int128_t>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_FLOAT, TYPE_FLOAT, false);
    test_non_deterministic_agg_function<float, float>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_DOUBLE, TYPE_DOUBLE, false);
    test_non_deterministic_agg_function<double, double>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_VARCHAR, TYPE_VARCHAR, false);
    test_non_deterministic_agg_function<Slice, Slice>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_DECIMALV2, TYPE_DECIMALV2, false);
    test_non_deterministic_agg_function<DecimalV2Value, DecimalV2Value>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_DATETIME, TYPE_DATETIME, false);
    test_non_deterministic_agg_function<TimestampValue, TimestampValue>(ctx, func);

    func = get_aggregate_function("any_value", TYPE_DATE, TYPE_DATE, false);
    test_non_deterministic_agg_function<DateValue, DateValue>(ctx, func);
}

TEST_F(AggregateTest, test_exchange_bytes) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_VARCHAR)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_BIGINT))};

    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_BIGINT));
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* exchange_bytes_function =
            get_aggregate_function("exchange_bytes", TYPE_BIGINT, TYPE_BIGINT, false);
    auto state = ManagedAggrState::create(ctx, exchange_bytes_function);

    auto data_column = BinaryColumn::create();

    data_column->append("abc");
    data_column->append("bcd");
    data_column->append("cde");

    auto data_column_bigint = Int64Column::create();
    data_column_bigint->append(21023);
    data_column_bigint->append(410223);
    data_column_bigint->append(710233);

    std::vector<const Column*> raw_columns;
    raw_columns.resize(2);
    raw_columns[0] = data_column.get();
    raw_columns[1] = data_column_bigint.get();

    // test update
    exchange_bytes_function->update_batch_single_state(local_ctx.get(), data_column->size(), raw_columns.data(),
                                                       state->state());

    auto result_column = Int64Column::create();
    exchange_bytes_function->finalize_to_column(local_ctx.get(), state->state(), result_column.get());

    ASSERT_EQ(data_column_bigint->byte_size() + data_column->byte_size(), result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_array_agg) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_VARCHAR)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_INT))};

    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_ARRAY));
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));
    std::vector<bool> is_asc_order{0};
    std::vector<bool> nulls_first{1};
    local_ctx->set_is_asc_order(is_asc_order);
    local_ctx->set_nulls_first(nulls_first);
    local_ctx->set_runtime_state(runtime_state.get());

    const AggregateFunction* array_agg_func = get_aggregate_function("array_agg2", TYPE_BIGINT, TYPE_ARRAY, false);
    auto state = ManagedAggrState::create(local_ctx.get(), array_agg_func);

    // nullable columns input
    {
        auto char_type = TypeDescriptor::create_varchar_type(30);
        auto char_column = ColumnHelper::create_column(char_type, true);
        char_column->append_datum(Datum());
        char_column->append_datum("bcd");
        char_column->append_datum("cdrdfe");
        char_column->append_datum(Datum());
        char_column->append_datum("esfg");

        auto int_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
        auto int_column = ColumnHelper::create_column(int_type, true);
        int_column->append_datum(Datum());
        int_column->append_datum(9);
        int_column->append_datum(Datum());
        int_column->append_datum(7);
        int_column->append_datum(6);

        std::vector<const Column*> raw_columns;
        std::vector<ColumnPtr> columns;
        columns.push_back(char_column);
        columns.push_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns->size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ((*agg_state->data_columns)[0]->debug_string(), char_column->debug_string());
        ASSERT_EQ((*agg_state->data_columns)[1]->debug_string(), int_column->debug_string());

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(TypeDescriptor(LogicalType::TYPE_VARCHAR));

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        auto res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL,'bcd','cdrdfe',NULL,'esfg'],int:[NULL,9,NULL,7,6]}]"),
                  0);

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), &res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL],int:[NULL]}, {vchar:['bcd'],int:[9]}, {vchar:['cdrdfe'],int:[NULL]}, "
                         "{vchar:[NULL],int:[7]}, {vchar:['esfg'],int:[6]}]"),
                  0);

        auto res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_EQ(strcmp(res_array_col->debug_string().c_str(), "[[NULL,'cdrdfe','bcd',NULL,'esfg']]"), 0);
    }
    // not nullable columns input
    state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
    {
        auto char_type = TypeDescriptor::create_varchar_type(30);
        auto char_column = ColumnHelper::create_column(char_type, false);
        char_column->append_datum("");
        char_column->append_datum("bcd");
        char_column->append_datum("cdrdfe");
        char_column->append_datum("Datum()");
        char_column->append_datum("esfg");

        auto int_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
        auto int_column = ColumnHelper::create_column(int_type, false);
        int_column->append_datum(2);
        int_column->append_datum(9);
        int_column->append_datum(5);
        int_column->append_datum(7);
        int_column->append_datum(6);

        std::vector<const Column*> raw_columns;
        std::vector<ColumnPtr> columns;
        columns.push_back(char_column);
        columns.push_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns->size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ((*agg_state->data_columns)[0]->debug_string(), char_column->debug_string());
        ASSERT_EQ((*agg_state->data_columns)[1]->debug_string(), int_column->debug_string());

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(TypeDescriptor(LogicalType::TYPE_VARCHAR));

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        auto res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['','bcd','cdrdfe','Datum()','esfg'],int:[2,9,5,7,6]}]"),
                  0);

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), &res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[''],int:[2]}, {vchar:['bcd'],int:[9]}, {vchar:['cdrdfe'],int:[5]}, "
                         "{vchar:['Datum()'],int:[7]}, {vchar:['esfg'],int:[6]}]"),
                  0);
        // should not finalize due to following appends
    }

    // append only column + const column
    {
        auto char_column = ColumnHelper::create_const_null_column(2);
        auto int_column = ColumnHelper::create_const_column<TYPE_INT>(3, 2);

        std::vector<const Column*> raw_columns;
        std::vector<ColumnPtr> columns;
        columns.push_back(char_column);
        columns.push_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns->size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ(strcmp((*agg_state->data_columns)[0]->debug_string().c_str(),
                         "['', 'bcd', 'cdrdfe', 'Datum()', 'esfg', NULL, NULL]"),
                  0);
        ASSERT_EQ(strcmp((*agg_state->data_columns)[1]->debug_string().c_str(), "[2, 9, 5, 7, 6, 3, 3]"), 0);

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(TypeDescriptor(LogicalType::TYPE_VARCHAR));

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        auto res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['','bcd','cdrdfe','Datum()','esfg',NULL,NULL],int:[2,9,5,7,6,3,3]}]"),
                  0);

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), &res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(), "[{vchar:[NULL],int:[3]}, {vchar:[NULL],int:[3]}]"),
                  0);

        auto res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_EQ(strcmp(res_array_col->debug_string().c_str(), "[['bcd','Datum()','esfg','cdrdfe',NULL,NULL,'']]"), 0);
    }

    // only column + const column
    state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
    {
        auto char_column = ColumnHelper::create_const_null_column(2);
        auto int_column = ColumnHelper::create_const_column<TYPE_INT>(3, 2);

        std::vector<const Column*> raw_columns;
        std::vector<ColumnPtr> columns;
        columns.push_back(char_column);
        columns.push_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns->size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ(strcmp((*agg_state->data_columns)[0]->debug_string().c_str(), "[NULL, NULL]"), 0);
        ASSERT_EQ(strcmp((*agg_state->data_columns)[1]->debug_string().c_str(), "[3, 3]"), 0);

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(TypeDescriptor(LogicalType::TYPE_VARCHAR));

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        auto res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(), "[{vchar:[NULL,NULL],int:[3,3]}]"), 0);

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), &res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(), "[{vchar:[NULL],int:[3]}, {vchar:[NULL],int:[3]}]"),
                  0);
    }

    // append nullable columns input
    {
        auto char_type = TypeDescriptor::create_varchar_type(30);
        auto char_column = ColumnHelper::create_column(char_type, true);
        char_column->append_datum(Datum());
        char_column->append_datum("bcd");
        char_column->append_datum("cdrdfe");
        char_column->append_datum(Datum());
        char_column->append_datum("esfg");

        auto int_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
        auto int_column = ColumnHelper::create_column(int_type, true);
        int_column->append_datum(Datum());
        int_column->append_datum(9);
        int_column->append_datum(Datum());
        int_column->append_datum(7);
        int_column->append_datum(6);

        std::vector<const Column*> raw_columns;
        std::vector<ColumnPtr> columns;
        columns.push_back(char_column);
        columns.push_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns->size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ(strcmp((*agg_state->data_columns)[0]->debug_string().c_str(),
                         "[NULL, NULL, NULL, 'bcd', 'cdrdfe', NULL, 'esfg']"),
                  0);
        ASSERT_EQ(strcmp((*agg_state->data_columns)[1]->debug_string().c_str(), "[3, 3, NULL, 9, NULL, 7, 6]"), 0);

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(TypeDescriptor(LogicalType::TYPE_VARCHAR));

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        auto res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL,NULL,NULL,'bcd','cdrdfe',NULL,'esfg'],int:[3,3,NULL,9,NULL,7,6]}]"),
                  0);

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), &res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL],int:[NULL]}, {vchar:['bcd'],int:[9]}, {vchar:['cdrdfe'],int:[NULL]}, "
                         "{vchar:[NULL],int:[7]}, {vchar:['esfg'],int:[6]}]"),
                  0);

        auto res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_EQ(strcmp(res_array_col->debug_string().c_str(), "[['cdrdfe',NULL,'bcd',NULL,'esfg',NULL,NULL]]"), 0);
    }
}

} // namespace starrocks

// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <gtest/gtest.h>
#include <math.h>

#include "column/nullable_column.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/agg/sum.h"
#include "exprs/anyval_util.h"
#include "exprs/vectorized/arithmetic_operation.h"
#include "gen_cpp/Data_types.h"
#include "gutil/casts.h"
#include "runtime/vectorized/time_types.h"
#include "testutil/function_utils.h"
#include "udf/udf_internal.h"
#include "util/bitmap_value.h"
#include "util/slice.h"

namespace starrocks::vectorized {

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

template <typename T, typename TResult>
void test_agg_function(FunctionContext* ctx, const AggregateFunction* func, TResult update_result1,
                       TResult update_result2, TResult merge_result) {
    using ResultColumn = typename ColumnTraits<TResult>::ColumnType;
    auto result_column = ResultColumn::create();

    // update input column 1
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(func);
    ColumnPtr column = gen_input_column1<T>();
    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state->mutable_data());
    func->finalize_to_column(ctx, state->data(), result_column.get());
    ASSERT_EQ(update_result1, result_column->get_data()[0]);

    // update input column 2
    std::unique_ptr<ManagedAggregateState> state2 = ManagedAggregateState::Make(func);
    ColumnPtr column2 = gen_input_column2<T>();
    row_column = column2.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state2->mutable_data());
    func->finalize_to_column(ctx, state2->data(), result_column.get());
    ASSERT_EQ(update_result2, result_column->get_data()[1]);

    // merge column 1 and column 2
    ColumnPtr serde_column = BinaryColumn::create();
    std::string func_name = func->get_name();
    if (func_name == "count" || func_name == "sum" || func_name == "maxmin") {
        serde_column = ResultColumn::create();
    }
    func->serialize_to_column(ctx, state->data(), serde_column.get());
    func->merge(ctx, serde_column.get(), state2->data(), 0);
    func->finalize_to_column(ctx, state2->data(), result_column.get());
    ASSERT_EQ(merge_result, result_column->get_data()[2]);
}

template <typename T, typename TResult>
void test_agg_variance_function(FunctionContext* ctx, const AggregateFunction* func, TResult update_result1,
                                TResult update_result2, TResult merge_result) {
    using ResultColumn = typename ColumnTraits<TResult>::ColumnType;
    auto result_column = ResultColumn::create();

    // update input column 1
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(func);
    ColumnPtr column = gen_input_column1<T>();
    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state->mutable_data());
    func->finalize_to_column(ctx, state->data(), result_column.get());
    ASSERT_EQ(update_result1, result_column->get_data()[0]);

    // update input column 2
    std::unique_ptr<ManagedAggregateState> state2 = ManagedAggregateState::Make(func);
    ColumnPtr column2 = gen_input_column2<T>();
    row_column = column2.get();
    func->update_batch_single_state(ctx, row_column->size(), &row_column, state2->mutable_data());
    func->finalize_to_column(ctx, state2->data(), result_column.get());
    ASSERT_EQ(update_result2, result_column->get_data()[1]);

    // merge column 1 and column 2
    ColumnPtr serde_column = BinaryColumn::create();
    func->serialize_to_column(ctx, state->data(), serde_column.get());
    func->merge(ctx, serde_column.get(), state2->data(), 0);
    func->finalize_to_column(ctx, state2->data(), result_column.get());
    ASSERT_TRUE(abs(merge_result - result_column->get_data()[2]) < 1e-8);
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
} // namespace starrocks::vectorized

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

TEST_F(AggregateTest, test_sum_nullable) {
    using NullableSumInt64 = NullableAggregateFunctionState<SumAggregateState<int64_t>>;
    const AggregateFunction* sum_null = get_aggregate_function("sum", TYPE_INT, TYPE_BIGINT, true);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(sum_null);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column->append(i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();

    // test update
    sum_null->update_batch_single_state(ctx, column->size(), &row_column, state->mutable_data());
    auto* null_state = (NullableSumInt64*)state->mutable_data();
    int64_t result = *reinterpret_cast<const int64_t*>(null_state->nested_state());
    ASSERT_EQ(2450, result);

    // test serialize
    auto serde_column2 = NullableColumn::create(Int64Column::create(), NullColumn::create());
    sum_null->serialize_to_column(ctx, state->data(), serde_column2.get());

    // test merge
    std::unique_ptr<ManagedAggregateState> state2 = ManagedAggregateState::Make(sum_null);

    auto data_column2 = Int32Column::create();
    auto null_column2 = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        data_column2->append(i);
        null_column2->append(i % 2 ? 0 : 1);
    }
    auto column2 = NullableColumn::create(std::move(data_column2), std::move(null_column2));
    const Column* row_column2 = column2.get();

    sum_null->update_batch_single_state(ctx, column2->size(), &row_column2, state2->mutable_data());

    sum_null->merge(ctx, serde_column2.get(), state2->mutable_data(), 0);

    auto result_column = NullableColumn::create(Int64Column::create(), NullColumn::create());
    sum_null->finalize_to_column(ctx, state2->data(), result_column.get());

    const Column& result_data_column = result_column->data_column_ref();
    const auto& result_data = static_cast<const Int64Column&>(result_data_column);
    ASSERT_EQ(4950, result_data.get_data()[0]);
}

TEST_F(AggregateTest, test_count_nullable) {
    const AggregateFunction* func = get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, true);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(func);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();

    for (int i = 0; i < 1024; i++) {
        data_column->append(i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));

    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, column->size(), &row_column, state->mutable_data());

    int64_t result = *reinterpret_cast<int64_t*>(state->mutable_data());
    ASSERT_EQ(512, result);
}

TEST_F(AggregateTest, test_bitmap_nullable) {
    const AggregateFunction* bitmap_null = get_aggregate_function("bitmap_union_int", TYPE_INT, TYPE_BIGINT, true);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(bitmap_null);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();

    for (int i = 0; i < 100; i++) {
        data_column->append(i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();

    // test update
    bitmap_null->update_batch_single_state(ctx, column->size(), &row_column, state->mutable_data());

    auto result_column = NullableColumn::create(Int64Column::create(), NullColumn::create());
    bitmap_null->finalize_to_column(ctx, state->data(), result_column.get());

    const Column& result_data_column = result_column->data_column_ref();
    const auto& result_data = static_cast<const Int64Column&>(result_data_column);
    ASSERT_EQ(false, result_column->is_null(0));
    ASSERT_EQ(50, result_data.get_data()[0]);
}

TEST_F(AggregateTest, test_group_concat) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("group_concat", TYPE_VARCHAR, TYPE_VARCHAR, false);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(group_concat_function);

    auto data_column = BinaryColumn::create();

    for (int i = 0; i < 6; i++) {
        std::string val("starrocks");
        val.append(std::to_string(i));
        data_column->append(val);
    }

    const Column* row_column = data_column.get();

    // test update
    group_concat_function->update_batch_single_state(ctx, data_column->size(), &row_column, state->mutable_data());

    auto result_column = BinaryColumn::create();
    group_concat_function->finalize_to_column(ctx, state->data(), result_column.get());

    ASSERT_EQ("starrocks0, starrocks1, starrocks2, starrocks3, starrocks4, starrocks5", result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_group_concat_const_seperator) {
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_primtive_type(TYPE_VARCHAR)),
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_primtive_type(TYPE_VARCHAR))};

    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types)));

    const AggregateFunction* group_concat_function =
            get_aggregate_function("group_concat", TYPE_VARCHAR, TYPE_VARCHAR, false);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(group_concat_function);

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
    local_ctx->impl()->set_constant_columns(const_columns);

    // test update
    group_concat_function->update_batch_single_state(local_ctx.get(), data_column->size(), raw_columns.data(),
                                                     state->mutable_data());

    auto result_column = BinaryColumn::create();
    group_concat_function->finalize_to_column(local_ctx.get(), state->data(), result_column.get());

    ASSERT_EQ("abcbcdcdedefefgfghghihijijk", result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_intersect_count) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("intersect_count", TYPE_INT, TYPE_BIGINT, false);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(group_concat_function);

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
    ctx->impl()->set_constant_columns(const_columns);

    // test update
    group_concat_function->update_batch_single_state(ctx, data_column->size(), raw_columns.data(),
                                                     state->mutable_data());

    auto result_column = Int64Column::create();
    group_concat_function->finalize_to_column(ctx, state->data(), result_column.get());

    ASSERT_EQ(1, result_column->get_data()[0]);
}

TEST_F(AggregateTest, test_bitmap_intersect) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("bitmap_intersect", TYPE_OBJECT, TYPE_OBJECT, false);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(group_concat_function);

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
    group_concat_function->update_batch_single_state(ctx, data_column->size(), &row_column, state->mutable_data());

    auto result_column = BitmapColumn::create();
    group_concat_function->finalize_to_column(ctx, state->data(), result_column.get());

    ASSERT_EQ("1", result_column->get_pool()[0].to_string());
}

TEST_F(AggregateTest, test_bitmap_intersect_nullable) {
    const AggregateFunction* group_concat_function =
            get_aggregate_function("bitmap_intersect", TYPE_OBJECT, TYPE_OBJECT, true);
    std::unique_ptr<ManagedAggregateState> state = ManagedAggregateState::Make(group_concat_function);

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
    group_concat_function->update_batch_single_state(ctx, column->size(), &row_column, state->mutable_data());

    auto result_column = NullableColumn::create(BitmapColumn::create(), NullColumn::create());
    group_concat_function->finalize_to_column(ctx, state->data(), result_column.get());

    const Column& result_data_column = result_column->data_column_ref();
    const auto& result_data = static_cast<const BitmapColumn&>(result_data_column);

    ASSERT_EQ("1", result_data.get_pool()[0].to_string());
}

} // namespace starrocks::vectorized

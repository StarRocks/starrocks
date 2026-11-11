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

// Parquet INT64 TIMESTAMP -> DATETIME and INT32 days -> DATETIME conversion, 4096 rows, UTC session:
//   OLD: the previous converter loops (runtime tick divisor, per-row null branch, from_unix_second)
//   NEW: the converters from ColumnConverterFactory
//
//   ./parquet_timestamp_convert_bench --benchmark_repetitions=5 --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>

#include <random>
#include <vector>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/schema.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"

namespace starrocks::parquet {

static constexpr size_t kRows = 4096;

static void old_int64_loop(const NullableColumn* src, NullableColumn* dst, int64_t second_mask) {
    const int64_t scale = NANOSECS_PER_SEC / second_mask;
    const auto& src_data = down_cast<const FixedLengthColumn<int64_t>*>(src->data_column_raw_ptr())->get_data();
    const auto& src_null = src->null_column()->get_data();
    dst->resize_uninitialized(src->size());
    auto& dst_data = down_cast<TimestampColumn*>(dst->data_column_raw_ptr())->get_data();
    auto& dst_null = dst->null_column_raw_ptr()->get_data();
    for (size_t i = 0; i < src->size(); i++) {
        dst_null[i] = src_null[i];
        if (!src_null[i]) {
            int64_t seconds = src_data[i] / second_mask;
            int64_t nanoseconds = (src_data[i] % second_mask) * scale;
            if (nanoseconds < 0) {
                seconds -= 1;
                nanoseconds += NANOSECS_PER_SEC;
            }
            dst_data[i].set_timestamp(timestamp::of_epoch_second(seconds, nanoseconds));
        }
    }
}

static void old_int32_days_loop(const NullableColumn* src, NullableColumn* dst) {
    const auto& src_data = down_cast<const FixedLengthColumn<int32_t>*>(src->data_column_raw_ptr())->get_data();
    const auto& src_null = src->null_column()->get_data();
    dst->resize_uninitialized(src->size());
    auto& dst_data = down_cast<TimestampColumn*>(dst->data_column_raw_ptr())->get_data();
    auto& dst_null = dst->null_column_raw_ptr()->get_data();
    for (size_t i = 0; i < src->size(); i++) {
        dst_null[i] = src_null[i];
        if (!src_null[i]) {
            int64_t day = src_data[i];
            TimestampValue ep;
            ep.from_unix_second(day * 24 * 60 * 60, 0);
            dst_data[i].set_timestamp(ep.timestamp());
        }
    }
}

template <typename T>
static ColumnPtr make_source(int null_pct, std::mt19937_64& rng, int64_t lo, int64_t hi) {
    auto data = FixedLengthColumn<T>::create();
    auto nulls = NullColumn::create();
    for (size_t i = 0; i < kRows; i++) {
        data->append(static_cast<T>(lo + static_cast<int64_t>(rng() % static_cast<uint64_t>(hi - lo))));
        nulls->append(static_cast<int>(rng() % 100) < null_pct);
    }
    return NullableColumn::create(std::move(data), std::move(nulls));
}

// range(0): 0 = old, 1 = new; range(1): tick unit (1000 / 1000000 / 1000000000); range(2): null %
static void BM_Int64Timestamp(benchmark::State& state) {
    const bool use_new = state.range(0) != 0;
    const int64_t mask = state.range(1);
    const int null_pct = static_cast<int>(state.range(2));
    std::mt19937_64 rng(3);
    // 1900 .. 2100
    ColumnPtr src = make_source<int64_t>(null_pct, rng, -2208988800LL * mask, 4102444800LL * mask);

    ParquetField field;
    field.physical_type = tparquet::Type::INT64;
    field.schema_element.type = tparquet::Type::INT64;
    tparquet::TimestampType ts;
    ts.isAdjustedToUTC = true;
    if (mask == 1000) {
        ts.unit.__set_MILLIS(tparquet::MilliSeconds());
    } else if (mask == 1000000) {
        ts.unit.__set_MICROS(tparquet::MicroSeconds());
    } else {
        ts.unit.__set_NANOS(tparquet::NanoSeconds());
    }
    tparquet::LogicalType logical_type;
    logical_type.__set_TIMESTAMP(ts);
    field.schema_element.__set_logicalType(logical_type);
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
    std::unique_ptr<ColumnConverter> converter;
    CHECK(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());
    auto dst = ColumnHelper::create_column(col_type, true);
    auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
    const auto* src_nullable = down_cast<const NullableColumn*>(src.get());

    for (auto _ : state) {
        if (use_new) {
            CHECK(converter->convert(src.get(), dst.get()).ok());
        } else {
            old_int64_loop(src_nullable, dst_nullable, mask);
        }
        benchmark::DoNotOptimize(dst->size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kRows);
    state.SetLabel(use_new ? "new" : "old");
}

static void BM_Int32DaysToDateTime(benchmark::State& state) {
    const bool use_new = state.range(0) != 0;
    const int null_pct = static_cast<int>(state.range(1));
    std::mt19937_64 rng(5);
    ColumnPtr src = make_source<int32_t>(null_pct, rng, -25567, 47482);

    ParquetField field;
    field.physical_type = tparquet::Type::INT32;
    field.schema_element.type = tparquet::Type::INT32;
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
    std::unique_ptr<ColumnConverter> converter;
    CHECK(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());
    auto dst = ColumnHelper::create_column(col_type, true);
    auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
    const auto* src_nullable = down_cast<const NullableColumn*>(src.get());

    for (auto _ : state) {
        if (use_new) {
            CHECK(converter->convert(src.get(), dst.get()).ok());
        } else {
            old_int32_days_loop(src_nullable, dst_nullable);
        }
        benchmark::DoNotOptimize(dst->size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kRows);
    state.SetLabel(use_new ? "new" : "old");
}

BENCHMARK(BM_Int64Timestamp)
        ->ArgNames({"new", "unit", "null_pct"})
        ->ArgsProduct({{0, 1}, {1000, 1000000, 1000000000}, {0, 10}})
        ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_Int32DaysToDateTime)
        ->ArgNames({"new", "null_pct"})
        ->ArgsProduct({{0, 1}, {0, 10}})
        ->Unit(benchmark::kMicrosecond);

} // namespace starrocks::parquet

BENCHMARK_MAIN();

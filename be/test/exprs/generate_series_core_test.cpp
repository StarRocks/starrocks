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

#include <limits>
#include <vector>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "exprs/table_function/generate_series.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class GenerateSeriesCoreTest : public ::testing::Test {
protected:
    template <LogicalType Type>
    struct Result {
        std::vector<RunTimeCppType<Type>> values;
        std::vector<uint32_t> offsets;
        size_t processed_rows = 0;
        Status status;
    };

    // Runs one `process()` batch of generate_series(start, stop[, step]) over a single input row.
    template <LogicalType Type>
    Result<Type> run_one_batch(RunTimeCppType<Type> start, RunTimeCppType<Type> stop, const RunTimeCppType<Type>* step,
                               int chunk_size) {
        GenerateSeries<Type> function;
        TableFunctionState* state = nullptr;
        CHECK(function.init(TFunction(), &state).ok());
        CHECK(state != nullptr);
        CHECK(function.prepare(state).ok());

        RuntimeState runtime_state;
        runtime_state.set_chunk_size(chunk_size);
        CHECK(function.open(&runtime_state, state).ok());

        Columns input_columns;
        auto start_column = RunTimeColumnType<Type>::create();
        start_column->append(start);
        input_columns.emplace_back(std::move(start_column));
        auto stop_column = RunTimeColumnType<Type>::create();
        stop_column->append(stop);
        input_columns.emplace_back(std::move(stop_column));
        if (step != nullptr) {
            auto step_column = RunTimeColumnType<Type>::create();
            step_column->append(*step);
            input_columns.emplace_back(std::move(step_column));
        }
        state->set_params(std::move(input_columns));

        auto [result_columns, offset_column] = function.process(&runtime_state, state);

        Result<Type> result;
        result.status = state->status();
        result.processed_rows = state->processed_rows();
        if (!result_columns.empty()) {
            auto column = ColumnHelper::cast_to<Type>(result_columns[0]);
            result.values.assign(column->immutable_data().begin(), column->immutable_data().end());
        }
        if (offset_column != nullptr) {
            result.offsets.assign(offset_column->immutable_data().begin(), offset_column->immutable_data().end());
        }
        CHECK(function.close(&runtime_state, state).ok());
        return result;
    }

    template <LogicalType Type>
    Result<Type> run_one_batch(RunTimeCppType<Type> start, RunTimeCppType<Type> stop, RunTimeCppType<Type> step,
                               int chunk_size) {
        return run_one_batch<Type>(start, stop, &step, chunk_size);
    }
};

TEST_F(GenerateSeriesCoreTest, basic_ascending) {
    auto result = run_one_batch<TYPE_INT>(1, 5, 1, 4096);
    ASSERT_OK(result.status);
    EXPECT_EQ(std::vector<int32_t>({1, 2, 3, 4, 5}), result.values);
    EXPECT_EQ(std::vector<uint32_t>({0, 5}), result.offsets);
    EXPECT_EQ(1, result.processed_rows);
}

TEST_F(GenerateSeriesCoreTest, basic_descending) {
    auto result = run_one_batch<TYPE_INT>(5, 1, -2, 4096);
    ASSERT_OK(result.status);
    EXPECT_EQ(std::vector<int32_t>({5, 3, 1}), result.values);
    EXPECT_EQ(std::vector<uint32_t>({0, 3}), result.offsets);
    EXPECT_EQ(1, result.processed_rows);
}

TEST_F(GenerateSeriesCoreTest, zero_step_is_rejected) {
    auto result = run_one_batch<TYPE_INT>(1, 5, 0, 4096);
    ASSERT_FALSE(result.status.ok());
}

// `(stop - current) / step` used to be evaluated in the argument type. With `step == -1` and a
// distance of exactly `numeric_limits<T>::min()` the quotient is not representable, so the idiv
// raised SIGFPE and killed the BE inside GenerateSeries<TYPE_INT>::process().
// Before the fix this test died with "AddressSanitizer: FPE" at generate_series.h:103.
TEST_F(GenerateSeriesCoreTest, int_min_distance_with_minus_one_step) {
    constexpr int32_t kIntMin = std::numeric_limits<int32_t>::min();
    auto result = run_one_batch<TYPE_INT>(0, kIntMin, -1, 16);
    ASSERT_OK(result.status);
    ASSERT_EQ(16, result.values.size());
    for (int32_t i = 0; i < 16; ++i) {
        EXPECT_EQ(-i, result.values[i]);
    }
    EXPECT_EQ(std::vector<uint32_t>({0, 16}), result.offsets);
    // The row is not exhausted yet, so it must be resumed on the next call.
    EXPECT_EQ(0, result.processed_rows);
}

// TINYINT and SMALLINT escaped the trap because their operands are promoted to `int` before the
// division. Pinned down so a future rewrite cannot regress the narrow types either.
TEST_F(GenerateSeriesCoreTest, tinyint_min_distance_with_minus_one_step) {
    constexpr int8_t kTinyMin = std::numeric_limits<int8_t>::min();
    auto result = run_one_batch<TYPE_TINYINT>(0, kTinyMin, -1, 4096);
    ASSERT_OK(result.status);
    ASSERT_EQ(129, result.values.size());
    for (int i = 0; i < 129; ++i) {
        EXPECT_EQ(static_cast<int8_t>(-i), result.values[i]);
    }
    EXPECT_EQ(std::vector<uint32_t>({0, 129}), result.offsets);
    EXPECT_EQ(1, result.processed_rows);
}

// Before the fix: same SIGFPE as the INT case.
TEST_F(GenerateSeriesCoreTest, bigint_min_distance_with_minus_one_step) {
    constexpr int64_t kBigMin = std::numeric_limits<int64_t>::min();
    auto result = run_one_batch<TYPE_BIGINT>(0, kBigMin, -1, 8);
    ASSERT_OK(result.status);
    ASSERT_EQ(8, result.values.size());
    for (int64_t i = 0; i < 8; ++i) {
        EXPECT_EQ(-i, result.values[i]);
    }
    EXPECT_EQ(0, result.processed_rows);
}

// __int128 division does not trap, so before the fix this one did not crash: the negative count
// skipped the fill loop entirely and the test read uninitialized column memory instead.
TEST_F(GenerateSeriesCoreTest, largeint_min_distance_with_minus_one_step) {
    constexpr __int128 kLargeMin = std::numeric_limits<__int128>::min();
    auto result = run_one_batch<TYPE_LARGEINT>(static_cast<__int128>(0), kLargeMin, static_cast<__int128>(-1), 8);
    ASSERT_OK(result.status);
    ASSERT_EQ(8, result.values.size());
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(static_cast<__int128>(-i), result.values[i]);
    }
    EXPECT_EQ(0, result.processed_rows);
}

// `stop - current` overflows the argument type here; the resulting garbage count used to feed the
// chunk-size clamp. The series itself is well-defined and must simply be produced in batches.
TEST_F(GenerateSeriesCoreTest, distance_overflows_argument_type) {
    constexpr int32_t kIntMax = std::numeric_limits<int32_t>::max();
    auto result = run_one_batch<TYPE_INT>(-1, kIntMax, 1, 16);
    ASSERT_OK(result.status);
    ASSERT_EQ(16, result.values.size());
    for (int32_t i = 0; i < 16; ++i) {
        EXPECT_EQ(i - 1, result.values[i]);
    }
    EXPECT_EQ(0, result.processed_rows);
}

// A single row that fits exactly in the chunk must be reported as fully consumed.
TEST_F(GenerateSeriesCoreTest, row_exactly_fills_chunk) {
    auto result = run_one_batch<TYPE_INT>(1, 16, 1, 16);
    ASSERT_OK(result.status);
    ASSERT_EQ(16, result.values.size());
    EXPECT_EQ(1, result.processed_rows);
}

TEST_F(GenerateSeriesCoreTest, implicit_step) {
    auto result = run_one_batch<TYPE_INT>(3, 6, nullptr, 4096);
    ASSERT_OK(result.status);
    EXPECT_EQ(std::vector<int32_t>({3, 4, 5, 6}), result.values);
    EXPECT_EQ(1, result.processed_rows);
}

} // namespace starrocks

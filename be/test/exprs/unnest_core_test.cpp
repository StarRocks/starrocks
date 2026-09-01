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

#include "exprs/table_function/unnest.h"

#include <gtest/gtest.h>

#include <vector>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class UnnestCoreTest : public ::testing::Test {
protected:
    struct Result {
        Columns columns;
        std::vector<uint32_t> copy_counts;
    };

    // Runs one `process()` batch over `input`, which must be the array argument of unnest().
    Result run_one_batch(ColumnPtr input, bool is_left_join) {
        Unnest function;
        TableFunctionState* state = nullptr;
        CHECK(function.init(TFunction(), &state).ok());
        CHECK(state != nullptr);
        CHECK(function.prepare(state).ok());

        RuntimeState runtime_state;
        CHECK(function.open(&runtime_state, state).ok());

        Columns input_columns;
        input_columns.emplace_back(std::move(input));
        state->set_params(std::move(input_columns));
        state->set_is_left_join(is_left_join);

        auto [result_columns, copy_count_column] = function.process(&runtime_state, state);

        Result result;
        result.columns = std::move(result_columns);
        if (copy_count_column != nullptr) {
            const auto counts = copy_count_column->immutable_data();
            result.copy_counts.assign(counts.begin(), counts.end());
        }
        CHECK(function.close(&runtime_state, state).ok());
        return result;
    }
};

// Guards the slow path taken when the array argument has NULLs or LEFT JOIN is in effect:
// NULL rows and empty arrays each expand to one NULL row, other rows expand element-wise.
TEST_F(UnnestCoreTest, slow_path_with_nulls_and_empty_arrays) {
    // Rows: [10, 20], [], NULL, [30]
    auto elements_data = Int32Column::create();
    elements_data->append(10);
    elements_data->append(20);
    elements_data->append(30);
    auto elements_nulls = NullColumn::create();
    elements_nulls->resize(3);
    auto elements = NullableColumn::create(std::move(elements_data), std::move(elements_nulls));

    auto offsets = UInt32Column::create();
    for (uint32_t offset : {0U, 2U, 2U, 2U, 3U}) {
        offsets->append(offset);
    }

    auto array_column = ArrayColumn::create(std::move(elements), std::move(offsets));
    ASSERT_EQ(4, array_column->size());

    auto array_nulls = NullColumn::create();
    array_nulls->append(0);
    array_nulls->append(0);
    array_nulls->append(1); // the third row is a NULL array
    array_nulls->append(0);
    auto nullable_array = NullableColumn::create(std::move(array_column), std::move(array_nulls));

    auto result = run_one_batch(std::move(nullable_array), /*is_left_join=*/true);

    ASSERT_EQ(1, result.columns.size());
    ASSERT_EQ(5, result.columns[0]->size());
    EXPECT_EQ(std::vector<uint32_t>({0, 2, 3, 4, 5}), result.copy_counts);

    const auto* values = ColumnHelper::get_data_column_by_type<TYPE_INT>(result.columns[0]);
    const auto data = values->immutable_data();
    EXPECT_FALSE(result.columns[0]->is_null(0));
    EXPECT_EQ(10, data[0]);
    EXPECT_FALSE(result.columns[0]->is_null(1));
    EXPECT_EQ(20, data[1]);
    // The empty array and the NULL array each contribute one NULL row.
    EXPECT_TRUE(result.columns[0]->is_null(2));
    EXPECT_TRUE(result.columns[0]->is_null(3));
    EXPECT_FALSE(result.columns[0]->is_null(4));
    EXPECT_EQ(30, data[4]);
}

// Regression test for https://github.com/StarRocks/starrocks/issues/76953.
//
// Reading the offsets through Datum::get_int32() reinterprets any offset in [2^31, 2^32) as
// negative, and passing that to Column::append(src, size_t offset, size_t count) converts
// modularly to ~1.8e19, breaking the offset + count <= src.size() precondition.
//
// Disabled by default: it needs an elements column holding more than 2^31 entries, i.e. about
// 4GiB of RSS (2GiB of data plus a 2GiB null column). Run it explicitly against a release
// build, which also avoids the ASAN shadow-memory overhead on those two buffers:
//
//   BUILD_TYPE=Release GTEST_OPTIONS=--gtest_also_run_disabled_tests \
//     ./run-be-ut.sh --build-target expr_test --module expr_test --without-java-ext \
//                    --gtest_filter='UnnestCoreTest.DISABLED_offsets_across_int32_boundary'
//
// The ArrayColumn constructor does not validate offsets against the elements size, so the
// offsets can start just below 2^31 instead of accumulating there row by row. That keeps the
// test to four rows, and only the source elements column has to be large.
TEST_F(UnnestCoreTest, DISABLED_offsets_across_int32_boundary) {
    constexpr uint32_t kFirstOffset = 0x7ffffffeU;
    constexpr uint32_t kRowCount = 4;
    constexpr size_t kElementCount = static_cast<size_t>(kFirstOffset) + kRowCount;

    // Rows start at 0x7ffffffe, 0x7fffffff, 0x80000000 and 0x80000001: the last two are the
    // ones that come back negative through get_int32().
    auto elements_data = Int8Column::create();
    elements_data->resize(kElementCount);
    for (uint32_t i = 0; i < kRowCount; ++i) {
        elements_data->get_data()[kFirstOffset + i] = static_cast<int8_t>(10 + i);
    }
    auto elements_nulls = NullColumn::create();
    elements_nulls->resize(kElementCount);
    auto elements = NullableColumn::create(std::move(elements_data), std::move(elements_nulls));

    auto offsets = UInt32Column::create();
    for (uint32_t i = 0; i <= kRowCount; ++i) {
        offsets->append(kFirstOffset + i);
    }

    auto array_column = ArrayColumn::create(std::move(elements), std::move(offsets));
    ASSERT_EQ(kRowCount, array_column->size());

    // LEFT JOIN forces the slow path; the fast path hands the elements and offsets columns
    // through untouched and never reads an offset.
    auto result = run_one_batch(std::move(array_column), /*is_left_join=*/true);

    ASSERT_EQ(1, result.columns.size());
    ASSERT_EQ(kRowCount, result.columns[0]->size());

    // One element per input row, so the cumulative copy counts are 0, 1, 2, 3, 4.
    EXPECT_EQ(std::vector<uint32_t>({0, 1, 2, 3, 4}), result.copy_counts);

    // Every row must carry the element its own offset points at, including the rows whose
    // start offset is past 2^31.
    const auto* values = ColumnHelper::get_data_column_by_type<TYPE_TINYINT>(result.columns[0]);
    const auto data = values->immutable_data();
    for (uint32_t i = 0; i < kRowCount; ++i) {
        EXPECT_EQ(static_cast<int8_t>(10 + i), data[i]) << "row " << i;
    }
}

} // namespace starrocks

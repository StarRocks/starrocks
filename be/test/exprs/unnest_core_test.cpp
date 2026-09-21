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

#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/table_function/unnest.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class UnnestCoreTest : public ::testing::Test {
protected:
    struct Result {
        Columns columns;
        std::vector<uint32_t> copy_counts;
        // Whether process() handed back the input's own element column, which is the observable
        // difference between the zero-copy and the rebuild path.
        bool zero_copy = false;
    };

    // Runs one `process()` batch over `input`, which must be the array argument of unnest().
    Result run_one_batch(const ColumnPtr& input, bool is_left_join) {
        const auto* col_array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(input.get()));
        const Column* source_elements = col_array->elements_column().get();

        Unnest function;
        TableFunctionState* state = nullptr;
        CHECK(function.init(TFunction(), &state).ok());
        CHECK(state != nullptr);
        CHECK(function.prepare(state).ok());

        RuntimeState runtime_state;
        CHECK(function.open(&runtime_state, state).ok());

        Columns input_columns;
        input_columns.emplace_back(input);
        state->set_params(std::move(input_columns));
        state->set_is_left_join(is_left_join);

        auto [result_columns, copy_count_column] = function.process(&runtime_state, state);

        Result result;
        result.columns = std::move(result_columns);
        if (copy_count_column != nullptr) {
            const auto counts = copy_count_column->immutable_data();
            result.copy_counts.assign(counts.begin(), counts.end());
        }
        result.zero_copy = !result.columns.empty() && result.columns[0].get() == source_elements;
        CHECK(function.close(&runtime_state, state).ok());
        return result;
    }

    // Rows [10, 20], [], NULL, [30]. `null_row_leaks` controls the only thing the path choice
    // depends on: whether the NULL row still occupies an element in the element column. It must
    // never show up in the output either way.
    static ColumnPtr make_array(bool null_row_leaks) {
        auto elements_data = Int32Column::create();
        elements_data->append(10);
        elements_data->append(20);
        if (null_row_leaks) {
            elements_data->append(99); // payload of the NULL row, never cleared
        }
        elements_data->append(30);
        auto elements_nulls = NullColumn::create();
        elements_nulls->resize(elements_data->size());
        auto elements = NullableColumn::create(std::move(elements_data), std::move(elements_nulls));

        auto offsets = UInt32Column::create();
        if (null_row_leaks) {
            for (uint32_t offset : {0U, 2U, 2U, 3U, 4U}) {
                offsets->append(offset);
            }
        } else {
            for (uint32_t offset : {0U, 2U, 2U, 2U, 3U}) {
                offsets->append(offset);
            }
        }

        auto array_column = ArrayColumn::create(std::move(elements), std::move(offsets));
        CHECK_EQ(4, static_cast<int>(array_column->size()));

        auto array_nulls = NullColumn::create();
        array_nulls->append(0);
        array_nulls->append(0);
        array_nulls->append(1); // the third row is a NULL array
        array_nulls->append(0);
        return NullableColumn::create(std::move(array_column), std::move(array_nulls));
    }
};

// Since #61558 the path is chosen by `has_null() && !null_rows_are_empty(...)` alone: a LEFT JOIN
// over a column whose NULL rows occupy no elements is handed downstream by reference, and the row
// that expands to nothing becomes a zero-length bracket which TableFunctionOperator turns into the
// LEFT JOIN NULL row while assembling its already-bounded output chunk.
TEST_F(UnnestCoreTest, zero_copy_path_when_null_rows_are_empty) {
    auto result = run_one_batch(make_array(/*null_row_leaks=*/false), /*is_left_join=*/true);

    ASSERT_EQ(1, result.columns.size());
    EXPECT_TRUE(result.zero_copy);
    // The source elements and its own offsets, untouched: the NULL row and the empty array are the
    // two zero-length brackets the operator injects into.
    ASSERT_EQ(3, result.columns[0]->size());
    EXPECT_EQ(std::vector<uint32_t>({0, 2, 2, 2, 3}), result.copy_counts);

    const auto* values = ColumnHelper::get_data_column_by_type<TYPE_INT>(result.columns[0].get());
    const auto data = values->immutable_data();
    EXPECT_EQ(10, data[0]);
    EXPECT_EQ(20, data[1]);
    EXPECT_EQ(30, data[2]);
}

// Guards the rebuild path, now reached only by a NULL row whose payload was never cleared: NULL rows
// and empty arrays each expand to one NULL row, other rows expand element-wise, and the leaked
// payload contributes nothing.
TEST_F(UnnestCoreTest, rebuild_path_with_dirty_null_row) {
    auto result = run_one_batch(make_array(/*null_row_leaks=*/true), /*is_left_join=*/true);

    ASSERT_EQ(1, result.columns.size());
    EXPECT_FALSE(result.zero_copy);
    ASSERT_EQ(5, result.columns[0]->size());
    EXPECT_EQ(std::vector<uint32_t>({0, 2, 3, 4, 5}), result.copy_counts);

    const auto* values = ColumnHelper::get_data_column_by_type<TYPE_INT>(result.columns[0].get());
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
// Marked SLOW: it needs an elements column holding more than 2^31 entries, i.e. about 4GiB of
// RSS (2GiB of data plus a 2GiB null column). GROUP_SLOW_TEST_F keeps it out of the default
// ASAN/Debug runs, where it would also pay the shadow-memory cost on those two buffers, and
// enables it in release builds:
//
//   BUILD_TYPE=Release ./run-be-ut.sh --without-java-ext \
//       --gtest_filter='UnnestCoreTest.SLOW_offsets_across_int32_boundary'
//
// The ArrayColumn constructor does not validate offsets against the elements size, so the
// offsets can start just below 2^31 instead of accumulating there row by row. That keeps the
// test to four rows, and only the source elements column has to be large.
GROUP_SLOW_TEST_F(UnnestCoreTest, offsets_across_int32_boundary) {
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

    // The first row is NULL over a one-element payload: that is what defeats null_rows_are_empty()
    // and forces the rebuild path, the only one that reads offsets. LEFT JOIN alone would not,
    // since #61558. The three rows after it are read element-wise, starting at 0x7fffffff,
    // 0x80000000 and 0x80000001 - so the length subtraction for the first of them straddles the
    // int32 boundary, and the last two are the appends whose start offset used to come back
    // negative.
    auto array_nulls = NullColumn::create();
    array_nulls->append(1);
    for (uint32_t i = 1; i < kRowCount; ++i) {
        array_nulls->append(0);
    }
    auto nullable_array = NullableColumn::create(std::move(array_column), std::move(array_nulls));

    auto result = run_one_batch(std::move(nullable_array), /*is_left_join=*/true);

    ASSERT_EQ(1, result.columns.size());
    EXPECT_FALSE(result.zero_copy);
    ASSERT_EQ(kRowCount, result.columns[0]->size());

    // One output row per input row - the NULL row for the first, one element for each of the rest -
    // so the cumulative copy counts are 0, 1, 2, 3, 4.
    EXPECT_EQ(std::vector<uint32_t>({0, 1, 2, 3, 4}), result.copy_counts);

    // Every non-NULL row must carry the element its own offset points at, including the rows whose
    // start offset is past 2^31. The first row's element is the leaked payload and must not appear.
    const auto* values = ColumnHelper::get_data_column_by_type<TYPE_TINYINT>(result.columns[0].get());
    const auto data = values->immutable_data();
    EXPECT_TRUE(result.columns[0]->is_null(0));
    for (uint32_t i = 1; i < kRowCount; ++i) {
        EXPECT_FALSE(result.columns[0]->is_null(i)) << "row " << i;
        EXPECT_EQ(static_cast<int8_t>(10 + i), data[i]) << "row " << i;
    }
}

} // namespace starrocks

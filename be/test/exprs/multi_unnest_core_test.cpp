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

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/table_function/multi_unnest.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "testutil/parallel_test.h"

namespace starrocks {

class MultiUnnestCoreTest : public ::testing::Test {
protected:
    struct Result {
        Columns columns;
        std::vector<uint32_t> copy_counts;
    };

    // Builds one nullable ARRAY<INT> column from an explicit elements/offsets/null-flags triple, so a
    // test can lay out offsets that no INSERT could produce.
    static ColumnPtr make_int_array(const std::vector<int32_t>& elements, const std::vector<uint32_t>& offsets,
                                    const std::vector<uint8_t>& row_nulls) {
        auto elements_data = Int32Column::create();
        for (int32_t v : elements) {
            elements_data->append(v);
        }
        auto elements_nulls = NullColumn::create();
        elements_nulls->resize(elements.size());
        auto array = ArrayColumn::create(NullableColumn::create(std::move(elements_data), std::move(elements_nulls)),
                                         make_offsets(offsets));

        auto array_nulls = NullColumn::create();
        for (uint8_t is_null : row_nulls) {
            array_nulls->append(is_null);
        }
        auto nullable = NullableColumn::create(std::move(array), std::move(array_nulls));
        nullable->update_has_null();
        return nullable;
    }

    static MutableColumnPtr make_offsets(const std::vector<uint32_t>& offsets) {
        auto column = UInt32Column::create();
        for (uint32_t offset : offsets) {
            column->append(offset);
        }
        return column;
    }

    // Runs one `process()` batch over the given array arguments.
    static Result run_one_batch(Columns args, bool is_left_join) {
        MultiUnnest function;
        TableFunctionState* state = nullptr;
        CHECK(function.init(TFunction(), &state).ok());
        CHECK(state != nullptr);
        CHECK(function.prepare(state).ok());

        RuntimeState runtime_state;
        CHECK(function.open(&runtime_state, state).ok());

        state->set_params(std::move(args));
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

    // Reads one output column back as {value, is_null} pairs.
    template <LogicalType LT>
    static std::vector<std::pair<int64_t, bool>> read_column(const ColumnPtr& column) {
        const auto* values = ColumnHelper::get_data_column_by_type<LT>(column.get());
        const auto data = values->immutable_data();
        std::vector<std::pair<int64_t, bool>> out;
        out.reserve(column->size());
        for (size_t i = 0; i < column->size(); ++i) {
            out.emplace_back(static_cast<int64_t>(data[i]), column->is_null(i));
        }
        return out;
    }
};

// Pins the zip this PR restructured: every input row expands to the longest of its arrays, shorter
// arrays and NULL arrays pad with NULLs to that length, and a row whose arrays are all empty or NULL
// still yields one all-NULL row under LEFT JOIN.
TEST_F(MultiUnnestCoreTest, zips_arrays_of_unequal_length) {
    //        a              b
    //  row 0 [10, 20]       [100]        zip pads b to 2
    //  row 1 []             [200, 201]   zip pads a to 2
    //  row 2 NULL           [300]        a is a NULL array
    //  row 3 [30]           NULL         b is a NULL array
    //  row 4 []             []           both empty, so LEFT JOIN keeps the row
    Columns args;
    args.emplace_back(make_int_array({10, 20, 30}, {0, 2, 2, 2, 3, 3}, {0, 0, 1, 0, 0}));
    args.emplace_back(make_int_array({100, 200, 201, 300}, {0, 1, 3, 4, 4, 4}, {0, 0, 0, 1, 0}));

    auto result = run_one_batch(std::move(args), /*is_left_join=*/true);

    ASSERT_EQ(2, result.columns.size());
    // Rows contribute 2, 2, 1, 1 and then 1 for the all-empty LEFT JOIN row.
    EXPECT_EQ(std::vector<uint32_t>({0, 2, 4, 5, 6, 7}), result.copy_counts);
    ASSERT_EQ(7, result.columns[0]->size());
    ASSERT_EQ(7, result.columns[1]->size());

    const std::vector<std::pair<int64_t, bool>> expected_a{
            {10, false}, {20, false}, // row 0
            {0, true},   {0, true},   // row 1, padded to b's length
            {0, true},                // row 2, NULL array
            {30, false},              // row 3
            {0, true},                // row 4, LEFT JOIN placeholder
    };
    const std::vector<std::pair<int64_t, bool>> expected_b{
            {100, false}, {0, true},    // row 0, padded to a's length
            {200, false}, {201, false}, // row 1
            {300, false},               // row 2
            {0, true},                  // row 3, NULL array
            {0, true},                  // row 4, LEFT JOIN placeholder
    };

    const auto actual_a = read_column<TYPE_INT>(result.columns[0]);
    const auto actual_b = read_column<TYPE_INT>(result.columns[1]);
    for (size_t i = 0; i < expected_a.size(); ++i) {
        EXPECT_EQ(expected_a[i].second, actual_a[i].second) << "column a, row " << i << " nullness";
        if (!expected_a[i].second) {
            EXPECT_EQ(expected_a[i].first, actual_a[i].first) << "column a, row " << i;
        }
        EXPECT_EQ(expected_b[i].second, actual_b[i].second) << "column b, row " << i << " nullness";
        if (!expected_b[i].second) {
            EXPECT_EQ(expected_b[i].first, actual_b[i].first) << "column b, row " << i;
        }
    }
}

// Regression test for https://github.com/StarRocks/starrocks/issues/76953.
//
// Reading the offsets through Datum::get_int32() reinterprets any offset in [2^31, 2^32) as
// negative, and passing that to Column::append(src, size_t offset, size_t count) converts modularly
// to ~1.8e19, breaking the offset + count <= src.size() precondition. Unlike Unnest, MultiUnnest has
// no fast path, so it reads offsets per row on every invocation.
//
// Marked SLOW: it needs an elements column holding more than 2^31 entries, i.e. about 4GiB of RSS
// (2GiB of TINYINT data plus a 2GiB null column). GROUP_SLOW_TEST_F keeps it out of the default ASAN/Debug
// runs, where it would also pay the shadow-memory cost on those two buffers, and enables it in
// release builds:
//
//   BUILD_TYPE=Release ./run-be-ut.sh --without-java-ext \
//       --gtest_filter='MultiUnnestCoreTest.SLOW_offsets_across_int32_boundary'
//
// The ArrayColumn constructor does not validate offsets against the elements size, so the offsets
// can start just below 2^31 instead of accumulating there row by row. That keeps the test to four
// rows, and only the first argument's elements column has to be large: every row holds one element,
// so the zip length stays 1 and nothing large is written to the output.
GROUP_SLOW_TEST_F(MultiUnnestCoreTest, offsets_across_int32_boundary) {
    constexpr uint32_t kFirstOffset = 0x7ffffffeU;
    constexpr uint32_t kRowCount = 4;
    constexpr size_t kElementCount = static_cast<size_t>(kFirstOffset) + kRowCount;

    // Rows start at 0x7ffffffe, 0x7fffffff, 0x80000000 and 0x80000001: the last two are the ones
    // that come back negative through get_int32().
    // TINYINT, not INT: the elements buffer is sized by kElementCount, so a 4-byte type would make
    // it 8GiB of data instead of 2GiB.
    auto big_elements = Int8Column::create();
    big_elements->resize(kElementCount);
    for (uint32_t i = 0; i < kRowCount; ++i) {
        big_elements->get_data()[kFirstOffset + i] = static_cast<int8_t>(10 + i);
    }
    auto big_element_nulls = NullColumn::create();
    big_element_nulls->resize(kElementCount);
    auto big_offsets = UInt32Column::create();
    for (uint32_t i = 0; i <= kRowCount; ++i) {
        big_offsets->append(kFirstOffset + i);
    }
    auto big_array = ArrayColumn::create(NullableColumn::create(std::move(big_elements), std::move(big_element_nulls)),
                                         std::move(big_offsets));
    ASSERT_EQ(kRowCount, big_array->size());

    Columns args;
    args.emplace_back(std::move(big_array));
    // A second argument of the same shape but ordinary size: MultiUnnest needs more than one array,
    // and matching lengths keep the zip from padding anything.
    auto small_elements = Int8Column::create();
    for (int8_t v : {20, 21, 22, 23}) {
        small_elements->append(v);
    }
    auto small_element_nulls = NullColumn::create();
    small_element_nulls->resize(4);
    auto small_offsets = UInt32Column::create();
    for (uint32_t i = 0; i <= kRowCount; ++i) {
        small_offsets->append(i);
    }
    args.emplace_back(
            ArrayColumn::create(NullableColumn::create(std::move(small_elements), std::move(small_element_nulls)),
                                std::move(small_offsets)));

    auto result = run_one_batch(std::move(args), /*is_left_join=*/false);

    ASSERT_EQ(2, result.columns.size());
    ASSERT_EQ(kRowCount, result.columns[0]->size());
    ASSERT_EQ(kRowCount, result.columns[1]->size());
    EXPECT_EQ(std::vector<uint32_t>({0, 1, 2, 3, 4}), result.copy_counts);

    // Every row must carry the element its own offset points at, including the rows whose start
    // offset is past 2^31.
    const auto actual_big = read_column<TYPE_TINYINT>(result.columns[0]);
    const auto actual_small = read_column<TYPE_TINYINT>(result.columns[1]);
    for (uint32_t i = 0; i < kRowCount; ++i) {
        EXPECT_FALSE(actual_big[i].second) << "row " << i;
        EXPECT_EQ(static_cast<int64_t>(10 + i), actual_big[i].first) << "row " << i;
        EXPECT_FALSE(actual_small[i].second) << "row " << i;
        EXPECT_EQ(static_cast<int64_t>(20 + i), actual_small[i].first) << "row " << i;
    }
}

} // namespace starrocks

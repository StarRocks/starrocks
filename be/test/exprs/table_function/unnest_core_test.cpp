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

#include <optional>
#include <string>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/table_function/table_function_harness.h"
#include "exprs/table_function/unnest.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Single-array UNNEST has two paths that must be indistinguishable from the outside:
//
//   * zero-copy - the elements column and the array's own offsets column are handed back by
//     reference, and a row that expands to nothing (NULL or empty array) becomes a zero-length
//     bracket that TableFunctionOperator turns into the LEFT JOIN NULL row while assembling its
//     already-bounded output chunk;
//   * rebuild - the element column is materialized row by row, skipping the payload of NULL rows and
//     emitting the LEFT JOIN NULL row itself.
//
// The choice between them is made by `has_null() && !null_rows_are_empty(...)`, i.e. purely by whether
// a NULL row still occupies elements in the element column. Nothing about the *result* may depend on
// it. That equivalence is what lets UNNEST avoid materializing a whole chunk expansion, and these
// tests are what pin it: every case below is driven through both shapes of the same logical input and
// the two outputs are compared against one reference expansion.

namespace {

// One input row. `leaked` is what a NULL row still occupies in the element column - the dirty shape
// that forces the rebuild path. It must never appear in the output.
struct InputRow {
    std::optional<std::vector<int32_t>> array; // nullopt -> the row is NULL
    std::vector<int32_t> leaked;
};
using InputRows = std::vector<InputRow>;

ColumnPtr make_array_column(const InputRows& rows, bool nullable) {
    auto array = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                     UInt32Column::create());
    auto nulls = NullColumn::create();
    uint32_t offset = 0;
    for (const auto& row : rows) {
        const std::vector<int32_t>& elements = row.array.has_value() ? *row.array : row.leaked;
        for (int32_t value : elements) {
            array->elements_column_raw_ptr()->append_datum(Datum(value));
        }
        offset += static_cast<uint32_t>(elements.size());
        array->offsets_column_raw_ptr()->append(offset);
        nulls->append(row.array.has_value() ? 0 : 1);
    }
    if (!nullable) {
        return std::move(array);
    }
    // _has_null is computed from the null column at construction, so it has to be complete by now.
    return NullableColumn::create(std::move(array), std::move(nulls));
}

// Spelled out rather than relying on brace-init of std::optional<std::vector<>>, where `{{}}` is easy
// to misread as nullopt.
std::optional<std::vector<int32_t>> arr(std::vector<int32_t> values) {
    return std::optional<std::vector<int32_t>>(std::move(values));
}
const std::optional<std::vector<int32_t>> kNullArray = std::nullopt;

// The reference expansion: a row yields its own elements, and nothing at all when it is NULL or empty -
// except under LEFT JOIN, where it yields a single NULL row. `leaked` is deliberately ignored.
std::vector<std::string> expected_expansion(const InputRows& rows, bool is_left_join, bool with_values = true) {
    std::vector<std::string> out;
    for (size_t row = 0; row < rows.size(); ++row) {
        const std::vector<int32_t> elements = rows[row].array.value_or(std::vector<int32_t>{});
        if (elements.empty()) {
            if (is_left_join) {
                out.emplace_back(
                        table_function_test::render_row(row, std::vector<std::string>(with_values ? 1 : 0, "NULL")));
            }
            continue;
        }
        for (int32_t value : elements) {
            std::vector<std::string> rendered;
            if (with_values) {
                rendered.emplace_back(std::to_string(value));
            }
            out.emplace_back(table_function_test::render_row(row, rendered));
        }
    }
    return out;
}

// Whether process() handed back the input's own element column, which is the observable difference
// between the two paths. Runs on a state of its own so the caller can still drive the same input from
// the start.
bool takes_zero_copy_path(const Unnest& fn, RuntimeState* runtime_state, const Columns& columns, bool is_left_join) {
    Unnest::UnnestState state;
    state.set_is_left_join(is_left_join);
    state.set_params(columns);
    auto [result_columns, offsets] = fn.process(runtime_state, &state);
    const auto* array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[0]));
    return result_columns[0].get() == array->elements_column().get();
}

// Mixed on purpose: leading elements, a NULL row, an empty array, a single element, a trailing NULL,
// and elements after it. `clean` is what every write path produces; `dirty` is the same logical input
// with the NULL rows' payload left behind.
const InputRows kCleanRows = {
        {arr({1, 2, 3}), {}}, {kNullArray, {}}, {arr({}), {}}, {arr({4}), {}}, {kNullArray, {}}, {arr({5, 6}), {}},
};
const InputRows kDirtyRows = {
        {arr({1, 2, 3}), {}}, {kNullArray, {99}},     {arr({}), {}},
        {arr({4}), {}},       {kNullArray, {98, 97}}, {arr({5, 6}), {}},
};

} // namespace

// The differential case. Two columns with the same logical content, one clean (zero-copy) and one
// whose NULL rows still carry elements (rebuild), must produce the same rows - including under LEFT
// JOIN, where one path relies on the operator injecting the NULL row and the other emits it itself.
TEST(UnnestTest, both_paths_expand_identically) {
    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4096);

    const Columns clean{make_array_column(kCleanRows, /*nullable=*/true)};
    const Columns dirty{make_array_column(kDirtyRows, /*nullable=*/true)};

    Unnest fn;
    for (bool is_left_join : {false, true}) {
        // The premise of the comparison: the two inputs really do take the two different paths.
        ASSERT_TRUE(takes_zero_copy_path(fn, &runtime_state, clean, is_left_join)) << "left_join=" << is_left_join;
        ASSERT_FALSE(takes_zero_copy_path(fn, &runtime_state, dirty, is_left_join)) << "left_join=" << is_left_join;

        const auto expected = expected_expansion(kCleanRows, is_left_join);
        for (const Columns* columns : {&clean, &dirty}) {
            Unnest::UnnestState state;
            state.set_is_left_join(is_left_join);
            state.set_params(*columns);

            const auto result = table_function_test::drive(fn, &runtime_state, &state);
            EXPECT_EQ(expected, result.rows) << "left_join=" << is_left_join << " zero_copy=" << (columns == &clean);
            // Whole chunk in one call either way: single-array UNNEST has no intra-row cursor, the
            // operator bounds the output while copying out of this result.
            EXPECT_EQ(1u, result.process_calls);
            // Which side emits the LEFT JOIN NULL row differs, and that is exactly what must not be
            // observable in `rows`: the zero-copy path leaves a zero-length bracket for the operator
            // to inject into, the rebuild path emits the row itself at offset += 1.
            if (is_left_join) {
                EXPECT_EQ(columns == &clean ? 3u : 0u, result.zero_length_brackets);
            } else {
                EXPECT_EQ(3u, result.zero_length_brackets);
            }
        }
    }
}

// A non-nullable array column never reaches the rebuild path (has_null() gates it), so the elements it
// hands back are its own - empty arrays included, which under LEFT JOIN is the operator's injection
// again.
TEST(UnnestTest, non_nullable_column_is_always_zero_copy) {
    const InputRows rows = {{arr({7, 8}), {}}, {arr({}), {}}, {arr({9}), {}}};
    const Columns columns{make_array_column(rows, /*nullable=*/false)};

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4096);

    Unnest fn;
    for (bool is_left_join : {false, true}) {
        ASSERT_TRUE(takes_zero_copy_path(fn, &runtime_state, columns, is_left_join));

        Unnest::UnnestState state;
        state.set_is_left_join(is_left_join);
        state.set_params(columns);

        const auto result = table_function_test::drive(fn, &runtime_state, &state);
        EXPECT_EQ(expected_expansion(rows, is_left_join), result.rows) << "left_join=" << is_left_join;
        EXPECT_EQ(1u, result.zero_length_brackets);
    }
}

// With fn_result_required false the operator reads nothing but the bracket counts. The rebuild path
// must therefore skip every append while still counting the rows it would have produced - including
// the LEFT JOIN NULL row, whose `offset += 1` sits outside the is_required() guard. The zero-copy path
// has nothing to skip: the column it returns already exists.
TEST(UnnestTest, counts_only_when_result_is_not_required) {
    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4096);

    const Columns clean{make_array_column(kCleanRows, /*nullable=*/true)};
    const Columns dirty{make_array_column(kDirtyRows, /*nullable=*/true)};

    Unnest fn;
    for (bool is_left_join : {false, true}) {
        const auto expected = expected_expansion(kCleanRows, is_left_join, /*with_values=*/false);
        for (const Columns* columns : {&clean, &dirty}) {
            Unnest::UnnestState state;
            state.set_is_left_join(is_left_join);
            state.set_is_required(false);
            state.set_params(*columns);

            const auto result = table_function_test::drive(fn, &runtime_state, &state);
            EXPECT_EQ(expected, result.rows) << "left_join=" << is_left_join << " zero_copy=" << (columns == &clean);
            // The rebuild path materializes nothing; the zero-copy path returns a column it already
            // had, by reference, so its rows come along for free.
            EXPECT_EQ(columns == &clean ? 6u : 0u, result.max_fn_result_rows) << "left_join=" << is_left_join;
        }
    }
}

// GROUP_SLOW_TEST_F needs a fixture; this case carries no per-test state.
class UnnestSlowTest : public ::testing::Test {};

// Regression test for https://github.com/StarRocks/starrocks/issues/76953, ported from #78480.
//
// The rebuild path is the only one that reads offsets per row, and it used to read them through
// Datum::get_int32(): Datum keeps an unsigned value in the matching signed slot, so any offset in
// [2^31, 2^32) came back negative. Two defects followed - the length subtraction between two
// int32_t operands overflowed for the array straddling the boundary, and the negative start offset
// passed to Column::append(src, size_t offset, size_t count) converted modularly to ~1.8e19,
// breaking the offset + count <= src.size() precondition.
//
// Marked SLOW: it needs an elements column holding more than 2^31 entries, i.e. about 4GiB of RSS
// (2GiB of data plus a 2GiB null column). GROUP_SLOW_TEST_F compiles it as DISABLED_ unless NDEBUG
// is set, keeping it out of the default ASAN/Debug runs - where it would also pay the shadow-memory
// cost on those two buffers - and enabling it in release builds:
//
//   BUILD_TYPE=Release ./run-be-ut.sh --build-target expr_test --module expr_test \
//       --without-java-ext --gtest_filter='UnnestSlowTest.SLOW_offsets_across_int32_boundary'
//
// The ArrayColumn constructor does not validate offsets against the elements size, so the offsets
// can start just below 2^31 instead of accumulating there row by row. That keeps the test to four
// rows, and only the source elements column has to be large.
GROUP_SLOW_TEST_F(UnnestSlowTest, offsets_across_int32_boundary) {
    constexpr uint32_t kFirstOffset = 0x7ffffffeU;
    constexpr uint32_t kRowCount = 4;
    constexpr size_t kElementCount = static_cast<size_t>(kFirstOffset) + kRowCount;

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

    auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
    ASSERT_EQ(kRowCount, array->size());

    // The first row is NULL over a one-element payload: that is what defeats null_rows_are_empty()
    // and forces the rebuild path. LEFT JOIN alone would not, since #61558. The three rows after it
    // are read element-wise, starting at 0x7fffffff, 0x80000000 and 0x80000001 - so the length
    // subtraction for the first of them straddles the int32 boundary, and the last two are the
    // appends whose start offset used to come back negative.
    auto array_nulls = NullColumn::create();
    array_nulls->append(1);
    for (uint32_t i = 1; i < kRowCount; ++i) {
        array_nulls->append(0);
    }
    const Columns columns{NullableColumn::create(std::move(array), std::move(array_nulls))};

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4096);

    Unnest fn;
    // The premise: this input really does take the rebuild path, the only one that reads offsets.
    ASSERT_FALSE(takes_zero_copy_path(fn, &runtime_state, columns, /*is_left_join=*/true));

    Unnest::UnnestState state;
    state.set_is_left_join(true);
    state.set_params(columns);

    auto [result_columns, copy_counts] = fn.process(&runtime_state, &state);
    ASSERT_EQ(1u, result_columns.size());

    // One output row per input row - the NULL row for the first, one element for each of the rest -
    // so the cumulative copy counts are 0, 1, 2, 3, 4.
    ASSERT_NE(nullptr, copy_counts);
    const auto counts = copy_counts->immutable_data();
    EXPECT_EQ(std::vector<uint32_t>({0, 1, 2, 3, 4}), std::vector<uint32_t>(counts.begin(), counts.end()));

    // Every non-NULL row must carry the element its own offset points at, including the rows whose
    // start offset is past 2^31.
    ASSERT_EQ(kRowCount, result_columns[0]->size());
    EXPECT_TRUE(result_columns[0]->is_null(0));
    const auto* values = ColumnHelper::get_data_column_by_type<TYPE_TINYINT>(result_columns[0]);
    const auto data = values->immutable_data();
    for (uint32_t i = 1; i < kRowCount; ++i) {
        EXPECT_FALSE(result_columns[0]->is_null(i)) << "row " << i;
        EXPECT_EQ(static_cast<int8_t>(10 + i), data[i]) << "row " << i;
    }
}

} // namespace starrocks

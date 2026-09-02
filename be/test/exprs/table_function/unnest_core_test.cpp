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

} // namespace starrocks

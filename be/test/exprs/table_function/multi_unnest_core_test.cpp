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
#include "exprs/table_function/multi_unnest.h"
#include "exprs/table_function/table_function_harness.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// MultiUnnest expands a bounded slice of the input rows per process() call, so its output now depends
// on chunk_size and on where the previous call stopped. table_function_test::drive() replays the
// contract TableFunctionOperator imposes - which input row each bracket belongs to included - and
// checks the invariants every bounded implementation shares. What this file asserts on top of that is
// the part specific to MultiUnnest: the zip and its NULL padding, against a reference expansion
// computed independently of the implementation. The result must not depend on chunk_size at all; only
// the number of calls and the per-call size may.

// One argument of UNNEST(a, b, ...): one entry per input row, std::nullopt for a NULL array.
using ArrayRows = std::vector<std::optional<std::vector<int32_t>>>;

namespace {

// Builds a nullable ARRAY<INT> column. A NULL row gets an empty element range, which is what
// append_nulls() produces on the real paths.
ColumnPtr make_nullable_array_column(const ArrayRows& rows) {
    auto array = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                     UInt32Column::create());
    auto nulls = NullColumn::create();
    uint32_t offset = 0;
    for (const auto& row : rows) {
        if (row.has_value()) {
            for (int32_t v : *row) {
                array->elements_column_raw_ptr()->append_datum(Datum(v));
            }
            offset += static_cast<uint32_t>(row->size());
            nulls->append(0);
        } else {
            nulls->append(1);
        }
        array->offsets_column_raw_ptr()->append(offset);
    }
    // _has_null is computed from the null column at construction, so it has to be complete by now.
    return NullableColumn::create(std::move(array), std::move(nulls));
}

// Spelled out rather than relying on brace-init of std::optional<std::vector<>>, where `{{}}` is
// easy to misread as nullopt.
std::optional<std::vector<int32_t>> arr(std::vector<int32_t> values) {
    return std::optional<std::vector<int32_t>>(std::move(values));
}
const std::optional<std::vector<int32_t>> kNullArray = std::nullopt;

// The reference expansion: every row is zipped to the maximum length of its arrays, each argument
// padded with NULLs beyond its own length; under LEFT JOIN a row that expands to nothing yields one
// all-NULL row. Rendered through the harness helper so the two compare directly.
std::vector<std::string> expected_expansion(const std::vector<ArrayRows>& args, bool is_left_join,
                                            bool with_values = true) {
    std::vector<std::string> out;
    const size_t input_rows = args[0].size();
    for (size_t row = 0; row < input_rows; ++row) {
        size_t max_len = 0;
        for (const auto& arg : args) {
            if (arg[row].has_value()) {
                max_len = std::max(max_len, arg[row]->size());
            }
        }
        const size_t len = (max_len == 0 && is_left_join) ? 1 : max_len;
        for (size_t k = 0; k < len; ++k) {
            std::vector<std::string> values;
            if (with_values) {
                for (const auto& arg : args) {
                    if (arg[row].has_value() && k < arg[row]->size()) {
                        values.emplace_back(std::to_string((*arg[row])[k]));
                    } else {
                        values.emplace_back("NULL");
                    }
                }
            }
            out.emplace_back(table_function_test::render_row(row, values));
        }
    }
    return out;
}

} // namespace

// A single process() call must not exceed chunk_size output rows, and the flattened result must be
// identical for every chunk_size, for both inner and LEFT JOIN.
TEST(MultiUnnestTest, expansion_is_independent_of_chunk_size) {
    // Deliberately mixed: unequal lengths, NULL arrays, empty arrays, and a row that is empty in
    // every argument (the LEFT JOIN NULL row).
    const std::vector<ArrayRows> args = {
            ArrayRows{arr({1, 2, 3}), arr({}), kNullArray, arr({7}), arr({8, 9}), kNullArray},
            ArrayRows{arr({10}), arr({11, 12}), arr({13}), kNullArray, arr({}), arr({})},
            ArrayRows{kNullArray, arr({}), arr({20, 21, 22, 23}), arr({24}), arr({25, 26}), arr({})},
    };
    Columns columns;
    for (const auto& arg : args) {
        columns.emplace_back(make_nullable_array_column(arg));
    }

    MultiUnnest fn;
    for (bool is_left_join : {false, true}) {
        const auto expected = expected_expansion(args, is_left_join);
        for (int chunk_size : {1, 2, 3, 5, 64}) {
            RuntimeState runtime_state{TQueryGlobals()};
            runtime_state.set_chunk_size(chunk_size);

            MultiUnnest::UnnestState state;
            state.set_is_left_join(is_left_join);
            state.set_params(columns);

            const auto result = table_function_test::drive(fn, &runtime_state, &state);
            EXPECT_EQ(expected, result.rows) << "chunk_size=" << chunk_size << " left_join=" << is_left_join;
            EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size))
                    << "chunk_size=" << chunk_size << " left_join=" << is_left_join;
            // Under LEFT JOIN, MultiUnnest emits the NULL row itself as a length-1 bracket, so no
            // bracket is ever zero-length and TableFunctionOperator's own injection path stays inert
            // for it - which is what its comment there claims. Under an inner join a row that expands
            // to nothing is exactly a zero-length bracket, so the claim is LEFT JOIN's alone.
            if (is_left_join) {
                EXPECT_EQ(0u, result.zero_length_brackets) << "chunk_size=" << chunk_size;
            }
            // The point of the change: with a chunk smaller than the expansion, the work is spread
            // over several calls instead of one unbounded allocation.
            if (static_cast<size_t>(chunk_size) < expected.size()) {
                EXPECT_GT(result.process_calls, 1u) << "chunk_size=" << chunk_size;
            }
        }
    }
}

// One row whose expansion is larger than a chunk: the row must be split across calls, and while it
// is only partially emitted processed_rows() must not advance - the operator would otherwise pair the
// remaining elements with the next input row's outer columns.
TEST(MultiUnnestTest, one_row_split_across_calls_keeps_its_input_row) {
    const std::vector<ArrayRows> args = {
            ArrayRows{arr({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})},
            ArrayRows{arr({100, 101, 102})},
    };
    Columns columns;
    for (const auto& arg : args) {
        columns.emplace_back(make_nullable_array_column(arg));
    }

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4);

    MultiUnnest fn;
    MultiUnnest::UnnestState state;
    state.set_params(columns);

    // First call on its own, to pin the cursor state in the middle of the row.
    auto [first_columns, first_offsets] = fn.process(&runtime_state, &state);
    EXPECT_EQ(4u, first_columns[0]->size());
    EXPECT_EQ(0u, state.processed_rows());
    EXPECT_EQ(4, state.get_offset());
    // A row contributes at most one bracket per call, otherwise two brackets would map to two input
    // rows.
    EXPECT_EQ(2u, first_offsets->size());

    state.set_params(columns); // restart cleanly for the full drive
    const auto result = table_function_test::drive(fn, &runtime_state, &state);
    EXPECT_EQ(expected_expansion(args, false), result.rows);
    EXPECT_EQ(3u, result.process_calls); // 10 rows / chunk 4
    EXPECT_EQ(4u, result.max_rows_per_call);
}

// With fn_result_required false the operator only uses the copy-count column, so the zip - including
// the NULL padding, which is the bulk of the work for arrays of unequal length - must be skipped
// while the cursor still advances exactly as before. The loop bound therefore cannot be the output
// column's size.
TEST(MultiUnnestTest, counts_only_when_result_is_not_required) {
    const std::vector<ArrayRows> args = {
            ArrayRows{arr({1, 2, 3, 4, 5}), kNullArray, arr({6})},
            ArrayRows{arr({9}), arr({}), arr({7, 8})},
    };
    Columns columns;
    for (const auto& arg : args) {
        columns.emplace_back(make_nullable_array_column(arg));
    }

    MultiUnnest fn;
    for (int chunk_size : {1, 2, 3, 64}) {
        RuntimeState runtime_state{TQueryGlobals()};
        runtime_state.set_chunk_size(chunk_size);

        MultiUnnest::UnnestState state;
        state.set_is_required(false);
        state.set_params(columns);

        const auto result = table_function_test::drive(fn, &runtime_state, &state);
        // Same row count and same bracket-to-input-row mapping, values not materialized.
        EXPECT_EQ(expected_expansion(args, false, /*with_values=*/false), result.rows) << "chunk_size=" << chunk_size;
        EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size));
        // Nothing was appended at all - the zip and its NULL padding are the bulk of the work here and
        // there is no column to hand back by reference, unlike single-array Unnest.
        EXPECT_EQ(0u, result.max_fn_result_rows) << "chunk_size=" << chunk_size;
    }
}

// set_params() resets processed_rows() but leaves the intra-row cursor to on_new_params(). If that
// reset is missing, the first row of the next input chunk starts at the leftover element offset -
// silently dropping its first elements.
TEST(MultiUnnestTest, intra_row_cursor_is_reset_for_new_params) {
    const std::vector<ArrayRows> first_chunk = {ArrayRows{arr({1, 2, 3, 4, 5})}, ArrayRows{arr({6, 7})}};
    const std::vector<ArrayRows> second_chunk = {ArrayRows{arr({11, 12, 13})}, ArrayRows{arr({14})}};

    Columns first_columns;
    for (const auto& arg : first_chunk) {
        first_columns.emplace_back(make_nullable_array_column(arg));
    }
    Columns second_columns;
    for (const auto& arg : second_chunk) {
        second_columns.emplace_back(make_nullable_array_column(arg));
    }

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(2);

    MultiUnnest fn;
    MultiUnnest::UnnestState state;

    // Abandon the first chunk mid-row, as reset_state() or a re-primed pipeline would.
    state.set_params(first_columns);
    (void)fn.process(&runtime_state, &state);
    ASSERT_EQ(2, state.get_offset());

    state.set_params(second_columns);
    EXPECT_EQ(0, state.get_offset());
    const auto result = table_function_test::drive(fn, &runtime_state, &state);
    EXPECT_EQ(expected_expansion(second_chunk, false), result.rows);
}

// A row that is NULL or empty in every argument expands to nothing under an inner join and to a
// single all-NULL row under LEFT JOIN, at every chunk size.
TEST(MultiUnnestTest, empty_and_null_rows) {
    const std::vector<ArrayRows> args = {
            ArrayRows{kNullArray, arr({}), arr({1})},
            ArrayRows{arr({}), kNullArray, arr({})},
    };
    Columns columns;
    for (const auto& arg : args) {
        columns.emplace_back(make_nullable_array_column(arg));
    }

    MultiUnnest fn;
    for (int chunk_size : {1, 2, 64}) {
        for (bool is_left_join : {false, true}) {
            RuntimeState runtime_state{TQueryGlobals()};
            runtime_state.set_chunk_size(chunk_size);

            MultiUnnest::UnnestState state;
            state.set_is_left_join(is_left_join);
            state.set_params(columns);

            const auto result = table_function_test::drive(fn, &runtime_state, &state);
            EXPECT_EQ(expected_expansion(args, is_left_join), result.rows)
                    << "chunk_size=" << chunk_size << " left_join=" << is_left_join;
            // Two of these three rows expand to nothing. Under LEFT JOIN each must come back as a
            // length-1 bracket carrying the NULL row, never as a zero-length one; under an inner join
            // a row that expands to nothing *is* a zero-length bracket.
            EXPECT_EQ(is_left_join ? 0u : 2u, result.zero_length_brackets) << "chunk_size=" << chunk_size;
        }
    }
}

// GROUP_SLOW_TEST_F needs a fixture; this case carries no per-test state.
class MultiUnnestSlowTest : public ::testing::Test {};

// Regression test for https://github.com/StarRocks/starrocks/issues/76953, ported from #78479.
//
// MultiUnnest reads offsets per row on every call - it has no zero-copy path, since the zip's
// padding NULLs exist in no source column - and it used to read them through Datum::get_int32():
// Datum keeps an unsigned value in the matching signed slot, so any offset in [2^31, 2^32) came
// back negative. Two defects followed - the length subtraction between two int32_t operands
// overflowed for the array straddling the boundary, and the negative start offset passed to
// Column::append(src, size_t offset, size_t count) converted modularly to ~1.8e19, breaking the
// offset + count <= src.size() precondition.
//
// The offsets are read straight off the buffer as uint32_t since #61578, which restructured this
// loop for batching; this test pins that property against a future rewrite of the same loop.
//
// Marked SLOW: it needs an elements column holding more than 2^31 entries, i.e. about 4GiB of RSS
// (2GiB of TINYINT data plus a 2GiB null column). GROUP_SLOW_TEST_F compiles it as DISABLED_ unless
// NDEBUG is set, keeping it out of the default ASAN/Debug runs - where it would also pay the
// shadow-memory cost on those two buffers - and enabling it in release builds:
//
//   BUILD_TYPE=Release ./run-be-ut.sh --build-target expr_test --module expr_test \
//       --without-java-ext --gtest_filter='MultiUnnestSlowTest.SLOW_offsets_across_int32_boundary'
//
// The ArrayColumn constructor does not validate offsets against the elements size, so the offsets
// can start just below 2^31 instead of accumulating there row by row. That keeps the test to four
// rows of one element each: only the first argument's elements buffer is large, and the zip length
// stays 1, so nothing large is written to the output either.
GROUP_SLOW_TEST_F(MultiUnnestSlowTest, offsets_across_int32_boundary) {
    constexpr uint32_t kFirstOffset = 0x7ffffffeU;
    constexpr uint32_t kRowCount = 4;
    constexpr size_t kElementCount = static_cast<size_t>(kFirstOffset) + kRowCount;

    // Rows start at 0x7ffffffe, 0x7fffffff, 0x80000000 and 0x80000001: the length subtraction for
    // the second straddles the int32 boundary, and the last two are the appends whose start offset
    // used to come back negative.
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

    // MultiUnnest needs more than one argument. The second is an ordinary column of the same shape,
    // one element per row, so the zip pads nothing and every output row is one element wide.
    auto small_elements = Int8Column::create();
    for (int8_t v : {20, 21, 22, 23}) {
        small_elements->append(v);
    }
    auto small_element_nulls = NullColumn::create();
    small_element_nulls->resize(kRowCount);
    auto small_offsets = UInt32Column::create();
    for (uint32_t i = 0; i <= kRowCount; ++i) {
        small_offsets->append(i);
    }
    auto small_array =
            ArrayColumn::create(NullableColumn::create(std::move(small_elements), std::move(small_element_nulls)),
                                std::move(small_offsets));

    Columns columns;
    columns.emplace_back(std::move(big_array));
    columns.emplace_back(std::move(small_array));

    RuntimeState runtime_state{TQueryGlobals()};
    // Larger than the expansion, so the four rows come back from a single call.
    runtime_state.set_chunk_size(4096);

    MultiUnnest fn;
    MultiUnnest::UnnestState state;
    state.set_params(columns);

    auto [result_columns, copy_counts] = fn.process(&runtime_state, &state);
    ASSERT_EQ(2u, result_columns.size());
    ASSERT_EQ(kRowCount, state.processed_rows());

    // One element per input row, so the cumulative copy counts are 0, 1, 2, 3, 4.
    ASSERT_NE(nullptr, copy_counts);
    const auto counts = copy_counts->immutable_data();
    EXPECT_EQ(std::vector<uint32_t>({0, 1, 2, 3, 4}), std::vector<uint32_t>(counts.begin(), counts.end()));

    // Every row must carry the element its own offset points at, including the rows whose start
    // offset is past 2^31.
    ASSERT_EQ(kRowCount, result_columns[0]->size());
    ASSERT_EQ(kRowCount, result_columns[1]->size());
    const auto big_values = ColumnHelper::get_data_column_by_type<TYPE_TINYINT>(result_columns[0])->immutable_data();
    const auto small_values = ColumnHelper::get_data_column_by_type<TYPE_TINYINT>(result_columns[1])->immutable_data();
    for (uint32_t i = 0; i < kRowCount; ++i) {
        EXPECT_FALSE(result_columns[0]->is_null(i)) << "row " << i;
        EXPECT_EQ(static_cast<int8_t>(10 + i), big_values[i]) << "row " << i;
        EXPECT_FALSE(result_columns[1]->is_null(i)) << "row " << i;
        EXPECT_EQ(static_cast<int8_t>(20 + i), small_values[i]) << "row " << i;
    }
}

} // namespace starrocks

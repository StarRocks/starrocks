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
#include <optional>
#include <string>
#include <vector>

#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/table_function/table_function_harness.h"
#include "exprs/table_function/unnest_bitmap.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "types/bitmap_value.h"

namespace starrocks {

// unnest_bitmap slices one bitmap across as many process() calls as chunk_size demands, so its cursor
// is the pair (processed_rows(), get_offset()) - the input row being expanded, and how far into that
// row's bitmap the previous call got. table_function_test::drive() replays the contract
// TableFunctionOperator imposes on that cursor; what this file adds is the part specific to
// unnest_bitmap: the expansion itself, against a reference computed independently of the
// implementation, and the state transitions the cursor has to survive.

// One input row: nullopt for a NULL bitmap, otherwise the values it holds.
using BitmapRows = std::vector<std::optional<std::vector<uint64_t>>>;

namespace {

// A NULL row still needs a placeholder object so the bitmap column and the null column stay 1:1, which
// is what the real write paths produce too.
ColumnPtr make_nullable_bitmap_column(const BitmapRows& rows) {
    auto bitmaps = BitmapColumn::create();
    auto nulls = NullColumn::create();
    for (const auto& row : rows) {
        BitmapValue value;
        if (row.has_value()) {
            for (uint64_t v : *row) {
                value.add(v);
            }
        }
        bitmaps->append(std::move(value));
        nulls->append(row.has_value() ? 0 : 1);
    }
    // _has_null is computed from the null column at construction, so it has to be complete by now.
    return NullableColumn::create(std::move(bitmaps), std::move(nulls));
}

std::optional<std::vector<uint64_t>> bits(std::vector<uint64_t> values) {
    return std::optional<std::vector<uint64_t>>(std::move(values));
}
const std::optional<std::vector<uint64_t>> kNullBitmap = std::nullopt;

// A bitmap with more than 32 values switches to the roaring BITMAP representation, which is the only
// one next_batch() reads through a resumable iterator - the others are recomputed per call. Cheap way
// to make sure the batching is exercised on that representation too.
std::vector<uint64_t> ascending(uint64_t begin, uint64_t count) {
    std::vector<uint64_t> values;
    values.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        values.emplace_back(begin + i);
    }
    return values;
}

// The reference expansion: a bitmap is a set, so a row expands to its distinct values in ascending
// order; a NULL row expands to nothing. Rendered through the harness helper so the two compare
// directly.
std::vector<std::string> expected_expansion(const BitmapRows& rows, bool with_values = true) {
    std::vector<std::string> out;
    for (size_t row = 0; row < rows.size(); ++row) {
        if (!rows[row].has_value()) {
            continue;
        }
        std::vector<uint64_t> values = *rows[row];
        std::sort(values.begin(), values.end());
        values.erase(std::unique(values.begin(), values.end()), values.end());
        for (uint64_t value : values) {
            std::vector<std::string> rendered;
            if (with_values) {
                rendered.emplace_back(std::to_string(value));
            }
            out.emplace_back(table_function_test::render_row(row, rendered));
        }
    }
    return out;
}

// UnnestBitmap keeps its state type private, so the test drives it exactly the way the operator does:
// through init()/close() and the TableFunctionState interface.
class ScopedState {
public:
    explicit ScopedState(const UnnestBitmap& fn) : _fn(fn) { EXPECT_TRUE(_fn.init(TFunction(), &_state).ok()); }
    ~ScopedState() { (void)_fn.close(nullptr, _state); }
    ScopedState(const ScopedState&) = delete;
    ScopedState& operator=(const ScopedState&) = delete;

    TableFunctionState* operator->() const { return _state; }
    TableFunctionState* get() const { return _state; }

private:
    const UnnestBitmap& _fn;
    TableFunctionState* _state = nullptr;
};

// Every bitmap representation, plus the rows that expand to nothing.
const BitmapRows kMixedRows = {
        bits({7, 3, 5}),          // SET
        bits({}),                 // EMPTY
        kNullBitmap,              //
        bits({42}),               // SINGLE
        bits(ascending(100, 70)), // BITMAP (over 32 values)
        bits({9, 9, 9}),          // duplicates collapse
};

} // namespace

// A single process() call must not exceed chunk_size output rows, and the flattened result must be
// identical for every chunk_size - only the number of calls and the per-call size may differ.
TEST(UnnestBitmapTest, expansion_is_independent_of_chunk_size) {
    const Columns columns{make_nullable_bitmap_column(kMixedRows)};
    const auto expected = expected_expansion(kMixedRows);

    UnnestBitmap fn;
    for (int chunk_size : {1, 2, 3, 7, 64, 4096}) {
        RuntimeState runtime_state{TQueryGlobals()};
        runtime_state.set_chunk_size(chunk_size);

        ScopedState state(fn);
        state->set_params(columns);

        const auto result = table_function_test::drive(fn, &runtime_state, state.get());
        EXPECT_EQ(expected, result.rows) << "chunk_size=" << chunk_size;
        EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size)) << "chunk_size=" << chunk_size;
        // The NULL row and the empty bitmap expand to nothing; with no LEFT JOIN (which the analyzer
        // only allows for `unnest`) that is exactly a zero-length bracket each.
        EXPECT_EQ(2u, result.zero_length_brackets) << "chunk_size=" << chunk_size;
        if (static_cast<size_t>(chunk_size) < expected.size()) {
            EXPECT_GT(result.process_calls, 1u) << "chunk_size=" << chunk_size;
        }
    }
}

// One bitmap larger than a chunk: while it is only partially emitted processed_rows() must not
// advance - the operator would otherwise pair the remaining values with the next input row's outer
// columns - and the cursor it did advance has to be visible in the state, which is the only thing
// telling the pipeline driver that the call made progress.
TEST(UnnestBitmapTest, one_bitmap_split_across_calls_keeps_its_input_row) {
    const BitmapRows rows = {bits(ascending(1000, 10))};
    const Columns columns{make_nullable_bitmap_column(rows)};

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4);

    UnnestBitmap fn;
    ScopedState state(fn);
    state->set_params(columns);

    // First call on its own, to pin the cursor state in the middle of the row.
    auto [first_columns, first_offsets] = fn.process(&runtime_state, state.get());
    EXPECT_EQ(4u, first_columns[0]->size());
    EXPECT_EQ(0u, state->processed_rows());
    EXPECT_EQ(4, state->get_offset());
    // A row contributes at most one bracket per call, otherwise two brackets would map to two input
    // rows.
    EXPECT_EQ(2u, first_offsets->size());

    state->set_params(columns); // restart cleanly for the full drive
    const auto result = table_function_test::drive(fn, &runtime_state, state.get());
    EXPECT_EQ(expected_expansion(rows), result.rows);
    EXPECT_EQ(3u, result.process_calls); // 10 values / chunk 4
    EXPECT_EQ(4u, result.max_rows_per_call);
}

// The regression this file exists for. set_params() resets processed_rows() but leaves the intra-row
// cursor to on_new_params(). Without that override the offset survives into the next input chunk, and
// process() reads a non-zero offset as "carry on with the row I was reading" - dropping the leading
// values of the new chunk's first row, and reading through a BitmapValueIter that still points at the
// abandoned chunk's BitmapValue (a use-after-free once that chunk is released, which is what the
// explicit release below turns into an ASAN failure rather than a silently wrong answer).
//
// The operator abandons a chunk mid-row through reset_state() -> set_params(Columns{}), which a
// query-cache lane switching to another tablet performs on the whole lane; TableFunctionNode is
// allowed inside a cacheable fragment.
TEST(UnnestBitmapTest, intra_row_cursor_is_reset_for_new_params) {
    const BitmapRows first_chunk = {bits(ascending(1, 10))};
    const BitmapRows second_chunk = {bits({11, 12, 13}), bits({14})};
    const Columns second_columns{make_nullable_bitmap_column(second_chunk)};

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4);

    UnnestBitmap fn;
    ScopedState state(fn);

    Columns first_columns{make_nullable_bitmap_column(first_chunk)};
    state->set_params(first_columns);
    (void)fn.process(&runtime_state, state.get());
    // Stopped in the middle of the row, so the row is not consumed and the cursor into it is published
    // through the state. Deliberately EXPECT and not ASSERT: the assertion that matters is the one on
    // the second chunk's rows below, and it has to run even if this part regresses.
    EXPECT_EQ(0u, state->processed_rows());
    EXPECT_EQ(4, state->get_offset());

    // The state's copy is now the last reference, so the next set_params() frees the bitmap the
    // iterator was reading.
    first_columns.clear();
    state->set_params(second_columns);
    EXPECT_EQ(0, state->get_offset());

    const auto result = table_function_test::drive(fn, &runtime_state, state.get());
    EXPECT_EQ(expected_expansion(second_chunk), result.rows);
}

// A row whose cardinality is an exact multiple of chunk_size is finished by the call that fills the
// last batch, not by an extra call that comes back and finds it empty - which would also hand the
// operator a spurious zero-length bracket for a row it has already fully expanded.
TEST(UnnestBitmapTest, cardinality_that_is_an_exact_multiple_of_chunk_size) {
    const BitmapRows rows = {bits(ascending(1, 8)), bits(ascending(50, 4))};
    const Columns columns{make_nullable_bitmap_column(rows)};

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(4);

    UnnestBitmap fn;
    ScopedState state(fn);
    state->set_params(columns);

    const auto result = table_function_test::drive(fn, &runtime_state, state.get());
    EXPECT_EQ(expected_expansion(rows), result.rows);
    EXPECT_EQ(3u, result.process_calls); // 8 values -> 2 calls, 4 values -> 1 call
    EXPECT_EQ(0u, result.zero_length_brackets);
}

// With fn_result_required false the operator reads nothing but the bracket counts, so the values need
// not be materialized - nor the bitmap iterated at all, since how many rows a row expands to is its
// cardinality. The cursor still has to advance exactly as before.
TEST(UnnestBitmapTest, counts_only_when_result_is_not_required) {
    const Columns columns{make_nullable_bitmap_column(kMixedRows)};

    UnnestBitmap fn;
    for (int chunk_size : {1, 3, 7, 4096}) {
        RuntimeState runtime_state{TQueryGlobals()};
        runtime_state.set_chunk_size(chunk_size);

        ScopedState state(fn);
        state->set_is_required(false);
        state->set_params(columns);

        const auto result = table_function_test::drive(fn, &runtime_state, state.get());
        // Same row count and same bracket-to-input-row mapping, values not materialized.
        EXPECT_EQ(expected_expansion(kMixedRows, /*with_values=*/false), result.rows) << "chunk_size=" << chunk_size;
        EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size)) << "chunk_size=" << chunk_size;
        EXPECT_EQ(0u, result.max_fn_result_rows) << "chunk_size=" << chunk_size;
    }
}

// chunk_size 0 would make a bounded implementation emit no bracket at all, and the operator's driver
// loop (`while processed_rows() < input_rows()`) would then spin on it forever. One row per call is
// the floor.
TEST(UnnestBitmapTest, chunk_size_zero_still_makes_progress) {
    const Columns columns{make_nullable_bitmap_column(kMixedRows)};

    RuntimeState runtime_state{TQueryGlobals()};
    runtime_state.set_chunk_size(0);

    UnnestBitmap fn;
    ScopedState state(fn);
    state->set_params(columns);

    const auto result = table_function_test::drive(fn, &runtime_state, state.get());
    EXPECT_EQ(expected_expansion(kMixedRows), result.rows);
    EXPECT_EQ(1u, result.max_rows_per_call);
}

} // namespace starrocks

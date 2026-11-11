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
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exprs/table_function/subdivide_bitmap.h"
#include "exprs/table_function/table_function_harness.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "types/bitmap_value.h"

namespace starrocks {

// SubdivideBitmap now emits a bounded slice of the expansion per process() call, cutting inside a row
// when one bitmap alone splits into more pieces than a chunk holds. table_function_test::drive()
// replays the contract TableFunctionOperator imposes - which input row each bracket belongs to
// included - and checks the invariants every bounded implementation shares. What this file asserts on
// top of that is the part specific to subdivide_bitmap: which values land in which sub-bitmap,
// against a reference expansion computed from the input values alone, and the fact that the result
// does not depend on chunk_size at all - only the number of calls and the per-call size may.

// One input row: the bitmap's values (ascending, distinct), or nullopt for a NULL bitmap.
using BitmapRows = std::vector<std::optional<std::vector<uint64_t>>>;
// The split size of each input row, or nullopt for a NULL size.
using SizeRows = std::vector<std::optional<int32_t>>;

namespace {

ColumnPtr make_nullable_bitmap_column(const BitmapRows& rows) {
    auto bitmaps = BitmapColumn::create();
    auto nulls = NullColumn::create();
    for (const auto& row : rows) {
        BitmapValue bitmap;
        if (row.has_value()) {
            for (uint64_t value : *row) {
                bitmap.add(value);
            }
            nulls->append(0);
        } else {
            nulls->append(1);
        }
        // A NULL row still occupies a slot in the data column, holding the empty placeholder that
        // append_nulls() would have produced on the real paths.
        bitmaps->append(std::move(bitmap));
    }
    // _has_null is computed from the null column at construction, so it has to be complete by now.
    return NullableColumn::create(std::move(bitmaps), std::move(nulls));
}

ColumnPtr make_nullable_largeint_column(const std::vector<int128_t>& rows) {
    auto values = Int128Column::create();
    auto nulls = NullColumn::create();
    for (int128_t row : rows) {
        values->append(row);
        nulls->append(0);
    }
    return NullableColumn::create(std::move(values), std::move(nulls));
}

ColumnPtr make_nullable_int_column(const SizeRows& rows) {
    auto values = Int32Column::create();
    auto nulls = NullColumn::create();
    for (const auto& row : rows) {
        // The placeholder under a NULL is deliberately a valid split size, so that a row skipped for
        // being NULL cannot be passing for the wrong reason.
        values->append(row.value_or(3));
        nulls->append(row.has_value() ? 0 : 1);
    }
    return NullableColumn::create(std::move(values), std::move(nulls));
}

std::string join(const std::vector<uint64_t>& values, size_t begin, size_t end) {
    std::string out;
    for (size_t i = begin; i < end; ++i) {
        if (i > begin) {
            out += ",";
        }
        out += std::to_string(values[i]);
    }
    return out;
}

// The reference expansion, from the input values alone: a row whose bitmap fits in one piece yields
// that bitmap whole (an empty bitmap included - one row holding nothing, not zero rows), otherwise
// piece k holds the ascending elements [k*n, (k+1)*n). A NULL bitmap, a NULL size and a non-positive
// size all expand to nothing. Rendered through the harness helper so the two compare directly;
// BitmapValue::debug_item() is its to_string(), i.e. its values ascending and comma-separated.
std::vector<std::string> expected_expansion(const BitmapRows& bitmaps, const SizeRows& sizes, bool with_values = true) {
    std::vector<std::string> out;
    for (size_t row = 0; row < bitmaps.size(); ++row) {
        if (!bitmaps[row].has_value() || !sizes[row].has_value() || *sizes[row] <= 0) {
            continue;
        }
        const std::vector<uint64_t>& values = *bitmaps[row];
        const auto split_size = static_cast<size_t>(*sizes[row]);
        const size_t pieces = values.size() <= split_size ? 1 : (values.size() + split_size - 1) / split_size;
        for (size_t k = 0; k < pieces; ++k) {
            std::vector<std::string> rendered;
            if (with_values) {
                rendered.emplace_back(join(values, k * split_size, std::min((k + 1) * split_size, values.size())));
            }
            out.emplace_back(table_function_test::render_row(row, rendered));
        }
    }
    return out;
}

// init() hands back a TableFunctionState whose concrete type is private to the function, so tests
// hold it the way the operator does and let close() delete it.
struct StateHolder {
    explicit StateHolder(const TableFunction& fn) {
        CHECK(fn.init(TFunction(), &state).ok());
        CHECK(fn.prepare(state).ok());
    }
    TableFunctionState* state = nullptr;
};

// `count` consecutive values from `begin`. Which BitmapValue representation that produces is part of
// what the tests vary: up to 32 values stay a flat_hash_set, beyond that it becomes a Roaring bitset,
// and only the latter makes BitmapValueIter cache an iterator into the source.
std::vector<uint64_t> ascending(uint64_t begin, uint64_t count) {
    std::vector<uint64_t> values;
    values.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        values.emplace_back(begin + i);
    }
    return values;
}

} // namespace

// Deliberately mixed: a NULL bitmap, a NULL size, a zero size, an empty bitmap, a bitmap that fits in
// one piece, a SET-representation bitmap split into several pieces, and one large enough to be a
// Roaring bitset split into many.
TEST(SubdivideBitmapTest, expansion_is_independent_of_chunk_size) {
    const BitmapRows bitmaps = {
            std::nullopt,                  // NULL bitmap
            std::vector<uint64_t>{},       // empty bitmap -> one empty piece
            std::vector<uint64_t>{1},      // fits in one piece
            ascending(1, 10),              // SET representation, several pieces
            ascending(100, 7),             // NULL split size
            ascending(1000, 5),            // zero split size
            ascending(8589934592ULL, 100), // Roaring representation, many pieces
    };
    const SizeRows sizes = {3, 1, 5, 3, std::nullopt, 0, 7};
    const auto expected = expected_expansion(bitmaps, sizes);

    SubdivideBitmap<TYPE_INT> fn;
    for (int chunk_size : {1, 2, 3, 5, 4096}) {
        RuntimeState runtime_state;
        runtime_state.set_chunk_size(chunk_size);

        StateHolder holder(fn);
        holder.state->set_params(Columns{make_nullable_bitmap_column(bitmaps), make_nullable_int_column(sizes)});

        const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
        EXPECT_EQ(expected, result.rows) << "chunk_size=" << chunk_size;
        EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size)) << "chunk_size=" << chunk_size;
        // The point of the change: with a chunk smaller than the expansion, the work is spread over
        // several calls instead of one unbounded allocation.
        if (static_cast<size_t>(chunk_size) < expected.size()) {
            EXPECT_GT(result.process_calls, 1u) << "chunk_size=" << chunk_size;
        }
        ASSERT_OK(fn.close(&runtime_state, holder.state));
    }
}

// One row that splits into more pieces than a chunk holds: it must be cut across calls, and while it
// is only partially emitted processed_rows() must not advance - the operator would otherwise pair the
// remaining pieces with the next input row's outer columns.
TEST(SubdivideBitmapTest, one_row_split_across_calls_keeps_its_input_row) {
    const BitmapRows bitmaps = {ascending(1, 10), ascending(100, 2)};
    const SizeRows sizes = {1, 1};

    RuntimeState runtime_state;
    runtime_state.set_chunk_size(4);

    SubdivideBitmap<TYPE_INT> fn;
    StateHolder holder(fn);
    holder.state->set_params(Columns{make_nullable_bitmap_column(bitmaps), make_nullable_int_column(sizes)});

    // First call on its own, to pin the cursor state in the middle of the row.
    auto [first_columns, first_offsets] = fn.process(&runtime_state, holder.state);
    EXPECT_EQ(4u, first_columns[0]->size());
    EXPECT_EQ(0u, holder.state->processed_rows());
    // The cursor counts elements consumed, i.e. 4 pieces of one element each.
    EXPECT_EQ(4, holder.state->get_offset());
    // A row contributes at most one bracket per call, otherwise two brackets would map to two input
    // rows: one leading 0 plus one closing bracket.
    EXPECT_EQ(2u, first_offsets->size());

    holder.state->set_params(Columns{make_nullable_bitmap_column(bitmaps), make_nullable_int_column(sizes)});
    const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
    EXPECT_EQ(expected_expansion(bitmaps, sizes), result.rows);
    EXPECT_EQ(3u, result.process_calls); // 10 pieces / chunk 4, then the second row
    EXPECT_EQ(4u, result.max_rows_per_call);
    ASSERT_OK(fn.close(&runtime_state, holder.state));
}

// With is_required() false the operator reads nothing but the bracket counts, so no sub-bitmap has to
// be built and the source bitmap need not be walked at all - while the cursor advances exactly as
// before. The harness asserts the result columns stay empty; what is checked here is that the
// bracket-to-input-row mapping is unchanged.
TEST(SubdivideBitmapTest, counts_only_when_result_is_not_required) {
    const BitmapRows bitmaps = {ascending(1, 10), std::nullopt, std::vector<uint64_t>{}, ascending(50, 33)};
    const SizeRows sizes = {3, 2, 4, 5};
    const auto expected = expected_expansion(bitmaps, sizes, /*with_values=*/false);

    SubdivideBitmap<TYPE_INT> fn;
    for (int chunk_size : {1, 2, 3, 4096}) {
        RuntimeState runtime_state;
        runtime_state.set_chunk_size(chunk_size);

        StateHolder holder(fn);
        holder.state->set_is_required(false);
        holder.state->set_params(Columns{make_nullable_bitmap_column(bitmaps), make_nullable_int_column(sizes)});

        const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
        EXPECT_EQ(expected, result.rows) << "chunk_size=" << chunk_size;
        EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size)) << "chunk_size=" << chunk_size;
        ASSERT_OK(fn.close(&runtime_state, holder.state));
    }
}

// set_params() resets processed_rows() but leaves the intra-row cursor to on_new_params(). Without
// that reset the first row of the next input chunk resumes from the leftover element offset, and -
// because the cursor is mirrored into a BitmapValueIter holding a BitmapValue* and, for the Roaring
// representation, an iterator into it - the stale iterator is read against a bitmap that belongs to
// the chunk just abandoned. Under ASAN that is a heap-use-after-free, not merely dropped rows, which
// is why the test drops its own reference to the first chunk before handing over the second one: the
// state's copy is then the last one, so the columns are really freed.
TEST(SubdivideBitmapTest, intra_row_cursor_is_reset_for_new_params) {
    // > 32 values, so the source is a Roaring bitset and the iterator caches a Roaring iterator into
    // it rather than just an offset.
    const BitmapRows first_bitmaps = {ascending(1, 40)};
    const SizeRows first_sizes = {1};
    const BitmapRows second_bitmaps = {ascending(500, 6), ascending(900, 3)};
    const SizeRows second_sizes = {2, 1};

    RuntimeState runtime_state;
    runtime_state.set_chunk_size(4);

    SubdivideBitmap<TYPE_INT> fn;
    StateHolder holder(fn);

    {
        Columns first = {make_nullable_bitmap_column(first_bitmaps), make_nullable_int_column(first_sizes)};
        holder.state->set_params(first);
        // Abandon the chunk mid-row, as reset_state() or a re-primed pipeline would.
        auto [columns, offsets] = fn.process(&runtime_state, holder.state);
        ASSERT_EQ(0u, holder.state->processed_rows());
        ASSERT_EQ(4, holder.state->get_offset());
        // Every reference the test holds to the first chunk, including the result that borrows
        // nothing but is built from it, goes away here.
    }

    holder.state->set_params(
            Columns{make_nullable_bitmap_column(second_bitmaps), make_nullable_int_column(second_sizes)});
    EXPECT_EQ(0, holder.state->get_offset());
    const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
    EXPECT_EQ(expected_expansion(second_bitmaps, second_sizes), result.rows);
    ASSERT_OK(fn.close(&runtime_state, holder.state));
}

// subdivide_bitmap is registered for every integer type, LARGEINT included (table_function_factory's
// APPLY_FOR_ALL_INT_TYPE, and the same list in TableFunction.java), so a split size can be larger than
// any cardinality a bitmap can hold. Narrowing it into the uint64 the cursor arithmetic uses turns
// 2^64 into 0 - a value that has already passed the `<= 0` guard, and that the piece count would then
// divide by. It must instead behave like every other oversized split size: the whole bitmap, in one
// piece, which is what `subdivide_bitmap(b, 9223372036854775807)` already does today.
TEST(SubdivideBitmapTest, split_size_wider_than_uint64) {
    const BitmapRows bitmaps = {ascending(1, 10), ascending(100, 40), std::vector<uint64_t>{}};
    // 2^64 and 2^64 + 3: both narrow to a small value (0 and 3) that would silently change the answer.
    const int128_t two_to_the_64 = static_cast<int128_t>(1) << 64;
    const std::vector<int128_t> sizes = {two_to_the_64, two_to_the_64 + 3, two_to_the_64};

    std::vector<std::string> expected;
    for (size_t row = 0; row < bitmaps.size(); ++row) {
        expected.emplace_back(table_function_test::render_row(row, {join(*bitmaps[row], 0, bitmaps[row]->size())}));
    }

    SubdivideBitmap<TYPE_LARGEINT> fn;
    for (int chunk_size : {1, 2, 4096}) {
        RuntimeState runtime_state;
        runtime_state.set_chunk_size(chunk_size);

        StateHolder holder(fn);
        holder.state->set_params(Columns{make_nullable_bitmap_column(bitmaps), make_nullable_largeint_column(sizes)});

        const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
        EXPECT_EQ(expected, result.rows) << "chunk_size=" << chunk_size;
        ASSERT_OK(fn.close(&runtime_state, holder.state));
    }
}

} // namespace starrocks

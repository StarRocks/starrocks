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

#include <array>
#include <string>
#include <vector>

#include "column/adaptive_nullable_column.h"
#include "column/binary_column.h"
#include "column/column_hash/column_hash.h"
#include "column/json_column.h"
#include "column/nullable_column.h"

namespace starrocks {
namespace {

using RangeHashFn = void (*)(const Column&, uint32_t*, uint32_t, uint32_t);
using SelectionHashFn = void (*)(const Column&, uint32_t*, uint8_t*, uint16_t, uint16_t);
using SelectiveHashFn = void (*)(const Column&, uint32_t*, uint16_t*, uint16_t);

struct HashFnSet {
    const char* name;
    RangeHashFn range;
    SelectionHashFn selection;
    SelectiveHashFn selective;
};

constexpr std::array<HashFnSet, 4> kHashFunctions = {
        HashFnSet{"fnv", fnv_hash_column, fnv_hash_column_with_selection, fnv_hash_column_selective},
        HashFnSet{"crc32", crc32_hash_column, crc32_hash_column_with_selection, crc32_hash_column_selective},
        HashFnSet{"murmur3", murmur_hash3_x86_32_column, murmur_hash3_x86_32_column_with_selection,
                  murmur_hash3_x86_32_column_selective},
        HashFnSet{"xxh3", xxh3_64_column, xxh3_64_column_with_selection, xxh3_64_column_selective},
};

void verify_hashes_for_column(const Column& column) {
    const uint32_t size = column.size();
    ASSERT_GT(size, 0u);

    for (const auto& hash_fn : kHashFunctions) {
        std::vector<uint32_t> range_hashes(size, 0);
        hash_fn.range(column, range_hashes.data(), 0, size);

        bool has_non_zero = false;
        for (uint32_t i = 0; i < size; ++i) {
            if (!column.is_null(i) && range_hashes[i] != 0) {
                has_non_zero = true;
                break;
            }
        }
        EXPECT_TRUE(has_non_zero) << hash_fn.name;

        std::vector<uint8_t> selection(size, 0);
        selection[0] = 1;
        if (size > 1) {
            selection[size - 1] = 1;
        }
        std::vector<uint32_t> selection_hashes(size, 0);
        hash_fn.selection(column, selection_hashes.data(), selection.data(), 0, static_cast<uint16_t>(size));
        if (!column.is_null(0)) {
            EXPECT_NE(0u, selection_hashes[0]) << hash_fn.name;
        }
        if (size > 1 && !column.is_null(size - 1)) {
            EXPECT_NE(0u, selection_hashes[size - 1]) << hash_fn.name;
        }

        std::vector<uint32_t> selective_hashes(size, 0);
        uint16_t sel[] = {0, static_cast<uint16_t>(size - 1)};
        hash_fn.selective(column, selective_hashes.data(), sel, size > 1 ? 2 : 1);
        if (!column.is_null(0)) {
            EXPECT_NE(0u, selective_hashes[0]) << hash_fn.name;
        }
        if (size > 1 && !column.is_null(size - 1)) {
            EXPECT_NE(0u, selective_hashes[size - 1]) << hash_fn.name;
        }
    }
}

// fnv_hash_column_rebased() must produce, in hashes[0, to - from), exactly what
// fnv_hash_column() produces in hashes[from, to).
void verify_rebased_matches_absolute(const Column& column, uint32_t from, uint32_t to) {
    ASSERT_LT(from, to);
    ASSERT_LE(to, column.size());

    std::vector<uint32_t> absolute(column.size(), 0);
    fnv_hash_column(column, absolute.data(), from, to);

    std::vector<uint32_t> rebased(to - from, 0);
    fnv_hash_column_rebased(column, rebased.data(), from, to);

    for (uint32_t i = from; i < to; ++i) {
        EXPECT_EQ(absolute[i], rebased[i - from]) << "row " << i << " of [" << from << ", " << to << ")";
    }
}

} // namespace

// The rebased selector carries its own origin because the nullable fast path narrows [from, to) to
// a run of equal null values and recurses; anchoring the destination to the narrowed `from` would
// misplace every hash after the first run. Sparse nulls are what force that recursion, so cover a
// nullable column whose null runs do not line up with the requested sub-range.
TEST(ColumnHashCoreTest, RebasedRangeMatchesAbsoluteRange) {
    auto data = Int32Column::create();
    auto nulls = NullColumn::create();
    for (int32_t i = 0; i < 40; ++i) {
        data->append(i * 7 + 1);
        // Irregular null runs of length 1, 2 and 3.
        nulls->append((i % 5 == 0 || i % 7 == 3) ? 1 : 0);
    }
    auto nullable_col = NullableColumn::create(data, nulls);
    ASSERT_TRUE(nullable_col->has_null());

    // A sub-range starting inside a null run, one starting on a non-null, and the whole column.
    verify_rebased_matches_absolute(*nullable_col, 10, 33);
    verify_rebased_matches_absolute(*nullable_col, 11, 12);
    verify_rebased_matches_absolute(*nullable_col, 0, nullable_col->size());

    // Same for a column with no nulls at all, which takes the non-recursing path.
    auto dense_col = Int32Column::create();
    for (int32_t i = 0; i < 40; ++i) {
        dense_col->append(i * 13 + 5);
    }
    verify_rebased_matches_absolute(*dense_col, 17, 40);

    // And for a variable-length column, whose visitor reads offsets by absolute index.
    auto binary = BinaryColumn::create();
    for (int32_t i = 0; i < 40; ++i) {
        binary->append(std::string(1 + (i % 6), static_cast<char>('a' + (i % 26))));
    }
    verify_rebased_matches_absolute(*binary, 9, 40);
}

// AdaptiveNullableColumn gets its own accept() from ColumnFactory, so the visitor reaches
// do_visit(const AdaptiveNullableColumn&) rather than the NullableColumn overload. That overload
// used to return NotSupported, and every entry point discards the status, so hashing an adaptive
// column left the caller's buffer exactly as it found it. Pin it against the equivalent
// materialized NullableColumn, for both the absolute and the rebased destination.
TEST(ColumnHashCoreTest, AdaptiveNullableColumnMatchesMaterializedNullable) {
    constexpr uint32_t kRows = 24;

    auto adaptive = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());
    auto data = Int32Column::create();
    auto nulls = NullColumn::create();
    for (uint32_t i = 0; i < kRows; ++i) {
        if (i % 4 == 1) {
            ASSERT_TRUE(adaptive->append_nulls(1));
            data->append_default();
            nulls->append(1);
        } else {
            adaptive->append_datum(Datum(static_cast<int32_t>(i * 31 + 7)));
            data->append(static_cast<int32_t>(i * 31 + 7));
            nulls->append(0);
        }
    }
    auto materialized = NullableColumn::create(data, nulls);
    ASSERT_EQ(materialized->size(), adaptive->size());

    std::vector<uint32_t> from_adaptive(kRows, 0);
    std::vector<uint32_t> from_nullable(kRows, 0);
    adaptive->fnv_hash(from_adaptive.data(), 0, kRows);
    materialized->fnv_hash(from_nullable.data(), 0, kRows);
    for (uint32_t i = 0; i < kRows; ++i) {
        EXPECT_EQ(from_nullable[i], from_adaptive[i]) << "fnv_hash row " << i;
    }
    // A column that hashed to nothing would leave the buffer zeroed, which is what this catches.
    EXPECT_NE(0u, from_adaptive[0]);

    std::vector<uint32_t> rebased(kRows - 5, 0);
    adaptive->fnv_hash_rebased(rebased.data(), 5, kRows);
    for (uint32_t i = 5; i < kRows; ++i) {
        EXPECT_EQ(from_nullable[i], rebased[i - 5]) << "fnv_hash_rebased row " << i;
    }
}

TEST(ColumnHashCoreTest, BinaryAndLargeBinaryColumns) {
    auto binary = BinaryColumn::create();
    binary->append("alpha");
    binary->append("beta");
    binary->append("gamma");
    verify_hashes_for_column(*binary);

    auto large_binary = LargeBinaryColumn::create();
    large_binary->append("large-alpha");
    large_binary->append("large-beta");
    verify_hashes_for_column(*large_binary);
}

TEST(ColumnHashCoreTest, NullableAndJsonColumns) {
    auto nullable_data = Int32Column::create();
    nullable_data->append(1);
    nullable_data->append(2);
    nullable_data->append(3);

    auto nullable_nulls = NullColumn::create();
    nullable_nulls->append(0);
    nullable_nulls->append(1);
    nullable_nulls->append(0);
    auto nullable_col = NullableColumn::create(nullable_data, nullable_nulls);
    verify_hashes_for_column(*nullable_col);

    auto json_col = JsonColumn::create();
    auto json1 = JsonValue::parse("1");
    auto json2 = JsonValue::parse(R"({"k": "v"})");
    ASSERT_TRUE(json1.ok());
    ASSERT_TRUE(json2.ok());
    json_col->append(json1.value());
    json_col->append(json2.value());
    verify_hashes_for_column(*json_col);
}

} // namespace starrocks

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
#include <vector>

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

} // namespace

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

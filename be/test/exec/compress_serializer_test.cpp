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

#include "exec/aggregate/compress_serializer.h"

#include <gtest/gtest.h>

#include <any>
#include <vector>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/nullable_column.h"

namespace starrocks {

// A group-by key can reach the hash table as a constant. Aggregator::_evaluate_group_by_exprs
// unpacks constants into real columns with one deliberate exception -- an only-null constant is
// left packed, on the stated grounds that "all hash table could handle only null". The compressed
// key serializer did not: such a column matched neither its FixedLengthColumn nor its DecimalV3Column
// overload, fell through to the generic one, and took the BE down on CHECK(false) << "unreachable".
//
// `SELECT DISTINCT c FROM t GROUP BY CUBE (c)` produces exactly that, because the grouping set that
// drops c substitutes a constant NULL for it -- and the compressed key path is taken whenever the FE
// has min/max statistics for every group-by key and the compressed width falls into a smaller size
// class than the raw one.
class CompressSerializerTest : public ::testing::Test {};

// Bit layout below follows could_apply_bitcompress_opt: each key takes one bit for its null flag
// followed by the bits its value range needs, offsets[i] is the accumulated width before key i and
// used_bits[i] the accumulated width after it.
TEST_F(CompressSerializerTest, SerializesAnOnlyNullConstantKey) {
    constexpr size_t kRows = 8;
    Columns columns{ColumnHelper::create_const_null_column(kRows)};
    std::vector<std::any> bases{int64_t(0)};
    std::vector<int> offsets{0};

    std::vector<int64_t> keys(kRows, 0);
    bitcompress_serialize(columns, bases, offsets, kRows, sizeof(int64_t), keys.data());

    // Null flag set at bit 0, value bits left zero: every row produces the same key, which is what
    // a key that is NULL for the whole chunk means.
    for (size_t i = 0; i < kRows; i++) {
        EXPECT_EQ(int64_t(1), keys[i]) << "row " << i;
    }
}

// The one that would have caught the corruption a naive fix invites: writing the null flag at the
// wrong offset silently rewrites the neighbouring key's bits instead of crashing.
TEST_F(CompressSerializerTest, OnlyNullConstantKeyLeavesItsNeighbourIntact) {
    constexpr size_t kRows = 8;

    auto data = Int64Column::create();
    for (size_t i = 0; i < kRows; i++) {
        data->append(static_cast<int64_t>(i));
    }
    auto key0 = NullableColumn::create(std::move(data), NullColumn::create(kRows, 0));

    Columns columns{std::move(key0), ColumnHelper::create_const_null_column(kRows)};
    std::vector<std::any> bases{int64_t(0), int64_t(0)};
    // key0: 1 null bit + 3 value bits, key1: 1 null bit + 3 value bits.
    std::vector<int> offsets{0, 4};
    std::vector<int> used_bits{4, 8};

    std::vector<int64_t> keys(kRows, 0);
    bitcompress_serialize(columns, bases, offsets, kRows, sizeof(int64_t), keys.data());

    for (size_t i = 0; i < kRows; i++) {
        // key0: not null (bit 0 clear), value at bits 1..3; key1: null flag at bit 4.
        EXPECT_EQ(static_cast<int64_t>((i << 1) | (1 << 4)), keys[i]) << "row " << i;
    }

    // And it all reads back: key0 keeps its values and is not null, key1 comes back entirely null.
    MutableColumns out;
    out.emplace_back(NullableColumn::create(Int64Column::create(), NullColumn::create()));
    out.emplace_back(NullableColumn::create(Int64Column::create(), NullColumn::create()));
    bitcompress_deserialize(out, bases, offsets, used_bits, kRows, sizeof(int64_t), keys.data());

    ASSERT_EQ(kRows, out[0]->size());
    ASSERT_EQ(kRows, out[1]->size());
    for (size_t i = 0; i < kRows; i++) {
        EXPECT_FALSE(out[0]->is_null(i)) << "row " << i;
        EXPECT_EQ(static_cast<int64_t>(i), out[0]->get(i).get_int64()) << "row " << i;
        EXPECT_TRUE(out[1]->is_null(i)) << "row " << i;
    }
}

} // namespace starrocks

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

#include "column/adaptive_nullable_column.h"
#include "column/binary_column.h"

namespace starrocks {

TEST(AdaptiveNullableColumnCoreTest, NullStateTransitionAndNullCount) {
    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());

    EXPECT_EQ(AdaptiveNullableColumn::State::kUninitialized, col->state());
    EXPECT_EQ(0, col->size());

    ASSERT_TRUE(col->append_nulls(2));
    EXPECT_EQ(AdaptiveNullableColumn::State::kNull, col->state());
    EXPECT_EQ(2, col->size());
    EXPECT_EQ(2, col->null_count());
    EXPECT_TRUE(col->has_null());

    col->append_datum(Datum(int32_t(9)));
    EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
    EXPECT_EQ(3, col->size());
    EXPECT_TRUE(col->is_null(0));
    EXPECT_FALSE(col->is_null(2));
    EXPECT_EQ(9, col->get(2).get_int32());
}

TEST(AdaptiveNullableColumnCoreTest, NotConstantPathAndDefaultNotNullValue) {
    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());

    col->append_datum(Datum(int32_t(3)));
    EXPECT_EQ(AdaptiveNullableColumn::State::kNotConstant, col->state());
    EXPECT_EQ(1, col->size());
    EXPECT_FALSE(col->has_null());

    col->append_default_not_null_value();
    EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
    EXPECT_EQ(2, col->size());

    ASSERT_TRUE(col->append_nulls(1));
    EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
    EXPECT_TRUE(col->has_null());
    EXPECT_EQ(1, col->null_count());
}

// The compact key encoding needs its own materializing overrides here. NullableColumn overrides
// max_one_element_serialize_size_compact() / serialize_compact() / deserialize_compact_and_append(),
// which shadows Column's defaults -- and those defaults are what would otherwise have dispatched
// back through the virtual serialize() / deserialize_and_append() that this class overrides purely
// to materialize first. Without an override per entry point, an unmaterialized column reads a
// _has_null and a data column that do not describe its contents yet.
// NOLINTNEXTLINE
TEST(AdaptiveNullableColumnCoreTest, CompactSerializationMaterializesFirst) {
    constexpr uint32_t kExpected = sizeof(bool) + 1 /* compact length byte */ + 8 /* "abcdefgh" */;

    // max_one_element_serialize_size_compact() sizes the hash-table staging buffer and runs on
    // every chunk, so an unmaterialized under-report here is a buffer overrun in serialize_batch().
    {
        auto col = AdaptiveNullableColumn::create(BinaryColumn::create(), NullColumn::create());
        col->append_datum(Datum(Slice("abcdefgh")));
        ASSERT_NE(AdaptiveNullableColumn::State::kMaterialized, col->state());

        const uint32_t bound = col->max_one_element_serialize_size_compact();
        EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
        EXPECT_EQ(kExpected, bound);
    }

    // serialize_compact() is the row-wise key builder the aggregator falls back to for wide keys.
    {
        auto col = AdaptiveNullableColumn::create(BinaryColumn::create(), NullColumn::create());
        col->append_datum(Datum(Slice("abcdefgh")));
        ASSERT_NE(AdaptiveNullableColumn::State::kMaterialized, col->state());

        uint8_t buf[64] = {};
        const uint32_t written = col->serialize_compact(0, buf);
        EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
        ASSERT_EQ(kExpected, written);
        EXPECT_EQ(0, buf[0]);            // null flag: not null
        EXPECT_EQ(8, buf[sizeof(bool)]); // one-byte compact length, not four
        EXPECT_EQ(0, memcmp(buf + sizeof(bool) + 1, "abcdefgh", 8));
    }

    // deserialize_compact_and_append() is the drain path, and appends into a column that may
    // itself still be unmaterialized.
    {
        auto src = AdaptiveNullableColumn::create(BinaryColumn::create(), NullColumn::create());
        src->append_datum(Datum(Slice("abcdefgh")));
        uint8_t buf[64] = {};
        const uint32_t written = src->serialize_compact(0, buf);

        auto dst = AdaptiveNullableColumn::create(BinaryColumn::create(), NullColumn::create());
        ASSERT_TRUE(dst->append_nulls(1));
        ASSERT_NE(AdaptiveNullableColumn::State::kMaterialized, dst->state());

        const uint8_t* end = dst->deserialize_compact_and_append(buf);
        EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, dst->state());
        EXPECT_EQ(buf + written, end);
        ASSERT_EQ(2u, dst->size());
        EXPECT_TRUE(dst->is_null(0));
        EXPECT_FALSE(dst->is_null(1));
        EXPECT_EQ(Slice("abcdefgh"), dst->get(1).get_slice());
    }
}

} // namespace starrocks

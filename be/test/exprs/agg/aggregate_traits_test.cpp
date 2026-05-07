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

#include "exprs/agg/aggregate_traits.h"

#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "runtime/mem_pool.h"
#include "types/bitmap_value.h"

namespace starrocks {

class AggregateTraitsTest : public testing::Test {
public:
    AggregateTraitsTest() = default;

    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(AggregateTraitsTest, FixedLength_append_values_int32) {
    using Traits = AggDataTypeTraits<TYPE_INT>;
    auto col = Int32Column::create();
    Traits::ValueType value = 42;
    Traits::append_values(col.get(), value, 5);
    EXPECT_EQ(col->debug_string(), "[42, 42, 42, 42, 42]");
}

TEST_F(AggregateTraitsTest, ObjectFamily_append_values_bitmap) {
    using Traits = AggDataTypeTraits<TYPE_OBJECT>;
    auto col = BitmapColumn::create();
    BitmapValue value;
    value.add(1);
    value.add(2);
    value.add(3);
    Traits::append_values(col.get(), value, 3);
    EXPECT_EQ(col->debug_string(), "[1,2,3, 1,2,3, 1,2,3]");
}

TEST_F(AggregateTraitsTest, StringOrBinary_append_values_varchar) {
    using Traits = AggDataTypeTraits<TYPE_VARCHAR>;
    auto col = BinaryColumn::create();
    Traits::ValueType value{'h', 'e', 'l', 'l', 'o'};
    Traits::append_values(col.get(), value, 4);
    EXPECT_EQ(col->debug_string(), "['hello', 'hello', 'hello', 'hello']");
}

TEST_F(AggregateTraitsTest, StringOrBinary_append_values_large_binary) {
    using Traits = AggDataTypeTraits<TYPE_VARCHAR>;
    auto col = LargeBinaryColumn::create();
    Traits::ValueType value{'f', 'o', 'o'};
    Traits::append_values(col.get(), value, 3);
    EXPECT_EQ(col->debug_string(), "['foo', 'foo', 'foo']");
}

TEST_F(AggregateTraitsTest, Array_append_values) {
    using Traits = AggDataTypeTraits<TYPE_ARRAY>;

    // Build value: ArrayColumn holding one row [1, 2, 3]
    auto value_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto value_offs = UInt32Column::create();
    auto value = ArrayColumn::create(std::move(value_elem), std::move(value_offs));
    value->append_datum(DatumArray{(int32_t)1, (int32_t)2, (int32_t)3});

    // Build destination column
    auto dst_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto dst_offs = UInt32Column::create();
    auto col = ArrayColumn::create(std::move(dst_elem), std::move(dst_offs));

    Traits::append_values(col.get(), value, 3);

    EXPECT_EQ(col->debug_string(), "[[1,2,3], [1,2,3], [1,2,3]]");
}
} // namespace starrocks
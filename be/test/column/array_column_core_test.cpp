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

#include <vector>

#include "column/array_column.h"
#include "column/nullable_column.h"

namespace starrocks {

TEST(ArrayColumnCoreTest, SerializeAndLengthCompare) {
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    auto array_col = ArrayColumn::create(std::move(elements), std::move(offsets));

    array_col->append_datum(DatumArray{Datum(int32_t(1)), Datum(int32_t(2))});
    array_col->append_datum(DatumArray{Datum(int32_t(3))});

    ASSERT_EQ(2, array_col->size());
    EXPECT_EQ(2, array_col->get_element_size(0));
    EXPECT_EQ(1, array_col->get_element_size(1));

    std::vector<uint8_t> encoded(array_col->serialize_size(0));
    array_col->serialize(0, encoded.data());

    auto restored = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                        UInt32Column::create());
    restored->deserialize_and_append(encoded.data());

    ASSERT_EQ(1, restored->size());
    EXPECT_EQ("[1,2]", restored->debug_item(0));

    auto v1 = ColumnPtr(array_col);
    auto v2_mut = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                      UInt32Column::create());
    v2_mut->append_datum(DatumArray{Datum(int32_t(5)), Datum(int32_t(6))});
    v2_mut->append_datum(DatumArray{Datum(int32_t(7))});
    auto v2 = ColumnPtr(v2_mut);

    EXPECT_TRUE(ArrayColumn::is_all_array_lengths_equal<true>(v1, v2, nullptr));
}

} // namespace starrocks

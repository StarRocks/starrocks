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

#include "column/decimalv3_column.h"
#include "column/json_column.h"
#include "gutil/casts.h"
#include "types/decimalv3.h"

namespace starrocks {

TEST(DecimalV3JsonCoreTest, Decimal64SerializeCompareAndDefault) {
    auto col = Decimal64Column::create(18, 2, 2);
    auto* decimal_col = down_cast<Decimal64Column*>(col.get());
    auto& data = decimal_col->get_data();

    ASSERT_EQ(2, data.size());
    data[0] = 1234;
    data[1] = 5678;
    EXPECT_LT(decimal_col->compare_at(0, 1, *decimal_col, -1), 0);

    decimal_col->append_default();
    ASSERT_EQ(3, decimal_col->size());

    std::vector<uint8_t> buffer(decimal_col->serialize_size(0));
    decimal_col->serialize(0, buffer.data());

    auto restored = Decimal64Column::create(18, 2, 0);
    restored->deserialize_and_append(buffer.data());
    ASSERT_EQ(1, restored->size());
    EXPECT_EQ(0, restored->compare_at(0, 0, *decimal_col, -1));
}

TEST(DecimalV3JsonCoreTest, JsonSerializeAndCompare) {
    auto col = JsonColumn::create();

    auto json1 = JsonValue::parse(R"({"a": 1})");
    auto json2 = JsonValue::parse(R"([1, 2, 3])");
    ASSERT_TRUE(json1.ok());
    ASSERT_TRUE(json2.ok());

    col->append(json1.value());
    col->append(json2.value());
    col->append_default();
    ASSERT_EQ(3, col->size());

    std::vector<uint8_t> buffer(col->serialize_size(1));
    col->serialize(1, buffer.data());

    auto restored = JsonColumn::create();
    restored->deserialize_and_append(buffer.data());
    ASSERT_EQ(1, restored->size());
    EXPECT_EQ(0, restored->compare_at(0, 1, *col, 0));
}

} // namespace starrocks

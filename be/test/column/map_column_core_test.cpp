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

#include "column/map_column.h"

namespace starrocks {
namespace {

MapColumn::MutablePtr create_empty_map() {
    auto key_col = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto value_col = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);
    return MapColumn::create(std::move(key_col), std::move(value_col), std::move(offsets));
}

MapColumn::MutablePtr create_single_row_map(const std::vector<Datum>& keys, const std::vector<Datum>& values) {
    auto key_col = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto value_col = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);

    if (keys.size() != values.size()) {
        return nullptr;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        key_col->append_datum(keys[i]);
        value_col->append_datum(values[i]);
    }
    offsets->append(static_cast<uint32_t>(keys.size()));

    return MapColumn::create(std::move(key_col), std::move(value_col), std::move(offsets));
}

} // namespace

TEST(MapColumnCoreTest, SerializeDeterministicForDifferentInputOrder) {
    auto col1 = create_single_row_map({Datum(int32_t(2)), Datum(int32_t(1)), Datum()},
                                      {Datum(int32_t(20)), Datum(int32_t(10)), Datum(int32_t(0))});
    auto col2 = create_single_row_map({Datum(), Datum(int32_t(2)), Datum(int32_t(1))},
                                      {Datum(int32_t(0)), Datum(int32_t(20)), Datum(int32_t(10))});
    ASSERT_NE(nullptr, col1);
    ASSERT_NE(nullptr, col2);

    std::vector<uint8_t> s1(col1->serialize_size(0));
    std::vector<uint8_t> s2(col2->serialize_size(0));
    col1->serialize(0, s1.data());
    col2->serialize(0, s2.data());

    EXPECT_EQ(s1, s2);
}

TEST(MapColumnCoreTest, SerializeUsesNullFirstThenAscendingKeys) {
    auto src = create_single_row_map({Datum(int32_t(2)), Datum(), Datum(int32_t(1))},
                                     {Datum(int32_t(20)), Datum(int32_t(0)), Datum(int32_t(10))});
    ASSERT_NE(nullptr, src);

    std::vector<uint8_t> encoded(src->serialize_size(0));
    src->serialize(0, encoded.data());

    auto restored = create_empty_map();
    ASSERT_NE(nullptr, restored);
    restored->deserialize_and_append(encoded.data());

    ASSERT_EQ(1, restored->size());

    auto* keys = down_cast<NullableColumn*>(restored->keys_column_raw_ptr());
    auto* key_data = down_cast<Int32Column*>(keys->data_column_raw_ptr());
    const auto& nulls = keys->immutable_null_column_data();

    ASSERT_EQ(3, nulls.size());
    EXPECT_EQ(1, nulls[0]);
    EXPECT_EQ(0, nulls[1]);
    EXPECT_EQ(0, nulls[2]);
    EXPECT_EQ(1, key_data->get(1).get_int32());
    EXPECT_EQ(2, key_data->get(2).get_int32());
}

} // namespace starrocks

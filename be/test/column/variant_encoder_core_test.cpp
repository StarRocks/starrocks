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

#include <string>

#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/variant_encoder.h"

namespace starrocks {
namespace {

MapColumn::MutablePtr build_single_row_map_column(ColumnPtr key_data, ColumnPtr value_data) {
    DCHECK_EQ(key_data->size(), value_data->size());
    size_t num_entries = key_data->size();
    auto key_nullable = NullableColumn::create(Column::mutate(std::move(key_data)), NullColumn::create(num_entries, 0));
    auto value_nullable =
            NullableColumn::create(Column::mutate(std::move(value_data)), NullColumn::create(num_entries, 0));
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(static_cast<uint32_t>(num_entries));
    return MapColumn::create(std::move(key_nullable), std::move(value_nullable), std::move(offsets));
}

} // namespace

TEST(VariantEncoderCoreTest, map_int_key_encode_succeeds) {
    auto keys = Int32Column::create();
    keys->append(1);
    keys->append(2);

    auto values = Int32Column::create();
    values->append(10);
    values->append(20);

    auto map_col = build_single_row_map_column(keys, values);
    auto map_type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT));

    auto variant_or = VariantEncoder::encode_shredded_column_row(map_col, map_type, 0, nullptr);
    ASSERT_TRUE(variant_or.ok()) << variant_or.status().to_string();

    auto json_or = variant_or.value().to_json();
    ASSERT_TRUE(json_or.ok()) << json_or.status().to_string();
    EXPECT_NE(json_or.value().find("\"1\""), std::string::npos);
    EXPECT_NE(json_or.value().find("\"2\""), std::string::npos);
}

TEST(VariantEncoderCoreTest, map_bool_key_uses_zero_one_string) {
    auto keys = BooleanColumn::create();
    keys->append(0);
    keys->append(1);

    auto values = Int32Column::create();
    values->append(7);
    values->append(8);

    auto map_col = build_single_row_map_column(keys, values);
    auto map_type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_INT));

    auto variant_or = VariantEncoder::encode_shredded_column_row(map_col, map_type, 0, nullptr);
    ASSERT_TRUE(variant_or.ok()) << variant_or.status().to_string();

    auto json_or = variant_or.value().to_json();
    ASSERT_TRUE(json_or.ok()) << json_or.status().to_string();
    EXPECT_NE(json_or.value().find("\"0\""), std::string::npos);
    EXPECT_NE(json_or.value().find("\"1\""), std::string::npos);
}

TEST(VariantEncoderCoreTest, unsupported_map_key_returns_variant_error) {
    auto keys = UInt32Column::create();
    keys->append(1);

    auto values = Int32Column::create();
    values->append(100);

    auto map_col = build_single_row_map_column(keys, values);
    auto map_type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_UNSIGNED_INT), TypeDescriptor(TYPE_INT));

    auto variant_or = VariantEncoder::encode_shredded_column_row(map_col, map_type, 0, nullptr);
    ASSERT_FALSE(variant_or.ok());
    EXPECT_NE(variant_or.status().message().find("Unsupported variant map key type"), std::string::npos);
}

TEST(VariantEncoderCoreTest, encode_column_allow_throw_false_appends_null_for_unsupported_map_key) {
    auto keys = UInt32Column::create();
    keys->append(3);

    auto values = Int32Column::create();
    values->append(30);

    auto map_col = build_single_row_map_column(keys, values);
    auto map_type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_UNSIGNED_INT), TypeDescriptor(TYPE_INT));

    ColumnBuilder<TYPE_VARIANT> non_throw_builder(1);
    Status non_throw_status = VariantEncoder::encode_column(map_col, map_type, &non_throw_builder, false);
    ASSERT_TRUE(non_throw_status.ok()) << non_throw_status.to_string();
    auto non_throw_col = non_throw_builder.build(false);
    ASSERT_EQ(1, non_throw_col->size());
    ASSERT_TRUE(non_throw_col->is_nullable());
    EXPECT_TRUE(non_throw_col->is_null(0));

    ColumnBuilder<TYPE_VARIANT> throw_builder(1);
    Status throw_status = VariantEncoder::encode_column(map_col, map_type, &throw_builder, true);
    ASSERT_FALSE(throw_status.ok());
    EXPECT_NE(throw_status.message().find("Unsupported variant map key type"), std::string::npos);
}

} // namespace starrocks

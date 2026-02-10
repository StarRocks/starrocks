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

#include <string_view>

#include "base/testutil/parallel_test.h"
#include "column/column_builder.h"
#include "column/type_traits.h"
#include "types/type_descriptor.h"
#include "types/variant.h"
#include "types/variant_value.h"

namespace starrocks {

static inline uint8_t primitive_header_ext(VariantType primitive) {
    return static_cast<uint8_t>(primitive) << 2;
}

// These scenarios are intentionally kept in starrocks_test because they depend on ColumnHelper/nullable paths.
// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnExtendedTest, test_builder_and_column_helper_clone_paths) {
    const uint8_t int_chars[] = {primitive_header_ext(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
    std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
    VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};

    // create from builder
    {
        ColumnBuilder<TYPE_VARIANT> builder(1);
        builder.append(&variant);
        auto column = builder.build(false);
        EXPECT_EQ("variant", column->get_name());
        EXPECT_TRUE(column->is_variant());

        auto column_ptr = ColumnHelper::cast_to<TYPE_VARIANT>(column);
        ASSERT_EQ(column_ptr->size(), 1);
        auto res = column_ptr->get_object(0);
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());
    }

    // clone nullable by helper
    {
        auto column = VariantColumn::create();
        column->append(&variant);

        TypeDescriptor desc = TypeDescriptor::create_variant_type();
        auto copy = ColumnHelper::clone_column(desc, true, column, column->size());
        ASSERT_EQ(copy->size(), 1);
        ASSERT_TRUE(copy->is_nullable());

        Column* unwrapped = ColumnHelper::get_data_column(copy.get());
        auto* variant_column_ptr = down_cast<VariantColumn*>(unwrapped);
        ASSERT_EQ(variant_column_ptr->size(), 1);
        auto res = variant_column_ptr->get(0).get_variant();
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());
    }

    // clone variant column by helper
    {
        auto column = VariantColumn::create();
        column->append(&variant);

        TypeDescriptor desc = TypeDescriptor::create_variant_type();
        ColumnPtr copy = ColumnHelper::clone_column(desc, false, column, column->size());
        ASSERT_EQ(copy->size(), 1);
        ASSERT_FALSE(copy->is_nullable());

        auto variant_column_ptr = ColumnHelper::cast_to<TYPE_VARIANT>(copy);
        ASSERT_EQ(variant_column_ptr->size(), 1);
        auto res = variant_column_ptr->get(0).get_variant();
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());

        auto variant_column = ColumnHelper::cast_to_raw<TYPE_VARIANT>(copy);
        ASSERT_EQ(variant_column->size(), 1);
        auto raw_res = variant_column->get(0).get_variant();
        ASSERT_EQ(raw_res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(raw_res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(raw_res->get_value(), variant.get_value());
        EXPECT_EQ(raw_res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", raw_res->to_string());
    }
}

} // namespace starrocks

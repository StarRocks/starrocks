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

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "formats/parquet/variant.h"
#include "testutil/parallel_test.h"
#include "types/logical_type.h"
#include "types/variant_value.h"

namespace starrocks {

uint8_t primitiveHeader(VariantPrimitiveType primitive) {
    return static_cast<uint8_t>(primitive) << 2;
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_build_column) {
    // create from type traits
    {
        const uint8_t int_chars[] = {primitiveHeader(VariantPrimitiveType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantValue variant{VariantMetadata::kEmptyMetadata, int_string};
        auto column = RunTimeColumnType<TYPE_VARIANT>::create();
        EXPECT_EQ("variant", column->get_name());
        EXPECT_TRUE(column->is_variant());
        column->append(&variant);
        auto res = column->get_object(0);
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());
    }
    // create from builder
    {
        ColumnBuilder<TYPE_VARIANT> builder(1);
        const uint8_t int_chars[] = {primitiveHeader(VariantPrimitiveType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantValue variant{VariantMetadata::kEmptyMetadata, int_string};
        builder.append(&variant);
        auto column = builder.build(false);
        EXPECT_EQ("variant", column->get_name());
        EXPECT_TRUE(column->is_variant());

        VariantColumn::Ptr column_ptr = ColumnHelper::cast_to<TYPE_VARIANT>(column);
        ASSERT_EQ(column_ptr->size(), 1);
        auto res = column_ptr->get_object(0);
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());
    }
    // clone
    {
        VariantColumn::Ptr column = VariantColumn::create();
        const uint8_t int_chars[] = {primitiveHeader(VariantPrimitiveType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantValue variant{VariantMetadata::kEmptyMetadata, int_string};
        column->append(&variant);

        {
            auto copy = column->clone();
            ASSERT_EQ(copy->size(), 1);
            auto res = copy->get(0).get_variant();
            ASSERT_EQ(res->serialize_size(), variant.serialize_size());
            ASSERT_EQ(res->get_metadata(), variant.get_metadata());
            ASSERT_EQ(res->get_value(), variant.get_value());
            EXPECT_EQ(res->to_string(), variant.to_string());
            EXPECT_EQ("1234567890", res->to_string());
        }
        // clone nullable by helper
        {
            TypeDescriptor desc = TypeDescriptor::create_variant_type();
            auto copy = ColumnHelper::clone_column(desc, true, column, column->size());
            ASSERT_EQ(copy->size(), 1);
            ASSERT_TRUE(copy->is_nullable());

            // unwrap nullable column
            Column* unwrapped = ColumnHelper::get_data_column(copy.get());

            VariantColumn* variant_column_ptr = down_cast<VariantColumn*>(unwrapped);
            ASSERT_EQ(variant_column_ptr->size(), 1);
            auto res = variant_column_ptr->get(0).get_variant();
            ASSERT_EQ(res->serialize_size(), variant.serialize_size());
            ASSERT_EQ(res->get_metadata(), variant.get_metadata());
            ASSERT_EQ(res->get_value(), variant.get_value());
            EXPECT_EQ(res->to_string(), variant.to_string());
            EXPECT_EQ("1234567890", res->to_string());
        }
        // clone variant_column by helper
        {
            TypeDescriptor desc = TypeDescriptor::create_variant_type();
            ColumnPtr copy = ColumnHelper::clone_column(desc, false, column, column->size());
            ASSERT_EQ(copy->size(), 1);
            ASSERT_FALSE(copy->is_nullable());

            VariantColumn::Ptr variant_column_ptr = ColumnHelper::cast_to<TYPE_VARIANT>(copy);
            ASSERT_EQ(variant_column_ptr->size(), 1);
            auto res = variant_column_ptr->get(0).get_variant();
            ASSERT_EQ(res->serialize_size(), variant.serialize_size());
            ASSERT_EQ(res->get_metadata(), variant.get_metadata());
            ASSERT_EQ(res->get_value(), variant.get_value());
            EXPECT_EQ(res->to_string(), variant.to_string());
            EXPECT_EQ("1234567890", res->to_string());

            VariantColumn* variant_column = ColumnHelper::cast_to_raw<TYPE_VARIANT>(copy.get());
            ASSERT_EQ(variant_column->size(), 1);
            auto raw_res = variant_column->get(0).get_variant();
            ASSERT_EQ(res->serialize_size(), variant.serialize_size());
            ASSERT_EQ(raw_res->get_metadata(), variant.get_metadata());
            ASSERT_EQ(raw_res->get_value(), variant.get_value());
            EXPECT_EQ(raw_res->to_string(), variant.to_string());
            EXPECT_EQ("1234567890", raw_res->to_string());
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_serialize) {
    std::string_view empty_metadata = VariantMetadata::kEmptyMetadata;
    const uint8_t uuid_chars[] = {primitiveHeader(VariantPrimitiveType::UUID),
                                  0xf2,
                                  0x4f,
                                  0x9b,
                                  0x64,
                                  0x81,
                                  0xfa,
                                  0x49,
                                  0xd1,
                                  0xb7,
                                  0x4e,
                                  0x8c,
                                  0x09,
                                  0xa6,
                                  0xe3,
                                  0x1c,
                                  0x56};

    std::string_view uuid_string(reinterpret_cast<const char*>(uuid_chars), sizeof(uuid_chars));
    VariantValue variant{empty_metadata, uuid_string};

    auto column = RunTimeColumnType<TYPE_VARIANT>::create();
    EXPECT_EQ("variant", column->get_name());
    EXPECT_TRUE(column->is_variant());
    column->append(&variant);
    EXPECT_EQ(variant.serialize_size(), column->serialize_size(0));

    // deserialize
    std::vector<uint8_t> buffer;
    buffer.resize(variant.serialize_size());
    column->serialize(0, buffer.data());
    auto new_column = column->clone_empty();
    new_column->deserialize_and_append(buffer.data());
    const VariantValue* deserialized_variant = new_column->get(0).get_variant();
    ASSERT_TRUE(deserialized_variant != nullptr);
    EXPECT_EQ(variant.serialize_size(), deserialized_variant->serialize_size());
    EXPECT_EQ(variant.to_string(), deserialized_variant->to_string());
    EXPECT_EQ("\"f24f9b64-81fa-49d1-b74e-8c09a6e31c56\"", deserialized_variant->to_json().value());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, put_mysql_row_buffer) {
    const uint8_t int_chars[] = {primitiveHeader(VariantPrimitiveType::INT32), 0xD2, 0x02, 0x96, 0x49};
    std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
    VariantValue variant{VariantMetadata::kEmptyMetadata, int_string};

    auto column = VariantColumn::create();
    column->append(&variant);

    MysqlRowBuffer buf;
    column->put_mysql_row_buffer(&buf, 0);
    EXPECT_EQ("\n1234567890", buf.data());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_create_variant_column) {
    auto variant_column = VariantColumn::create();

    // Test basic column operations that exercise visitor patterns
    EXPECT_EQ(0, variant_column->size());
    EXPECT_TRUE(variant_column->empty());
    EXPECT_FALSE(variant_column->is_nullable());
    EXPECT_FALSE(variant_column->is_constant());

    // Test column cloning which uses visitor patterns internally
    auto cloned = variant_column->clone();
    EXPECT_EQ(0, cloned->size());

    // Test memory operations
    size_t memory_usage = variant_column->memory_usage();
    EXPECT_GE(memory_usage, 0);
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_append_strings) {
    const auto variant_column = VariantColumn::create();
    const uint8_t int1_value[] = {primitiveHeader(VariantPrimitiveType::INT8), 0x01};
    const std::string_view int1_value_str(reinterpret_cast<const char*>(int1_value), sizeof(int1_value));
    constexpr uint32_t int1_total_size = sizeof(int1_value) + VariantMetadata::kEmptyMetadata.size();
    std::string variant_string;
    variant_string.resize(int1_total_size + sizeof(uint32_t));
    memcpy(variant_string.data(), &int1_total_size, sizeof(uint32_t));
    memcpy(variant_string.data() + sizeof(uint32_t), VariantMetadata::kEmptyMetadata.data(),
           VariantMetadata::kEmptyMetadata.size());
    memcpy(variant_string.data() + sizeof(uint32_t) + VariantMetadata::kEmptyMetadata.size(), int1_value_str.data(),
           int1_value_str.size());
    const Slice slice(variant_string.data(), variant_string.size());
    variant_column->append_strings(&slice, 1);

    ASSERT_EQ(1, variant_column->size());
    auto expected = VariantValue::create(slice);
    ASSERT_TRUE(expected.ok());
    const VariantValue* actual = variant_column->get_object(0);
    ASSERT_EQ(expected->serialize_size(), actual->serialize_size());
    ASSERT_EQ(expected->get_metadata(), actual->get_metadata());
    ASSERT_EQ(expected->get_value(), actual->get_value());
    EXPECT_EQ(expected->to_string(), actual->to_string());
    EXPECT_EQ("1", actual->to_string());

    // Append bad data
    const Slice bad_slice("");
    const bool result = variant_column->append_strings(&bad_slice, 1);
    ASSERT_FALSE(result) << "Appending empty slice should fail";
}

} // namespace starrocks

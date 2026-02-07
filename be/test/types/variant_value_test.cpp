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

#include "types/variant_value.h"

#include <fs/fs.h>
#include <gtest/gtest.h>

#include <boost/uuid/uuid_io.hpp>

#include "base/time/timezone_utils.h"
#include "cctz/time_zone.h"
#include "util/variant.h"

namespace starrocks {

static inline uint8_t primitive_header(VariantType primitive) {
    return (static_cast<uint8_t>(primitive) << 2);
}

class VariantRowValueTest : public testing::Test {
public:
    VariantRowValueTest() = default;
    ~VariantRowValueTest() override = default;

protected:
    void SetUp() override {
        test_exec_dir = "./be/test/formats/parquet/test_data/variant";

        _primitive_metadata_file_names = {
                "primitive_null.metadata",      "primitive_boolean_true.metadata", "primitive_boolean_false.metadata",
                "primitive_date.metadata",      "primitive_decimal4.metadata",     "primitive_decimal8.metadata",
                "primitive_decimal16.metadata", "primitive_float.metadata",        "primitive_double.metadata",
                "primitive_int8.metadata",      "primitive_int16.metadata",        "primitive_int32.metadata",
                "primitive_int64.metadata",     "primitive_binary.metadata",       "primitive_string.metadata",
                "array_primitive.metadata",
        };

        _boolean_file_names = {
                {"primitive_boolean_true.metadata", "primitive_boolean_true.value"},
                {"primitive_boolean_false.metadata", "primitive_boolean_false.value"},
        };
    }

    std::string read_file_content(const std::string& file_path) {
        FileSystem* fs = FileSystem::Default();
        auto random_access_file = *fs->new_random_access_file(file_path);
        return *random_access_file->read_all();
    }

    std::pair<std::string, std::string> load_variant_data(const std::string& metadata_file,
                                                          const std::string& value_file) {
        std::string metadata_content = read_file_content(test_exec_dir + "/" + metadata_file);
        std::string value_content = read_file_content(test_exec_dir + "/" + value_file);

        return {std::move(metadata_content), std::move(value_content)};
    }

protected:
    std::string test_exec_dir;
    std::vector<std::string> _primitive_metadata_file_names;
    std::vector<std::pair<std::string, std::string>> _boolean_file_names;
};

TEST_F(VariantRowValueTest, NullToJson) {
    uint8_t null_chars[] = {static_cast<uint8_t>(VariantType::NULL_TYPE) << 2};
    std::string_view null_value(reinterpret_cast<const char*>(null_chars), 1);
    VariantRowValue v(VariantMetadata::kEmptyMetadata, null_value);
    auto json = v.to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ("null", *json);
}

TEST_F(VariantRowValueTest, BooleanToJson) {
    auto [t_fst, t_snd] = _boolean_file_names[0];
    auto [t_metadata, t_value] = load_variant_data(t_fst, t_snd);
    VariantRowValue variant_true{std::string_view(t_metadata), std::string_view(t_value)};
    auto json_true = variant_true.to_json();
    ASSERT_TRUE(json_true.ok());
    EXPECT_EQ("true", *json_true);

    auto [f_fst, f_snd] = _boolean_file_names[1];
    auto [f_metadata, f_value] = load_variant_data(f_fst, f_snd);
    VariantRowValue variant_false{std::string_view(f_metadata), std::string_view(f_value)};
    auto json_false = variant_false.to_json();
    ASSERT_TRUE(json_false.ok());
    EXPECT_EQ("false", *json_false);
}

TEST_F(VariantRowValueTest, IntegerToJson) {
    // Test int8
    {
        auto [int8_metadata, int8_value] = load_variant_data("primitive_int8.metadata", "primitive_int8.value");
        VariantRowValue variant{std::string_view(int8_metadata), std::string_view(int8_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("42", *json);
    }

    // Test int16
    {
        auto [int16_metadata, int16_value] = load_variant_data("primitive_int16.metadata", "primitive_int16.value");
        VariantRowValue variant{std::string_view(int16_metadata), std::string_view(int16_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("1234", *json);
    }

    // Test int32
    {
        auto [int32_metadata, int32_value] = load_variant_data("primitive_int32.metadata", "primitive_int32.value");
        VariantRowValue variant{std::string_view(int32_metadata), std::string_view(int32_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("123456", *json);
    }

    // Test int64
    {
        auto [int64_metadata, int64_value] = load_variant_data("primitive_int64.metadata", "primitive_int64.value");
        VariantRowValue variant{std::string_view(int64_metadata), std::string_view(int64_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("1234567890123456789", *json);
    }
}

TEST_F(VariantRowValueTest, FloatToJson) {
    // Test float
    {
        auto [float_metadata, float_value] = load_variant_data("primitive_float.metadata", "primitive_float.value");
        VariantRowValue variant{std::string_view(float_metadata), std::string_view(float_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("1.23456794e+09", *json);
    }

    // Test double
    {
        auto [double_metadata, double_value] = load_variant_data("primitive_double.metadata", "primitive_double.value");
        VariantRowValue variant{std::string_view(double_metadata), std::string_view(double_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("1234567890.1234", *json);
    }
}

TEST_F(VariantRowValueTest, StringToJson) {
    // Test long string
    {
        auto [string_metadata, string_value] = load_variant_data("primitive_string.metadata", "primitive_string.value");
        VariantRowValue variant{std::string_view(string_metadata), std::string_view(string_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ(
                "\"This string is longer than 64 bytes and therefore does not fit in a short_string and it also "
                "includes "
                "several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!\"",
                *json);
    }

    // Test short string
    {
        auto [short_string_metadata, short_string_value] =
                load_variant_data("short_string.metadata", "short_string.value");
        VariantRowValue variant{std::string_view(short_string_metadata), std::string_view(short_string_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("\"Less than 64 bytes (❤️ with utf8)\"", *json);
    }
}

TEST_F(VariantRowValueTest, BinaryToJson) {
    auto [binary_metadata, binary_value] = load_variant_data("primitive_binary.metadata", "primitive_binary.value");
    VariantRowValue variant{std::string_view(binary_metadata), std::string_view(binary_value)};
    auto json = variant.to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ("\"AxM33q2+78r+\"", *json);
}

TEST_F(VariantRowValueTest, DecimalToJson) {
    // Test decimal4
    {
        auto [decimal4_metadata, decimal4_value] =
                load_variant_data("primitive_decimal4.metadata", "primitive_decimal4.value");
        VariantRowValue variant{std::string_view(decimal4_metadata), std::string_view(decimal4_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("12.34", *json);
    }

    // Test decimal8
    {
        auto [decimal8_metadata, decimal8_value] =
                load_variant_data("primitive_decimal8.metadata", "primitive_decimal8.value");
        VariantRowValue variant{std::string_view(decimal8_metadata), std::string_view(decimal8_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("12345678.9", *json);
    }

    // Test decimal16
    {
        auto [decimal16_metadata, decimal16_value] =
                load_variant_data("primitive_decimal16.metadata", "primitive_decimal16.value");
        VariantRowValue variant{std::string_view(decimal16_metadata), std::string_view(decimal16_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("12345678912345678.9", *json);
    }
}

TEST_F(VariantRowValueTest, UUIDToJson) {
    std::string_view empty_metadata = VariantMetadata::kEmptyMetadata;
    const uint8_t uuid_chars[] = {primitive_header(VariantType::UUID),
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
    VariantRowValue variant{empty_metadata, uuid_string};
    auto json = variant.to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ("\"f24f9b64-81fa-49d1-b74e-8c09a6e31c56\"", *json);
}

TEST_F(VariantRowValueTest, TimestampToJson) {
    // Test timestamp with timezone
    {
        auto [ts_metadata, ts_value] = load_variant_data("primitive_timestamp.metadata", "primitive_timestamp.value");
        VariantRowValue variant{std::string_view(ts_metadata), std::string_view(ts_value)};

        // Set timezone to -04:00
        cctz::time_zone tz;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("-04:00", tz));

        auto json = variant.to_json(tz);
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("\"2025-04-16 12:34:56.78-04:00\"", *json);
    }

    // Test timestamp without timezone
    {
        auto [ts_ntz_metadata, ts_ntz_value] =
                load_variant_data("primitive_timestampntz.metadata", "primitive_timestampntz.value");
        VariantRowValue variant{std::string_view(ts_ntz_metadata), std::string_view(ts_ntz_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("\"2025-04-16 12:34:56.780000\"", *json);
    }
}

TEST_F(VariantRowValueTest, DateToJson) {
    auto [date_metadata, date_value] = load_variant_data("primitive_date.metadata", "primitive_date.value");
    VariantRowValue variant{std::string_view(date_metadata), std::string_view(date_value)};
    auto json = variant.to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ("\"2025-04-16\"", *json);
}

TEST_F(VariantRowValueTest, ObjectToJson) {
    // Test simple object
    {
        auto [object_metadata, object_value] = load_variant_data("object_primitive.metadata", "object_primitive.value");
        VariantRowValue variant{std::string_view(object_metadata), std::string_view(object_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());

        // Should be a valid JSON object
        EXPECT_TRUE(json->front() == '{');
        EXPECT_TRUE(json->back() == '}');
        EXPECT_EQ(
                "{\"boolean_false_field\":false,\"boolean_true_field\":true,\"double_field\":1.23456789,\"int_field\":"
                "1,\"null_field\":null,\"string_field\":\"Apache "
                "Parquet\",\"timestamp_field\":\"2025-04-16T12:34:56.78\"}",
                *json);
    }

    // Test empty object
    {
        auto [object_empty_metadata, object_empty_value] =
                load_variant_data("object_empty.metadata", "object_empty.value");
        VariantRowValue variant{std::string_view(object_empty_metadata), std::string_view(object_empty_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("{}", *json);
    }

    // Test nested object
    {
        auto [object_nested_metadata, object_nested_value] =
                load_variant_data("object_nested.metadata", "object_nested.value");
        VariantRowValue variant{std::string_view(object_nested_metadata), std::string_view(object_nested_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());

        // Should be a valid JSON object
        EXPECT_TRUE(json->front() == '{');
        EXPECT_TRUE(json->back() == '}');
        EXPECT_EQ(
                "{\"id\":1,\"observation\":{\"location\":\"In the "
                "Volcano\",\"time\":\"12:34:56\",\"value\":{\"humidity\":456,\"temperature\":123}},\"species\":{"
                "\"name\":\"lava monster\",\"population\":6789}}",
                *json);
    }
}

TEST_F(VariantRowValueTest, ArrayToJson) {
    // Test primitive array
    {
        auto [array_metadata, array_value] = load_variant_data("array_primitive.metadata", "array_primitive.value");
        VariantRowValue variant{std::string_view(array_metadata), std::string_view(array_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());

        // Should be a valid JSON array
        EXPECT_TRUE(json->front() == '[');
        EXPECT_TRUE(json->back() == ']');
        EXPECT_EQ("[2,1,5,9]", *json);
    }

    // Test empty array
    {
        auto [array_empty_metadata, array_empty_value] = load_variant_data("array_empty.metadata", "array_empty.value");
        VariantRowValue variant{std::string_view(array_empty_metadata), std::string_view(array_empty_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());
        EXPECT_EQ("[]", *json);
    }

    // Test nested array
    {
        auto [array_nested_metadata, array_nested_value] =
                load_variant_data("array_nested.metadata", "array_nested.value");
        VariantRowValue variant{std::string_view(array_nested_metadata), std::string_view(array_nested_value)};
        auto json = variant.to_json();
        ASSERT_TRUE(json.ok());

        // Should be a valid JSON array
        EXPECT_TRUE(json->front() == '[');
        EXPECT_TRUE(json->back() == ']');
        EXPECT_EQ(
                "[{\"id\":1,\"thing\":{\"names\":[\"Contrarian\",\"Spider\"]}},null,{\"id\":2,\"names\":[\"Apple\","
                "\"Ray\",null],\"type\":\"if\"}]",
                *json);
    }
}

TEST_F(VariantRowValueTest, InvalidVariant) {
    // Test invalid variant with empty metadata
    {
        Slice empty_slice(""); // Empty slice
        auto empty_variant = VariantRowValue::create(empty_slice);
        ASSERT_FALSE(empty_variant.ok());
        EXPECT_EQ("Invalid argument: Invalid variant slice: too small to contain size header",
                  empty_variant.status().to_string());
    }
    // Test invalid variant with unsupported version
    {
        // Create proper format: 4-byte size header + variant data
        constexpr char v2_metadata_char[] = {0x2, 0x0, 0x0}; // version 2 metadata
        constexpr uint32_t data_size = sizeof(v2_metadata_char);

        // Construct proper slice with size header
        std::string variant_data;
        variant_data.resize(sizeof(uint32_t) + data_size);

        // Write size header (little endian)
        std::memcpy(variant_data.data(), &data_size, sizeof(uint32_t));
        // Write variant data
        std::memcpy(variant_data.data() + sizeof(uint32_t), v2_metadata_char, data_size);

        Slice variant_slice(variant_data);
        auto unsupported_variant = VariantRowValue::create(variant_slice);
        ASSERT_FALSE(unsupported_variant.ok());
        EXPECT_EQ("Not supported: Unsupported variant version: 2", unsupported_variant.status().to_string(false));
    }
    // Test exceed maximum variant size
    {
        // Create size header that indicates data larger than max size
        constexpr uint32_t oversized_data_size = VariantRowValue::kMaxVariantSize + 1;
        std::string variant_data;
        variant_data.resize(sizeof(uint32_t));

        // Write oversized size in header
        std::memcpy(variant_data.data(), &oversized_data_size, sizeof(uint32_t));

        Slice variant_slice(variant_data);
        auto large_variant = VariantRowValue::create(variant_slice);
        ASSERT_FALSE(large_variant.ok());
        EXPECT_EQ("Invalid argument: Variant size exceeds maximum limit: " +
                          std::to_string(VariantRowValue::kMaxVariantSize + 1) + " > " +
                          std::to_string(VariantRowValue::kMaxVariantSize),
                  large_variant.status().to_string());
    }

    // Test variant with size header but insufficient actual data
    {
        constexpr uint32_t claimed_size = 100;    // Claim 100 bytes
        constexpr uint32_t actual_data_size = 10; // But only provide 10 bytes

        std::string variant_data;
        variant_data.resize(sizeof(uint32_t) + actual_data_size);

        // Write size header claiming more data than available
        std::memcpy(variant_data.data(), &claimed_size, sizeof(uint32_t));
        // Fill with some dummy data
        std::memset(variant_data.data() + sizeof(uint32_t), 0x42, actual_data_size);

        Slice variant_slice(variant_data);
        auto insufficient_variant = VariantRowValue::create(variant_slice);
        ASSERT_FALSE(insufficient_variant.ok());
        EXPECT_EQ("Invalid argument: Invalid variant size: 100 exceeds available data: 10",
                  insufficient_variant.status().to_string());
    }
}

// Tests for move/copy semantics and the new VariantRowValue architecture
TEST_F(VariantRowValueTest, CopyConstructor) {
    // Create a variant with some data
    auto [metadata, value] = load_variant_data("primitive_int32.metadata", "primitive_int32.value");
    VariantRowValue original{std::string_view(metadata), std::string_view(value)};

    // Copy construct
    VariantRowValue copy(original);

    // Both should produce the same JSON
    auto original_json = original.to_json();
    auto copy_json = copy.to_json();
    ASSERT_TRUE(original_json.ok());
    ASSERT_TRUE(copy_json.ok());
    EXPECT_EQ(*original_json, *copy_json);
    EXPECT_EQ("123456", *copy_json);

    // Verify that they are independent (different underlying storage)
    EXPECT_NE(&original.get_metadata().raw()[0], &copy.get_metadata().raw()[0]);
}

TEST_F(VariantRowValueTest, MoveConstructor) {
    // Create a variant with some data
    auto [metadata, value] = load_variant_data("primitive_string.metadata", "primitive_string.value");
    VariantRowValue original{std::string_view(metadata), std::string_view(value)};

    auto expected_json = original.to_json();
    ASSERT_TRUE(expected_json.ok());

    // Move construct
    VariantRowValue moved(std::move(original));

    // Moved-to object should have the data
    auto moved_json = moved.to_json();
    ASSERT_TRUE(moved_json.ok());
    EXPECT_EQ(*expected_json, *moved_json);

    // Moved-from object should be in a valid empty state
    auto original_json = original.to_json();
    ASSERT_TRUE(original_json.ok());
    EXPECT_EQ("null", *original_json); // Should be empty variant (null)
}

TEST_F(VariantRowValueTest, CopyAssignment) {
    // Create source variant
    auto [metadata1, value1] = load_variant_data("primitive_int64.metadata", "primitive_int64.value");
    VariantRowValue source{std::string_view(metadata1), std::string_view(value1)};

    // Create destination variant with different data
    auto [metadata2, value2] = load_variant_data("primitive_int32.metadata", "primitive_int32.value");
    VariantRowValue dest{std::string_view(metadata2), std::string_view(value2)};

    // Verify dest has different data initially
    auto dest_json_before = dest.to_json();
    ASSERT_TRUE(dest_json_before.ok());
    EXPECT_EQ("123456", *dest_json_before);

    // Copy assign
    dest = source;

    // Both should now have the same data
    auto source_json = source.to_json();
    auto dest_json = dest.to_json();
    ASSERT_TRUE(source_json.ok());
    ASSERT_TRUE(dest_json.ok());
    EXPECT_EQ(*source_json, *dest_json);
    EXPECT_EQ("1234567890123456789", *dest_json);

    // Verify they are independent
    EXPECT_NE(&source.get_metadata().raw()[0], &dest.get_metadata().raw()[0]);
}

TEST_F(VariantRowValueTest, MoveAssignment) {
    // Create source variant
    auto [metadata1, value1] = load_variant_data("object_primitive.metadata", "object_primitive.value");
    VariantRowValue source{std::string_view(metadata1), std::string_view(value1)};

    auto expected_json = source.to_json();
    ASSERT_TRUE(expected_json.ok());

    // Create destination variant with different data
    auto [metadata2, value2] = load_variant_data("primitive_int32.metadata", "primitive_int32.value");
    VariantRowValue dest{std::string_view(metadata2), std::string_view(value2)};

    // Move assign
    dest = std::move(source);

    // Destination should have the moved data
    auto dest_json = dest.to_json();
    ASSERT_TRUE(dest_json.ok());
    EXPECT_EQ(*expected_json, *dest_json);

    // Source should be in a valid empty state
    auto source_json = source.to_json();
    ASSERT_TRUE(source_json.ok());
    EXPECT_EQ("null", *source_json);
}

TEST_F(VariantRowValueTest, SelfAssignment) {
    // Create a variant
    auto [metadata, value] = load_variant_data("primitive_int32.metadata", "primitive_int32.value");
    VariantRowValue variant{std::string_view(metadata), std::string_view(value)};

    // Self-assign (copy)
    variant = variant;

    // Should still work correctly
    auto json = variant.to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ("123456", *json);
}

TEST_F(VariantRowValueTest, ChainedMoves) {
    // Create original variant
    auto [metadata, value] = load_variant_data("array_primitive.metadata", "array_primitive.value");
    VariantRowValue v1{std::string_view(metadata), std::string_view(value)};

    auto expected_json = v1.to_json();
    ASSERT_TRUE(expected_json.ok());

    // Chain of moves: v1 -> v2 -> v3
    VariantRowValue v2(std::move(v1));
    VariantRowValue v3(std::move(v2));

    // v3 should have the data
    auto v3_json = v3.to_json();
    ASSERT_TRUE(v3_json.ok());
    EXPECT_EQ(*expected_json, *v3_json);

    // v1 and v2 should be in valid empty state
    auto v1_json = v1.to_json();
    auto v2_json = v2.to_json();
    ASSERT_TRUE(v1_json.ok());
    ASSERT_TRUE(v2_json.ok());
    EXPECT_EQ("null", *v1_json);
    EXPECT_EQ("null", *v2_json);
}

TEST_F(VariantRowValueTest, EmptyVariantHandling) {
    // Test default constructor creates valid empty variant
    VariantRowValue empty_variant;

    auto json = empty_variant.to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ("null", *json);

    // Test that empty variant can be copied
    VariantRowValue copy(empty_variant);
    auto copy_json = copy.to_json();
    ASSERT_TRUE(copy_json.ok());
    EXPECT_EQ("null", *copy_json);

    // Test that empty variant can be moved
    VariantRowValue moved(std::move(copy));
    auto moved_json = moved.to_json();
    ASSERT_TRUE(moved_json.ok());
    EXPECT_EQ("null", *moved_json);
}

TEST_F(VariantRowValueTest, ComplexObjectCopyMove) {
    // Test copy/move with complex nested structures
    auto [metadata, value] = load_variant_data("object_nested.metadata", "object_nested.value");
    VariantRowValue original{std::string_view(metadata), std::string_view(value)};

    auto expected_json = original.to_json();
    ASSERT_TRUE(expected_json.ok());

    // Copy
    VariantRowValue copy = original;
    auto copy_json = copy.to_json();
    ASSERT_TRUE(copy_json.ok());
    EXPECT_EQ(*expected_json, *copy_json);

    // Move from copy
    VariantRowValue moved = std::move(copy);
    auto moved_json = moved.to_json();
    ASSERT_TRUE(moved_json.ok());
    EXPECT_EQ(*expected_json, *moved_json);

    // Verify the metadata and value are correctly bound
    const VariantMetadata& meta = moved.get_metadata();
    const VariantValue& var = moved.get_value();
    EXPECT_EQ(VariantType::OBJECT, var.type());
}

TEST_F(VariantRowValueTest, MetadataValueRebinding) {
    // This test verifies that _rebind_views() correctly updates the views
    // after copy/move operations
    auto [metadata, value] = load_variant_data("primitive_int32.metadata", "primitive_int32.value");
    VariantRowValue v1{std::string_view(metadata), std::string_view(value)};

    // Get pointers to the original storage
    const char* original_meta_data = v1.get_metadata().raw().data();
    const char* original_value_data = v1.get_value().raw().data();

    // Copy construct - this should create new storage and rebind views
    VariantRowValue v2(v1);

    // The views should point to different storage
    const char* copy_meta_data = v2.get_metadata().raw().data();
    const char* copy_value_data = v2.get_value().raw().data();

    EXPECT_NE(original_meta_data, copy_meta_data);
    EXPECT_NE(original_value_data, copy_value_data);

    // But the data should be the same
    EXPECT_EQ(v1.get_metadata().raw(), v2.get_metadata().raw());
    EXPECT_EQ(v1.get_value().raw(), v2.get_value().raw());
}

} // namespace starrocks

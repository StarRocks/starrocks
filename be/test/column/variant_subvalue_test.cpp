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

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "column/variant_encoder.h"
#include "types/variant_value.h"

namespace starrocks {
namespace {

std::string make_primitive_value(VariantType type, std::string_view payload = {}) {
    std::string value;
    value.push_back(static_cast<char>(static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift));
    value.append(payload);
    return value;
}

} // namespace

TEST(VariantSubvalueTest, ObjectSubvaluesHaveExactEncodedSize) {
    auto root = VariantEncoder::encode_json_text_to_variant(R"({"a":0,"z":0})");
    ASSERT_TRUE(root.ok());
    std::vector<std::string> values;
    for (const auto& [type, payload_size] :
         std::vector<std::pair<VariantType, size_t>>{{VariantType::NULL_TYPE, 0},
                                                     {VariantType::BOOLEAN_TRUE, 0},
                                                     {VariantType::BOOLEAN_FALSE, 0},
                                                     {VariantType::INT8, 1},
                                                     {VariantType::INT16, 2},
                                                     {VariantType::INT32, 4},
                                                     {VariantType::INT64, 8},
                                                     {VariantType::FLOAT, 4},
                                                     {VariantType::DOUBLE, 8},
                                                     {VariantType::DECIMAL4, 5},
                                                     {VariantType::DECIMAL8, 9},
                                                     {VariantType::DECIMAL16, 17},
                                                     {VariantType::DATE, 4},
                                                     {VariantType::TIMESTAMP_TZ, 8},
                                                     {VariantType::TIMESTAMP_NTZ, 8},
                                                     {VariantType::TIME_NTZ, 8},
                                                     {VariantType::TIMESTAMP_TZ_NANOS, 8},
                                                     {VariantType::TIMESTAMP_NTZ_NANOS, 8},
                                                     {VariantType::UUID, 16}}) {
        values.emplace_back(make_primitive_value(type, std::string(payload_size, '\0')));
    }
    for (const auto& json : std::vector<std::string>{R"("")", R"("abc")", "\"" + std::string(200, 'x') + "\"", "[]",
                                                     "{}", "[1,[2,3]]", R"({"a":[1,2]})"}) {
        auto encoded = VariantEncoder::encode_json_text_to_variant(json);
        ASSERT_TRUE(encoded.ok());
        values.emplace_back(encoded->get_value().raw());
    }
    std::string binary_payload;
    VariantEncoder::append_uint_le(&binary_payload, 3, 4);
    binary_payload.append("abc");
    values.emplace_back(make_primitive_value(VariantType::BINARY, binary_payload));

    const std::string sibling = make_primitive_value(VariantType::INT64, std::string(8, '\0'));
    for (const auto& value : values) {
        std::string object;
        VariantEncoder::append_object_container(
                &object, {0, 1},
                {static_cast<uint32_t>(value.size()), static_cast<uint32_t>(value.size() + sibling.size())},
                value + sibling);
        auto field = VariantValue(object).get_object_by_key(root->get_metadata(), "a");
        ASSERT_TRUE(field.ok()) << field.status();
        EXPECT_EQ(value, field->raw());
        auto owned = VariantRowValue::from_variant(root->get_metadata(), field.value());
        EXPECT_EQ(value, owned.get_value().raw());
    }
}

TEST(VariantSubvalueTest, ObjectSubvaluesSupportNonMonotonicOffsets) {
    auto root = VariantEncoder::encode_json_text_to_variant(R"({"a":1,"b":2,"c":3})");
    ASSERT_TRUE(root.ok());
    // IDs are in key order a,b,c, but physical values are c,a,b.
    const std::string object = {0x02, 3, 0, 1, 2, 2, 4, 0, 6, 0x0c, 3, 0x0c, 1, 0x0c, 2};
    for (const auto& [key, expected] : std::vector<std::pair<std::string, int8_t>>{{"a", 1}, {"b", 2}, {"c", 3}}) {
        auto field = VariantValue(object).get_object_by_key(root->get_metadata(), key);
        ASSERT_TRUE(field.ok()) << field.status();
        EXPECT_EQ(2, field->raw().size());
        auto number = field->get_int8();
        ASSERT_TRUE(number.ok());
        EXPECT_EQ(expected, number.value());
    }
    EXPECT_TRUE(VariantValue(object).get_object_by_key(root->get_metadata(), "absent")->is_null());
}

TEST(VariantSubvalueTest, ArraySubvaluesHaveExactRangesForAllOffsetWidths) {
    const VariantMetadata metadata;
    for (uint8_t width = 1; width <= 4; ++width) {
        std::string array(1, static_cast<char>(((width - 1) << 2) | 3));
        array.push_back(2);
        for (uint32_t offset : {0, 2, 4}) {
            VariantEncoder::append_uint_le(&array, offset, width);
        }
        array.append("\x0c\x01\x0c\x02", 4);
        // The incoming view may itself still include bytes after this array.
        array.append(16, 'x');
        for (uint32_t i = 0; i < 2; ++i) {
            auto element = VariantValue(array).get_element_at_index(metadata, i);
            ASSERT_TRUE(element.ok());
            EXPECT_EQ(2, element->raw().size());
            EXPECT_EQ(i + 1, element->get_int8().value());
        }
        EXPECT_TRUE(VariantValue(array).get_element_at_index(metadata, 2)->is_null());
    }
}

TEST(VariantSubvalueTest, ArraySubvalueBytesGrowLinearly) {
    const std::string element = make_primitive_value(VariantType::INT8, std::string(1, 7));
    for (uint32_t count : {1, 16, 256, 4096}) {
        std::string payload;
        std::vector<uint32_t> offsets;
        for (uint32_t i = 0; i < count; ++i) {
            payload.append(element);
            offsets.emplace_back(payload.size());
        }
        std::string array;
        VariantEncoder::append_array_container(&array, offsets, payload);
        size_t copied_bytes = 0;
        for (uint32_t i = 0; i < count; ++i) {
            auto field = VariantValue(array).get_element_at_index(VariantMetadata(), i);
            ASSERT_TRUE(field.ok());
            EXPECT_EQ(element, field->raw());
            copied_bytes += field->raw().size();
        }
        EXPECT_EQ(payload.size(), copied_bytes);
    }
}

TEST(VariantSubvalueTest, SubvalueSlicesRejectInvalidBounds) {
    const VariantMetadata metadata;
    for (const auto& array : {std::string("\x03\x02\x00\x03\x02\x0c\x01", 7),
                              std::string("\x03\x01\x00\xff\x0c\x01", 6), std::string("\x03\x01\x00\x00", 4)}) {
        EXPECT_FALSE(VariantValue(array).get_element_at_index(metadata, 0).ok());
    }
    const std::string overflowing_array("\x13\xff\xff\xff\xff", 5);
    EXPECT_FALSE(VariantValue(overflowing_array).get_array_info().ok());

    auto root = VariantEncoder::encode_json_text_to_variant(R"({"a":0})");
    ASSERT_TRUE(root.ok());
    for (const auto& value : {make_primitive_value(VariantType::INT64), std::string("\x0d"), std::string("\xfc"),
                              std::string("\x03\x00\xff", 3)}) {
        std::string object;
        VariantEncoder::append_object_container(&object, {0}, {static_cast<uint32_t>(value.size())}, value);
        EXPECT_FALSE(VariantValue(object).get_object_by_key(root->get_metadata(), "a").ok());
    }
}

} // namespace starrocks

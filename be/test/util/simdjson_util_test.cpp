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

#include "util/simdjson_util.h"

#include <gtest/gtest.h>

namespace starrocks {

class SimdJsonUtilTest : public ::testing::Test {
public:
    void TearDown() override { buffer.clear(); }

    // Helper to parse JSON and get string value
    simdjson::ondemand::value get_value(const simdjson::padded_string& json) {
        doc = parser.iterate(json);
        return doc.find_field("value");
    }

protected:
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    faststring buffer;
};

TEST_F(SimdJsonUtilTest, HandlesUnescapedKey) {
    auto json = R"({"normal_key": 123})"_padded;
    auto doc = parser.iterate(json);

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), "normal_key");
        ASSERT_EQ(result.value(), field.unescaped_key().value());
        ASSERT_NE((void*)result.value().data(), (void*)buffer.data());
        ASSERT_EQ(buffer.size(), 0);
    }
}

TEST_F(SimdJsonUtilTest, HandlesEscapedKey) {
    auto json = R"({"escaped_\"key": 456})"_padded;
    auto doc = parser.iterate(json);

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), std::string_view("escaped_\"key"));
        ASSERT_EQ(result.value(), field.unescaped_key().value());
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        ASSERT_GT(buffer.size(), 0);
    }
}

TEST_F(SimdJsonUtilTest, HandlesMultipleEscapes) {
    auto json = R"({"complex\\\"key\/": true})"_padded;
    auto doc = parser.iterate(json);

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), std::string_view("complex\\\"key/"));
        ASSERT_EQ(result.value(), field.unescaped_key().value());
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        ASSERT_GT(buffer.size(), 12);
    }
}

TEST_F(SimdJsonUtilTest, FieldUnescapeKeyHandlesBufferReuse) {
    auto json = R"({"key1": 1, "key\"2": 2})"_padded;
    auto doc = parser.iterate(json);

    size_t first_buffer_size = 0;
    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), field.unescaped_key().value());
        if (result.value() == std::string_view("key1")) {
            ASSERT_EQ(buffer.size(), 0);
        } else {
            first_buffer_size = buffer.size();
            ASSERT_GT(first_buffer_size, 0);
        }
    }

    doc.rewind();
    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), field.unescaped_key().value());
        if (result.value() == std::string_view("key\"2")) {
            ASSERT_GE(buffer.capacity(), first_buffer_size);
        }
    }
}

TEST_F(SimdJsonUtilTest, HandlesInvalidField) {
    simdjson::simdjson_result<simdjson::ondemand::field> invalid_field;
    ASSERT_THROW(field_unescaped_key_safe(invalid_field, &buffer), simdjson::simdjson_error);
}

TEST_F(SimdJsonUtilTest, HandlesLargeKey) {
    const std::string big_key(1024, 'a');
    auto json = R"({")" + big_key + R"(": "value"})";
    auto padded_json = simdjson::padded_string(json);
    auto doc = parser.iterate(padded_json);

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), field.unescaped_key().value());
        ASSERT_NE((void*)result.value().data(), (void*)buffer.data());
        ASSERT_EQ(result.value(), big_key);
        ASSERT_EQ(buffer.size(), 0);
    }
}

TEST_F(SimdJsonUtilTest, HandlesBasicString) {
    auto json = R"({"value": "simple string"})"_padded;
    auto value = get_value(json);

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    ASSERT_EQ(result.value(), value.get_string().value());
    EXPECT_EQ(result.value(), std::string_view("simple string"));
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_EQ(buffer.size(), value.raw_json_token().size() + simdjson::SIMDJSON_PADDING);
}

TEST_F(SimdJsonUtilTest, HandlesEscapedCharacters) {
    auto json = R"({"value": "Escaped\nNewline\tTab\"Quote"})"_padded;
    auto value = get_value(json);

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    EXPECT_EQ(result.value(), std::string_view("Escaped\nNewline\tTab\"Quote"));
    ASSERT_EQ(result.value(), value.get_string().value());
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_GT(buffer.size(), value.raw_json_token().size());
}

TEST_F(SimdJsonUtilTest, HandlesEmptyString) {
    auto json = R"({"value": ""})"_padded;
    auto value = get_value(json);

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    ASSERT_EQ(result.value(), value.get_string().value());
    EXPECT_EQ(result.value(), std::string_view(""));
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_EQ(buffer.size(), simdjson::SIMDJSON_PADDING + 2); // including the two quotes
}

TEST_F(SimdJsonUtilTest, HandlesUnicodeEscape) {
    auto json = R"({"value": "Utf-8: \u6C49\u5B57"})"_padded;
    auto value = get_value(json);

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    ASSERT_EQ(result.value(), value.get_string().value());
    EXPECT_EQ(result.value(), std::string_view("Utf-8: 汉字"));
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_GT(buffer.size(), value.raw_json_token().size());
}

TEST_F(SimdJsonUtilTest, ValueGetStringHandlesBufferReuse) {
    uint8_t* buffer_pos = nullptr;
    // Firs call with larger string
    {
        auto json = R"({"value": "buffer reuse with escapes\n"})"_padded;
        auto value = get_value(json);
        auto result = value_get_string_safe(&value, &buffer);
        ASSERT_EQ(result.value(), value.get_string().value());
        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        buffer_pos = buffer.data();
    }
    // Second call with small string
    {
        auto json = R"({"value": "test"})"_padded;
        auto value = get_value(json);
        auto result = value_get_string_safe(&value, &buffer);
        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), value.get_string().value());
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        EXPECT_EQ(result.value(), std::string_view("test"));

        // buffer_pos pointed to the same location
        EXPECT_EQ(buffer_pos, buffer.data());
    }
}

TEST_F(SimdJsonUtilTest, HandlesInvalidStringType) {
#ifdef NDEBUG
    auto json = R"({"value": 123})"_padded;
    auto value = get_value(json);
    // Release build should throw
    EXPECT_THROW(value_get_string_safe(&value, &buffer), simdjson::simdjson_error);
#endif
}

TEST_F(SimdJsonUtilTest, HandlesMalformedEscapes) {
    auto json = R"({"value": "bad escape \xC3\x28"})"_padded;
    auto value = get_value(json);

    auto result = value_get_string_safe(&value, &buffer);

    // Should propagate parsing error
    EXPECT_EQ(result.error(), simdjson::STRING_ERROR);
}

TEST_F(SimdJsonUtilTest, HandlesLargeString) {
    const std::string big_str(4096, 'x');
    auto json = simdjson::padded_string(R"({"value": ")" + big_str + R"("})");
    auto value = get_value(json);

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    ASSERT_EQ(result.value(), value.get_string().value());
    EXPECT_EQ(result.value(), big_str);
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_EQ(buffer.size(), big_str.size() + 2 + simdjson::SIMDJSON_PADDING); // 2 for quotes
}

} // namespace starrocks

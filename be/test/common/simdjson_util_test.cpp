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

#ifndef SIMDJSON_DEVELOPMENT_CHECKS
#define SIMDJSON_DEVELOPMENT_CHECKS 1
#endif

#include "common/simdjson_util.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <limits>
#include <thread>

#include "base/utility/defer_op.h"

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

    simdjson::ondemand::parser ref_parser;
    auto ref_doc = ref_parser.iterate(json);
    auto ref_obj = ref_doc.get_object();
    auto ref_it = ref_obj.begin();

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), "normal_key");
        ASSERT_EQ(result.value(), (*ref_it).unescaped_key().value());
        ASSERT_NE((void*)result.value().data(), (void*)buffer.data());
        ASSERT_EQ(buffer.size(), 0);
        ++ref_it;
    }
}

TEST_F(SimdJsonUtilTest, HandlesEscapedKey) {
    auto json = R"({"escaped_\"key": 456})"_padded;
    auto doc = parser.iterate(json);

    simdjson::ondemand::parser ref_parser;
    auto ref_doc = ref_parser.iterate(json);
    auto ref_obj = ref_doc.get_object();
    auto ref_it = ref_obj.begin();

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), std::string_view("escaped_\"key"));
        ASSERT_EQ(result.value(), (*ref_it).unescaped_key().value());
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        ASSERT_GT(buffer.size(), 0);
        ++ref_it;
    }
}

TEST_F(SimdJsonUtilTest, HandlesMultipleEscapes) {
    auto json = R"({"complex\\\"key\/": true})"_padded;
    auto doc = parser.iterate(json);

    simdjson::ondemand::parser ref_parser;
    auto ref_doc = ref_parser.iterate(json);
    auto ref_obj = ref_doc.get_object();
    auto ref_it = ref_obj.begin();

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), std::string_view("complex\\\"key/"));
        ASSERT_EQ(result.value(), (*ref_it).unescaped_key().value());
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        ASSERT_GT(buffer.size(), 12);
        ++ref_it;
    }
}

TEST_F(SimdJsonUtilTest, FieldUnescapeKeyHandlesBufferReuse) {
    auto json = R"({"key1": 1, "key\"2": 2})"_padded;
    auto doc = parser.iterate(json);

    simdjson::ondemand::parser ref_parser;
    auto ref_doc = ref_parser.iterate(json);
    auto ref_obj = ref_doc.get_object();
    auto ref_it = ref_obj.begin();

    size_t first_buffer_size = 0;
    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), (*ref_it).unescaped_key().value());
        if (result.value() == std::string_view("key1")) {
            ASSERT_EQ(buffer.size(), 0);
        } else {
            first_buffer_size = buffer.size();
            ASSERT_GT(first_buffer_size, 0);
        }
        ++ref_it;
    }

    doc.rewind();
    ASSERT_EQ(simdjson::SUCCESS, ref_doc.rewind());
    auto ref_obj2 = ref_doc.get_object();
    auto ref_it2 = ref_obj2.begin();
    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), (*ref_it2).unescaped_key().value());
        if (result.value() == std::string_view("key\"2")) {
            ASSERT_GE(buffer.capacity(), first_buffer_size);
        }
        ++ref_it2;
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

    simdjson::ondemand::parser ref_parser;
    auto ref_doc = ref_parser.iterate(padded_json);
    auto ref_obj = ref_doc.get_object();
    auto ref_it = ref_obj.begin();

    for (auto field : doc.get_object()) {
        auto result = field_unescaped_key_safe(field, &buffer);

        ASSERT_EQ(simdjson::SUCCESS, result.error());
        ASSERT_EQ(result.value(), (*ref_it).unescaped_key().value());
        ASSERT_NE((void*)result.value().data(), (void*)buffer.data());
        ASSERT_EQ(result.value(), big_key);
        ASSERT_EQ(buffer.size(), 0);
        ++ref_it;
    }
}

TEST_F(SimdJsonUtilTest, HandlesBasicString) {
    auto json = R"({"value": "simple string"})"_padded;
    auto value = get_value(json);
    const size_t raw_token_size = value.raw_json_token().size();

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    doc.rewind();
    auto expected_val = doc.find_field("value");
    std::string_view expected_str = expected_val.get_string().value();
    ASSERT_EQ(result.value(), expected_str);
    EXPECT_EQ(result.value(), std::string_view("simple string"));
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_EQ(buffer.size(), raw_token_size + simdjson::SIMDJSON_PADDING);
}

TEST_F(SimdJsonUtilTest, HandlesEscapedCharacters) {
    auto json = R"({"value": "Escaped\nNewline\tTab\"Quote"})"_padded;
    auto value = get_value(json);
    const size_t raw_token_size = value.raw_json_token().size();

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    EXPECT_EQ(result.value(), std::string_view("Escaped\nNewline\tTab\"Quote"));
    doc.rewind();
    auto expected_val = doc.find_field("value");
    std::string_view expected_str = expected_val.get_string().value();
    ASSERT_EQ(result.value(), expected_str);
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_GT(buffer.size(), raw_token_size);
}

TEST_F(SimdJsonUtilTest, HandlesEmptyString) {
    auto json = R"({"value": ""})"_padded;
    auto value = get_value(json);
    const size_t raw_token_size = value.raw_json_token().size();

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    doc.rewind();
    auto expected_val = doc.find_field("value");
    std::string_view expected_str = expected_val.get_string().value();
    ASSERT_EQ(result.value(), expected_str);
    EXPECT_EQ(result.value(), std::string_view(""));
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_EQ(raw_token_size, 2);
    EXPECT_EQ(buffer.size(), simdjson::SIMDJSON_PADDING + 2); // including the two quotes
}

TEST_F(SimdJsonUtilTest, HandlesUnicodeEscape) {
    auto json = R"({"value": "Utf-8: \u6C49\u5B57"})"_padded;
    auto value = get_value(json);
    const size_t raw_token_size = value.raw_json_token().size();

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    doc.rewind();
    auto expected_val = doc.find_field("value");
    std::string_view expected_str = expected_val.get_string().value();
    ASSERT_EQ(result.value(), expected_str);
    EXPECT_EQ(result.value(), std::string_view("Utf-8: 汉字"));
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_GT(buffer.size(), raw_token_size);
}

TEST_F(SimdJsonUtilTest, ValueGetStringHandlesBufferReuse) {
    uint8_t* buffer_pos = nullptr;
    // Firs call with larger string
    {
        auto json = R"({"value": "buffer reuse with escapes\n"})"_padded;
        auto value = get_value(json);
        const size_t raw_token_size = value.raw_json_token().size();
        auto result = value_get_string_safe(&value, &buffer);
        ASSERT_EQ(simdjson::SUCCESS, result.error());
        doc.rewind();
        auto expected_val = doc.find_field("value");
        std::string_view expected_str = expected_val.get_string().value();
        ASSERT_EQ(result.value(), expected_str);
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        EXPECT_GT(buffer.size(), raw_token_size);
        buffer_pos = buffer.data();
    }
    // Second call with small string
    {
        auto json = R"({"value": "test"})"_padded;
        auto value = get_value(json);
        const size_t raw_token_size = value.raw_json_token().size();
        auto result = value_get_string_safe(&value, &buffer);
        ASSERT_EQ(simdjson::SUCCESS, result.error());
        doc.rewind();
        auto expected_val = doc.find_field("value");
        std::string_view expected_str = expected_val.get_string().value();
        ASSERT_EQ(result.value(), expected_str);
        ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
        EXPECT_EQ(result.value(), std::string_view("test"));
        EXPECT_EQ(buffer.size(), raw_token_size + simdjson::SIMDJSON_PADDING);

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
    const size_t raw_token_size = value.raw_json_token().size();

    auto result = value_get_string_safe(&value, &buffer);

    ASSERT_EQ(simdjson::SUCCESS, result.error());
    doc.rewind();
    auto expected_val = doc.find_field("value");
    std::string_view expected_str = expected_val.get_string().value();
    ASSERT_EQ(result.value(), expected_str);
    EXPECT_EQ(result.value(), big_str);
    ASSERT_EQ((void*)result.value().data(), (void*)buffer.data());
    EXPECT_EQ(buffer.size(), raw_token_size + simdjson::SIMDJSON_PADDING);
    EXPECT_EQ(buffer.size(), big_str.size() + 2 + simdjson::SIMDJSON_PADDING); // 2 for quotes
}

TEST_F(SimdJsonUtilTest, ReportsVersion511AndActiveImplementation) {
    EXPECT_STREQ(SIMDJSON_VERSION, "5.1.1");
    const simdjson::implementation* active = simdjson::get_active_implementation();
    ASSERT_NE(active, nullptr);
    const char* forced_impl = std::getenv("SIMDJSON_FORCE_IMPLEMENTATION");
    if (forced_impl != nullptr && forced_impl[0] != '\0') {
        EXPECT_EQ(active->name(), std::string_view(forced_impl));
        return;
    }
#if defined(__x86_64__) || defined(_M_X64)
    const simdjson::implementation* icelake = simdjson::get_available_implementations()["icelake"];
    ASSERT_NE(icelake, nullptr);
    if (icelake->supported_by_runtime_system()) {
        EXPECT_EQ(active->name(), "icelake");
    }
#endif
}

TEST_F(SimdJsonUtilTest, CrossImplementationEquivalence) {
    const simdjson::implementation* original_impl = simdjson::get_active_implementation();
    DeferOp restore_impl([original_impl]() { simdjson::get_active_implementation() = original_impl; });
    auto json =
            R"({"key_\u6C49\u5B57": "\u6C49\u5B57\u4E2D\u6587\uD83D\uDE00_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", "u64_max": 18446744073709551615, "big": 99999999999999999999, "i64_min": -9223372036854775808})"_padded;
    auto bad_surrogate_json = R"({"value": "\uD83D_tail"})"_padded;
    bool tested_fallback = false;
    for (const simdjson::implementation* impl : simdjson::get_available_implementations()) {
        if (impl == nullptr || !impl->supported_by_runtime_system()) {
            continue;
        }
        if (impl->name() == "fallback") {
            tested_fallback = true;
        }
        std::thread t([&]() {
            simdjson::get_active_implementation() = impl;
            faststring local_buffer;
            simdjson::ondemand::parser local_parser;
            auto doc = local_parser.iterate(json);
            simdjson::ondemand::object obj = doc.get_object();
            auto it = obj.begin();
            auto first_field = *it;
            auto key_res = field_unescaped_key_safe(first_field, &local_buffer);
            ASSERT_EQ(simdjson::SUCCESS, key_res.error()) << "impl=" << impl->name();
            EXPECT_EQ(key_res.value(), "key_汉字") << "impl=" << impl->name();
            simdjson::ondemand::value str_val = first_field.value();
            auto val_res = value_get_string_safe(&str_val, &local_buffer);
            ASSERT_EQ(simdjson::SUCCESS, val_res.error()) << "impl=" << impl->name();
            EXPECT_EQ(val_res.value(),
                      "汉字中文\xF0\x9F\x98\x80_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
                    << "impl=" << impl->name();
            EXPECT_EQ(uint64_t(doc["u64_max"].get_uint64()), 18446744073709551615ULL) << "impl=" << impl->name();
            EXPECT_EQ(doc["big"].get_number_type().value(), simdjson::ondemand::number_type::big_integer)
                    << "impl=" << impl->name();
            EXPECT_EQ(int64_t(doc["i64_min"].get_int64()), std::numeric_limits<int64_t>::min())
                    << "impl=" << impl->name();

            auto bad_doc = local_parser.iterate(bad_surrogate_json);
            simdjson::ondemand::value bad_val = bad_doc.find_field("value");
            auto bad_res = value_get_string_safe(&bad_val, &local_buffer);
            EXPECT_EQ(simdjson::STRING_ERROR, bad_res.error()) << "impl=" << impl->name();
        });
        t.join();
    }
    const simdjson::implementation* fallback_impl = simdjson::get_available_implementations()["fallback"];
    if (fallback_impl != nullptr && fallback_impl->supported_by_runtime_system()) {
        EXPECT_TRUE(tested_fallback);
    }
}

TEST_F(SimdJsonUtilTest, HandlesConsecutiveUnicodeEscapesAndSurrogatePairs) {
    auto json = R"({"value": "\u6C49\u5B57\u4E2D\u6587\uD83D\uDE00_tail"})"_padded;
    auto value = get_value(json);
    auto result = value_get_string_safe(&value, &buffer);
    ASSERT_EQ(simdjson::SUCCESS, result.error());
    EXPECT_EQ(result.value(), std::string_view("汉字中文\xF0\x9F\x98\x80_tail"));

    // Unpaired high surrogate followed by non-surrogate must fail with STRING_ERROR
    auto unpaired_high_json = R"({"value": "\uD83D_tail"})"_padded;
    auto unpaired_high_val = get_value(unpaired_high_json);
    auto unpaired_high_res = value_get_string_safe(&unpaired_high_val, &buffer);
    EXPECT_EQ(simdjson::STRING_ERROR, unpaired_high_res.error());

    // Reversed surrogate pair (low surrogate followed by high surrogate) must fail with STRING_ERROR
    auto reversed_pair_json = R"({"value": "\uDE00\uD83D"})"_padded;
    auto reversed_pair_val = get_value(reversed_pair_json);
    auto reversed_pair_res = value_get_string_safe(&reversed_pair_val, &buffer);
    EXPECT_EQ(simdjson::STRING_ERROR, reversed_pair_res.error());
}

} // namespace starrocks

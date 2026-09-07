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

#include "exprs/ai/ai_provider_options_builder.h"

#include <gtest/gtest.h>

#include <cmath>
#include <future>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "platform/llm/openai_compatible_provider.h"
#include "types/json_value.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

MapColumn::MutablePtr make_map(MutableColumnPtr keys, MutableColumnPtr values, uint32_t element_count) {
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(element_count);
    return MapColumn::create(ColumnHelper::cast_to_nullable_column(std::move(keys)),
                             ColumnHelper::cast_to_nullable_column(std::move(values)), std::move(offsets));
}

BinaryColumn::MutablePtr make_keys(std::initializer_list<std::string> values) {
    auto keys = BinaryColumn::create();
    for (const auto& value : values) {
        keys->append(value);
    }
    return keys;
}

int128_t parse_int128(std::string_view digits) {
    int128_t value = 0;
    for (char digit : digits) {
        value = value * 10 + (digit - '0');
    }
    return value;
}

std::string expected_body(std::string_view option_members) {
    std::string body =
            R"({"model":"test-model","messages":[{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":"test-prompt"}],"stream":false)";
    if (!option_members.empty()) {
        body.push_back(',');
        body.append(option_members);
    }
    body.push_back('}');
    return body;
}

std::string request_body(const AIProviderOptions& options) {
    OpenAICompatibleProvider provider;
    auto request = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/chat",
            .model = "test-model",
            .api_key = "test-api-key",
            .prompt = "test-prompt",
            .options = &options,
    });
    CHECK(request.ok()) << request.status().message();
    return std::move(request).value().body;
}

void expect_redacted(const Status& status, std::initializer_list<std::string> sentinels) {
    ASSERT_FALSE(status.ok());
    const std::string message(status.message());
    for (const auto& sentinel : sentinels) {
        EXPECT_EQ(std::string::npos, message.find(sentinel)) << message;
    }
}

TEST(AIProviderOptionsBuilderTest, EmptyMapProducesAnEmptyPreparedObject) {
    auto options = make_map(BinaryColumn::create(), BinaryColumn::create(), 0);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(""), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, UntypedEmptyMapProducesAnEmptyPreparedObject) {
    auto options = make_map(BinaryColumn::create(), BinaryColumn::create(), 0);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_NULL), TypeDescriptor(TYPE_NULL)), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(""), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, WireNormalizedUntypedEmptyMapProducesAnEmptyPreparedObject) {
    auto options = make_map(BooleanColumn::create(), BooleanColumn::create(), 0);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_BOOLEAN)), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(""), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, NonEmptyBooleanKeyMapIsNotTreatedAsUntyped) {
    auto keys = BooleanColumn::create();
    keys->append(true);
    auto values = BooleanColumn::create();
    values->append(false);
    auto options = make_map(std::move(keys), std::move(values), 1);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_BOOLEAN)), 0);

    ASSERT_FALSE(result.ok());
    EXPECT_EQ("AI provider options are invalid", result.status().message());
}

TEST(AIProviderOptionsBuilderTest, EmptyMapWithANonStringTypedKeyIsRejected) {
    auto options = make_map(Int32Column::create(), BinaryColumn::create(), 0);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR)), 0);

    ASSERT_FALSE(result.ok());
    EXPECT_EQ("AI provider options are invalid", result.status().message());
}

TEST(AIProviderOptionsBuilderTest, ConstAndNullableColumnsAreUnwrappedAtTheSelectedRow) {
    auto values = Int64Column::create();
    values->append(7);
    auto options = make_map(make_keys({"count"}), std::move(values), 1);
    auto nulls = NullColumn::create();
    nulls->append(0);
    ColumnPtr constant = ConstColumn::create(NullableColumn::create(std::move(options), std::move(nulls)), 4096);
    auto result = build_ai_provider_options(
            *constant, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BIGINT)),
            2048);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("count":7)"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, NullTopLevelMapMeansNoOptionsWithoutExposingValues) {
    auto values = BinaryColumn::create();
    values->append("option-value-secret");
    auto options = make_map(make_keys({"safe"}), std::move(values), 1);
    auto nulls = NullColumn::create();
    nulls->append(1);
    ColumnPtr nullable = NullableColumn::create(std::move(options), std::move(nulls));

    auto result = build_ai_provider_options(
            *nullable, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(""), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, SqlNullMapValueBecomesJsonNull) {
    auto values = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    values->append_datum(Datum());
    auto options = make_map(make_keys({"nullable"}), std::move(values), 1);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("nullable":null)"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, SqlScalarCategoriesRemainJsonCategories) {
    {
        auto values = BooleanColumn::create();
        values->append(1);
        auto options = make_map(make_keys({"enabled"}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BOOLEAN)),
                0);
        ASSERT_TRUE(result.ok()) << result.status().message();
        EXPECT_EQ(expected_body(R"("enabled":true)"), request_body(result.value()));
    }
    {
        auto values = Int64Column::create();
        values->append(-9);
        auto options = make_map(make_keys({"count"}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BIGINT)),
                0);
        ASSERT_TRUE(result.ok()) << result.status().message();
        EXPECT_EQ(expected_body(R"("count":-9)"), request_body(result.value()));
    }
    {
        auto values = DoubleColumn::create();
        values->append(0.25);
        auto options = make_map(make_keys({"ratio"}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_DOUBLE)),
                0);
        ASSERT_TRUE(result.ok()) << result.status().message();
        EXPECT_EQ(expected_body(R"("ratio":0.25)"), request_body(result.value()));
    }
    {
        auto values = BinaryColumn::create();
        values->append("false");
        auto options = make_map(make_keys({"text"}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)),
                0);
        ASSERT_TRUE(result.ok()) << result.status().message();
        EXPECT_EQ(expected_body(R"("text":"false")"), request_body(result.value()));
    }
}

TEST(AIProviderOptionsBuilderTest, DecimalRemainsAJsonNumberInsteadOfBecomingAString) {
    auto values = Decimal64Column::create(18, 2);
    values->append(1234);
    auto options = make_map(make_keys({"price"}), std::move(values), 1);
    auto result = build_ai_provider_options(
            *options,
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR),
                                            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2)),
            0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("price":12.34)"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, Decimal128RetainsAllThirtyEightDigitsAsANumberToken) {
    auto values = Decimal128Column::create(38, 4);
    values->append(parse_int128("12345678901234567890123456789012345678"));
    auto options = make_map(make_keys({"precise"}), std::move(values), 1);
    auto result = build_ai_provider_options(
            *options,
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR),
                                            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 4)),
            0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("precise":1234567890123456789012345678901234.5678)"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, LargeIntBeyondInt64RemainsAnExactJsonNumber) {
    auto values = Int128Column::create();
    values->append((int128_t(1) << 100) + 123);
    auto options = make_map(make_keys({"large"}), std::move(values), 1);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_LARGEINT)), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("large":1267650600228229401496703205499)"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, JsonValuesPreserveNestedCategoriesAndJsonNull) {
    auto values = JsonColumn::create();
    values->append(
            JsonValue::parse(R"({"bool":true,"number":3,"string":"3","array":[1,null],"object":{"k":"v"}})").value());
    values->append(JsonValue::from_null());
    auto options = make_map(make_keys({"payload", "json_null"}), std::move(values), 2);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_JSON)), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(
            expected_body(
                    R"("payload":{"array":[1,null],"bool":true,"number":3,"object":{"k":"v"},"string":"3"},"json_null":null)"),
            request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, NestedArrayMapStructAndSqlNullAreSerializedRecursively) {
    auto array_elements = NullableColumn::create(Int128Column::create(), NullColumn::create());
    auto array_offsets = UInt32Column::create();
    array_offsets->append(0);
    auto array = ArrayColumn::create(std::move(array_elements), std::move(array_offsets));
    array->append_datum(DatumArray{Datum((int128_t(1) << 100) + 123), Datum(), Datum(-(int128_t(1) << 100))});

    auto nested_values = NullableColumn::create(BooleanColumn::create(), NullColumn::create());
    nested_values->append_datum(Datum(true));
    auto nested_map = make_map(make_keys({"inner"}), std::move(nested_values), 1);

    auto json = JsonColumn::create();
    json->append(JsonValue::parse(R"({"nested":null})").value());

    MutableColumns fields;
    fields.emplace_back(std::move(array));
    fields.emplace_back(std::move(nested_map));
    fields.emplace_back(std::move(json));
    auto values = StructColumn::create(std::move(fields), std::vector<std::string>{"array", "map", "json"});
    auto options = make_map(make_keys({"complex"}), std::move(values), 1);

    const auto value_type = TypeDescriptor::create_struct_type(
            {"array", "map", "json"},
            {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_LARGEINT)),
             TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_BOOLEAN)),
             TypeDescriptor(TYPE_JSON)});
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), value_type), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(
            expected_body(
                    R"("complex":{"array":[1267650600228229401496703205499,null,-1267650600228229401496703205376],"map":{"inner":true},"json":{"nested":null}})"),
            request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, NestedEmptyArrayAndMapRemainEmptyJsonContainers) {
    auto empty_array_offsets = UInt32Column::create();
    empty_array_offsets->append(0);
    empty_array_offsets->append(0);
    auto empty_array = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(Int32Column::create()),
                                           std::move(empty_array_offsets));

    auto empty_map = make_map(BinaryColumn::create(), BinaryColumn::create(), 0);
    MutableColumns fields;
    fields.emplace_back(std::move(empty_array));
    fields.emplace_back(std::move(empty_map));
    auto values = StructColumn::create(std::move(fields), std::vector<std::string>{"empty_array", "empty_map"});
    auto options = make_map(make_keys({"nested"}), std::move(values), 1);
    const auto value_type = TypeDescriptor::create_struct_type(
            {"empty_array", "empty_map"},
            {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT)),
             TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR))});

    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), value_type), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("nested":{"empty_array":[],"empty_map":{}})"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, NestedUntypedEmptyMapRemainsAnEmptyJsonObject) {
    auto empty_map = make_map(BinaryColumn::create(), BinaryColumn::create(), 0);
    auto options = make_map(make_keys({"nested"}), std::move(empty_map), 1);
    const auto untyped_map = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_NULL), TypeDescriptor(TYPE_NULL));

    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), untyped_map), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("nested":{})"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, NestedWireNormalizedUntypedEmptyMapRemainsAnEmptyJsonObject) {
    auto empty_map = make_map(BooleanColumn::create(), BooleanColumn::create(), 0);
    auto options = make_map(make_keys({"nested"}), std::move(empty_map), 1);
    const auto normalized_untyped_map =
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_BOOLEAN));

    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), normalized_untyped_map), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("nested":{})"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, DuplicateKeysAreRejectedAtEveryMapDepthWithoutEchoingValues) {
    {
        auto values = BinaryColumn::create();
        values->append("first-secret");
        values->append("second-secret");
        auto options = make_map(make_keys({"duplicate", "duplicate"}), std::move(values), 2);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)),
                0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"duplicate", "first-secret", "second-secret"});
    }
    {
        auto nested_values = BinaryColumn::create();
        nested_values->append("first-nested-secret");
        nested_values->append("second-nested-secret");
        auto nested = make_map(make_keys({"nested-duplicate", "nested-duplicate"}), std::move(nested_values), 2);
        auto options = make_map(make_keys({"outer"}), std::move(nested), 1);
        const auto nested_type =
                TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR));
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), nested_type), 0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"nested-duplicate", "first-nested-secret", "second-nested-secret"});
    }
}

TEST(AIProviderOptionsBuilderTest, InvalidUtf8InKeysOrStringValuesIsRejectedWithoutEchoingBytes) {
    const std::string invalid_utf8("\xC3\x28", 2);
    {
        auto values = BinaryColumn::create();
        values->append("safe");
        auto options = make_map(make_keys({invalid_utf8}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)),
                0);

        ASSERT_FALSE(result.ok());
        EXPECT_EQ("AI provider options are invalid", result.status().message());
        expect_redacted(result.status(), {invalid_utf8});
    }
    {
        auto values = BinaryColumn::create();
        values->append(invalid_utf8);
        auto options = make_map(make_keys({"safe"}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)),
                0);

        ASSERT_FALSE(result.ok());
        EXPECT_EQ("AI provider options are invalid", result.status().message());
        expect_redacted(result.status(), {invalid_utf8});
    }
}

TEST(AIProviderOptionsBuilderTest, NullAndEmptyKeysAreRejectedAtEveryMapDepth) {
    {
        auto key_data = BinaryColumn::create();
        key_data->append("null-key-secret");
        auto key_nulls = NullColumn::create();
        key_nulls->append(1);
        auto keys = NullableColumn::create(std::move(key_data), std::move(key_nulls));
        auto values = BinaryColumn::create();
        values->append("value-secret");
        auto options = make_map(std::move(keys), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)),
                0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"null-key-secret", "value-secret"});
    }
    {
        auto values = BinaryColumn::create();
        values->append("top-empty-value-secret");
        auto options = make_map(make_keys({""}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)),
                0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"top-empty-value-secret"});
    }
    {
        auto nested_key_data = BinaryColumn::create();
        nested_key_data->append("nested-null-key-secret");
        auto nested_key_nulls = NullColumn::create();
        nested_key_nulls->append(1);
        auto nested_keys = NullableColumn::create(std::move(nested_key_data), std::move(nested_key_nulls));
        auto nested_values = BinaryColumn::create();
        nested_values->append("nested-null-value-secret");
        auto nested = make_map(std::move(nested_keys), std::move(nested_values), 1);
        auto options = make_map(make_keys({"outer"}), std::move(nested), 1);
        const auto nested_type =
                TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR));
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), nested_type), 0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"nested-null-key-secret", "nested-null-value-secret"});
    }
    {
        auto nested_values = BinaryColumn::create();
        nested_values->append("nested-value-secret");
        auto nested = make_map(make_keys({""}), std::move(nested_values), 1);
        auto options = make_map(make_keys({"outer"}), std::move(nested), 1);
        const auto nested_type =
                TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR));
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), nested_type), 0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"nested-value-secret"});
    }
}

TEST(AIProviderOptionsBuilderTest, OnlyExactCaseSensitiveTopLevelReservedKeysAreRejected) {
    for (const std::string& reserved : {"model", "messages", "stream"}) {
        auto values = BinaryColumn::create();
        values->append("reserved-value-secret");
        auto options = make_map(make_keys({reserved}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)),
                0);
        ASSERT_FALSE(result.ok()) << reserved;
        expect_redacted(result.status(), {reserved, "reserved-value-secret"});
    }

    auto values = BinaryColumn::create();
    values->append("allowed");
    auto options = make_map(make_keys({"Model"}), std::move(values), 1);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)), 0);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("Model":"allowed")"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, ReservedNamesRemainValidInsideNestedMaps) {
    auto nested_values = BinaryColumn::create();
    nested_values->append("nested-model");
    auto nested = make_map(make_keys({"model"}), std::move(nested_values), 1);
    auto options = make_map(make_keys({"outer"}), std::move(nested), 1);
    const auto nested_type =
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR));
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), nested_type), 0);

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(expected_body(R"("outer":{"model":"nested-model"})"), request_body(result.value()));
}

TEST(AIProviderOptionsBuilderTest, NonFiniteFloatingPointValuesAreRejectedWithoutEchoingInput) {
    for (double value : {std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
                         std::numeric_limits<double>::quiet_NaN()}) {
        auto values = DoubleColumn::create();
        values->append(value);
        auto options = make_map(make_keys({"non-finite-secret-key"}), std::move(values), 1);
        auto result = build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_DOUBLE)),
                0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"non-finite-secret-key"});
    }
}

TEST(AIProviderOptionsBuilderTest, UnsupportedOpaqueColumnsAreRejectedInsteadOfBecomingJsonNull) {
    auto values = BitmapColumn::create();
    values->append_default();
    auto options = make_map(make_keys({"opaque-secret-key"}), std::move(values), 1);
    auto result = build_ai_provider_options(
            *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_OBJECT)), 0);

    ASSERT_FALSE(result.ok());
    expect_redacted(result.status(), {"opaque-secret-key"});
}

TEST(AIProviderOptionsBuilderTest, PreparedOptionsOwnTheirStateAndSupportConcurrentReadOnlyRequestBuilding) {
    auto prepared_result = [&] {
        auto values = JsonColumn::create();
        values->append(JsonValue::parse(R"({"immutable":[1,2,3]})").value());
        auto options = make_map(make_keys({"payload"}), std::move(values), 1);
        return build_ai_provider_options(
                *options, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_JSON)), 0);
    }();
    ASSERT_TRUE(prepared_result.ok()) << prepared_result.status().message();
    auto prepared = std::move(prepared_result).value();
    auto copied = prepared;
    const auto expected = expected_body(R"("payload":{"immutable":[1,2,3]})");

    std::vector<std::future<bool>> reads;
    for (int i = 0; i < 16; ++i) {
        reads.emplace_back(std::async(std::launch::async, [&prepared, &copied, &expected, i] {
            const auto& options = i % 2 == 0 ? prepared : copied;
            for (int request = 0; request < 32; ++request) {
                if (request_body(options) != expected) {
                    return false;
                }
            }
            return true;
        }));
    }
    for (auto& read : reads) {
        EXPECT_TRUE(read.get());
    }
}

} // namespace
} // namespace starrocks

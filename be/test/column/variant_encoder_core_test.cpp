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
#include <vector>

#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "column/variant_encoder.h"
#include "gutil/casts.h"
#include "types/json_value.h"

namespace starrocks {

namespace {

StatusOr<JsonValue> parse_json_string(const std::string& text) {
    return JsonValue::parse(Slice(text));
}

} // namespace

TEST(VariantEncoderCoreTest, EncodeJsonToVariantScalarAndObject) {
    JsonValue scalar_json = JsonValue::from_int(123);
    auto scalar_variant = VariantEncoder::encode_json_to_variant(scalar_json);
    ASSERT_TRUE(scalar_variant.ok()) << scalar_variant.status();
    EXPECT_EQ(VariantMetadata::kEmptyMetadata, scalar_variant->get_metadata().raw());
    auto scalar_text = scalar_variant->to_json();
    ASSERT_TRUE(scalar_text.ok()) << scalar_text.status();
    EXPECT_EQ("123", scalar_text.value());

    auto object_json = JsonValue::parse(Slice(R"({"b":2,"a":1})"));
    ASSERT_TRUE(object_json.ok()) << object_json.status();
    auto object_variant = VariantEncoder::encode_json_to_variant(object_json.value());
    ASSERT_TRUE(object_variant.ok()) << object_variant.status();

    auto key0 = object_variant->get_metadata().get_key(0);
    auto key1 = object_variant->get_metadata().get_key(1);
    ASSERT_TRUE(key0.ok()) << key0.status();
    ASSERT_TRUE(key1.ok()) << key1.status();
    EXPECT_EQ("a", key0.value());
    EXPECT_EQ("b", key1.value());

    auto object_text = object_variant->to_json();
    ASSERT_TRUE(object_text.ok()) << object_text.status();
    auto roundtrip_object = parse_json_string(object_text.value());
    ASSERT_TRUE(roundtrip_object.ok()) << roundtrip_object.status();
    EXPECT_EQ(0, roundtrip_object->compare(object_json.value()));
}

TEST(VariantEncoderCoreTest, EncodeColumnPrimitiveNullableRoundTripAndNulls) {
    auto input = NullableColumn::create(Int32Column::create(), NullColumn::create());
    input->append_datum(Datum(int32_t(7)));
    input->append_datum(Datum());
    input->append_datum(Datum(int32_t(11)));

    ColumnBuilder<TYPE_VARIANT> builder(3);
    Status status = VariantEncoder::encode_column(input, TypeDescriptor(TYPE_INT), &builder, true);
    ASSERT_TRUE(status.ok()) << status;

    auto output = builder.build(false);
    ASSERT_TRUE(output->is_nullable());
    auto* output_nullable = down_cast<NullableColumn*>(output.get());
    ASSERT_EQ(3, output_nullable->size());
    EXPECT_FALSE(output_nullable->is_null(0));
    EXPECT_TRUE(output_nullable->is_null(1));
    EXPECT_FALSE(output_nullable->is_null(2));

    auto* output_values = down_cast<const VariantColumn*>(output_nullable->data_column().get());
    auto row0_text = output_values->get_object(0)->to_json();
    auto row2_text = output_values->get_object(2)->to_json();
    ASSERT_TRUE(row0_text.ok()) << row0_text.status();
    ASSERT_TRUE(row2_text.ok()) << row2_text.status();
    EXPECT_EQ("7", row0_text.value());
    EXPECT_EQ("11", row2_text.value());
}

TEST(VariantEncoderCoreTest, EncodeShreddedColumnRowSimpleTypedRow) {
    MutableColumns fields;
    auto ints = Int32Column::create();
    ints->append(5);
    auto strings = BinaryColumn::create();
    strings->append(Slice("x"));
    fields.emplace_back(std::move(ints));
    fields.emplace_back(std::move(strings));
    auto row_column = StructColumn::create(std::move(fields), {"z", "a"});

    TypeDescriptor row_type = TypeDescriptor::create_struct_type(
            {"z", "a"}, {TypeDescriptor(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)});

    auto encoded = VariantEncoder::encode_shredded_column_row(row_column, row_type, 0, nullptr);
    ASSERT_TRUE(encoded.ok()) << encoded.status();

    auto key0 = encoded->get_metadata().get_key(0);
    auto key1 = encoded->get_metadata().get_key(1);
    ASSERT_TRUE(key0.ok()) << key0.status();
    ASSERT_TRUE(key1.ok()) << key1.status();
    EXPECT_EQ("a", key0.value());
    EXPECT_EQ("z", key1.value());

    auto json_text = encoded->to_json();
    ASSERT_TRUE(json_text.ok()) << json_text.status();
    auto parsed = parse_json_string(json_text.value());
    auto expected = JsonValue::parse(Slice(R"({"z":5,"a":"x"})"));
    ASSERT_TRUE(parsed.ok()) << parsed.status();
    ASSERT_TRUE(expected.ok()) << expected.status();
    EXPECT_EQ(0, parsed->compare(expected.value()));
}

TEST(VariantEncoderCoreTest, EncodeColumnUnsupportedTypeThrowAndNullFallback) {
    auto input = Int32Column::create();
    input->append(1);
    input->append(2);

    ColumnBuilder<TYPE_VARIANT> throw_builder(2);
    auto throw_status = VariantEncoder::encode_column(input, TypeDescriptor(TYPE_UNKNOWN), &throw_builder, true);
    EXPECT_TRUE(throw_status.is_not_supported()) << throw_status;

    ColumnBuilder<TYPE_VARIANT> null_builder(2);
    auto null_status = VariantEncoder::encode_column(input, TypeDescriptor(TYPE_UNKNOWN), &null_builder, false);
    ASSERT_TRUE(null_status.ok()) << null_status;
    auto null_result = null_builder.build(false);
    ASSERT_TRUE(null_result->is_nullable());
    auto* nullable = down_cast<NullableColumn*>(null_result.get());
    ASSERT_EQ(2, nullable->size());
    EXPECT_TRUE(nullable->is_null(0));
    EXPECT_TRUE(nullable->is_null(1));
}

} // namespace starrocks

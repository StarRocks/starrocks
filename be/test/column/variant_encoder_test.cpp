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

#include "column/variant_encoder.h"

#include <gtest/gtest.h>

#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "gutil/casts.h"
#include "types/type_descriptor.h"
#include "types/variant.h"

namespace starrocks {

static ColumnPtr make_nullable_variant_column(const std::vector<std::optional<std::string>>& json_values) {
    auto data = VariantColumn::create();
    auto nulls = NullColumn::create();
    for (const auto& json : json_values) {
        if (!json.has_value()) {
            data->append_default();
            nulls->append(DATUM_NULL);
            continue;
        }
        auto encoded = VariantEncoder::encode_json_text_to_variant(*json);
        CHECK(encoded.ok()) << encoded.status().to_string();
        data->append(encoded.value());
        nulls->append(DATUM_NOT_NULL);
    }
    return NullableColumn::create(std::move(data), std::move(nulls));
}

static UInt32Column::Ptr make_offsets(std::initializer_list<uint32_t> offsets) {
    auto column = UInt32Column::create();
    for (uint32_t offset : offsets) {
        column->append(offset);
    }
    return column;
}

static void expect_typed_column_variant_json(const ColumnPtr& column, size_t row, const TypeDescriptor& type,
                                             std::string_view expected_json) {
    auto encoded = VariantColumn::encode_typed_row_as_variant(column.get(), row, type);
    ASSERT_TRUE(encoded.ok()) << encoded.status().to_string();
    ASSERT_EQ(VariantColumn::EncodedVariantState::kValue, encoded->state);
    auto json = encoded->value.to_json();
    ASSERT_TRUE(json.ok()) << json.status().to_string();
    EXPECT_EQ(expected_json, json.value());
}

// Verifies plain non-JSON text falls back to VARIANT string encoding.
PARALLEL_TEST(VariantEncoderTest, encode_plain_text_fallback_to_string) {
    auto encoded = VariantEncoder::encode_json_text_to_variant("abc");
    ASSERT_TRUE(encoded.ok());
    auto json = encoded->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"("abc")", json.value());
}

// Verifies JSON object text can be encoded into VARIANT row value.
PARALLEL_TEST(VariantEncoderTest, encode_json_text_object) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(R"({"a":1,"b":"x"})");
    ASSERT_TRUE(encoded.ok());
    auto json = encoded->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":1,"b":"x"})", json.value());
}

// Verifies scalar typed column can be encoded by encode_column.
PARALLEL_TEST(VariantEncoderTest, encode_int_column) {
    MutableColumnPtr int_col = Int32Column::create();
    int_col->append_datum(Datum(static_cast<int32_t>(7)));
    int_col->append_datum(Datum(static_cast<int32_t>(42)));

    ColumnBuilder<TYPE_VARIANT> builder(2);
    Status st = VariantEncoder::encode_column(int_col, TypeDescriptor(TYPE_INT), &builder, true);
    ASSERT_TRUE(st.ok());

    ColumnPtr out = builder.build(false);
    ASSERT_EQ(2, out->size());

    const auto* out_data = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(out.get()));
    ASSERT_NE(nullptr, out_data);
    ASSERT_EQ(2, out_data->size());
    ASSERT_FALSE(out_data->is_null(0));
    ASSERT_FALSE(out_data->is_null(1));

    VariantRowValue row0_buf;
    VariantRowValue row1_buf;
    const VariantRowValue* row0 = out_data->get_row_value(0, &row0_buf);
    const VariantRowValue* row1 = out_data->get_row_value(1, &row1_buf);
    ASSERT_NE(nullptr, row0);
    ASSERT_NE(nullptr, row1);
    auto json0 = row0->to_json();
    auto json1 = row1->to_json();
    ASSERT_TRUE(json0.ok());
    ASSERT_TRUE(json1.ok());
    ASSERT_EQ("7", json0.value());
    ASSERT_EQ("42", json1.value());
}

PARALLEL_TEST(VariantEncoderTest, encode_array_with_variant_children) {
    auto elements = make_nullable_variant_column({std::string(R"({"a":1})"), std::string(R"({"b":2})"), std::nullopt});
    auto array = ArrayColumn::create(elements, make_offsets({0, 3}));
    TypeDescriptor type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_VARIANT));

    expect_typed_column_variant_json(array, 0, type, R"([{"a":1},{"b":2},null])");
}

PARALLEL_TEST(VariantEncoderTest, encode_map_with_variant_values_and_empty_row) {
    auto key_data = BinaryColumn::create();
    key_data->append_datum(Datum(Slice("left")));
    key_data->append_datum(Datum(Slice("right")));
    key_data->append_datum(Datum(Slice("null_child")));
    auto key_nulls = NullColumn::create(3, DATUM_NOT_NULL);
    auto keys = NullableColumn::create(std::move(key_data), std::move(key_nulls));
    auto values = make_nullable_variant_column({std::string(R"({"a":1})"), std::string(R"({"b":2})"), std::nullopt});
    auto map = MapColumn::create(keys, values, make_offsets({0, 3, 3}));
    TypeDescriptor type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARIANT));

    expect_typed_column_variant_json(map, 0, type, R"({"left":{"a":1},"null_child":null,"right":{"b":2}})");
    expect_typed_column_variant_json(map, 1, type, R"({})");
}

PARALLEL_TEST(VariantEncoderTest, encode_map_with_varbinary_keys_and_variant_values) {
    auto key_data = BinaryColumn::create();
    key_data->append_datum(Datum(Slice("left")));
    key_data->append_datum(Datum(Slice("right")));
    auto key_nulls = NullColumn::create(2, DATUM_NOT_NULL);
    auto keys = NullableColumn::create(std::move(key_data), std::move(key_nulls));
    auto values = make_nullable_variant_column({std::string("1"), std::string("2")});
    auto map = MapColumn::create(keys, values, make_offsets({0, 2}));
    TypeDescriptor type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_VARIANT));

    expect_typed_column_variant_json(map, 0, type, R"({"left":1,"right":2})");
}

PARALLEL_TEST(VariantEncoderTest, encode_struct_with_variant_fields) {
    auto left = make_nullable_variant_column({std::string(R"({"a":1})"), std::string(R"({"b":2})")});
    auto right = make_nullable_variant_column({std::nullopt, std::string(R"([1,{"deep":3}])")});
    auto structure = StructColumn::create(Columns{left, right}, {"left", "right"});
    TypeDescriptor type = TypeDescriptor::create_struct_type(
            {"left", "right"}, {TypeDescriptor(TYPE_VARIANT), TypeDescriptor(TYPE_VARIANT)});

    expect_typed_column_variant_json(structure, 0, type, R"({"left":{"a":1},"right":null})");
    expect_typed_column_variant_json(structure, 1, type, R"({"left":{"b":2},"right":[1,{"deep":3}]})");
}

PARALLEL_TEST(VariantEncoderTest, encode_deep_array_struct_variant) {
    auto payload = make_nullable_variant_column({std::string(R"({"outer":{"x":1}})"), std::string(R"([2,{"y":3}])")});
    auto structures = StructColumn::create(Columns{payload}, {"payload"});
    auto struct_nulls = NullColumn::create(2, DATUM_NOT_NULL);
    auto nullable_structures = NullableColumn::create(std::move(structures), std::move(struct_nulls));
    auto array = ArrayColumn::create(nullable_structures, make_offsets({0, 2}));
    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"payload"}, {TypeDescriptor(TYPE_VARIANT)});
    TypeDescriptor array_type = TypeDescriptor::create_array_type(struct_type);

    expect_typed_column_variant_json(array, 0, array_type,
                                     R"([{"payload":{"outer":{"x":1}}},{"payload":[2,{"y":3}]}])");
}

PARALLEL_TEST(VariantEncoderTest, encode_deep_nested_json_roundtrip) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(R"({"a":{"b":[{"c":{"d":[1,2,{"e":"x"}]}}]}})");
    ASSERT_TRUE(encoded.ok());
    auto json = encoded->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"b":[{"c":{"d":[1,2,{"e":"x"}]}}]}})", json.value());
}

PARALLEL_TEST(VariantEncoderTest, encode_large_array_offsets) {
    std::string array_text = "[";
    for (int i = 0; i < 300; ++i) {
        if (i > 0) {
            array_text.push_back(',');
        }
        array_text.append(std::to_string(i));
    }
    array_text.push_back(']');

    auto encoded = VariantEncoder::encode_json_text_to_variant(array_text);
    ASSERT_TRUE(encoded.ok());
    const VariantValue& value = encoded->get_value();
    ASSERT_EQ(VariantType::ARRAY, value.type());
    auto info = value.get_array_info();
    ASSERT_TRUE(info.ok());
    ASSERT_EQ(300, info->num_elements);
    ASSERT_GE(info->offset_size, 2);
}

} // namespace starrocks

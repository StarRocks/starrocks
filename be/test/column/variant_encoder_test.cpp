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

#include <string>

#include "base/testutil/parallel_test.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/variant_column.h"
#include "gutil/casts.h"
#include "types/type_descriptor.h"
#include "types/variant.h"

namespace starrocks {

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

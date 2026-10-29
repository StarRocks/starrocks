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

#include "column/variant_column.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/variant_builder.h"
#include "column/variant_encoder.h"

namespace starrocks {
namespace {

BinaryColumn::MutablePtr binary_cell(std::string_view raw) {
    auto column = BinaryColumn::create();
    column->append(Slice(raw.data(), raw.size()));
    return column;
}

MutableColumnPtr nullable_bigint(int64_t value, bool is_null = false) {
    auto data = Int64Column::create();
    data->append(value);
    auto nulls = NullColumn::create();
    nulls->append(is_null ? 1 : 0);
    return NullableColumn::create(std::move(data), std::move(nulls));
}

VariantColumn::MutablePtr single_path(std::string_view metadata, std::string_view residual,
                                      MutableColumnPtr typed, TypeDescriptor type = TypeDescriptor(TYPE_BIGINT),
                                      MutableColumnPtr fallback = nullptr) {
    auto column = VariantColumn::create();
    MutableColumns typed_columns;
    typed_columns.emplace_back(std::move(typed));
    MutableColumns fallback_columns;
    fallback_columns.emplace_back(std::move(fallback));
    column->set_shredded_columns({"a"}, {std::move(type)}, std::move(typed_columns), std::move(fallback_columns),
                                 binary_cell(metadata), binary_cell(residual));
    return column;
}

StatusOr<VariantValue> field(const VariantRowValue& row, std::string_view key) {
    return row.get_value().get_object_by_key(row.get_metadata(), key);
}

} // namespace

PARALLEL_TEST(VariantStrictMaterializationTest, empty_residual_with_metadata_and_typed_overlay) {
    auto column = single_path(VariantMetadata::kEmptyMetadata, {}, nullable_bigint(42));
    const Column* typed_before = column->typed_columns()[0].get();
    const auto metadata_before = std::string(column->metadata_column()->get_slice(0));
    auto result = column->materialize_row_strict(0);
    ASSERT_TRUE(result.ok()) << result.status();
    auto a = field(result.value(), "a");
    ASSERT_TRUE(a.ok());
    ASSERT_EQ(42, a->get_int64().value());
    ASSERT_EQ(typed_before, column->typed_columns()[0].get());
    ASSERT_EQ(42, column->typed_columns()[0]->get(0).get_int64());
    ASSERT_EQ(metadata_before, std::string(column->metadata_column()->get_slice(0)));
    ASSERT_EQ(0, column->remain_value_column()->get_slice(0).size);
}

PARALLEL_TEST(VariantStrictMaterializationTest, typed_only_and_constant_overlay) {
    auto column = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(ConstColumn::create(nullable_bigint(7), 2));
    column->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);
    auto result = column->materialize_row_strict(1);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(7, field(result.value(), "a")->get_int64().value());
    ASSERT_FALSE(column->materialize_row_strict(2).ok());
    ASSERT_FALSE(column->has_metadata_column());
    ASSERT_FALSE(column->has_remain_value());
}

PARALLEL_TEST(VariantStrictMaterializationTest, metadata_only_without_effective_overlay_is_not_null) {
    auto column = single_path(VariantMetadata::kEmptyMetadata, {}, nullable_bigint(0, true));
    ASSERT_FALSE(column->materialize_row_strict(0).ok());
    // Keep the query-side permissive behavior: strict writes do not redefine its fallback.
    VariantRowValue query_result;
    ASSERT_TRUE(column->try_materialize_row(0, &query_result));
    ASSERT_TRUE(query_result.get_value().is_null());

    auto metadata_only = VariantColumn::create();
    metadata_only->set_shredded_columns({}, {}, {}, binary_cell(VariantMetadata::kEmptyMetadata), binary_cell({}));
    ASSERT_FALSE(metadata_only->materialize_row_strict(0).ok());
    ASSERT_FALSE(metadata_only->materialize_row_strict(1).ok());
}

PARALLEL_TEST(VariantStrictMaterializationTest, nonmonotonic_residual_and_false_overlay_are_lossless) {
    const char metadata[] = {0x11, 2, 0, 1, 2, 'b', 'c'};
    const char residual[] = {2, 2, 0, 1, 1, 0, 5, 0, 13, 'x', 'y', 'z'};
    auto typed = BooleanColumn::create();
    typed->append(0);
    auto column = single_path(std::string_view(metadata, sizeof(metadata)),
                              std::string_view(residual, sizeof(residual)), std::move(typed),
                              TypeDescriptor(TYPE_BOOLEAN));
    auto result = column->materialize_row_strict(0);
    ASSERT_TRUE(result.ok()) << result.status();
    auto a = field(result.value(), "a");
    auto b = field(result.value(), "b");
    auto c = field(result.value(), "c");
    ASSERT_TRUE(a.ok());
    ASSERT_TRUE(b.ok());
    ASSERT_TRUE(c.ok());
    ASSERT_EQ(VariantType::BOOLEAN_FALSE, a->type());
    ASSERT_EQ(std::string_view(residual + 8, 4), b->raw());
    ASSERT_EQ(VariantValue::kEmptyValue, c->raw());
    ASSERT_EQ(std::string_view(residual, sizeof(residual)),
              std::string_view(column->remain_value_column()->get_slice(0)));
}

PARALLEL_TEST(VariantStrictMaterializationTest, fallback_uses_row_metadata_before_child_overlay) {
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"b":1})");
    ASSERT_TRUE(base.ok());
    auto column = VariantColumn::create();
    MutableColumns typed;
    // Intentionally put the child first: the parent's residual must be applied first.
    typed.emplace_back(nullable_bigint(9));
    typed.emplace_back(nullable_bigint(0, true));
    MutableColumns fallbacks;
    fallbacks.emplace_back(nullptr);
    auto parent_fallback = VariantColumn::create_fallback_column();
    parent_fallback->append_datum(Datum(Slice(base->get_value().raw())));
    fallbacks.emplace_back(std::move(parent_fallback));
    column->set_shredded_columns({"a.c", "a"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                                 std::move(typed), std::move(fallbacks), binary_cell(base->get_metadata().raw()),
                                 binary_cell({}));
    auto result = column->materialize_row_strict(0);
    ASSERT_TRUE(result.ok()) << result.status();
    auto a = field(result.value(), "a");
    ASSERT_TRUE(a.ok());
    ASSERT_EQ(1, a->get_object_by_key(result->get_metadata(), "b")->get_int8().value());
    ASSERT_EQ(9, a->get_object_by_key(result->get_metadata(), "c")->get_int64().value());
    ASSERT_TRUE(column->typed_columns()[1]->is_null(0));
    ASSERT_TRUE(column->has_fallback_value(1, 0));
}

PARALLEL_TEST(VariantStrictMaterializationTest, fallback_null_is_value_but_empty_nonnull_fallback_is_error) {
    auto fallback = VariantColumn::create_fallback_column();
    fallback->append_datum(Datum(Slice(VariantValue::kEmptyValue)));
    auto column = single_path(VariantMetadata::kEmptyMetadata, {}, nullable_bigint(0, true),
                              TypeDescriptor(TYPE_BIGINT), std::move(fallback));
    auto result = column->materialize_row_strict(0);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(1, result->get_value().num_elements().value());
    ASSERT_EQ(VariantValue::kEmptyValue, field(result.value(), "a")->raw());

    auto empty = VariantColumn::create_fallback_column();
    empty->append_datum(Datum(Slice("", 0)));
    auto invalid = single_path(VariantMetadata::kEmptyMetadata, {}, nullable_bigint(0, true),
                               TypeDescriptor(TYPE_BIGINT), std::move(empty));
    ASSERT_FALSE(invalid->materialize_row_strict(0).ok());
}

PARALLEL_TEST(VariantStrictMaterializationTest, malformed_base_or_overlay_does_not_fall_back) {
    const char truncated_int[] = {static_cast<char>(static_cast<uint8_t>(VariantType::INT64) << 2), 1};
    auto bad_base = single_path(VariantMetadata::kEmptyMetadata, std::string_view(truncated_int, sizeof(truncated_int)),
                                nullable_bigint(9));
    ASSERT_FALSE(bad_base->materialize_row_strict(0).ok());

    const char invalid_dictionary[] = {0x11, 1, 0, 9, 'a'};
    auto bad_metadata = single_path(std::string_view(invalid_dictionary, sizeof(invalid_dictionary)), {},
                                    nullable_bigint(9));
    ASSERT_FALSE(bad_metadata->materialize_row_strict(0).ok());

    auto bad_overlay = VariantColumn::create();
    bad_overlay->append_shredded(Slice(VariantMetadata::kEmptyMetadata), Slice(truncated_int, sizeof(truncated_int)));
    auto outer = single_path(VariantMetadata::kEmptyMetadata, VariantValue::kEmptyValue, std::move(bad_overlay),
                             TypeDescriptor(TYPE_VARIANT));
    ASSERT_FALSE(outer->materialize_row_strict(0).ok());
    ASSERT_EQ(VariantValue::kEmptyValue, std::string_view(outer->remain_value_column()->get_slice(0)));
}

PARALLEL_TEST(VariantStrictMaterializationTest, explicit_variant_null_is_not_sql_null_or_missing) {
    auto typed_variant = VariantColumn::create();
    typed_variant->append(VariantRowValue::from_null());
    auto column = single_path(VariantMetadata::kEmptyMetadata, {}, std::move(typed_variant),
                              TypeDescriptor(TYPE_VARIANT));
    auto result = column->materialize_row_strict(0);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(1, result->get_value().num_elements().value());
    ASSERT_EQ(VariantValue::kEmptyValue, field(result.value(), "a")->raw());

    // SQL NULL is owned by the outer NullableColumn, not synthesized by the materializer.
    auto nulls = NullColumn::create();
    nulls->append(1);
    auto sql_null = NullableColumn::create(std::move(column), std::move(nulls));
    ASSERT_TRUE(sql_null->is_null(0));
    auto encoded = VariantColumn::encode_typed_row_as_variant(sql_null.get(), 0, TypeDescriptor(TYPE_VARIANT));
    ASSERT_TRUE(encoded.ok());
    ASSERT_EQ(VariantColumn::EncodedVariantState::kNull, encoded->state);

    auto base_null = VariantColumn::create();
    base_null->append(VariantRowValue::from_null());
    auto materialized_null = base_null->materialize_row_strict(0);
    ASSERT_TRUE(materialized_null.ok()) << materialized_null.status();
    ASSERT_EQ(VariantValue::kEmptyValue, materialized_null->get_value().raw());
}

PARALLEL_TEST(VariantStrictMaterializationTest, encoded_null_overlay_replaces_existing_residual_field) {
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"a":17,"b":false})");
    ASSERT_TRUE(base.ok());
    auto typed = VariantColumn::create();
    typed->append(VariantRowValue::from_null());
    auto column = single_path(base->get_metadata().raw(), base->get_value().raw(), std::move(typed),
                              TypeDescriptor(TYPE_VARIANT));
    auto result = column->materialize_row_strict(0);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(VariantValue::kEmptyValue, field(result.value(), "a")->raw());
    ASSERT_EQ(VariantType::BOOLEAN_FALSE, field(result.value(), "b")->type());
    ASSERT_EQ(base->get_value().raw(), std::string_view(column->remain_value_column()->get_slice(0)));
}

PARALLEL_TEST(VariantStrictMaterializationTest, missing_typed_cell_preserves_residual_field) {
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"a":17,"b":false})");
    ASSERT_TRUE(base.ok());
    auto column = single_path(base->get_metadata().raw(), base->get_value().raw(), nullable_bigint(0, true));
    auto result = column->materialize_row_strict(0);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(base->get_metadata().raw(), result->get_metadata().raw());
    ASSERT_EQ(base->get_value().raw(), result->get_value().raw());
}

PARALLEL_TEST(VariantStrictMaterializationTest, full_int128_payload_is_preserved_without_decimal_narrowing) {
    for (const int128_t value : {std::numeric_limits<int128_t>::min(), std::numeric_limits<int128_t>::max()}) {
        auto typed = Int128Column::create();
        typed->append(value);
        auto encoded = VariantColumn::encode_typed_row_as_variant(typed.get(), 0, TypeDescriptor(TYPE_LARGEINT));
        ASSERT_TRUE(encoded.ok());
        auto column = single_path(VariantMetadata::kEmptyMetadata, {}, std::move(typed), TypeDescriptor(TYPE_LARGEINT));
        auto result = column->materialize_row_strict(0);
        ASSERT_TRUE(result.ok()) << result.status();
        auto a = field(result.value(), "a");
        ASSERT_TRUE(a.ok());
        ASSERT_EQ(encoded->value.get_value().raw(), a->raw());
        auto decimal = a->get_decimal16();
        ASSERT_TRUE(decimal.ok());
        ASSERT_TRUE(decimal->value == value);
        ASSERT_EQ(0, decimal->scale);
    }
}

PARALLEL_TEST(VariantStrictMaterializationTest, array_with_nested_variant_uses_strict_materialization) {
    auto elements = VariantColumn::create();
    const char truncated[] = {static_cast<char>(static_cast<uint8_t>(VariantType::INT32) << 2)};
    elements->append_shredded(Slice(VariantMetadata::kEmptyMetadata), Slice(truncated, sizeof(truncated)));
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(1);
    auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
    auto column = single_path(VariantMetadata::kEmptyMetadata, VariantValue::kEmptyValue, std::move(array),
                              TypeDescriptor::create_array_type(TypeDescriptor(TYPE_VARIANT)));
    ASSERT_FALSE(column->materialize_row_strict(0).ok());
}

} // namespace starrocks

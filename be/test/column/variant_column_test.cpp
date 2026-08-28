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
#include "testutil/parallel_test.h"
#include "types/logical_type.h"
#include "types/variant_value.h"
#include "util/variant.h"

namespace starrocks {

static inline uint8_t primitive_header(VariantType primitive) {
    return static_cast<uint8_t>(primitive) << 2;
}

<<<<<<< HEAD
=======
static MutableColumnPtr build_nullable_int64_column(const std::vector<int64_t>& values,
                                                    const std::vector<uint8_t>& is_null) {
    auto data = Int64Column::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_int16_column(const std::vector<int16_t>& values,
                                                    const std::vector<uint8_t>& is_null) {
    auto data = Int16Column::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_varchar_column(const std::vector<std::string>& values,
                                                      const std::vector<uint8_t>& is_null) {
    auto data = BinaryColumn::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_int_array_column(const std::vector<DatumArray>& values,
                                                        const std::vector<uint8_t>& is_null) {
    TypeDescriptor array_type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT));
    auto col = ColumnHelper::create_column(array_type, true);
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        if (is_null[i] != 0) {
            col->append_nulls(1);
        } else {
            col->append_datum(Datum(values[i]));
        }
    }
    return col;
}

static MutableColumnPtr build_nullable_variant_column(const std::vector<std::string>& json_values,
                                                      const std::vector<uint8_t>& is_null) {
    auto data = VariantColumn::create();
    auto null = NullColumn::create();
    DCHECK_EQ(json_values.size(), is_null.size());
    for (size_t i = 0; i < json_values.size(); ++i) {
        if (is_null[i] != 0) {
            VariantRowValue row = VariantRowValue::from_null();
            data->append(&row);
            null->append(1);
            continue;
        }
        auto encoded = VariantEncoder::encode_json_text_to_variant(json_values[i]);
        DCHECK(encoded.ok()) << encoded.status().to_string();
        data->append(&encoded.value());
        null->append(0);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static void append_primitive_int8_row(BinaryColumn* metadata, BinaryColumn* remain, int8_t value) {
    const std::string metadata_bytes(VariantMetadata::kEmptyMetadata);
    const char payload[2] = {static_cast<char>(primitive_header(VariantType::INT8)), static_cast<char>(value)};
    metadata->append(Slice(metadata_bytes.data(), metadata_bytes.size()));
    remain->append(Slice(payload, sizeof(payload)));
}

static auto build_shredded_variant_column_for_ut() {
    auto col = VariantColumn::create();

    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_primitive_int8_row(metadata.get(), remain.get(), 1);
    append_primitive_int8_row(metadata.get(), remain.get(), 2);
    append_primitive_int8_row(metadata.get(), remain.get(), 3);

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({10, 20, 30}, {0, 1, 0}));

    col->set_shredded_columns({"typed_only"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                              std::move(remain));
    return col;
}

static void append_json_variant_row(BinaryColumn* metadata, BinaryColumn* remain, std::string_view json_text) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(json_text);
    ASSERT_TRUE(encoded.ok()) << encoded.status().to_string();
    std::string_view metadata_raw = encoded->get_metadata().raw();
    std::string_view value_raw = encoded->get_value().raw();
    metadata->append(Slice(metadata_raw.data(), metadata_raw.size()));
    remain->append(Slice(value_raw.data(), value_raw.size()));
}

static VariantRowValue create_variant_row_from_json_text(std::string_view json_text) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(json_text);
    DCHECK(encoded.ok()) << encoded.status().to_string();
    return encoded.value();
}

static MutableColumnPtr build_single_path_bigint_shredded_variant(std::string path, int64_t typed_value) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_primitive_int8_row(metadata.get(), remain.get(), 1);
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({typed_value}, {0}));
    col->set_shredded_columns({std::move(path)}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                              std::move(remain));
    return col;
}

static void assert_variant_row_json(const VariantColumn* col, size_t row, std::string_view expected_json) {
    VariantRowValue buffer;
    const VariantRowValue* value = col->get_row_value(row, &buffer);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(expected_json, json.value());
}

static void assert_null_base_payload(const VariantColumn* col, size_t row) {
    ASSERT_TRUE(col->has_metadata_column());
    ASSERT_TRUE(col->has_remain_value());
    VariantRowValue null_base = VariantRowValue::from_null();
    std::string_view null_metadata = null_base.get_metadata().raw();
    std::string_view null_value = null_base.get_value().raw();
    auto metadata_slice = col->metadata_column()->get(row).get_slice();
    auto remain_slice = col->remain_value_column()->get(row).get_slice();
    ASSERT_EQ(null_metadata.size(), metadata_slice.size);
    ASSERT_EQ(null_value.size(), remain_slice.size);
    ASSERT_EQ(0, memcmp(null_metadata.data(), metadata_slice.data, metadata_slice.size));
    ASSERT_EQ(0, memcmp(null_value.data(), remain_slice.data, remain_slice.size));
}

enum class TypedOnlyIntoBaseShreddedAppendMode {
    kAppend,
    kAppendSelective,
    kAppendValueMultipleTimes,
};

static void verify_typed_only_into_base_shredded_fast_path(TypedOnlyIntoBaseShreddedAppendMode mode) {
    auto src = VariantColumn::create();
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10, 11}, {0, 0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), nullptr, nullptr);

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_json_variant_row(dst_metadata.get(), dst_remain.get(), R"({"a":7})");
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({7}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    switch (mode) {
    case TypedOnlyIntoBaseShreddedAppendMode::kAppend:
        dst->append(*src, 0, 2);
        ASSERT_EQ(3, dst->size());
        ASSERT_EQ(7, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(0).get_int64());
        ASSERT_EQ(10, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(1).get_int64());
        ASSERT_EQ(11, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(2).get_int64());
        assert_null_base_payload(dst.get(), 1);
        assert_null_base_payload(dst.get(), 2);
        return;
    case TypedOnlyIntoBaseShreddedAppendMode::kAppendSelective: {
        uint32_t indexes[] = {1};
        dst->append_selective(*src, indexes, 0, 1);
        ASSERT_EQ(2, dst->size());
        ASSERT_EQ(7, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(0).get_int64());
        ASSERT_EQ(11, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(1).get_int64());
        assert_null_base_payload(dst.get(), 1);
        return;
    }
    case TypedOnlyIntoBaseShreddedAppendMode::kAppendValueMultipleTimes:
        dst->append_value_multiple_times(*src, 1, 2);
        ASSERT_EQ(3, dst->size());
        ASSERT_EQ(7, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(0).get_int64());
        ASSERT_EQ(11, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(1).get_int64());
        ASSERT_EQ(11, down_cast<const NullableColumn*>(dst->typed_column_by_index(0))->get(2).get_int64());
        assert_null_base_payload(dst.get(), 1);
        assert_null_base_payload(dst.get(), 2);
        return;
    }
}

PARALLEL_TEST(VariantColumnTest, test_remove_first_n_values_from_nullable_variant) {
    auto column = build_nullable_variant_column({"1", "2", "3"}, {0, 1, 0});

    column->remove_first_n_values(1);

    ASSERT_EQ(2, column->size());
    const auto* nullable = down_cast<const NullableColumn*>(column.get());
    ASSERT_TRUE(nullable->is_null(0));
    ASSERT_FALSE(nullable->is_null(1));
    const auto* data = down_cast<const VariantColumn*>(nullable->data_column().get());
    ASSERT_EQ(2, data->size());
    assert_variant_row_json(data, 0, "null");
    assert_variant_row_json(data, 1, "3");
}

PARALLEL_TEST(VariantColumnTest, test_remove_first_n_values_from_shredded_variant) {
    auto column = build_shredded_variant_column_for_ut();

    column->remove_first_n_values(1);

    ASSERT_EQ(2, column->size());
    ASSERT_EQ(2, column->metadata_column()->size());
    ASSERT_EQ(2, column->remain_value_column()->size());
    ASSERT_EQ(2, column->typed_column_by_index(0)->size());
    const auto* typed = down_cast<const NullableColumn*>(column->typed_column_by_index(0));
    ASSERT_TRUE(typed->is_null(0));
    ASSERT_FALSE(typed->is_null(1));
    const auto* typed_data = down_cast<const Int64Column*>(typed->data_column().get());
    ASSERT_EQ(30, typed_data->immutable_data()[1]);
}

PARALLEL_TEST(VariantColumnTest, test_remove_values_from_const_typed_variant) {
    auto column = VariantColumn::create();
    auto typed_data = Int64Column::create();
    typed_data->append(42);
    MutableColumns typed;
    typed.emplace_back(ConstColumn::create(std::move(typed_data), 3));
    column->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    column->remove_first_n_values(1);

    ASSERT_EQ(2, column->size());
    ASSERT_EQ(2, column->typed_column_by_index(0)->size());

    column->remove_first_n_values(column->size());

    ASSERT_EQ(0, column->size());
    ASSERT_EQ(0, column->typed_column_by_index(0)->size());
}

>>>>>>> 45fdd3c ([BugFix] Fix shredded Variant compatibility in generic operations (#78296))
// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_build_column) {
    // create from type traits
    {
        const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};
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
        const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};
        builder.append(&variant);
        auto column = builder.build(false);
        EXPECT_EQ("variant", column->get_name());
        EXPECT_TRUE(column->is_variant());

        auto column_ptr = ColumnHelper::cast_to<TYPE_VARIANT>(column);
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
        auto column = VariantColumn::create();
        const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};
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

            auto variant_column_ptr = ColumnHelper::cast_to<TYPE_VARIANT>(copy);
            ASSERT_EQ(variant_column_ptr->size(), 1);
            auto res = variant_column_ptr->get(0).get_variant();
            ASSERT_EQ(res->serialize_size(), variant.serialize_size());
            ASSERT_EQ(res->get_metadata(), variant.get_metadata());
            ASSERT_EQ(res->get_value(), variant.get_value());
            EXPECT_EQ(res->to_string(), variant.to_string());
            EXPECT_EQ("1234567890", res->to_string());

            auto variant_column = ColumnHelper::cast_to_raw<TYPE_VARIANT>(copy);
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
    const VariantRowValue* deserialized_variant = new_column->get(0).get_variant();
    ASSERT_TRUE(deserialized_variant != nullptr);
    EXPECT_EQ(variant.serialize_size(), deserialized_variant->serialize_size());
    EXPECT_EQ(variant.to_string(), deserialized_variant->to_string());
    EXPECT_EQ("\"f24f9b64-81fa-49d1-b74e-8c09a6e31c56\"", deserialized_variant->to_json().value());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, put_mysql_row_buffer) {
    const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
    std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
    VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};

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
    const uint8_t int1_value[] = {primitive_header(VariantType::INT8), 0x01};
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
    auto expected = VariantRowValue::create(slice);
    ASSERT_TRUE(expected.ok());
    const VariantRowValue* actual = variant_column->get_object(0);
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

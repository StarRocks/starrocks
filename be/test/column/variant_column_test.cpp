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

#include <cstring>
#include <string_view>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/variant_encoder.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/variant.h"
#include "types/variant_value.h"

namespace starrocks {

static inline uint8_t primitive_header(VariantType primitive) {
    return static_cast<uint8_t>(primitive) << 2;
}

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

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_build_column) {
    // create column
    {
        const uint8_t int_chars[] = {primitive_header(VariantType::INT32), 0xD2, 0x02, 0x96, 0x49};
        std::string_view int_string(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
        VariantRowValue variant{VariantMetadata::kEmptyMetadata, int_string};
        auto column = VariantColumn::create();
        EXPECT_EQ("variant", column->get_name());
        EXPECT_TRUE(column->is_variant());
        column->append(&variant);

        VariantRowValue row_buffer;
        const VariantRowValue* res = column->get_row_value(0, &row_buffer);
        ASSERT_NE(nullptr, res);
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

        auto copy = column->clone();
        ASSERT_EQ(copy->size(), 1);

        auto* variant_column = down_cast<VariantColumn*>(copy.get());
        VariantRowValue row_buffer;
        const VariantRowValue* res = variant_column->get_row_value(0, &row_buffer);
        ASSERT_NE(nullptr, res);
        ASSERT_EQ(res->serialize_size(), variant.serialize_size());
        ASSERT_EQ(res->get_metadata(), variant.get_metadata());
        ASSERT_EQ(res->get_value(), variant.get_value());
        EXPECT_EQ(res->to_string(), variant.to_string());
        EXPECT_EQ("1234567890", res->to_string());
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

    auto column = VariantColumn::create();
    EXPECT_EQ("variant", column->get_name());
    EXPECT_TRUE(column->is_variant());
    column->append(&variant);
    // Column uses shredded wire format; size differs from VariantRowValue::serialize_size().
    uint32_t col_sz = column->serialize_size(0);
    EXPECT_GT(col_sz, 0u);

    std::vector<uint8_t> buffer;
    buffer.resize(col_sz);
    uint32_t written = column->serialize(0, buffer.data());
    EXPECT_EQ(col_sz, written);
    auto new_column = column->clone_empty();
    new_column->deserialize_and_append(buffer.data());
    auto* deserialized_column = down_cast<VariantColumn*>(new_column.get());
    VariantRowValue deserialized_row_buffer;
    const VariantRowValue* deserialized_variant = deserialized_column->get_row_value(0, &deserialized_row_buffer);
    ASSERT_NE(nullptr, deserialized_variant);
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

    EXPECT_EQ(0, variant_column->size());
    EXPECT_TRUE(variant_column->empty());
    EXPECT_FALSE(variant_column->is_nullable());
    EXPECT_FALSE(variant_column->is_constant());

    auto cloned = variant_column->clone();
    EXPECT_EQ(0, cloned->size());

    size_t memory_usage = variant_column->memory_usage();
    EXPECT_GE(memory_usage, 0);
}

PARALLEL_TEST(VariantColumnTest, test_clone_shredded_schema_integrity) {
    auto src = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({})");
    MutableColumns typed;
    typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(typed), std::move(metadata),
                              std::move(remain));

    auto cloned_holder = src->clone();
    auto* cloned = down_cast<VariantColumn*>(cloned_holder.get());
    ASSERT_EQ(1, cloned->shredded_paths().size());
    ASSERT_EQ(1, cloned->shredded_types().size());
    ASSERT_EQ(1, cloned->typed_columns().size());
    ASSERT_TRUE(cloned->has_metadata_column());
    ASSERT_TRUE(cloned->has_remain_value());

    auto schema_st = VariantColumn::validate_shredded_schema(cloned->shredded_paths(), cloned->shredded_types(),
                                                             cloned->typed_columns(), cloned->metadata_column(),
                                                             cloned->remain_value_column());
    ASSERT_TRUE(schema_st.ok()) << schema_st.to_string();

    VariantRowValue row_buf;
    const VariantRowValue* row = cloned->get_row_value(0, &row_buf);
    ASSERT_NE(nullptr, row);
    auto json = row->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":"x"})", json.value());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_append_strings) {
    const auto variant_column = VariantColumn::create();
    const uint8_t int1_value[] = {primitive_header(VariantType::INT8), 0x01};
    const std::string_view int1_value_str(reinterpret_cast<const char*>(int1_value), sizeof(int1_value));

    // Build binary in VariantRowValue serialize format:
    // [uint32 total_size LE][metadata bytes][remain bytes]
    const auto& empty_meta = VariantMetadata::kEmptyMetadata;
    const uint32_t total_size = static_cast<uint32_t>(empty_meta.size() + int1_value_str.size());
    std::string serialize_buf;
    serialize_buf.append(reinterpret_cast<const char*>(&total_size), sizeof(uint32_t));
    serialize_buf.append(empty_meta.data(), empty_meta.size());
    serialize_buf.append(int1_value_str.data(), int1_value_str.size());
    variant_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(serialize_buf.data()));

    ASSERT_EQ(1, variant_column->size());

    // Build expected using VariantRowValue Slice format: [uint32 total_size][metadata][value]
    constexpr uint32_t int1_total_size = sizeof(int1_value) + VariantMetadata::kEmptyMetadata.size();
    std::string variant_string;
    variant_string.resize(int1_total_size + sizeof(uint32_t));
    memcpy(variant_string.data(), &int1_total_size, sizeof(uint32_t));
    memcpy(variant_string.data() + sizeof(uint32_t), VariantMetadata::kEmptyMetadata.data(),
           VariantMetadata::kEmptyMetadata.size());
    memcpy(variant_string.data() + sizeof(uint32_t) + VariantMetadata::kEmptyMetadata.size(), int1_value_str.data(),
           int1_value_str.size());
    const Slice slice(variant_string.data(), variant_string.size());
    auto expected = VariantRowValue::create(slice);
    ASSERT_TRUE(expected.ok());

    VariantRowValue row_buffer;
    const VariantRowValue* actual = variant_column->get_row_value(0, &row_buffer);
    ASSERT_NE(nullptr, actual);
    ASSERT_EQ(expected->get_metadata(), actual->get_metadata());
    ASSERT_EQ(expected->get_value(), actual->get_value());
    EXPECT_EQ(expected->to_string(), actual->to_string());
    EXPECT_EQ("1", actual->to_string());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_append_interop) {
    auto src = build_shredded_variant_column_for_ut();

    auto dst_append = VariantColumn::create();
    dst_append->append(*src, 0, src->size());
    ASSERT_EQ(3, dst_append->size());

    const auto* typed_append = down_cast<const NullableColumn*>(dst_append->typed_column_by_index(0));
    ASSERT_FALSE(typed_append->get(0).is_null());
    ASSERT_EQ(10, typed_append->get(0).get_int64());
    ASSERT_TRUE(typed_append->get(1).is_null());
    ASSERT_EQ(30, typed_append->get(2).get_int64());

    const auto m0 = dst_append->metadata_column()->get(0).get_slice();
    const auto r0 = dst_append->remain_value_column()->get(0).get_slice();
    ASSERT_EQ(VariantMetadata::kEmptyMetadata.size(), m0.size);
    ASSERT_EQ(2, r0.size);
    ASSERT_EQ(static_cast<char>(primitive_header(VariantType::INT8)), r0.data[0]);
    ASSERT_EQ(1, static_cast<int8_t>(r0.data[1]));

    uint32_t indexes[] = {2, 0};
    auto dst_selective = VariantColumn::create();
    dst_selective->append_selective(*src, indexes, 0, 2);
    ASSERT_EQ(2, dst_selective->size());

    const auto* typed_selective = down_cast<const NullableColumn*>(dst_selective->typed_column_by_index(0));
    ASSERT_EQ(30, typed_selective->get(0).get_int64());
    ASSERT_EQ(10, typed_selective->get(1).get_int64());

    const auto r_sel0 = dst_selective->remain_value_column()->get(0).get_slice();
    const auto r_sel1 = dst_selective->remain_value_column()->get(1).get_slice();
    ASSERT_EQ(3, static_cast<int8_t>(r_sel0.data[1]));
    ASSERT_EQ(1, static_cast<int8_t>(r_sel1.data[1]));

    auto dst_repeat = VariantColumn::create();
    dst_repeat->append_value_multiple_times(*src, 1, 2);
    ASSERT_EQ(2, dst_repeat->size());

    const auto* typed_repeat = down_cast<const NullableColumn*>(dst_repeat->typed_column_by_index(0));
    ASSERT_TRUE(typed_repeat->get(0).is_null());
    ASSERT_TRUE(typed_repeat->get(1).is_null());
    const auto r_rep0 = dst_repeat->remain_value_column()->get(0).get_slice();
    const auto r_rep1 = dst_repeat->remain_value_column()->get(1).get_slice();
    ASSERT_EQ(2, static_cast<int8_t>(r_rep0.data[1]));
    ASSERT_EQ(2, static_cast<int8_t>(r_rep1.data[1]));
}

// Verifies appending source into non-empty destination preserves existing rows.
PARALLEL_TEST(VariantColumnTest, test_append_shredded_into_non_empty_row_preserves_existing_rows) {
    auto dst = VariantColumn::create();
    VariantRowValue kept = create_variant_row_from_json_text(R"({"keep":1})");
    dst->append(&kept);
    ASSERT_EQ(1, dst->size());

    auto src = build_single_path_bigint_shredded_variant("a", 10);

    dst->append(*src, 0, src->size());
    ASSERT_EQ(2, dst->size());
    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());

    assert_variant_row_json(dst.get(), 0, R"({"keep":1})");
    assert_variant_row_json(dst.get(), 1, R"({"a":10})");
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_append_schema_alignment) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 2);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10, 11}, {0, 0})); // a
    src_typed.emplace_back(build_nullable_int64_column({20, 21}, {0, 0})); // b
    src->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                              std::move(src_typed), std::move(src_metadata), std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({200}, {0})); // b
    dst_typed.emplace_back(build_nullable_int64_column({300}, {0})); // c
    dst->set_shredded_columns({"b", "c"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                              std::move(dst_typed), std::move(dst_metadata), std::move(dst_remain));

    dst->append(*src, 0, src->size());

    ASSERT_EQ(3, dst->size());
    ASSERT_EQ((std::vector<std::string>{"a", "b", "c"}), dst->shredded_paths());

    const auto* typed_a = down_cast<const NullableColumn*>(dst->typed_column_by_index(0));
    const auto* typed_b = down_cast<const NullableColumn*>(dst->typed_column_by_index(1));
    const auto* typed_c = down_cast<const NullableColumn*>(dst->typed_column_by_index(2));

    ASSERT_TRUE(typed_a->get(0).is_null());
    ASSERT_EQ(10, typed_a->get(1).get_int64());
    ASSERT_EQ(11, typed_a->get(2).get_int64());

    ASSERT_EQ(200, typed_b->get(0).get_int64());
    ASSERT_EQ(20, typed_b->get(1).get_int64());
    ASSERT_EQ(21, typed_b->get(2).get_int64());

    ASSERT_EQ(300, typed_c->get(0).get_int64());
    ASSERT_TRUE(typed_c->get(1).is_null());
    ASSERT_TRUE(typed_c->get(2).is_null());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_schema_alignment_type_conflict) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 2);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    ASSERT_FALSE(dst->align_schema_from(*src));
    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());
    ASSERT_EQ(1, dst->size());
}

PARALLEL_TEST(VariantColumnTest, test_append_type_conflict_numeric_widen_via_merger_arbitration) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({100000}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 2);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int16_column({12}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_SMALLINT)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append(*src, 0, src->size());

    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_BIGINT)}), dst->shredded_types());
    const auto* typed_a = down_cast<const NullableColumn*>(dst->typed_column_by_index(0));
    ASSERT_EQ(12, typed_a->get(0).get_int64());
    ASSERT_EQ(100000, typed_a->get(1).get_int64());
}

PARALLEL_TEST(VariantColumnTest, test_append_type_conflict_hoist_variant_via_merger_arbitration) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 2);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append(*src, 0, src->size());

    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), dst->shredded_types());
    assert_variant_row_json(dst.get(), 0, R"({"a":"x"})");
    assert_variant_row_json(dst.get(), 1, R"({"a":10})");
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_schema_accept_non_empty_path) {
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({10}, {0}));
    auto st = VariantColumn::validate_shredded_schema({"a"}, {TypeDescriptor(TYPE_BIGINT)}, typed, nullptr, nullptr);
    ASSERT_TRUE(st.ok());
}

// Verifies append on disjoint typed paths preserves destination schema while keeping row JSON semantics.
PARALLEL_TEST(VariantColumnTest, test_shredded_append_disjoint_typed_paths_preserves_schema_and_json_semantics) {
    auto src = VariantColumn::create();
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10, 11}, {0, 0}));
    src->set_shredded_columns({"b"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), nullptr, nullptr);

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_json_variant_row(dst_metadata.get(), dst_remain.get(), R"({"a":1})");
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({1}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append(*src, 0, src->size());

    ASSERT_EQ((std::vector<std::string>{"b", "a"}), dst->shredded_paths());
    ASSERT_EQ(3, dst->size());
    assert_variant_row_json(dst.get(), 0, R"({"a":1})");
    assert_variant_row_json(dst.get(), 1, R"({"b":10})");
    assert_variant_row_json(dst.get(), 2, R"({"b":11})");
}

// Verifies typed-only TYPE_VARIANT overlays on disjoint paths keep JSON semantics after append.
PARALLEL_TEST(VariantColumnTest, test_shredded_append_disjoint_variant_typed_path_preserves_row_json_semantics) {
    auto src = VariantColumn::create();
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_variant_column({R"({"x":1})", R"([1,2])"}, {0, 0}));
    src->set_shredded_columns({"b"}, {TypeDescriptor(TYPE_VARIANT)}, std::move(src_typed), nullptr, nullptr);

    VariantRowValue src_row0;
    VariantRowValue src_row1;
    const VariantRowValue* src_v0 = src->get_row_value(0, &src_row0);
    const VariantRowValue* src_v1 = src->get_row_value(1, &src_row1);
    ASSERT_NE(nullptr, src_v0);
    ASSERT_NE(nullptr, src_v1);
    auto src_json0 = src_v0->to_json();
    auto src_json1 = src_v1->to_json();
    ASSERT_TRUE(src_json0.ok());
    ASSERT_TRUE(src_json1.ok());

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_json_variant_row(dst_metadata.get(), dst_remain.get(), R"({"a":1})");
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({1}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append(*src, 0, src->size());

    ASSERT_EQ((std::vector<std::string>{"b", "a"}), dst->shredded_paths());
    ASSERT_EQ(3, dst->size());
    assert_variant_row_json(dst.get(), 0, R"({"a":1})");
    assert_variant_row_json(dst.get(), 1, src_json0.value());
    assert_variant_row_json(dst.get(), 2, src_json1.value());
}

// Verifies single-path typed-only scalar rows are materialized via typed column when metadata/remain are absent.
PARALLEL_TEST(VariantColumnTest, test_single_path_typed_only_scalar_materialization) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({777, 0}, {0, 1}));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    ASSERT_TRUE(col->is_typed_only_variant());
    ASSERT_EQ(1, col->shredded_paths().size());
    ASSERT_EQ("a", col->shredded_paths()[0]);
    ASSERT_EQ(1, col->typed_columns().size());
    ASSERT_FALSE(col->has_metadata_column());
    ASSERT_FALSE(col->has_remain_value());
    ASSERT_EQ(2, col->size());

    VariantRowValue row0;
    const VariantRowValue* v0 = col->get_row_value(0, &row0);
    ASSERT_NE(nullptr, v0);
    auto json0 = v0->to_json();
    ASSERT_TRUE(json0.ok());
    ASSERT_EQ(R"({"a":777})", json0.value());

    VariantRowValue row1;
    const VariantRowValue* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v1);
    auto json1 = v1->to_json();
    ASSERT_TRUE(json1.ok());
    ASSERT_EQ("null", json1.value());

    EXPECT_GT(col->serialize_size(0), 0);
    EXPECT_GT(col->serialize_size(1), 0);
}

// Verifies single-path typed-only supports const typed column row access (row index should map to 0 for const data).
PARALLEL_TEST(VariantColumnTest, test_single_path_typed_only_const_typed_column) {
    auto col = VariantColumn::create();
    auto typed_data = Int64Column::create();
    typed_data->append(42);
    MutableColumns typed;
    typed.emplace_back(ConstColumn::create(std::move(typed_data), 3));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    ASSERT_EQ(1, col->shredded_paths().size());
    ASSERT_EQ("a", col->shredded_paths()[0]);
    ASSERT_EQ(1, col->typed_columns().size());
    ASSERT_EQ(3, col->size());
    for (size_t row = 0; row < 3; ++row) {
        VariantRowValue row_buffer;
        const VariantRowValue* v = col->get_row_value(row, &row_buffer);
        ASSERT_NE(nullptr, v);
        auto json = v->to_json();
        ASSERT_TRUE(json.ok());
        ASSERT_EQ(R"({"a":42})", json.value());
    }
}

// Verifies single-path typed-only ARRAY rows can be materialized through get_row_value().
PARALLEL_TEST(VariantColumnTest, test_single_path_typed_only_array_materialization) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int_array_column({DatumArray{Datum(int64_t(1)), Datum(int64_t(2))}}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT))}, std::move(typed),
                              nullptr, nullptr);

    ASSERT_EQ(1, col->shredded_paths().size());
    ASSERT_EQ("a", col->shredded_paths()[0]);
    ASSERT_EQ(1, col->typed_columns().size());
    ASSERT_TRUE(col->is_typed_only_variant());

    VariantRowValue row;
    const VariantRowValue* v = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, v);
    auto json = v->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":[1,2]})", json.value());
}

// Verifies multi-key typed-only rows are materialized as root OBJECT without metadata/remain.
PARALLEL_TEST(VariantColumnTest, test_multi_key_typed_only_object_materialization) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({10}, {0}));
    typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    col->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_VARCHAR)}, std::move(typed),
                              nullptr, nullptr);

    ASSERT_TRUE(col->is_typed_only_variant());
    ASSERT_FALSE(col->shredded_paths().size() == 1 && col->shredded_paths()[0].empty());
    ASSERT_FALSE(col->has_metadata_column());
    ASSERT_FALSE(col->has_remain_value());

    VariantRowValue row;
    const VariantRowValue* v = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, v);
    auto json = v->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ("{\"a\":10,\"b\":\"x\"}", json.value());
}

// NOLINTNEXTLINE
PARALLEL_TEST(VariantColumnTest, test_shredded_append_type_conflict_hoist_variant) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append(*src, 0, 1);

    ASSERT_EQ(2, dst->size());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), dst->shredded_types());
    assert_variant_row_json(dst.get(), 0, R"({"a":"x"})");
    assert_variant_row_json(dst.get(), 1, R"({"a":10})");
}

// Verifies append_selective follows the same type arbitration behavior on conflict.
PARALLEL_TEST(VariantColumnTest, test_shredded_append_selective_type_conflict_hoist_variant) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    uint32_t indexes[] = {0};
    dst->append_selective(*src, indexes, 0, 1);

    ASSERT_EQ(2, dst->size());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), dst->shredded_types());
    assert_variant_row_json(dst.get(), 0, R"({"a":"x"})");
    assert_variant_row_json(dst.get(), 1, R"({"a":10})");
}

// Verifies appending typed-only source into base_shredded destination keeps typed values in typed columns
// and writes null-base payload for appended rows instead of row-wise materialization.
PARALLEL_TEST(VariantColumnTest, test_append_typed_only_into_base_shredded_fast_path) {
    verify_typed_only_into_base_shredded_fast_path(TypedOnlyIntoBaseShreddedAppendMode::kAppend);
}

// Verifies append_selective has the same typed-only -> base_shredded fast-path behavior.
PARALLEL_TEST(VariantColumnTest, test_append_selective_typed_only_into_base_shredded_fast_path) {
    verify_typed_only_into_base_shredded_fast_path(TypedOnlyIntoBaseShreddedAppendMode::kAppendSelective);
}

// Verifies append_value_multiple_times has the same typed-only -> base_shredded fast-path behavior.
PARALLEL_TEST(VariantColumnTest, test_append_value_multiple_times_typed_only_into_base_shredded_fast_path) {
    verify_typed_only_into_base_shredded_fast_path(TypedOnlyIntoBaseShreddedAppendMode::kAppendValueMultipleTimes);
}

// Verifies append_value_multiple_times follows the same type arbitration behavior on conflict.
PARALLEL_TEST(VariantColumnTest, test_shredded_append_value_multiple_times_type_conflict_hoist_variant) {
    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 9);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    dst->append_value_multiple_times(*src, 0, 2);

    ASSERT_EQ(3, dst->size());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), dst->shredded_types());
    assert_variant_row_json(dst.get(), 0, R"({"a":"x"})");
    assert_variant_row_json(dst.get(), 1, R"({"a":10})");
    assert_variant_row_json(dst.get(), 2, R"({"a":10})");
}

// Verifies base_shredded row reconstruction merges typed overlays with base remain payload.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_overrides_remain) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({"a":1,"b":2})");

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({99}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":99,"b":2})", json.value());
}

// Verifies typed NULL acts as tombstone and removes same-path key from materialized object.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_null_tombstone) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    // "a" is a typed path, so remain must only contain non-typed keys.
    append_json_variant_row(metadata.get(), remain.get(), R"({"b":2})");

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({0}, {1}));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"b":2})", json.value());
}

// Verifies typed TYPE_VARIANT overlays with independent metadata are re-encoded into destination base metadata.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_variant_overlay) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({"a":0,"b":2})");

    MutableColumns typed;
    typed.emplace_back(build_nullable_variant_column({R"({"x":1,"y":"z"})"}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor::create_variant_type()}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"x":1,"y":"z"},"b":2})", json.value());
}

// Verifies TYPE_VARIANT typed overlays remap by key-string (not raw dict-id) when source/target metadata namespaces differ.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_materialization_typed_variant_overlay_dict_namespace_remap) {
    auto col = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_json_variant_row(metadata.get(), remain.get(), R"({"a":0,"zzz":9,"k":2})");

    // Source typed variant has its own local metadata dictionary where "k" is field-id 0.
    // Target row metadata dictionary field-id 0 is for "a", so raw id reuse would be wrong.
    MutableColumns typed;
    typed.emplace_back(build_nullable_variant_column({R"({"k":1})"}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor::create_variant_type()}, std::move(typed), std::move(metadata),
                              std::move(remain));

    VariantRowValue row;
    const VariantRowValue* value = col->get_row_value(0, &row);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_TRUE(json.value() == R"({"a":{"k":1},"zzz":9,"k":2})" || json.value() == R"({"a":{"k":1},"k":2,"zzz":9})");
}

// Verifies duplicate shredded paths are rejected as invalid schema.
PARALLEL_TEST(VariantColumnTest, test_shredded_schema_reject_duplicate_paths) {
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({1}, {0}));
    typed.emplace_back(build_nullable_int64_column({2}, {0}));
    auto st = VariantColumn::validate_shredded_schema(
            {"a", "a"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)}, typed, nullptr, nullptr);
    ASSERT_FALSE(st.ok());
}

// Verifies malformed canonical shredded paths are rejected by schema validation.
PARALLEL_TEST(VariantColumnTest, test_shredded_schema_reject_invalid_paths) {
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({1}, {0}));
    auto st = VariantColumn::validate_shredded_schema({"a..b"}, {TypeDescriptor(TYPE_BIGINT)}, typed, nullptr, nullptr);
    ASSERT_FALSE(st.ok());
}

// Verifies typed_only -> base_shredded promotion keeps historical typed rows,
// and missing typed value on appended row falls back to remain payload.
PARALLEL_TEST(VariantColumnTest, test_typed_only_promotion_preserves_historical_rows_with_null_base) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({7, 8}, {0, 0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    ASSERT_TRUE(col->is_typed_only_variant());
    ASSERT_FALSE(col->has_metadata_column());
    ASSERT_FALSE(col->has_remain_value());

    VariantRowValue appended = create_variant_row_from_json_text(R"({"x":1})");
    col->append(&appended);

    ASSERT_TRUE(col->has_metadata_column());
    ASSERT_TRUE(col->has_remain_value());
    ASSERT_EQ(3, col->size());

    VariantRowValue row0;
    VariantRowValue row1;
    VariantRowValue row2;
    auto* v0 = col->get_row_value(0, &row0);
    auto* v1 = col->get_row_value(1, &row1);
    auto* v2 = col->get_row_value(2, &row2);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    ASSERT_NE(nullptr, v2);
    auto j0 = v0->to_json();
    auto j1 = v1->to_json();
    auto j2 = v2->to_json();
    ASSERT_TRUE(j0.ok());
    ASSERT_TRUE(j1.ok());
    ASSERT_TRUE(j2.ok());
    ASSERT_EQ(R"({"a":7})", j0.value());
    ASSERT_EQ(R"({"a":8})", j1.value());
    ASSERT_EQ(R"({"x":1})", j2.value());

    VariantRowValue null_base = VariantRowValue::from_null();
    std::string_view null_meta = null_base.get_metadata().raw();
    std::string_view null_value = null_base.get_value().raw();
    auto m0 = col->metadata_column()->get(0).get_slice();
    auto r0 = col->remain_value_column()->get(0).get_slice();
    auto m1 = col->metadata_column()->get(1).get_slice();
    auto r1 = col->remain_value_column()->get(1).get_slice();
    ASSERT_EQ(null_meta.size(), m0.size);
    ASSERT_EQ(null_value.size(), r0.size);
    ASSERT_EQ(null_meta.size(), m1.size);
    ASSERT_EQ(null_value.size(), r1.size);
    ASSERT_EQ(0, memcmp(null_meta.data(), m0.data, m0.size));
    ASSERT_EQ(0, memcmp(null_value.data(), r0.data, r0.size));
    ASSERT_EQ(0, memcmp(null_meta.data(), m1.data, m1.size));
    ASSERT_EQ(0, memcmp(null_value.data(), r1.data, r1.size));
}

// Verifies promotion keeps typed ownership for existing rows while appended rows can still use remain payload.
PARALLEL_TEST(VariantColumnTest, test_typed_only_promotion_new_rows_use_remain_payload) {
    auto col = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({42}, {0}));
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);

    VariantRowValue appended = create_variant_row_from_json_text(R"({"k":"v"})");
    col->append(&appended);

    ASSERT_EQ(2, col->size());
    const auto* typed_root = down_cast<const NullableColumn*>(col->typed_column_by_index(0));
    ASSERT_FALSE(typed_root->get(0).is_null());
    ASSERT_TRUE(typed_root->get(1).is_null());

    VariantRowValue row0;
    VariantRowValue row1;
    auto* v0 = col->get_row_value(0, &row0);
    auto* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    auto j0 = v0->to_json();
    auto j1 = v1->to_json();
    ASSERT_TRUE(j0.ok());
    ASSERT_TRUE(j1.ok());
    ASSERT_EQ(R"({"a":42})", j0.value());
    ASSERT_EQ(R"({"k":"v"})", j1.value());
}

// Verifies base_shredded rows with null metadata/remain can still materialize from typed overlays.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_null_metadata_or_remain_row_returns_null) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"x":1})");
    std::string metadata(base.get_metadata().raw());
    std::string value(base.get_value().raw());

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({7, 8}, {0, 0}));
    // Row 1 is a typed-only row promoted to base_shredded: both metadata and remain are null
    // sentinels (kEmptyMetadata + kEmptyValue). VariantBuilder handles null base + overlays by
    // creating a fresh object from the typed columns.
    VariantRowValue null_base = VariantRowValue::from_null();
    std::string null_metadata_str(null_base.get_metadata().raw());
    std::string null_remain_str(null_base.get_value().raw());
    auto metadata_col = BinaryColumn::create();
    metadata_col->append(metadata);          // row 0: actual metadata
    metadata_col->append(null_metadata_str); // row 1: null sentinel
    auto remain_col = BinaryColumn::create();
    remain_col->append(value);           // row 0
    remain_col->append(null_remain_str); // row 1: null sentinel (typed-only path)
    col->set_shredded_columns({"x"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata_col),
                              std::move(remain_col));

    VariantRowValue row0;
    VariantRowValue row1;
    const VariantRowValue* v0 = col->get_row_value(0, &row0);
    const VariantRowValue* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    auto j0 = v0->to_json();
    auto j1 = v1->to_json();
    ASSERT_TRUE(j0.ok());
    ASSERT_TRUE(j1.ok());
    ASSERT_EQ(R"({"x":7})", j0.value());
    ASSERT_EQ(R"({"x":8})", j1.value());
}

// Verifies base_shredded with const typed column uses row=0 typed value for all rows.
PARALLEL_TEST(VariantColumnTest, test_base_shredded_const_typed_column_materialization) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"a":0})");
    std::string metadata(base.get_metadata().raw());
    std::string value(base.get_value().raw());

    auto typed_data = Int64Column::create();
    typed_data->append(42);
    MutableColumns typed;
    typed.emplace_back(ConstColumn::create(std::move(typed_data), 2));
    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    metadata_col->append(metadata);
    metadata_col->append(metadata);
    remain_col->append(value);
    remain_col->append(value);
    col->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata_col),
                              std::move(remain_col));

    VariantRowValue row0;
    VariantRowValue row1;
    auto* v0 = col->get_row_value(0, &row0);
    auto* v1 = col->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    auto j0 = v0->to_json();
    auto j1 = v1->to_json();
    ASSERT_TRUE(j0.ok());
    ASSERT_TRUE(j1.ok());
    ASSERT_EQ(R"({"a":42})", j0.value());
    ASSERT_EQ(R"({"a":42})", j1.value());
}

PARALLEL_TEST(VariantColumnTest, test_try_get_row_ref_base_payload_only) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"k":"v"})");
    col->append(&base);

    VariantRowRef row_ref;
    ASSERT_TRUE(col->try_get_row_ref(0, &row_ref));
    EXPECT_EQ(base.get_metadata(), row_ref.get_metadata());
    EXPECT_EQ(base.get_value(), row_ref.get_value());
}

PARALLEL_TEST(VariantColumnTest, test_serialize_uses_row_ref_fast_path) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"k":"v"})");
    col->append(&base);

    VariantRowRef row_ref;
    ASSERT_TRUE(col->try_get_row_ref(0, &row_ref));

    std::vector<uint8_t> row_ref_buf(row_ref.serialize_size(), 0);
    std::vector<uint8_t> col_buf(col->serialize_size(0), 0);
    ASSERT_EQ(row_ref.serialize_size(), col->serialize_size(0));
    ASSERT_EQ(row_ref.serialize(row_ref_buf.data()), row_ref_buf.size());
    ASSERT_EQ(col->serialize(0, col_buf.data()), col_buf.size());
    EXPECT_EQ(row_ref_buf, col_buf);
}

PARALLEL_TEST(VariantColumnTest, test_put_mysql_row_buffer_row_ref_fast_path) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"k":"v"})");
    col->append(&base);

    MysqlRowBuffer buf;
    col->put_mysql_row_buffer(&buf, 0);
    ASSERT_EQ(10, buf.data().size());
    EXPECT_EQ(0x09, buf.data()[0]);
    EXPECT_EQ(R"({"k":"v"})", buf.data().substr(1));
}

PARALLEL_TEST(VariantColumnTest, test_debug_item_row_ref_fast_path) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"k":"v"})");
    col->append(&base);

    EXPECT_EQ(R"({"k":"v"})", col->debug_item(0));
}

PARALLEL_TEST(VariantColumnTest, test_try_get_row_ref_returns_false_when_typed_overlay_exists) {
    auto col = VariantColumn::create();
    VariantRowValue base = create_variant_row_from_json_text(R"({"x":1})");
    std::string metadata(base.get_metadata().raw());
    std::string value(base.get_value().raw());

    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({7}, {0}));
    auto metadata_col = BinaryColumn::create();
    auto remain_col = BinaryColumn::create();
    metadata_col->append(metadata);
    remain_col->append(value);
    col->set_shredded_columns({"x"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata_col),
                              std::move(remain_col));

    VariantRowRef row_ref;
    ASSERT_FALSE(col->try_get_row_ref(0, &row_ref));

    VariantRowValue materialized;
    const VariantRowValue* value_ptr = col->get_row_value(0, &materialized);
    ASSERT_NE(nullptr, value_ptr);
    auto json = value_ptr->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"x":7})", json.value());
}

PARALLEL_TEST(VariantColumnTest, test_equals_and_compare_at_row_ref_fast_path) {
    auto lhs = VariantColumn::create();
    auto rhs = VariantColumn::create();

    VariantRowValue v1 = create_variant_row_from_json_text(R"({"a":1})");
    VariantRowValue v2 = create_variant_row_from_json_text(R"({"a":2})");
    VariantRowValue v3 = create_variant_row_from_json_text(R"({"a":3})");

    lhs->append(&v1);
    lhs->append(&v2);
    rhs->append(&v1);
    rhs->append(&v3);

    ASSERT_EQ(1, lhs->equals(0, *rhs, 0, false));
    ASSERT_EQ(0, lhs->equals(1, *rhs, 1, false));

    ASSERT_EQ(0, lhs->compare_at(0, 0, *rhs, -1));
    int cmp = lhs->compare_at(1, 1, *rhs, -1);
    ASSERT_NE(0, cmp);
    ASSERT_EQ(-cmp, rhs->compare_at(1, 1, *lhs, -1));
}

PARALLEL_TEST(VariantColumnTest, test_equals_row_ref_fast_path_unsafe_null) {
    auto lhs = VariantColumn::create();
    auto rhs = VariantColumn::create();

    VariantRowValue null_row = VariantRowValue::from_null();
    lhs->append(&null_row);
    rhs->append(&null_row);

    ASSERT_EQ(-1, lhs->equals(0, *rhs, 0, false));
    ASSERT_EQ(1, lhs->equals(0, *rhs, 0, true));
}

PARALLEL_TEST(VariantColumnTest, test_equals_and_compare_at_fallback_materialized_row) {
    auto build_overlay_column = [](int64_t typed_value) {
        auto col = VariantColumn::create();
        VariantRowValue base = create_variant_row_from_json_text(R"({"x":1})");
        std::string metadata(base.get_metadata().raw());
        std::string value(base.get_value().raw());

        MutableColumns typed;
        typed.emplace_back(build_nullable_int64_column({typed_value}, {0}));
        auto metadata_col = BinaryColumn::create();
        auto remain_col = BinaryColumn::create();
        metadata_col->append(metadata);
        remain_col->append(value);
        col->set_shredded_columns({"x"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata_col),
                                  std::move(remain_col));
        return col;
    };

    auto lhs = build_overlay_column(10);
    auto rhs_equal = build_overlay_column(10);
    auto rhs_diff = build_overlay_column(11);

    VariantRowRef row_ref;
    ASSERT_FALSE(lhs->try_get_row_ref(0, &row_ref));

    ASSERT_EQ(1, lhs->equals(0, *rhs_equal, 0, false));
    ASSERT_EQ(0, lhs->compare_at(0, 0, *rhs_equal, -1));

    ASSERT_EQ(0, lhs->equals(0, *rhs_diff, 0, false));
    int cmp = lhs->compare_at(0, 0, *rhs_diff, -1);
    ASSERT_NE(0, cmp);
}

PARALLEL_TEST(VariantColumnTest, test_equals_and_compare_at_semantic_numeric_across_encodings) {
    auto lhs = VariantColumn::create();
    auto rhs = VariantColumn::create();

    const uint8_t int8_chars[] = {primitive_header(VariantType::INT8), 0x02};
    const uint8_t int16_chars[] = {primitive_header(VariantType::INT16), 0x02, 0x00};
    VariantRowValue v_int8(VariantMetadata::kEmptyMetadata,
                           std::string_view(reinterpret_cast<const char*>(int8_chars), sizeof(int8_chars)));
    VariantRowValue v_int16(VariantMetadata::kEmptyMetadata,
                            std::string_view(reinterpret_cast<const char*>(int16_chars), sizeof(int16_chars)));

    lhs->append(&v_int8);
    rhs->append(&v_int16);

    ASSERT_EQ(1, lhs->equals(0, *rhs, 0, false));
    ASSERT_EQ(0, lhs->compare_at(0, 0, *rhs, -1));
}

PARALLEL_TEST(VariantColumnTest, test_equals_and_compare_at_invalid_metadata_fallback_stable) {
    auto lhs = VariantColumn::create();
    auto rhs = VariantColumn::create();

    std::string invalid_type_one;
    std::string invalid_type_two;
    invalid_type_one.push_back(static_cast<char>((63u << VariantValue::kValueHeaderBitShift)));
    invalid_type_one.push_back(static_cast<char>(1));
    invalid_type_two.push_back(static_cast<char>((63u << VariantValue::kValueHeaderBitShift)));
    invalid_type_two.push_back(static_cast<char>(2));
    VariantRowValue v1(VariantMetadata::kEmptyMetadata, invalid_type_one);
    VariantRowValue v2(VariantMetadata::kEmptyMetadata, invalid_type_two);

    lhs->append(&v1);
    rhs->append(&v2);

    ASSERT_EQ(0, lhs->equals(0, *rhs, 0, false));
    ASSERT_LT(lhs->compare_at(0, 0, *rhs, -1), 0);
    ASSERT_GT(rhs->compare_at(0, 0, *lhs, -1), 0);
}

} // namespace starrocks

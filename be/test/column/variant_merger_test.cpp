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

#include "column/variant_merger.h"

#include <gtest/gtest.h>

#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/decimalv3_column.h"
#include "column/nullable_column.h"
#include "column/variant_column.h"
#include "column/variant_encoder.h"
#include "gutil/casts.h"
#include "types/decimalv2_value.h"

namespace starrocks {

static uint8_t primitive_header(VariantType type) {
    return static_cast<uint8_t>(type) << 2;
}

static void append_primitive_int8_row(BinaryColumn* metadata, BinaryColumn* remain, int8_t value) {
    const std::string metadata_bytes(VariantMetadata::kEmptyMetadata);
    const char payload[2] = {static_cast<char>(primitive_header(VariantType::INT8)), static_cast<char>(value)};
    metadata->append(Slice(metadata_bytes));
    remain->append(Slice(payload, sizeof(payload)));
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

static MutableColumnPtr build_nullable_root_int64_typed_only_column(const std::vector<int64_t>& values,
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

static MutableColumnPtr build_nullable_decimalv2_column(const std::vector<DecimalV2Value>& values,
                                                        const std::vector<uint8_t>& is_null) {
    auto data = DecimalColumn::create();
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_decimal32_column(const std::vector<int32_t>& values,
                                                        const std::vector<uint8_t>& is_null, int precision, int scale) {
    auto data = Decimal32Column::create(precision, scale);
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_decimal64_column(const std::vector<int64_t>& values,
                                                        const std::vector<uint8_t>& is_null, int precision, int scale) {
    auto data = Decimal64Column::create(precision, scale);
    auto null = NullColumn::create();
    DCHECK_EQ(values.size(), is_null.size());
    for (size_t i = 0; i < values.size(); ++i) {
        data->append(values[i]);
        null->append(is_null[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null));
}

static MutableColumnPtr build_nullable_array_int64_column(const std::vector<std::vector<int64_t>>& rows,
                                                          const std::vector<uint8_t>& row_is_null) {
    DCHECK_EQ(rows.size(), row_is_null.size());
    auto elem_data = Int64Column::create();
    auto elem_null = NullColumn::create();
    auto offsets = UInt32Column::create();
    offsets->append(0);
    uint32_t offset = 0;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (!row_is_null[i]) {
            for (int64_t v : rows[i]) {
                elem_data->append(v);
                elem_null->append(0);
                ++offset;
            }
        }
        offsets->append(offset);
    }
    auto row_null = NullColumn::create();
    for (uint8_t v : row_is_null) {
        row_null->append(v);
    }
    auto elements = NullableColumn::create(std::move(elem_data), std::move(elem_null));
    auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
    return NullableColumn::create(std::move(array), std::move(row_null));
}

static MutableColumnPtr build_nullable_array_double_column(const std::vector<std::vector<double>>& rows,
                                                           const std::vector<uint8_t>& row_is_null) {
    DCHECK_EQ(rows.size(), row_is_null.size());
    auto elem_data = DoubleColumn::create();
    auto elem_null = NullColumn::create();
    auto offsets = UInt32Column::create();
    offsets->append(0);
    uint32_t offset = 0;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (!row_is_null[i]) {
            for (double v : rows[i]) {
                elem_data->append(v);
                elem_null->append(0);
                ++offset;
            }
        }
        offsets->append(offset);
    }
    auto row_null = NullColumn::create();
    for (uint8_t v : row_is_null) {
        row_null->append(v);
    }
    auto elements = NullableColumn::create(std::move(elem_data), std::move(elem_null));
    auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
    return NullableColumn::create(std::move(array), std::move(row_null));
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

PARALLEL_TEST(VariantMergerTest, merge_shredded_schema_union) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src0_typed.emplace_back(build_nullable_int64_column({20}, {0}));
    src0->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                               std::move(src0_typed), std::move(src0_metadata), std::move(src0_remain));

    auto src1 = VariantColumn::create();
    auto src1_metadata = BinaryColumn::create();
    auto src1_remain = BinaryColumn::create();
    append_primitive_int8_row(src1_metadata.get(), src1_remain.get(), 2);
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_int64_column({200}, {0}));
    src1_typed.emplace_back(build_nullable_int64_column({300}, {0}));
    src1->set_shredded_columns({"b", "c"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                               std::move(src1_typed), std::move(src1_metadata), std::move(src1_remain));

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ((std::vector<std::string>{"b", "c", "a"}), merged->shredded_paths());

    const auto* typed_b = down_cast<const NullableColumn*>(merged->typed_column_by_index(0));
    const auto* typed_c = down_cast<const NullableColumn*>(merged->typed_column_by_index(1));
    const auto* typed_a = down_cast<const NullableColumn*>(merged->typed_column_by_index(2));
    ASSERT_EQ(20, typed_b->get(0).get_int64());
    ASSERT_EQ(200, typed_b->get(1).get_int64());
    ASSERT_TRUE(typed_c->get(0).is_null());
    ASSERT_EQ(300, typed_c->get(1).get_int64());
    ASSERT_EQ(10, typed_a->get(0).get_int64());
    ASSERT_TRUE(typed_a->get(1).is_null());
}

PARALLEL_TEST(VariantMergerTest, merge_shredded_type_conflict_hoist_variant) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src0->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src0_typed), std::move(src0_metadata),
                               std::move(src0_remain));

    auto src1 = VariantColumn::create();
    auto src1_metadata = BinaryColumn::create();
    auto src1_remain = BinaryColumn::create();
    append_primitive_int8_row(src1_metadata.get(), src1_remain.get(), 2);
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    src1->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(src1_typed), std::move(src1_metadata),
                               std::move(src1_remain));

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), merged->shredded_types());
    assert_variant_row_json(merged, 0, R"({"a":10})");
    assert_variant_row_json(merged, 1, R"({"a":"x"})");
}

PARALLEL_TEST(VariantMergerTest, merge_shredded_numeric_widen) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src0->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src0_typed), std::move(src0_metadata),
                               std::move(src0_remain));

    auto src2 = VariantColumn::create();
    auto src2_metadata = BinaryColumn::create();
    auto src2_remain = BinaryColumn::create();
    append_primitive_int8_row(src2_metadata.get(), src2_remain.get(), 3);
    MutableColumns src2_typed;
    auto data = DoubleColumn::create();
    auto null = NullColumn::create();
    data->append(30.5);
    null->append(0);
    src2_typed.emplace_back(NullableColumn::create(std::move(data), std::move(null)));
    src2->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_DOUBLE)}, std::move(src2_typed), std::move(src2_metadata),
                               std::move(src2_remain));

    Columns inputs{src0, src2};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_DOUBLE)}), merged->shredded_types());

    const auto* typed_a = down_cast<const NullableColumn*>(merged->typed_column_by_index(0));
    ASSERT_DOUBLE_EQ(10.0, typed_a->get(0).get_double());
    ASSERT_DOUBLE_EQ(30.5, typed_a->get(1).get_double());
}

PARALLEL_TEST(VariantMergerTest, merge_shredded_array_conflict_hoist_variant) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    TypeDescriptor src0_array_type(TYPE_ARRAY);
    src0_array_type.children.emplace_back(TypeDescriptor(TYPE_BIGINT));
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_array_int64_column({{10, 20}}, {0}));
    src0->set_shredded_columns({"a"}, {src0_array_type}, std::move(src0_typed), std::move(src0_metadata),
                               std::move(src0_remain));

    auto src1 = VariantColumn::create();
    auto src1_metadata = BinaryColumn::create();
    auto src1_remain = BinaryColumn::create();
    append_primitive_int8_row(src1_metadata.get(), src1_remain.get(), 2);
    TypeDescriptor src1_array_type(TYPE_ARRAY);
    src1_array_type.children.emplace_back(TypeDescriptor(TYPE_DOUBLE));
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_array_double_column({{30.5}}, {0}));
    src1->set_shredded_columns({"a"}, {src1_array_type}, std::move(src1_typed), std::move(src1_metadata),
                               std::move(src1_remain));

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), merged->shredded_types());
    assert_variant_row_json(merged, 0, R"({"a":[10,20]})");
    assert_variant_row_json(merged, 1, R"({"a":[30.5]})");
}

PARALLEL_TEST(VariantMergerTest, merge_shredded_array_nonarray_conflict_hoist_variant) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    TypeDescriptor src0_array_type(TYPE_ARRAY);
    src0_array_type.children.emplace_back(TypeDescriptor(TYPE_BIGINT));
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_array_int64_column({{10, 20}}, {0}));
    src0->set_shredded_columns({"a"}, {src0_array_type}, std::move(src0_typed), std::move(src0_metadata),
                               std::move(src0_remain));

    auto src1 = VariantColumn::create();
    auto src1_metadata = BinaryColumn::create();
    auto src1_remain = BinaryColumn::create();
    append_primitive_int8_row(src1_metadata.get(), src1_remain.get(), 2);
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_int64_column({7}, {0}));
    src1->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src1_typed), std::move(src1_metadata),
                               std::move(src1_remain));

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), merged->shredded_types());
    assert_variant_row_json(merged, 0, R"({"a":[10,20]})");
    assert_variant_row_json(merged, 1, R"({"a":7})");
}

PARALLEL_TEST(VariantMergerTest, merge_invalid_input_fail) {
    Columns inputs;
    inputs.emplace_back(Int64Column::create());
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_FALSE(merged_status.ok());
    ASSERT_TRUE(merged_status.status().is_invalid_argument());
}

PARALLEL_TEST(VariantMergerTest, merge_into_row_dst_with_existing_rows_and_shredded_src_ok) {
    auto dst = VariantColumn::create();
    auto row = VariantEncoder::encode_json_text_to_variant(R"({"keep":1})");
    ASSERT_TRUE(row.ok());
    dst->append(&row.value());

    auto src = build_single_path_bigint_shredded_variant("a", 10);

    auto st = VariantMerger::merge_into(dst.get(), *down_cast<const VariantColumn*>(src.get()));
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(2, dst->size());
    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());

    assert_variant_row_json(dst.get(), 0, R"({"keep":1})");
    assert_variant_row_json(dst.get(), 1, R"({"a":10})");
}

PARALLEL_TEST(VariantMergerTest, merge_into_empty_row_dst_with_shredded_src_ok) {
    auto dst = VariantColumn::create();

    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 1);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto st = VariantMerger::merge_into(dst.get(), *down_cast<const VariantColumn*>(src.get()));
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, dst->size());
    ASSERT_EQ((std::vector<std::string>{"a"}), dst->shredded_paths());

    const auto* typed_a = down_cast<const NullableColumn*>(dst->typed_column_by_index(0));
    ASSERT_EQ(10, typed_a->get(0).get_int64());
}

PARALLEL_TEST(VariantMergerTest, merge_shredded_decimal_conflict_hoist_variant) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_decimalv2_column({DecimalV2Value(10)}, {0}));
    src0->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_DECIMALV2)}, std::move(src0_typed), std::move(src0_metadata),
                               std::move(src0_remain));

    auto src1 = VariantColumn::create();
    auto src1_metadata = BinaryColumn::create();
    auto src1_remain = BinaryColumn::create();
    append_primitive_int8_row(src1_metadata.get(), src1_remain.get(), 2);
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_varchar_column({"x"}, {0}));
    src1->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_VARCHAR)}, std::move(src1_typed), std::move(src1_metadata),
                               std::move(src1_remain));

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), merged->shredded_types());
}

PARALLEL_TEST(VariantMergerTest, merge_shredded_decimalv3_widen_same_scale) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_decimal32_column({12345}, {0}, 9, 2));
    src0->set_shredded_columns({"a"}, {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2)},
                               std::move(src0_typed), std::move(src0_metadata), std::move(src0_remain));

    auto src1 = VariantColumn::create();
    auto src1_metadata = BinaryColumn::create();
    auto src1_remain = BinaryColumn::create();
    append_primitive_int8_row(src1_metadata.get(), src1_remain.get(), 2);
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_decimal64_column({67890}, {0}, 18, 2));
    src1->set_shredded_columns({"a"}, {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2)},
                               std::move(src1_typed), std::move(src1_metadata), std::move(src1_remain));

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2)}),
              merged->shredded_types());

    const auto* typed_a = down_cast<const NullableColumn*>(merged->typed_column_by_index(0));
    ASSERT_EQ(typed_a->get(0).get_int64(), 12345);
    ASSERT_EQ(typed_a->get(1).get_int64(), 67890);
}

PARALLEL_TEST(VariantMergerTest, merge_shredded_decimalv3_scale_mismatch_hoist_variant) {
    auto src0 = VariantColumn::create();
    auto src0_metadata = BinaryColumn::create();
    auto src0_remain = BinaryColumn::create();
    append_primitive_int8_row(src0_metadata.get(), src0_remain.get(), 1);
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_decimal32_column({12345}, {0}, 9, 2));
    src0->set_shredded_columns({"a"}, {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2)},
                               std::move(src0_typed), std::move(src0_metadata), std::move(src0_remain));

    auto src1 = VariantColumn::create();
    auto src1_metadata = BinaryColumn::create();
    auto src1_remain = BinaryColumn::create();
    append_primitive_int8_row(src1_metadata.get(), src1_remain.get(), 2);
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_decimal64_column({67890}, {0}, 18, 3));
    src1->set_shredded_columns({"a"}, {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 3)},
                               std::move(src1_typed), std::move(src1_metadata), std::move(src1_remain));

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARIANT)}), merged->shredded_types());
}

PARALLEL_TEST(VariantMergerTest, merge_root_typed_only_with_base_shredded_ok) {
    auto root_typed_only = VariantColumn::create();
    MutableColumns root_typed_only_typed;
    root_typed_only_typed.emplace_back(build_nullable_root_int64_typed_only_column({7}, {0}));
    root_typed_only->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(root_typed_only_typed),
                                          nullptr, nullptr);

    auto base_shredded = VariantColumn::create();
    auto full_metadata = BinaryColumn::create();
    auto full_remain = BinaryColumn::create();
    append_primitive_int8_row(full_metadata.get(), full_remain.get(), 1);
    MutableColumns full_typed;
    full_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    base_shredded->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(full_typed),
                                        std::move(full_metadata), std::move(full_remain));

    Columns inputs{root_typed_only, base_shredded};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_TRUE(merged->has_metadata_column());
    ASSERT_TRUE(merged->has_remain_value());

    VariantRowValue row0;
    VariantRowValue row1;
    const VariantRowValue* v0 = merged->get_row_value(0, &row0);
    const VariantRowValue* v1 = merged->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    auto json0 = v0->to_json();
    auto json1 = v1->to_json();
    ASSERT_TRUE(json0.ok());
    ASSERT_TRUE(json1.ok());
    ASSERT_EQ(R"({"a":7})", json0.value());
    ASSERT_EQ(R"({"a":10})", json1.value());
}

PARALLEL_TEST(VariantMergerTest, merge_root_typed_only_with_root_typed_only_ok) {
    auto src0 = VariantColumn::create();
    MutableColumns src0_typed;
    src0_typed.emplace_back(build_nullable_root_int64_typed_only_column({7}, {0}));
    src0->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src0_typed), nullptr, nullptr);

    auto src1 = VariantColumn::create();
    MutableColumns src1_typed;
    src1_typed.emplace_back(build_nullable_root_int64_typed_only_column({8}, {0}));
    src1->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src1_typed), nullptr, nullptr);

    Columns inputs{src0, src1};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_TRUE(merged->has_metadata_column());
    ASSERT_TRUE(merged->has_remain_value());

    VariantRowValue row0;
    VariantRowValue row1;
    const VariantRowValue* v0 = merged->get_row_value(0, &row0);
    const VariantRowValue* v1 = merged->get_row_value(1, &row1);
    ASSERT_NE(nullptr, v0);
    ASSERT_NE(nullptr, v1);
    auto json0 = v0->to_json();
    auto json1 = v1->to_json();
    ASSERT_TRUE(json0.ok());
    ASSERT_TRUE(json1.ok());
    ASSERT_EQ(R"({"a":7})", json0.value());
    ASSERT_EQ(R"({"a":8})", json1.value());
}

PARALLEL_TEST(VariantMergerTest, merge_prefers_shredded_even_when_row_input_comes_first) {
    auto row_src = VariantColumn::create();
    row_src->append_default();

    auto shredded_src = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    append_primitive_int8_row(metadata.get(), remain.get(), 1);
    MutableColumns typed;
    typed.emplace_back(build_nullable_int64_column({42}, {0}));
    shredded_src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                                       std::move(remain));

    Columns inputs{row_src, shredded_src};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_TRUE(merged_status.ok()) << merged_status.status().to_string();

    auto* merged = down_cast<VariantColumn*>(merged_status.value().get());
    ASSERT_EQ(2, merged->size());
    ASSERT_EQ((std::vector<std::string>{"a"}), merged->shredded_paths());
    ASSERT_EQ((std::vector<TypeDescriptor>{TypeDescriptor(TYPE_BIGINT)}), merged->shredded_types());
}

PARALLEL_TEST(VariantMergerTest, merge_rejects_duplicate_shredded_paths) {
    auto bad_src = VariantColumn::create();
    auto bad_metadata = BinaryColumn::create();
    auto bad_remain = BinaryColumn::create();
    append_primitive_int8_row(bad_metadata.get(), bad_remain.get(), 1);
    MutableColumns bad_typed;
    bad_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    bad_typed.emplace_back(build_nullable_int64_column({20}, {0}));
    bad_src->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                                  std::move(bad_typed), std::move(bad_metadata), std::move(bad_remain));

    auto ok_src = VariantColumn::create();
    auto ok_metadata = BinaryColumn::create();
    auto ok_remain = BinaryColumn::create();
    append_primitive_int8_row(ok_metadata.get(), ok_remain.get(), 2);
    MutableColumns ok_typed;
    ok_typed.emplace_back(build_nullable_int64_column({30}, {0}));
    ok_src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(ok_typed), std::move(ok_metadata),
                                 std::move(ok_remain));

    // Simulate corrupted/non-conforming input and verify merger rejects it explicitly.
    auto& bad_paths = const_cast<std::vector<std::string>&>(bad_src->shredded_paths());
    bad_paths[1] = "a";

    Columns inputs{bad_src, ok_src};
    auto merged_status = VariantMerger::merge(inputs);
    ASSERT_FALSE(merged_status.ok());
    ASSERT_TRUE(merged_status.status().is_invalid_argument());
}

PARALLEL_TEST(VariantMergerTest, merge_into_rejects_duplicate_paths_in_src) {
    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 1);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    dst->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(dst_typed), std::move(dst_metadata),
                              std::move(dst_remain));

    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 2);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({20}, {0}));
    src_typed.emplace_back(build_nullable_int64_column({30}, {0}));
    src->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                              std::move(src_typed), std::move(src_metadata), std::move(src_remain));
    auto& src_paths = const_cast<std::vector<std::string>&>(src->shredded_paths());
    src_paths[1] = "a";

    auto st = VariantMerger::merge_into(dst.get(), *down_cast<const VariantColumn*>(src.get()));
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_not_supported() || st.is_invalid_argument());
}

PARALLEL_TEST(VariantMergerTest, merge_into_rejects_duplicate_paths_in_dst) {
    auto dst = VariantColumn::create();
    auto dst_metadata = BinaryColumn::create();
    auto dst_remain = BinaryColumn::create();
    append_primitive_int8_row(dst_metadata.get(), dst_remain.get(), 1);
    MutableColumns dst_typed;
    dst_typed.emplace_back(build_nullable_int64_column({10}, {0}));
    dst_typed.emplace_back(build_nullable_int64_column({11}, {0}));
    dst->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT)},
                              std::move(dst_typed), std::move(dst_metadata), std::move(dst_remain));
    auto& dst_paths = const_cast<std::vector<std::string>&>(dst->shredded_paths());
    dst_paths[1] = "a";

    auto src = VariantColumn::create();
    auto src_metadata = BinaryColumn::create();
    auto src_remain = BinaryColumn::create();
    append_primitive_int8_row(src_metadata.get(), src_remain.get(), 2);
    MutableColumns src_typed;
    src_typed.emplace_back(build_nullable_int64_column({20}, {0}));
    src->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(src_typed), std::move(src_metadata),
                              std::move(src_remain));

    auto st = VariantMerger::merge_into(dst.get(), *down_cast<const VariantColumn*>(src.get()));
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument());
}

} // namespace starrocks

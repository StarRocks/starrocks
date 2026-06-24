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

#include <cstdio>
#include <limits>
#include <type_traits>

#include "base/coding.h"
#include "base/failpoint/fail_point.h"
#include "base/hash/hash_std.hpp"
#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/chunk_extra_data.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "column/variant_column.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "serde/column_array_serde.h"
#include "serde/encode_level.h"
#include "serde/protobuf_serde.h"
#include "types/hll.h"
#include "types/json_value.h"
#include "types/type_descriptor.h"
#include "types/variant.h"

#ifdef FIU_ENABLE
namespace starrocks {
DECLARE_FAIL_POINT(mem_chunk_allocator_allocate_fail);
}
#endif

namespace starrocks::serde {

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, json_column) {
    auto c1 = JsonColumn::create();

    ASSERT_EQ(8, ColumnArraySerde::max_serialized_size(*c1));

    for (int i = 0; i < 10; i++) {
        JsonValue json;
        std::string json_str = strings::Substitute("{\"a\": $0}", i);
        ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());
        c1->append(&json);
    }

    ASSERT_EQ(148, ColumnArraySerde::max_serialized_size(*c1));

    auto c2 = JsonColumn::create();

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_EQ(10, c2->size());
    for (size_t i = 0; i < c1->size(); i++) {
        const JsonValue* datum1 = c1->get(i).get_json();
        const JsonValue* datum2 = c2->get(i).get_json();
        std::string str1 = datum1->to_string().value();
        std::string str2 = datum2->to_string().value();
        ASSERT_EQ(str1, str2);
        ASSERT_EQ(0, datum1->compare(*datum2));
    }

    // no effect
    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1), level);
        const auto* end = buffer.data() + buffer.size();
        ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        ASSERT_EQ(buffer.data() + buffer.size(), p1);
        ASSERT_EQ(buffer.data() + buffer.size(), p2);

        ASSERT_EQ(10, c2->size());
        for (size_t i = 0; i < c1->size(); i++) {
            const JsonValue* datum1 = c1->get(i).get_json();
            const JsonValue* datum2 = c2->get(i).get_json();
            std::string str1 = datum1->to_string().value();
            std::string str2 = datum2->to_string().value();
            ASSERT_EQ(str1, str2);
            ASSERT_EQ(0, datum1->compare(*datum2));
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, variant_column) {
    auto c1 = VariantColumn::create();

    auto primitive_header = [](VariantType type) { return (static_cast<uint8_t>(type) << 2); };

    // Prepare 5 int8 variant values
    const uint8_t int8_values[][2] = {
            {primitive_header(VariantType::INT8), 0x01}, // 1
            {primitive_header(VariantType::INT8), 0x02}, // 2
            {primitive_header(VariantType::INT8), 0x03}, // 3
            {primitive_header(VariantType::INT8), 0x04}, // 4
            {primitive_header(VariantType::INT8), 0x05}, // 5
    };
    for (size_t i = 0; i < std::size(int8_values); ++i) {
        std::string_view value(reinterpret_cast<const char*>(int8_values[i]), sizeof(int8_values[i]));
        VariantRowValue variant(VariantMetadata::kEmptyMetadata, value);
        c1->append(&variant);
    }

    auto c2 = VariantColumn::create();
    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_EQ(5, c2->size());
    for (size_t i = 0; i < c1->size(); i++) {
        VariantRowValue row1;
        VariantRowValue row2;
        const VariantRowValue* datum1 = c1->get_row_value(i, &row1);
        const VariantRowValue* datum2 = c2->get_row_value(i, &row2);
        ASSERT_NE(nullptr, datum1);
        ASSERT_NE(nullptr, datum2);
        ASSERT_EQ(datum1->serialize_size(), datum2->serialize_size());
        ASSERT_EQ(datum1->get_metadata(), datum2->get_metadata());
        ASSERT_EQ(datum1->get_value(), datum2->get_value());
        EXPECT_EQ(datum1->to_string(), datum2->to_string());
    }

    // no effect
    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1), level);
        const auto* end = buffer.data() + buffer.size();
        ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        ASSERT_EQ(buffer.data() + buffer.size(), p1);
        ASSERT_EQ(buffer.data() + buffer.size(), p2);

        ASSERT_EQ(5, c2->size());
        for (size_t i = 0; i < c1->size(); i++) {
            VariantRowValue row1;
            VariantRowValue row2;
            const VariantRowValue* datum1 = c1->get_row_value(i, &row1);
            const VariantRowValue* datum2 = c2->get_row_value(i, &row2);
            ASSERT_NE(nullptr, datum1);
            ASSERT_NE(nullptr, datum2);
            ASSERT_EQ(datum1->serialize_size(), datum2->serialize_size());
            ASSERT_EQ(datum1->get_metadata(), datum2->get_metadata());
            ASSERT_EQ(datum1->get_value(), datum2->get_value());
            EXPECT_EQ(datum1->to_string(), datum2->to_string());
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_shredded_complex_type_descriptor_roundtrip) {
    auto c1 = VariantColumn::create();
    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"k"}, {TYPE_INT_DESC});
    struct_type.field_ids = {7};
    struct_type.field_physical_names = {"k_phys"};

    MutableColumns typed;
    typed.emplace_back(ColumnHelper::create_column(struct_type, true));
    c1->set_shredded_columns({"obj"}, {struct_type}, std::move(typed), nullptr, nullptr);
    ASSERT_EQ(0, c1->size());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    auto c2 = VariantColumn::create();
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), buffer.data() + buffer.size(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_EQ(1u, c2->shredded_paths().size());
    ASSERT_EQ("obj", c2->shredded_paths()[0]);
    ASSERT_EQ(1u, c2->shredded_types().size());
    const TypeDescriptor& out = c2->shredded_types()[0];
    ASSERT_EQ(TYPE_STRUCT, out.type);
    ASSERT_EQ(1u, out.children.size());
    ASSERT_EQ(TYPE_INT, out.children[0].type);
    ASSERT_EQ(1u, out.field_names.size());
    ASSERT_EQ("k", out.field_names[0]);
    ASSERT_EQ(1u, out.field_ids.size());
    ASSERT_EQ(7, out.field_ids[0]);
    ASSERT_EQ(1u, out.field_physical_names.size());
    ASSERT_EQ("k_phys", out.field_physical_names[0]);
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_shredded_base_only_without_typed_columns) {
    auto c1 = VariantColumn::create();
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();

    metadata->append(Slice("m0", 2));
    metadata->append(Slice("m1", 2));
    remain->append(Slice("r0", 2));
    remain->append(Slice("r1", 2));
    c1->set_shredded_columns({}, {}, {}, std::move(metadata), std::move(remain));
    ASSERT_EQ(2, c1->size());
    ASSERT_TRUE(c1->typed_columns().empty());
    ASSERT_TRUE(c1->has_metadata_column());
    ASSERT_TRUE(c1->has_remain_value());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    auto c2 = VariantColumn::create();
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), buffer.data() + buffer.size(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_EQ(2, c2->size());
    ASSERT_TRUE(c2->typed_columns().empty());
    ASSERT_TRUE(c2->has_metadata_column());
    ASSERT_TRUE(c2->has_remain_value());
    ASSERT_TRUE(c2->shredded_paths().empty());
    ASSERT_TRUE(c2->shredded_types().empty());
    for (int i = 0; i < 2; ++i) {
        ASSERT_EQ(c1->metadata_column()->get(i).get_slice(), c2->metadata_column()->get(i).get_slice());
        ASSERT_EQ(c1->remain_value_column()->get(i).get_slice(), c2->remain_value_column()->get(i).get_slice());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_shredded_count_mismatch_corruption) {
    auto c1 = VariantColumn::create();
    MutableColumns typed;
    typed.emplace_back(ColumnHelper::create_column(TYPE_BIGINT_DESC, true));
    c1->set_shredded_columns({"a"}, {TYPE_BIGINT_DESC}, std::move(typed), nullptr, nullptr);

    auto write_u32 = [](uint8_t* p, uint32_t v) { encode_fixed32_le(p, v); };
    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    // Layout for one short path "a":
    // [num_paths:4][path_len:4][path:1][num_types:4][type_desc:32][num_typed_cols:4]...
    constexpr size_t kNumTypesOffset = 4 + 4 + 1;
    constexpr size_t kNumTypedColsOffset = kNumTypesOffset + 4 + 32;

    // num_types mismatch
    {
        auto corrupted = buffer;
        write_u32(corrupted.data() + kNumTypesOffset, 0);
        auto c2 = VariantColumn::create();
        auto st = ColumnArraySerde::deserialize(corrupted.data(), corrupted.data() + corrupted.size(), c2.get());
        ASSERT_FALSE(st.ok());
        ASSERT_TRUE(st.status().is_corruption());
    }

    // num_typed_cols mismatch
    {
        auto corrupted = buffer;
        write_u32(corrupted.data() + kNumTypedColsOffset, 0);
        auto c2 = VariantColumn::create();
        auto st = ColumnArraySerde::deserialize(corrupted.data(), corrupted.data() + corrupted.size(), c2.get());
        ASSERT_FALSE(st.ok());
        ASSERT_TRUE(st.status().is_corruption());
    }
}

#if !DCHECK_IS_ON()
// We have DCHECK inside VariantColumn deserialize to check version,
// so this test case is only enabled when DCHECK is off.

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_failed_deserialize) {
    auto c1 = VariantColumn::create();

    // Prepare a variant value with an unsupported version
    constexpr uint8_t v2_metadata_charts[] = {0x02, 0x00, 0x00};
    const std::string_view v2_metadata(reinterpret_cast<const char*>(v2_metadata_charts), sizeof(v2_metadata_charts));
    const VariantRowValue variant(v2_metadata, "");
    c1->append(&variant);
    ASSERT_EQ(1, c1->size());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data()));

    auto c2 = VariantColumn::create();
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    ASSERT_EQ(1, c2->size());
    ASSERT_TRUE(c2->has_metadata_column());
    ASSERT_TRUE(c2->has_remain_value());
    Slice meta_slice = c2->metadata_column()->get(0).get_slice();
    Slice remain_slice = c2->remain_value_column()->get(0).get_slice();
    ASSERT_EQ(Slice(v2_metadata.data(), v2_metadata.size()), meta_slice);
    ASSERT_EQ(0, remain_slice.size);
}
#endif

#ifdef FIU_ENABLE

// NOLINTNEXTLINE
// Shredded variant: typed-only (no metadata/remain columns)
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_shredded_typed_only) {
    auto c1 = VariantColumn::create();
    {
        auto data = Int64Column::create();
        auto nulls = NullColumn::create();
        for (auto [v, n] : std::initializer_list<std::pair<int64_t, uint8_t>>{{10, 0}, {20, 1}, {30, 0}}) {
            data->append(v);
            nulls->append(n);
        }
        MutableColumns typed;
        typed.emplace_back(NullableColumn::create(std::move(data), std::move(nulls)));
        c1->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);
    }
    ASSERT_EQ(3, c1->size());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    auto c2 = VariantColumn::create();
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), buffer.data() + buffer.size(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_FALSE(c2->has_metadata_column());
    ASSERT_FALSE(c2->has_remain_value());
    ASSERT_EQ(3, c2->size());
    ASSERT_EQ(1u, c2->shredded_paths().size());
    ASSERT_EQ("a", c2->shredded_paths()[0]);
    ASSERT_EQ(TYPE_BIGINT, c2->shredded_types()[0].type);
    ASSERT_EQ(0, c2->find_shredded_path("a"));
    ASSERT_EQ(-1, c2->find_shredded_path("not_exists"));

    const auto& tc = c2->typed_columns()[0];
    ASSERT_EQ(3, tc->size());
    // row 0: value=10, non-null
    ASSERT_FALSE(tc->is_null(0));
    ASSERT_EQ(10, tc->get(0).get_int64());
    // row 1: null
    ASSERT_TRUE(tc->is_null(1));
    // row 2: value=30, non-null
    ASSERT_FALSE(tc->is_null(2));
    ASSERT_EQ(30, tc->get(2).get_int64());
}

// NOLINTNEXTLINE
// Shredded variant: base_shredded (typed + metadata + remain)
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_shredded_base_shredded) {
    auto c1 = VariantColumn::create();
    {
        auto metadata = BinaryColumn::create();
        auto remain = BinaryColumn::create();
        // 3 rows of simple binary payloads
        for (int i = 0; i < 3; ++i) {
            std::string m(1, static_cast<char>('m' + i));
            std::string r(1, static_cast<char>('r' + i));
            metadata->append(Slice(m.data(), m.size()));
            remain->append(Slice(r.data(), r.size()));
        }

        auto data = Int64Column::create();
        auto nulls = NullColumn::create();
        data->append(100);
        nulls->append(0);
        data->append(200);
        nulls->append(0);
        data->append(300);
        nulls->append(0);

        MutableColumns typed;
        typed.emplace_back(NullableColumn::create(std::move(data), std::move(nulls)));
        c1->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), std::move(metadata),
                                 std::move(remain));
    }
    ASSERT_TRUE(c1->has_metadata_column());
    ASSERT_EQ(3, c1->size());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    auto c2 = VariantColumn::create();
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), buffer.data() + buffer.size(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_TRUE(c2->has_metadata_column());
    ASSERT_TRUE(c2->has_remain_value());
    ASSERT_EQ(3, c2->size());
    ASSERT_EQ("a", c2->shredded_paths()[0]);
    ASSERT_EQ(TYPE_BIGINT, c2->shredded_types()[0].type);
    ASSERT_EQ(3, c2->metadata_column()->size());
    ASSERT_EQ(3, c2->remain_value_column()->size());

    const auto& tc = c2->typed_columns()[0];
    ASSERT_EQ(3, tc->size());
    ASSERT_EQ(100, tc->get(0).get_int64());
    ASSERT_EQ(200, tc->get(1).get_int64());
    ASSERT_EQ(300, tc->get(2).get_int64());

    // Verify metadata/remain content round-trips correctly
    for (int i = 0; i < 3; ++i) {
        Slice m_slice = c1->metadata_column()->get(i).get_slice();
        Slice r_slice = c1->remain_value_column()->get(i).get_slice();
        Slice m2_slice = c2->metadata_column()->get(i).get_slice();
        Slice r2_slice = c2->remain_value_column()->get(i).get_slice();
        ASSERT_EQ(m_slice, m2_slice) << "metadata mismatch at row " << i;
        ASSERT_EQ(r_slice, r2_slice) << "remain mismatch at row " << i;
    }
}

// NOLINTNEXTLINE
// Shredded variant: multiple paths with different types
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_shredded_multiple_paths) {
    auto c1 = VariantColumn::create();
    {
        auto metadata = BinaryColumn::create();
        auto remain = BinaryColumn::create();
        metadata->append(Slice("meta", 4));
        metadata->append(Slice("meta", 4));
        remain->append(Slice("rval", 4));
        remain->append(Slice("rval", 4));

        // path "a": nullable BIGINT
        auto int_data = Int64Column::create();
        auto int_nulls = NullColumn::create();
        int_data->append(42);
        int_nulls->append(0);
        int_data->append(0);
        int_nulls->append(1); // null
        MutableColumnPtr int_col = NullableColumn::create(std::move(int_data), std::move(int_nulls));

        // path "b": nullable VARCHAR
        auto str_data = BinaryColumn::create();
        auto str_nulls = NullColumn::create();
        str_data->append("hello");
        str_nulls->append(0);
        str_data->append("world");
        str_nulls->append(0);
        MutableColumnPtr str_col = NullableColumn::create(std::move(str_data), std::move(str_nulls));

        MutableColumns typed;
        typed.emplace_back(std::move(int_col));
        typed.emplace_back(std::move(str_col));

        TypeDescriptor varchar_type(TYPE_VARCHAR);
        varchar_type.len = TypeDescriptor::MAX_VARCHAR_LENGTH;
        c1->set_shredded_columns({"a", "b"}, {TypeDescriptor(TYPE_BIGINT), varchar_type}, std::move(typed),
                                 std::move(metadata), std::move(remain));
    }
    ASSERT_EQ(2, c1->size());
    ASSERT_EQ(2u, c1->shredded_paths().size());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    auto c2 = VariantColumn::create();
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), buffer.data() + buffer.size(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_EQ(2, c2->size());
    ASSERT_EQ(2u, c2->shredded_paths().size());
    ASSERT_EQ("a", c2->shredded_paths()[0]);
    ASSERT_EQ("b", c2->shredded_paths()[1]);
    ASSERT_EQ(TYPE_BIGINT, c2->shredded_types()[0].type);
    ASSERT_EQ(TYPE_VARCHAR, c2->shredded_types()[1].type);
    ASSERT_EQ(TypeDescriptor::MAX_VARCHAR_LENGTH, c2->shredded_types()[1].len);
    ASSERT_EQ(0, c2->find_shredded_path("a"));
    ASSERT_EQ(1, c2->find_shredded_path("b"));
    ASSERT_EQ(-1, c2->find_shredded_path("c"));
    ASSERT_EQ(2u, c2->typed_columns().size());

    // verify BIGINT column
    const auto& int_tc = c2->typed_columns()[0];
    ASSERT_FALSE(int_tc->is_null(0));
    ASSERT_EQ(42, int_tc->get(0).get_int64());
    ASSERT_TRUE(int_tc->is_null(1));

    // verify VARCHAR column
    const auto& str_tc = c2->typed_columns()[1];
    ASSERT_FALSE(str_tc->is_null(0));
    ASSERT_EQ("hello", str_tc->get(0).get_slice().to_string());
    ASSERT_FALSE(str_tc->is_null(1));
    ASSERT_EQ("world", str_tc->get(1).get_slice().to_string());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, hll_column_failed_deserialize) {
    auto c1 = HyperLogLogColumn::create();
    // prepare a sparse-encoded HLL (few non-zero registers)
    HyperLogLog sparse_hll;
    for (int i = 0; i < 200; ++i) {
        sparse_hll.update(HashUtil::murmur_hash64A(&i, sizeof(i), HashUtil::MURMUR_SEED));
    }
    // prepare a full-encoded HLL (many non-zero registers)
    HyperLogLog full_hll;
    for (int i = 0; i < 5000; ++i) {
        full_hll.update(HashUtil::murmur_hash64A(&i, sizeof(i), HashUtil::MURMUR_SEED));
    }
    c1->append(&sparse_hll);
    c1->append(&full_hll);
    ASSERT_EQ(2, c1->size());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data()));

#ifdef FIU_ENABLE
    (void)::starrocks::fp_mem_chunk_allocator_allocate_fail.name();
#endif
    auto* fp = failpoint::FailPointRegistry::GetInstance()->get("mem_chunk_allocator_allocate_fail");
    ASSERT_NE(fp, nullptr);
    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp->setMode(mode);

    auto c2 = HyperLogLogColumn::create();
    ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(2, c2->size());
    for (int i = 0; i < c2->size(); ++i) {
        const HyperLogLog* h = c2->get(i).get_hyperloglog();
        ASSERT_NE(h, nullptr);
        EXPECT_EQ(0, h->estimate_cardinality()); // should be empty after failed deserialize
    }

    mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(mode);
}
#endif

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, decimal_column) {
    auto c1 = DecimalColumn::create();

    c1->append(DecimalV2Value(1));
    c1->append(DecimalV2Value(2));
    c1->append(DecimalV2Value(3));

    ASSERT_EQ(sizeof(uint32_t) + c1->size() * sizeof(DecimalV2Value), ColumnArraySerde::max_serialized_size(*c1));

    auto c2 = DecimalColumn::create();

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, int_column) {
    std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));

    ASSERT_EQ(sizeof(uint32_t) + c1->size() * sizeof(int32_t), ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < numbers.size(); i++) {
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i],
                  ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i],
                      ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[i]);
        }
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), true, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), true, level));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i],
                      ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[i]);
        }
    }
}

PARALLEL_TEST(ColumnArraySerdeTest, corrupted_data) {
    constexpr int encode_level = 2;
    std::vector<uint8_t> buffer;
    {
        const auto* end = buffer.data() + buffer.size();
        auto c3 = Int32Column::create();
        ASSERT_ERROR(ColumnArraySerde::deserialize(buffer.data(), end, c3.get(), false, encode_level));
    }
    {
        // insufficient data
        buffer.resize(10);
        buffer[0] = 0xFF;
        buffer[1] = 0xFF;
        buffer[2] = 0xFF;
        buffer[3] = 0xFF;
        auto c1 = Int32Column::create();
        const auto* end = buffer.data() + buffer.size();
        ASSERT_ERROR(ColumnArraySerde::deserialize(buffer.data(), end, c1.get(), false, encode_level));
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, double_column) {
    std::vector<double> numbers{1.0, 2, 3.3, 4, 5.9, 6, 7};
    auto c1 = DoubleColumn::create();
    auto c2 = DoubleColumn::create();
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(double));

    ASSERT_EQ(sizeof(uint32_t) + c1->size() * sizeof(double), ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(end, p1);
    ASSERT_EQ(end, p2);
    for (size_t i = 0; i < numbers.size(); i++) {
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<double>>(c1.get())->get_data()[i],
                  ColumnHelper::as_raw_column<FixedLengthColumn<double>>(c2.get())->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<double>>(c1.get())->get_data()[i],
                      ColumnHelper::as_raw_column<FixedLengthColumn<double>>(c2.get())->get_data()[i]);
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, nullable_int32_column) {
    std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
    auto c1 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto c2 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
    c1->append_nulls(2);

    ASSERT_EQ(ColumnArraySerde::max_serialized_size(*c1->null_column()) +
                      ColumnArraySerde::max_serialized_size(*c1->data_column()),
              ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->is_null(i), c2->is_null(i));
        if (!c1->is_null(i)) {
            ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
        }
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->is_null(i), c2->is_null(i));
            if (!c1->is_null(i)) {
                ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
            }
        }
    }
}

namespace {

template <typename ColumnType>
void append_binary_serde_test_strings(ColumnType* column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    column->append_strings(strings.data(), strings.size());
}

template <typename ColumnType>
void force_large_offsets(ColumnType* column) {
    AdaptiveOffsets::Large large_offsets;
    large_offsets.resize(column->get_offset().size());
    for (size_t i = 0; i < column->get_offset().size(); ++i) {
        large_offsets[i] = column->get_offset()[i];
    }
    column->get_offset().set_large_buffer(std::move(large_offsets));
}

template <typename ColumnType>
void assert_binary_column_equal(const ColumnType& lhs, const ColumnType& rhs) {
    ASSERT_EQ(lhs.size(), rhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        ASSERT_EQ(lhs.get_slice(i), rhs.get_slice(i));
    }
}

bool has_binary_serde_extended_escape(const uint8_t* buffer) {
    const auto bytes_size = decode_fixed32_le(buffer);
    return bytes_size == 0 && decode_fixed32_le(buffer + sizeof(uint32_t)) == 0;
}

template <typename ColumnType>
void assert_binary_serde_round_trip(const ColumnType& src, ColumnType* dst, int64_t expected_size,
                                    bool expect_extended_escape, bool expect_dst_large_offsets) {
    const auto max_size = ColumnArraySerde::max_serialized_size(src);
    ASSERT_EQ(expected_size, max_size);

    ASSERT_GE(max_size, 0);
    const auto expected_size_bytes = static_cast<size_t>(expected_size);
    std::vector<uint8_t> buffer(expected_size_bytes);
    ASSIGN_OR_ABORT(auto serialized_end, ColumnArraySerde::serialize(src, buffer.data()));
    ASSERT_EQ(buffer.data() + expected_size_bytes, serialized_end);

    if (expect_extended_escape) {
        ASSERT_TRUE(has_binary_serde_extended_escape(buffer.data()));
    } else if constexpr (std::is_same_v<ColumnType, BinaryColumn>) {
        ASSERT_FALSE(has_binary_serde_extended_escape(buffer.data()));
    }

    ASSIGN_OR_ABORT(auto deserialized_end, ColumnArraySerde::deserialize(buffer.data(), serialized_end, dst));
    ASSERT_EQ(serialized_end, deserialized_end);
    ASSERT_EQ(expect_dst_large_offsets, dst->get_offset().is_large());
    assert_binary_column_equal(src, *dst);
}

} // namespace

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, binary_column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    auto c1 = BinaryColumn::create();
    auto c2 = BinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());

    ASSERT_EQ(c1->get_immutable_bytes().size() + c1->get_offset().size() * sizeof(uint32_t) + sizeof(uint32_t) * 2,
              ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, binary_column_empty_legacy_format) {
    auto c1 = BinaryColumn::create();
    auto c2 = BinaryColumn::create();

    const int64_t expected_size = sizeof(uint32_t) * 3;
    ASSERT_EQ(expected_size, ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer(static_cast<size_t>(expected_size));
    ASSIGN_OR_ABORT(auto serialized_end, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + static_cast<size_t>(expected_size), serialized_end);
    ASSERT_EQ(0, decode_fixed32_le(buffer.data()));
    ASSERT_EQ(static_cast<uint32_t>(sizeof(uint32_t)), decode_fixed32_le(buffer.data() + sizeof(uint32_t)));
    ASSERT_FALSE(has_binary_serde_extended_escape(buffer.data()));

    ASSIGN_OR_ABORT(auto deserialized_end, ColumnArraySerde::deserialize(buffer.data(), serialized_end, c2.get()));
    ASSERT_EQ(serialized_end, deserialized_end);
    ASSERT_EQ(0, c2->size());
    ASSERT_FALSE(c2->get_offset().is_large());
    ASSERT_EQ(1, c2->get_offset().size());
    ASSERT_EQ(0, c2->get_offset().back());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, binary_column_serde_offset_width_layouts) {
    {
        auto c1 = BinaryColumn::create();
        auto c2 = BinaryColumn::create();
        append_binary_serde_test_strings(c1.get());
        ASSERT_FALSE(c1->get_offset().is_large());

        const auto expected_size =
                c1->get_immutable_bytes().size() + c1->get_offset().size() * sizeof(uint32_t) + sizeof(uint32_t) * 2;
        assert_binary_serde_round_trip(*c1, c2.get(), expected_size, false, false);
    }

    {
        auto c1 = BinaryColumn::create();
        auto c2 = BinaryColumn::create();
        append_binary_serde_test_strings(c1.get());
        force_large_offsets(c1.get());
        ASSERT_TRUE(c1->get_offset().is_large());

        const auto expected_size =
                c1->get_immutable_bytes().size() + c1->get_offset().size() * sizeof(uint32_t) + sizeof(uint32_t) * 2;
        assert_binary_serde_round_trip(*c1, c2.get(), expected_size, false, false);
    }

    {
        auto c1 = LargeBinaryColumn::create();
        auto c2 = LargeBinaryColumn::create();
        append_binary_serde_test_strings(c1.get());
        ASSERT_FALSE(c1->get_offset().is_large());

        const auto expected_size =
                c1->get_immutable_bytes().size() + c1->get_offset().size() * sizeof(uint64_t) + sizeof(uint64_t) * 2;
        assert_binary_serde_round_trip(*c1, c2.get(), expected_size, false, true);
    }

    {
        auto c1 = LargeBinaryColumn::create();
        auto c2 = LargeBinaryColumn::create();
        append_binary_serde_test_strings(c1.get());
        force_large_offsets(c1.get());
        ASSERT_TRUE(c1->get_offset().is_large());

        const auto expected_size =
                c1->get_immutable_bytes().size() + c1->get_offset().size() * sizeof(uint64_t) + sizeof(uint64_t) * 2;
        assert_binary_serde_round_trip(*c1, c2.get(), expected_size, false, true);
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, binary_column_sticky_large_offsets_integer_encoding) {
    auto c1 = BinaryColumn::create();
    auto c2 = BinaryColumn::create();

    std::vector<std::string> values;
    std::vector<Slice> slices;
    values.reserve(80);
    slices.reserve(80);
    for (int i = 0; i < 80; ++i) {
        values.emplace_back(strings::Substitute("v$0", i));
        slices.emplace_back(values.back());
    }
    c1->append_strings(slices.data(), slices.size());
    force_large_offsets(c1.get());
    ASSERT_TRUE(c1->get_offset().is_large());

    const auto max_size = ColumnArraySerde::max_serialized_size(*c1, ENCODE_INTEGER);
    ASSERT_GE(max_size, 0);
    std::vector<uint8_t> buffer(static_cast<size_t>(max_size));
    ASSIGN_OR_ABORT(auto serialized_end, ColumnArraySerde::serialize(*c1, buffer.data(), false, ENCODE_INTEGER));
    ASSERT_LE(serialized_end, buffer.data() + buffer.size());
    ASSERT_FALSE(has_binary_serde_extended_escape(buffer.data()));

    ASSIGN_OR_ABORT(auto deserialized_end,
                    ColumnArraySerde::deserialize(buffer.data(), serialized_end, c2.get(), false, ENCODE_INTEGER));
    ASSERT_EQ(serialized_end, deserialized_end);
    ASSERT_FALSE(c2->get_offset().is_large());
    assert_binary_column_equal(*c1, *c2);
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, large_binary_column_empty_format) {
    auto c1 = LargeBinaryColumn::create();
    auto c2 = LargeBinaryColumn::create();

    const int64_t expected_size = sizeof(uint64_t) * 3;
    ASSERT_EQ(expected_size, ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer(static_cast<size_t>(expected_size));
    ASSIGN_OR_ABORT(auto serialized_end, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + static_cast<size_t>(expected_size), serialized_end);
    ASSERT_EQ(0, decode_fixed64_le(buffer.data()));
    ASSERT_EQ(static_cast<uint64_t>(sizeof(uint64_t)), decode_fixed64_le(buffer.data() + sizeof(uint64_t)));

    ASSIGN_OR_ABORT(auto deserialized_end, ColumnArraySerde::deserialize(buffer.data(), serialized_end, c2.get()));
    ASSERT_EQ(serialized_end, deserialized_end);
    ASSERT_EQ(0, c2->size());
    ASSERT_TRUE(c2->get_offset().is_large());
    ASSERT_EQ(1, c2->get_offset().size());
    ASSERT_EQ(0, c2->get_offset().back());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, binary_column_extended_format_deserialize) {
    auto append_u32 = [](std::vector<uint8_t>* buffer, uint32_t value) {
        const auto old_size = buffer->size();
        buffer->resize(old_size + sizeof(value));
        encode_fixed32_le(buffer->data() + old_size, value);
    };
    auto append_u64 = [](std::vector<uint8_t>* buffer, uint64_t value) {
        const auto old_size = buffer->size();
        buffer->resize(old_size + sizeof(value));
        encode_fixed64_le(buffer->data() + old_size, value);
    };

    const std::string bytes = "aabbcc";
    const uint64_t offsets[] = {0, 2, 4, 6};

    std::vector<uint8_t> buffer;
    append_u32(&buffer, 0);
    append_u32(&buffer, 0);
    append_u64(&buffer, bytes.size());
    buffer.insert(buffer.end(), bytes.begin(), bytes.end());
    append_u64(&buffer, sizeof(offsets));
    for (auto offset : offsets) {
        append_u64(&buffer, offset);
    }

    auto column = BinaryColumn::create();
    ASSIGN_OR_ABORT(auto p, ColumnArraySerde::deserialize(buffer.data(), buffer.data() + buffer.size(), column.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p);
    ASSERT_EQ(3, column->size());
    ASSERT_TRUE(column->get_offset().is_large());
    ASSERT_EQ(Slice("aa"), column->get_slice(0));
    ASSERT_EQ(Slice("bb"), column->get_slice(1));
    ASSERT_EQ(Slice("cc"), column->get_slice(2));
}

#ifdef FIU_ENABLE
// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, binary_column_extended_format_serialize_round_trip) {
    // The real overflow trigger (bytes/offsets payload > UINT32_MAX) cannot be
    // allocated in a unit test, so force the extended layout via a failpoint and
    // verify the serialize + max_serialized_size + round-trip path end to end.
    auto* fp = failpoint::FailPointRegistry::GetInstance()->get("binary_column_serde_force_extended_format");
    ASSERT_NE(fp, nullptr);
    PFailPointTriggerMode enable_mode;
    enable_mode.set_mode(FailPointTriggerModeType::ENABLE);
    PFailPointTriggerMode disable_mode;
    disable_mode.set_mode(FailPointTriggerModeType::DISABLE);

    auto run_case = [&](int num_strings, int encode_level, bool check_exact_size) {
        auto src = BinaryColumn::create();
        std::vector<std::string> values;
        std::vector<Slice> slices;
        values.reserve(num_strings);
        slices.reserve(num_strings);
        for (int i = 0; i < num_strings; ++i) {
            values.emplace_back(strings::Substitute("payload-value-$0", i));
            slices.emplace_back(values.back());
        }
        src->append_strings(slices.data(), slices.size());
        // A normal small BinaryColumn: only the failpoint, not the real payload,
        // makes it take the extended path.
        ASSERT_FALSE(src->get_offset().is_large());

        // Keep the failpoint scoped to size-computation + serialization only, so a
        // later assertion failure cannot leak the forced-extended state into other
        // tests. Deserialization auto-detects the escape from the wire and needs
        // no failpoint.
        fp->setMode(enable_mode);
        const auto max_size = ColumnArraySerde::max_serialized_size(*src, encode_level);
        std::vector<uint8_t> buffer(max_size > 0 ? static_cast<size_t>(max_size) : 0);
        auto serialize_result = ColumnArraySerde::serialize(*src, buffer.data(), false, encode_level);
        fp->setMode(disable_mode);

        ASSERT_GT(max_size, 0);
        ASSERT_TRUE(serialize_result.ok()) << serialize_result.status();
        auto* serialized_end = serialize_result.value();
        ASSERT_LE(serialized_end, buffer.data() + buffer.size());
        // The extended layout always begins with the u32 0 / u32 0 escape header.
        ASSERT_TRUE(has_binary_serde_extended_escape(buffer.data()));

        if (check_exact_size) {
            // u32 escape + u32 escape + u64 bytes_size + bytes + u64 offset_bytes_size + u64 offsets
            // (small payload, so neither string nor integer encoding kicks in).
            const auto expected_size = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2 + src->get_immutable_bytes().size() +
                                       src->get_offset().size() * sizeof(uint64_t);
            ASSERT_EQ(static_cast<int64_t>(expected_size), max_size);
            ASSERT_EQ(buffer.data() + expected_size, serialized_end);
        }

        auto dst = BinaryColumn::create();
        ASSIGN_OR_ABORT(auto deserialized_end,
                        ColumnArraySerde::deserialize(buffer.data(), serialized_end, dst.get(), false, encode_level));
        ASSERT_EQ(serialized_end, deserialized_end);
        // An extended payload encodes offsets as u64, so it always decodes into large offsets.
        ASSERT_TRUE(dst->get_offset().is_large());
        assert_binary_column_equal(*src, *dst);
    };

    // Plain layout: assert the exact extended size and escape header.
    run_case(3, 0, /*check_exact_size=*/true);
    // Larger payload with encoding enabled: exercises LZ4 string encoding and
    // streamvbyte integer encoding of u64 offsets through the extended path.
    run_case(80, ENCODE_INTEGER | ENCODE_STRING, /*check_exact_size=*/false);
}
#endif

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, large_binary_column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    auto c1 = LargeBinaryColumn::create();
    auto c2 = LargeBinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());

    ASSERT_EQ(c1->get_immutable_bytes().size() + c1->get_offset().size() * sizeof(uint64_t) + sizeof(uint64_t) * 2,
              ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, const_column) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(std::move(c), size);
    };

    auto c1 = create_const_column(100, 10);
    auto c2 = c1->clone_empty();

    ASSERT_EQ(sizeof(uint64_t) + ColumnArraySerde::max_serialized_size(*c1->data_column()),
              ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    ASSERT_EQ(c1->size(), c2->size());
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2.get(), false, level));
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, array_column) {
    auto off1 = UInt32Column::create();
    auto elem1 = NullableColumn::create(Int32Column::create(), NullColumn ::create());
    auto c1 = ArrayColumn::create(elem1, off1);

    // insert [1, 2, 3], [4, 5, 6]
    elem1->append_datum(1);
    elem1->append_datum(2);
    elem1->append_datum(3);
    off1->append(3);

    elem1->append_datum(4);
    elem1->append_datum(5);
    elem1->append_datum(6);
    off1->append(6);

    ASSERT_EQ(ColumnArraySerde::max_serialized_size(c1->offsets()) +
                      ColumnArraySerde::max_serialized_size(c1->elements()),
              ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    auto off2 = UInt32Column::create();
    auto elem2 = NullableColumn::create(Int32Column::create(), NullColumn ::create());
    auto c2 = ArrayColumn::create(elem1, off2);

    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), end, c2->as_mutable_raw_ptr()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    ASSERT_EQ("[1,2,3]", c2->debug_item(0));
    ASSERT_EQ("[4,5,6]", c2->debug_item(1));

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));

        off2 = UInt32Column::create();
        elem2 = NullableColumn::create(Int32Column::create(), NullColumn ::create());
        c2 = ArrayColumn::create(elem1, off2);

        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), end, c2->as_mutable_raw_ptr(), false, level));

        ASSERT_EQ("[1,2,3]", c2->debug_item(0));
        ASSERT_EQ("[4,5,6]", c2->debug_item(1));
    }
}

namespace protobuf_serde_test {

std::string make_string(size_t i) {
    return std::string("c").append(std::to_string(static_cast<int32_t>(i)));
}

FieldPtr make_field(size_t i) {
    return std::make_shared<Field>(i, make_string(i), get_type_info(TYPE_INT), false);
}

Fields make_fields(size_t size) {
    Fields fields;
    for (size_t i = 0; i < size; i++) {
        fields.emplace_back(make_field(i));
    }
    return fields;
}

SchemaPtr make_schema(size_t i) {
    Fields fields = make_fields(i);
    return std::make_shared<Schema>(fields);
}

ColumnPtr make_column(size_t start) {
    auto column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i < 100; i++) {
        column->append(start + i);
    }
    return column;
}

Columns make_columns(size_t size) {
    Columns columns;
    for (size_t i = 0; i < size; i++) {
        columns.emplace_back(make_column(i));
    }
    return columns;
}

} // namespace protobuf_serde_test

// NOLINTNEXTLINE
PARALLEL_TEST(ProtobufChunkSerde, test_serde) {
    auto chunk = std::make_unique<Chunk>(protobuf_serde_test::make_columns(2), protobuf_serde_test::make_schema(2));

    StatusOr<ChunkPB> res = serde::ProtobufChunkSerde::serialize_without_meta(*chunk);
    ASSERT_TRUE(res.ok()) << res.status();
    const std::string& serialized_data = res->data();

    ProtobufChunkMeta meta;
    meta.slot_id_to_index[0] = 0;
    meta.slot_id_to_index[1] = 1;
    meta.is_nulls.resize(2, false);
    meta.is_consts.resize(2, false);
    meta.types.resize(2);
    meta.types[0] = TypeDescriptor(LogicalType::TYPE_INT);
    meta.types[1] = TypeDescriptor(LogicalType::TYPE_INT);

    ProtobufChunkDeserializer deserializer(meta);
    auto chunk_or = deserializer.deserialize(serialized_data);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();
    Chunk& new_chunk = *chunk_or;
    ASSERT_EQ(new_chunk.num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk.columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk.columns()[i]->get(j).get_int32());
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ProtobufChunkSerde, deserialize_with_schema) {
    auto chunk = std::make_unique<Chunk>(protobuf_serde_test::make_columns(2), protobuf_serde_test::make_schema(2));

    StatusOr<ChunkPB> res = serde::ProtobufChunkSerde::serialize_without_meta(*chunk);
    ASSERT_TRUE(res.ok()) << res.status();
    const std::string& serialized_data = res->data();

    auto chunk_or = serde::ProtobufChunkSerde::deserialize_with_schema(*chunk->schema(), serialized_data);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();
    Chunk& new_chunk = *chunk_or;
    ASSERT_EQ(new_chunk.num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk.columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk.columns()[i]->get(j).get_int32());
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ProtobufChunkSerde, TestChunkWithExtraData) {
    auto chunk = std::make_unique<Chunk>(protobuf_serde_test::make_columns(2), protobuf_serde_test::make_schema(2));
    auto extra_data_meta = std::vector<ChunkExtraColumnsMeta>{
            ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_INT), .is_null = false, .is_const = false}};
    auto extra_data_cols = protobuf_serde_test::make_columns(2);
    auto extra_data = std::make_shared<ChunkExtraColumnsData>(std::move(extra_data_meta), std::move(extra_data_cols));
    chunk->set_extra_data(extra_data);

    StatusOr<ChunkPB> res = serde::ProtobufChunkSerde::serialize_without_meta(*chunk);
    ASSERT_TRUE(res.ok()) << res.status();
    const std::string& serialized_data = res->data();

    ProtobufChunkMeta meta;
    meta.slot_id_to_index[0] = 0;
    meta.slot_id_to_index[1] = 1;
    meta.is_nulls.resize(2, false);
    meta.is_consts.resize(2, false);
    meta.types.resize(2);
    meta.types[0] = TypeDescriptor(LogicalType::TYPE_INT);
    meta.types[1] = TypeDescriptor(LogicalType::TYPE_INT);
    meta.extra_data_metas = std::vector<ChunkExtraColumnsMeta>{
            ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_INT), .is_null = false, .is_const = false}};

    ProtobufChunkDeserializer deserializer(meta);
    auto chunk_or = deserializer.deserialize(serialized_data);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();

    // check original chunk data
    Chunk& new_chunk = *chunk_or;
    ASSERT_EQ(new_chunk.num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk.columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk.columns()[i]->get(j).get_int32());
        }
    }

    // check extra chunk data
    DCHECK(new_chunk.has_extra_data());
    auto new_extra_data = dynamic_cast<ChunkExtraColumnsData*>(new_chunk.get_extra_data().get());
    auto old_extra_data = dynamic_cast<ChunkExtraColumnsData*>(chunk->get_extra_data().get());
    for (size_t i = 0; i < new_extra_data->columns().size(); ++i) {
        ASSERT_EQ(old_extra_data->columns()[i]->size(), new_extra_data->columns()[i]->size());
        for (size_t j = 0; j < old_extra_data->columns()[i]->size(); ++j) {
            ASSERT_EQ(old_extra_data->columns()[i]->get(j).get_int32(),
                      new_extra_data->columns()[i]->get(j).get_int32());
        }
    }
}

} // namespace starrocks::serde

namespace {

bool allocate_hll_registers_with_mem_chunk_allocator(size_t size, void* /*ctx*/, starrocks::MemChunk* chunk) {
    return starrocks::MemChunkAllocator::allocate(size, chunk);
}

void free_hll_registers_with_mem_chunk_allocator(const starrocks::MemChunk& chunk, void* /*ctx*/) {
    starrocks::MemChunkAllocator::free(chunk);
}

} // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    starrocks::HyperLogLog::RegistersAllocator allocator;
    allocator.allocate = allocate_hll_registers_with_mem_chunk_allocator;
    allocator.free = free_hll_registers_with_mem_chunk_allocator;
    auto st = starrocks::HyperLogLog::set_registers_allocator(allocator);
    if (!st.ok()) {
        fprintf(stderr, "failed to register HLL registers allocator: %s\n", st.to_string().c_str());
        return 1;
    }

    return RUN_ALL_TESTS();
}

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

#include "serde/column_array_serde.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/variant_column.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"
#include "types/hll.h"
#include "util/failpoint/fail_point.h"
#include "util/hash_util.hpp"
#include "util/json.h"
#include "util/variant.h"

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
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
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
        ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
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
    ASSERT_EQ(4, ColumnArraySerde::max_serialized_size(*c1));

    auto primitive_header = [](VariantType type) { return (static_cast<uint8_t>(type) << 2); };

    // Prepare 5 int8 variant values
    const uint8_t int8_values[][2] = {
            {primitive_header(VariantType::INT8), 0x01}, // 1
            {primitive_header(VariantType::INT8), 0x02}, // 2
            {primitive_header(VariantType::INT8), 0x03}, // 3
            {primitive_header(VariantType::INT8), 0x04}, // 4
            {primitive_header(VariantType::INT8), 0x05}, // 5
    };
    size_t expected_max_size = sizeof(uint32_t);
    for (size_t i = 0; i < std::size(int8_values); ++i) {
        std::string_view value(reinterpret_cast<const char*>(int8_values[i]), sizeof(int8_values[i]));
        VariantRowValue variant(VariantMetadata::kEmptyMetadata, value);
        c1->append(&variant);
        expected_max_size += sizeof(uint64_t) + variant.serialize_size();
    }
    ASSERT_EQ(expected_max_size, ColumnArraySerde::max_serialized_size(*c1));

    auto c2 = VariantColumn::create();
    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);

    ASSERT_EQ(5, c2->size());
    for (size_t i = 0; i < c1->size(); i++) {
        const VariantRowValue* datum1 = c1->get(i).get_variant();
        const VariantRowValue* datum2 = c2->get(i).get_variant();
        ASSERT_EQ(datum1->serialize_size(), datum2->serialize_size());
        ASSERT_EQ(datum1->get_metadata(), datum2->get_metadata());
        ASSERT_EQ(datum1->get_value(), datum2->get_value());
        EXPECT_EQ(datum1->to_string(), datum2->to_string());
    }

    // no effect
    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1), level);
        ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
        ASSERT_EQ(buffer.data() + buffer.size(), p1);
        ASSERT_EQ(buffer.data() + buffer.size(), p2);

        ASSERT_EQ(5, c2->size());
        for (size_t i = 0; i < c1->size(); i++) {
            const VariantRowValue* datum1 = c1->get(i).get_variant();
            const VariantRowValue* datum2 = c2->get(i).get_variant();
            ASSERT_EQ(datum1->serialize_size(), datum2->serialize_size());
            ASSERT_EQ(datum1->get_metadata(), datum2->get_metadata());
            ASSERT_EQ(datum1->get_value(), datum2->get_value());
            EXPECT_EQ(datum1->to_string(), datum2->to_string());
        }
    }
}

#if !DCHECK_IS_ON()
// we have DCHECK inside VariantColumn deserialize to check version,
// so this test case is only enabled when DCHECK is off

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, variant_column_failed_deserialize) {
    auto c1 = VariantColumn::create();
    ASSERT_EQ(4, ColumnArraySerde::max_serialized_size(*c1));

    // Prepare a variant value with an unsupported version
    constexpr uint8_t v2_metadata_charts[] = {0x02, 0x00, 0x00};
    const std::string_view v2_metadata(reinterpret_cast<const char*>(v2_metadata_charts), sizeof(v2_metadata_charts));
    const VariantRowValue variant(v2_metadata, "");
    c1->append(&variant);
    ASSERT_EQ(1, c1->size());

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data()));

    auto c2 = VariantColumn::create();
    ASSERT_ERROR(ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(0, c2->size()); // Deserialization should fail, resulting in an empty column
}
#endif

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
    ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data()));

    auto* fp = failpoint::FailPointRegistry::GetInstance()->get("mem_chunk_allocator_allocate_fail");
    ASSERT_NE(fp, nullptr);
    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp->setMode(mode);

    auto c2 = HyperLogLogColumn::create();
    ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(2, c2->size());
    for (int i = 0; i < c2->size(); ++i) {
        const HyperLogLog* h = c2->get(i).get_hyperloglog();
        ASSERT_NE(h, nullptr);
        EXPECT_EQ(0, h->estimate_cardinality()); // should be empty after failed deserialize
    }

    mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(mode);
}

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
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
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
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < numbers.size(); i++) {
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i],
                  ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i],
                      ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[i]);
        }
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), true, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), true, level));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i],
                      ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[i]);
        }
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
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < numbers.size(); i++) {
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<double>>(c1.get())->get_data()[i],
                  ColumnHelper::as_raw_column<FixedLengthColumn<double>>(c2.get())->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
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
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
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
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->is_null(i), c2->is_null(i));
            if (!c1->is_null(i)) {
                ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
            }
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, binary_column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    auto c1 = BinaryColumn::create();
    auto c2 = BinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());

    ASSERT_EQ(c1->byte_size() + sizeof(uint32_t) * 2, ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, large_binary_column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    auto c1 = LargeBinaryColumn::create();
    auto c2 = LargeBinaryColumn::create();
    c1->append_strings(strings.data(), strings.size());

    ASSERT_EQ(c1->byte_size() + sizeof(uint64_t) * 2, ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
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
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    ASSERT_EQ(c1->size(), c2->size());
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));
        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level));
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
    ASSIGN_OR_ABORT(auto p1, ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), p1);

    auto off2 = UInt32Column::create();
    auto elem2 = NullableColumn::create(Int32Column::create(), NullColumn ::create());
    auto c2 = ArrayColumn::create(elem1, off2);

    ASSIGN_OR_ABORT(auto p2, ColumnArraySerde::deserialize(buffer.data(), c2->as_mutable_raw_ptr()));
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    ASSERT_EQ("[1,2,3]", c2->debug_item(0));
    ASSERT_EQ("[4,5,6]", c2->debug_item(1));

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ASSERT_OK(ColumnArraySerde::serialize(*c1, buffer.data(), false, level));

        off2 = UInt32Column::create();
        elem2 = NullableColumn::create(Int32Column::create(), NullColumn ::create());
        c2 = ArrayColumn::create(elem1, off2);

        ASSERT_OK(ColumnArraySerde::deserialize(buffer.data(), c2->as_mutable_raw_ptr(), false, level));

        ASSERT_EQ("[1,2,3]", c2->debug_item(0));
        ASSERT_EQ("[4,5,6]", c2->debug_item(1));
    }
}

} // namespace starrocks::serde

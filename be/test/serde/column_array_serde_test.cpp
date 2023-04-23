// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "serde/column_array_serde.h"

#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_visitor.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "gutil/strings/substitute.h"
#include "testutil/parallel_test.h"
#include "util/json.h"

namespace starrocks::serde {

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, json_column) {
    auto c1 = vectorized::JsonColumn::create();

    ASSERT_EQ(8, ColumnArraySerde::max_serialized_size(*c1));

    for (int i = 0; i < 10; i++) {
        JsonValue json;
        std::string json_str = strings::Substitute("{\"a\": $0}", i);
        ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());
        c1->append(&json);
    }

    ASSERT_EQ(148, ColumnArraySerde::max_serialized_size(*c1));

    auto c2 = vectorized::JsonColumn::create();

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    auto p1 = ColumnArraySerde::serialize(*c1, buffer.data());
    auto p2 = ColumnArraySerde::deserialize(buffer.data(), c2.get());
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
        p1 = ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        p2 = ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
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
PARALLEL_TEST(ColumnArraySerdeTest, decimal_column) {
    auto c1 = vectorized::DecimalColumn::create();

    c1->append(DecimalV2Value(1));
    c1->append(DecimalV2Value(2));
    c1->append(DecimalV2Value(3));

    ASSERT_EQ(sizeof(uint32_t) + c1->size() * sizeof(DecimalV2Value), ColumnArraySerde::max_serialized_size(*c1));

    auto c2 = vectorized::DecimalColumn::create();

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    auto p1 = ColumnArraySerde::serialize(*c1, buffer.data());
    auto p2 = ColumnArraySerde::deserialize(buffer.data(), c2.get());
    ASSERT_EQ(buffer.data() + buffer.size(), p1);
    ASSERT_EQ(buffer.data() + buffer.size(), p2);
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, int_column) {
    std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
    auto c1 = vectorized::Int32Column::create();
    auto c2 = vectorized::Int32Column::create();
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));

    ASSERT_EQ(sizeof(uint32_t) + c1->size() * sizeof(int32_t), ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    for (size_t i = 0; i < numbers.size(); i++) {
        ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
        }
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), true, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), true, level);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, double_column) {
    std::vector<double> numbers{1.0, 2, 3.3, 4, 5.9, 6, 7};
    auto c1 = vectorized::DoubleColumn::create();
    auto c2 = vectorized::DoubleColumn::create();
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(double));

    ASSERT_EQ(sizeof(uint32_t) + c1->size() * sizeof(double), ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    for (size_t i = 0; i < numbers.size(); i++) {
        ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(c1->get_data()[i], c2->get_data()[i]);
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, nullable_int32_column) {
    std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
    auto c1 = vectorized::NullableColumn::create(vectorized::Int32Column::create(), vectorized::NullColumn::create());
    auto c2 = vectorized::NullableColumn::create(vectorized::Int32Column::create(), vectorized::NullColumn::create());
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
    c1->append_nulls(2);

    ASSERT_EQ(ColumnArraySerde::max_serialized_size(*c1->null_column()) +
                      ColumnArraySerde::max_serialized_size(*c1->data_column()),
              ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->is_null(i), c2->is_null(i));
        if (!c1->is_null(i)) {
            ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
        }
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
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
    auto c1 = vectorized::BinaryColumn::create();
    auto c2 = vectorized::BinaryColumn::create();
    c1->append_strings(strings);

    ASSERT_EQ(c1->byte_size() + sizeof(uint32_t) * 2, ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, large_binary_column) {
    std::vector<Slice> strings{{"bbb"}, {"bbc"}, {"ccc"}};
    auto c1 = vectorized::LargeBinaryColumn::create();
    auto c2 = vectorized::LargeBinaryColumn::create();
    c1->append_strings(strings);

    ASSERT_EQ(c1->byte_size() + sizeof(uint64_t) * 2, ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get_slice(i), c2->get_slice(i));
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, const_column) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = vectorized::Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return vectorized::ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(100, 10);
    auto c2 = c1->clone_empty();

    ASSERT_EQ(sizeof(uint64_t) + ColumnArraySerde::max_serialized_size(*c1->data_column()),
              ColumnArraySerde::max_serialized_size(*c1));

    std::vector<uint8_t> buffer;
    buffer.resize(ColumnArraySerde::max_serialized_size(*c1));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::serialize(*c1, buffer.data()));
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ(c1->size(), c2->size());
    for (size_t i = 0; i < c1->size(); i++) {
        ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
    }

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);
        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
        for (size_t i = 0; i < c1->size(); i++) {
            ASSERT_EQ(c1->get(i).get_int32(), c2->get(i).get_int32());
        }
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ColumnArraySerdeTest, array_column) {
    auto off1 = vectorized::UInt32Column::create();
    auto elem1 =
            vectorized::NullableColumn::create(vectorized::Int32Column::create(), vectorized::NullColumn ::create());
    auto c1 = vectorized::ArrayColumn::create(elem1, off1);

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
    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::serialize(*c1, buffer.data()));

    auto off2 = vectorized::UInt32Column::create();
    auto elem2 =
            vectorized::NullableColumn::create(vectorized::Int32Column::create(), vectorized::NullColumn ::create());
    auto c2 = vectorized::ArrayColumn::create(elem1, off2);

    ASSERT_EQ(buffer.data() + buffer.size(), ColumnArraySerde::deserialize(buffer.data(), c2.get()));
    ASSERT_EQ("[1,2,3]", c2->debug_item(0));
    ASSERT_EQ("[4,5,6]", c2->debug_item(1));

    for (auto level = -1; level < 8; ++level) {
        buffer.resize(ColumnArraySerde::max_serialized_size(*c1, level));
        ColumnArraySerde::serialize(*c1, buffer.data(), false, level);

        off2 = vectorized::UInt32Column::create();
        elem2 = vectorized::NullableColumn::create(vectorized::Int32Column::create(),
                                                   vectorized::NullColumn ::create());
        c2 = vectorized::ArrayColumn::create(elem1, off2);

        ColumnArraySerde::deserialize(buffer.data(), c2.get(), false, level);
        ASSERT_EQ("[1,2,3]", c2->debug_item(0));
        ASSERT_EQ("[4,5,6]", c2->debug_item(1));
    }
}

} // namespace starrocks::serde

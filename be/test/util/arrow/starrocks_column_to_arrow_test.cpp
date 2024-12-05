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

#include "util/arrow/starrocks_column_to_arrow.h"

#include <gtest/gtest.h>

#include <set>

#include "column/array_column.h"
#include "column/map_column.h"
#include "common/logging.h"

#define ARROW_UTIL_LOGGING_H
#include <arrow/buffer.h>
#include <arrow/json/api.h>
#include <arrow/result.h>

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <arrow/json/test_common.h>
DIAGNOSTIC_POP

#include <arrow/ipc/json_simple.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <column/chunk.h>
#include <column/type_traits.h>
#include <exec/arrow_type_traits.h>

#include "column/column_helper.h"
#include "storage/tablet_schema_helper.h"
#include "types/large_int_value.h"

namespace starrocks {
struct StarRocksColumnToArrowTest : public testing::Test {};

template <LogicalType LT, ArrowTypeId AT>
void compare_arrow_value(const RunTimeCppType<LT>& datum, const ArrowTypeIdToArrayType<AT>* data_array, size_t i) {
    using CppType = RunTimeCppType<LT>;
    ASSERT_TRUE(data_array->IsValid(i));
    if constexpr (lt_is_decimalv2<LT>) {
        auto actual_value = unaligned_load<int128_t>(data_array->Value(i));
        ASSERT_EQ(actual_value, datum.value());
    } else if constexpr (lt_is_decimal<LT>) {
        auto actual_value = unaligned_load<int128_t>(data_array->Value(i));
        int128_t expect_value;
        DecimalV3Cast::to_decimal_trivial<CppType, int128_t, false>(datum, &expect_value);
        ASSERT_EQ(actual_value, expect_value);
    } else if constexpr (lt_is_float<LT> || (lt_is_integer<LT> && !lt_is_largeint<LT>)) {
        ASSERT_EQ(data_array->Value(i), datum);
    } else if constexpr (lt_is_largeint<LT>) {
        ASSERT_EQ(data_array->GetString(i), LargeIntValue::to_string(datum));
    } else if constexpr (lt_is_string<LT> || lt_is_date_or_datetime<LT>) {
        ASSERT_EQ(data_array->GetString(i), datum.to_string());
    } else if constexpr (lt_is_hll<LT>) {
        std::string s;
        raw::make_room(&s, datum->max_serialized_size());
        size_t n = datum->serialize((uint8_t*)&s.front());
        s.resize(n);
        ASSERT_EQ(data_array->GetString(i), s);
    } else if constexpr (lt_is_json<LT>) {
        ASSERT_EQ(data_array->GetString(i), datum->to_string_uncheck());
    }
}

template <LogicalType LT, ArrowTypeId AT>
struct NotNullableColumnTester {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const std::vector<CppType>& data, const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<LogicalType> primitive_types(1, LT);
        auto column = ColumnType::create();
        auto data_column = down_cast<ColumnType*>(column.get());
        data_column->reserve(num_rows);
        auto k = 0;
        const auto data_size = data.size();
        for (auto i = 0; i < num_rows; ++i) {
            auto datum = data[k++ % data_size];
            data_column->append(datum);
        }
        chunk->append_column(column, SlotId(0));
        std::shared_ptr<ArrowType> arrow_type;
        if constexpr (lt_is_decimalv2<LT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (lt_is_decimal<LT>) {
            arrow_type = std::make_shared<ArrowType>(type_desc.precision, type_desc.scale);
            data_column->set_precision(type_desc.precision);
            data_column->set_scale(type_desc.scale);
        } else {
            arrow_type = std::make_shared<ArrowType>();
        }

        std::vector<std::shared_ptr<arrow::Field>> fields(1);
        fields[0] = arrow::field("col", arrow_type, false);
        auto arrow_schema = arrow::schema(std::move(fields));
        auto memory_pool = arrow::MemoryPool::CreateDefault();
        std::shared_ptr<arrow::RecordBatch> result;
        std::vector<const TypeDescriptor*> slot_types{&type_desc};
        std::vector<SlotId> slot_ids{SlotId(0)};
        convert_chunk_to_arrow_batch(chunk.get(), slot_types, slot_ids, arrow_schema, memory_pool.get(), &result);
        ASSERT_TRUE(result);
        ASSERT_FALSE(result->columns().empty());
        auto array = result->columns()[0];
        ASSERT_TRUE(array);
        ASSERT_EQ(array->type()->ToString(), arrow_type->ToString());
        auto* data_array = down_cast<ArrowArrayType*>(array.get());
        ASSERT_EQ(data_array->length(), num_rows);
        k = 0;
        for (auto i = 0; i < num_rows; ++i) {
            auto datum = data[k++ % data_size];
            compare_arrow_value<LT, AT>(datum, data_array, i);
        }
    }
};

template <LogicalType LT, ArrowTypeId AT>
struct NullableColumnTester {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const std::set<size_t>& null_index, const std::vector<CppType>& data,
                             const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<LogicalType> primitive_types(1, LT);
        auto column = ColumnType::create();
        auto data_column = down_cast<ColumnType*>(column.get());
        data_column->reserve(num_rows);
        auto null_column = NullColumn::create();
        null_column->reserve(num_rows);
        auto k = 0;
        const auto data_size = data.size();
        for (auto i = 0; i < num_rows; ++i) {
            if (null_index.count(i) > 0) {
                null_column->append(DATUM_NULL);
                data_column->append_default();
            } else {
                null_column->append(DATUM_NOT_NULL);
                auto datum = data[k++ % data_size];
                data_column->append(datum);
            }
        }
        chunk->append_column(NullableColumn::create(column, null_column), SlotId(0));
        std::shared_ptr<ArrowType> arrow_type;
        if constexpr (lt_is_decimalv2<LT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (lt_is_decimal<LT>) {
            arrow_type = std::make_shared<ArrowType>(type_desc.precision, type_desc.scale);
            data_column->set_precision(type_desc.precision);
            data_column->set_scale(type_desc.scale);
        } else {
            arrow_type = std::make_shared<ArrowType>();
        }

        std::vector<std::shared_ptr<arrow::Field>> fields(1);
        fields[0] = arrow::field("col", arrow_type, false);
        auto arrow_schema = arrow::schema(std::move(fields));
        auto memory_pool = arrow::MemoryPool::CreateDefault();
        std::shared_ptr<arrow::RecordBatch> result;
        std::vector<const TypeDescriptor*> slot_types{&type_desc};
        std::vector<SlotId> slot_ids{SlotId(0)};
        convert_chunk_to_arrow_batch(chunk.get(), slot_types, slot_ids, arrow_schema, memory_pool.get(), &result);
        ASSERT_TRUE(result);
        ASSERT_FALSE(result->columns().empty());
        auto array = result->columns()[0];
        ASSERT_TRUE(array);
        ASSERT_EQ(array->type()->ToString(), arrow_type->ToString());
        auto* data_array = down_cast<ArrowArrayType*>(array.get());
        ASSERT_EQ(data_array->length(), num_rows);
        k = 0;
        for (auto i = 0; i < num_rows; ++i) {
            if (null_index.count(i) > 0) {
                ASSERT_FALSE(data_array->IsValid(i));
            } else {
                auto datum = data[k++ % data_size];
                compare_arrow_value<LT, AT>(datum, data_array, i);
            }
        }
    }
};

template <LogicalType LT, ArrowTypeId AT>
struct ConstNullColumnTester {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<LogicalType> primitive_types(1, LT);
        auto column = ColumnHelper::create_const_null_column(num_rows);
        chunk->append_column(column, SlotId(0));
        std::shared_ptr<ArrowType> arrow_type;
        if constexpr (lt_is_decimalv2<LT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (lt_is_decimal<LT>) {
            arrow_type = std::make_shared<ArrowType>(type_desc.precision, type_desc.scale);
        } else {
            arrow_type = std::make_shared<ArrowType>();
        }

        std::vector<std::shared_ptr<arrow::Field>> fields(1);
        fields[0] = arrow::field("col", arrow_type, false);
        auto arrow_schema = arrow::schema(std::move(fields));
        auto memory_pool = arrow::MemoryPool::CreateDefault();
        std::shared_ptr<arrow::RecordBatch> result;
        std::vector<const TypeDescriptor*> slot_types{&type_desc};
        std::vector<SlotId> slot_ids{SlotId(0)};
        convert_chunk_to_arrow_batch(chunk.get(), slot_types, slot_ids, arrow_schema, memory_pool.get(), &result);
        ASSERT_TRUE(result);
        ASSERT_FALSE(result->columns().empty());
        auto array = result->columns()[0];
        ASSERT_TRUE(array);
        ASSERT_EQ(array->type()->ToString(), arrow_type->ToString());
        auto* data_array = down_cast<ArrowArrayType*>(array.get());
        ASSERT_EQ(data_array->length(), num_rows);

        for (auto i = 0; i < num_rows; ++i) {
            ASSERT_FALSE(data_array->IsValid(i));
        }
    }
};

template <LogicalType LT, ArrowTypeId AT>
struct ConstColumnTester {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const CppType& datum, const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<LogicalType> primitive_types(1, LT);
        auto data_column = ColumnType::create();
        std::shared_ptr<ArrowType> arrow_type;
        if constexpr (lt_is_decimalv2<LT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (lt_is_decimal<LT>) {
            arrow_type = std::make_shared<ArrowType>(type_desc.precision, type_desc.scale);
            data_column->set_precision(type_desc.precision);
            data_column->set_scale(type_desc.scale);
        } else {
            arrow_type = std::make_shared<ArrowType>();
        }
        data_column->append(datum);
        chunk->append_column(ConstColumn::create(data_column, num_rows), SlotId(0));

        std::vector<std::shared_ptr<arrow::Field>> fields(1);
        fields[0] = arrow::field("col", arrow_type, false);
        auto arrow_schema = arrow::schema(std::move(fields));
        auto memory_pool = arrow::MemoryPool::CreateDefault();
        std::shared_ptr<arrow::RecordBatch> result;
        std::vector<const TypeDescriptor*> slot_types{&type_desc};
        std::vector<SlotId> slot_ids{SlotId(0)};
        convert_chunk_to_arrow_batch(chunk.get(), slot_types, slot_ids, arrow_schema, memory_pool.get(), &result);
        ASSERT_TRUE(result);
        ASSERT_FALSE(result->columns().empty());
        auto array = result->columns()[0];
        ASSERT_TRUE(array);
        ASSERT_EQ(array->type()->ToString(), arrow_type->ToString());
        auto* data_array = down_cast<ArrowArrayType*>(array.get());
        ASSERT_EQ(data_array->length(), num_rows);

        for (auto i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(data_array->IsValid(i));
            compare_arrow_value<LT, AT>(datum, data_array, i);
        }
    }
};

TEST_F(StarRocksColumnToArrowTest, testDecimalColumn) {
    DecimalV2Value datum0("123456789012345678.123456789");
    DecimalV2Value datum1("3.1415926");
    std::vector<DecimalV2Value> data{datum0, datum1};
    auto type_desc = TypeDescriptor::create_decimalv2_type(27, 9);
    NotNullableColumnTester<TYPE_DECIMALV2, ArrowTypeId::DECIMAL>::apply(137, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testBooleanColumn) {
    std::vector<uint8_t> data{0, 1, 0, 0, 1};
    TypeDescriptor type_desc(TYPE_BOOLEAN);
    NotNullableColumnTester<TYPE_BOOLEAN, ArrowTypeId::BOOL>::apply(201, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testSmallIntColumn) {
    std::vector<int16_t> data{1, 6654, -4291, 804, -1, -32768, 32767};
    TypeDescriptor type_desc(TYPE_SMALLINT);
    NotNullableColumnTester<TYPE_SMALLINT, ArrowTypeId::INT16>::apply(253, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testDoubleColumn) {
    std::vector<double> data{3.14, -1E307, 1E307, 10000.124, -99999.34};
    TypeDescriptor type_desc(TYPE_DOUBLE);
    NotNullableColumnTester<TYPE_DOUBLE, ArrowTypeId::DOUBLE>::apply(253, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testStringColumn) {
    std::vector<std::string> strings{"a", "", "abc", "", "", "abcdefg"};
    std::vector<Slice> data;
    for (auto& s : strings) {
        data.emplace_back(s);
    }
    auto type_desc = TypeDescriptor::create_varchar_type(10000);
    NotNullableColumnTester<TYPE_VARCHAR, ArrowTypeId::STRING>::apply(997, data, type_desc);
    NotNullableColumnTester<TYPE_CHAR, ArrowTypeId::STRING>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testLargeIntColumn) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverflow"
    TypeDescriptor type_desc(TYPE_LARGEINT);
    std::vector<int128_t> data{int128_t(-1), (int128_t(1) << 127), (int128_t(1) << 127) - 1, 123456,
                               (int128_t(789101112131415ll) << 64) + 16171819292122ll};
#pragma GCC diagnostic pop
    NotNullableColumnTester<TYPE_LARGEINT, ArrowTypeId::STRING>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testDecimal32Column) {
    std::vector<int32_t> data{-1, 999999999, -999999999, 0, 123456789, -123456789};
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2);
    NotNullableColumnTester<TYPE_DECIMAL32, ArrowTypeId::DECIMAL>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testDecimal64Column) {
    std::vector<int64_t> data{-1ll, 99999'99999'99999'999ll, -99999'99999'99999'999ll,
                              0,    123456789'123456789ll,   -123456789'123456789ll};
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9);
    NotNullableColumnTester<TYPE_DECIMAL64, ArrowTypeId::DECIMAL>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testDecimal128Column) {
    DecimalV2Value datum0("123456789012345678.123456789");
    DecimalV2Value datum1("3.1415926");
    std::vector<int128_t> data{datum0.value(), datum1.value()};
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 27, 9);
    NotNullableColumnTester<TYPE_DECIMAL128, ArrowTypeId::DECIMAL>::apply(137, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testDateColumn) {
    std::vector<DateValue> data(3);
    data[0].from_date_literal(19881218);
    data[1].from_date_literal(20210101);
    data[2].from_date_literal(20010808);
    TypeDescriptor type_desc(TYPE_DATE);
    NotNullableColumnTester<TYPE_DATE, ArrowTypeId::STRING>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testDateTimeColumn) {
    std::vector<TimestampValue> data(3);
    data[0].from_timestamp_literal(19881218230159ll);
    data[1].from_timestamp_literal(20180119000114ll);
    data[2].from_timestamp_literal(20190228010131ll);
    TypeDescriptor type_desc(TYPE_DATETIME);
    NotNullableColumnTester<TYPE_DATETIME, ArrowTypeId::STRING>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testHllColumn) {
    std::vector<HyperLogLog> hll_data(3);
    hll_data[0].update(1);
    hll_data[0].update(100000);
    hll_data[1].update(11);
    hll_data[1].update(100);
    hll_data[1].update(999999);
    hll_data[2].update(22);
    hll_data[2].update(222);
    hll_data[2].update(2222);
    hll_data[2].update(22222);
    std::vector<HyperLogLog*> data{&hll_data[0], &hll_data[1], &hll_data[2]};
    auto type_desc = TypeDescriptor::create_hll_type();
    NotNullableColumnTester<TYPE_HLL, ArrowTypeId::STRING>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testJsonColumn) {
    std::vector<JsonValue> json_data(3);
    json_data[0] = JsonValue::from_string("{}");
    json_data[1] = JsonValue::from_string(
            "{\"array\":[1,2,3],\"boolean\":true,"
            "\"color\":\"gold\",\"null\":null,"
            "\"number\":123,\"object\":{\"a\":\"b\",\"c\":\"d\"},\"string\":\"Hello World\"}");
    json_data[2] = JsonValue::from_string(
            "{\"name\":\"alice\",\"age\":25,\"gender\":\"women\","
            "\"dept\":\"R&D\",\"lang\":[\"CPP\",\"JAVA\",\"PHP\"],"
            "\"projects\":[{\"name\":\"feature1\",\"deadline\":\"2023-01-02\"},"
            "{\"name\":\"feature2\",\"deadline\":\"2023-01-03\"}]}");
    std::vector<JsonValue*> data{&json_data[0], &json_data[1], &json_data[2]};
    auto type_desc = TypeDescriptor::create_json_type();
    NotNullableColumnTester<TYPE_JSON, ArrowTypeId::STRING>::apply(997, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableDecimalColumn) {
    DecimalV2Value datum0("123456789012345678.123456789");
    DecimalV2Value datum1("3.1415926");
    std::vector<DecimalV2Value> data{datum0, datum1};
    auto type_desc = TypeDescriptor::create_decimalv2_type(27, 9);
    std::set<size_t> null_index{1, 10, 127};
    NullableColumnTester<TYPE_DECIMALV2, ArrowTypeId::DECIMAL>::apply(137, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableBooleanColumn) {
    std::vector<uint8_t> data{0, 1, 0, 0, 1};
    std::set<size_t> null_index{0, 100, 110, 111, 252};
    TypeDescriptor type_desc(TYPE_BOOLEAN);
    NullableColumnTester<TYPE_BOOLEAN, ArrowTypeId::BOOL>::apply(253, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableSmallIntColumn) {
    std::vector<int16_t> data{1, 6654, -4291, 804, -1, -32768, 32767};
    std::set<size_t> null_index{0, 100, 110, 111, 252};
    TypeDescriptor type_desc(TYPE_SMALLINT);
    NullableColumnTester<TYPE_SMALLINT, ArrowTypeId::INT16>::apply(253, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableDoubleColumn) {
    std::vector<double> data{3.14, -1E307, 1E307, 10000.124, -99999.34};
    std::set<size_t> null_index{1, 99, 100, 111, 252};
    TypeDescriptor type_desc(TYPE_DOUBLE);
    NullableColumnTester<TYPE_DOUBLE, ArrowTypeId::DOUBLE>::apply(253, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableStringColumn) {
    std::vector<std::string> strings{"a", "", "abc", "", "", "abcdefg"};
    std::vector<Slice> data;
    for (auto& s : strings) {
        data.emplace_back(s);
    }
    auto type_desc = TypeDescriptor::create_varchar_type(10000);
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    NullableColumnTester<TYPE_VARCHAR, ArrowTypeId::STRING>::apply(997, null_index, data, type_desc);
    NullableColumnTester<TYPE_CHAR, ArrowTypeId::STRING>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableLargeIntColumn) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverflow"
    std::vector<int128_t> data{int128_t(-1), (int128_t(1) << 127), (int128_t(1) << 127) - 1, 123456,
                               (int128_t(789101112131415ll) << 64) + 16171819292122ll};
#pragma GCC diagnostic pop
    TypeDescriptor type_desc(TYPE_LARGEINT);
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    NullableColumnTester<TYPE_LARGEINT, ArrowTypeId::STRING>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableDecimal32Column) {
    std::vector<int32_t> data{-1, 999999999, -999999999, 0, 123456789, -123456789};
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2);
    NullableColumnTester<TYPE_DECIMAL32, ArrowTypeId::DECIMAL>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableDecimal64Column) {
    std::vector<int64_t> data{-1ll, 99999'99999'99999'999ll, -99999'99999'99999'999ll,
                              0,    123456789'123456789ll,   -123456789'123456789ll};
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9);
    NullableColumnTester<TYPE_DECIMAL64, ArrowTypeId::DECIMAL>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableDecimal128Column) {
    DecimalV2Value datum0("123456789012345678.123456789");
    DecimalV2Value datum1("3.1415926");
    std::vector<int128_t> data{datum0.value(), datum1.value()};
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 27, 9);
    NullableColumnTester<TYPE_DECIMAL128, ArrowTypeId::DECIMAL>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableDateColumn) {
    std::vector<DateValue> data(3);
    data[0].from_date_literal(19881218);
    data[1].from_date_literal(20210101);
    data[2].from_date_literal(20010808);
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    TypeDescriptor type_desc(TYPE_DATE);
    NullableColumnTester<TYPE_DATE, ArrowTypeId::STRING>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableDateTimeColumn) {
    std::vector<TimestampValue> data(3);
    data[0].from_timestamp_literal(19881218230159ll);
    data[1].from_timestamp_literal(20180119000114ll);
    data[2].from_timestamp_literal(20190228010131ll);
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    TypeDescriptor type_desc(TYPE_DATETIME);
    NullableColumnTester<TYPE_DATETIME, ArrowTypeId::STRING>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testNullableHllColumn) {
    std::vector<HyperLogLog> hll_data(3);
    hll_data[0].update(1);
    hll_data[0].update(100000);
    hll_data[1].update(11);
    hll_data[1].update(100);
    hll_data[1].update(999999);
    hll_data[2].update(22);
    hll_data[2].update(222);
    hll_data[2].update(2222);
    hll_data[2].update(22222);
    std::vector<HyperLogLog*> data{&hll_data[0], &hll_data[1], &hll_data[2]};
    std::set<size_t> null_index{1, 99, 100, 111, 252, 900, 993};
    auto type_desc = TypeDescriptor::create_hll_type();
    NullableColumnTester<TYPE_HLL, ArrowTypeId::STRING>::apply(997, null_index, data, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testConstDateTimeColumn) {
    TimestampValue datum;
    datum.from_timestamp_literal(19881218230159ll);
    TypeDescriptor type_desc(TYPE_DATETIME);
    ConstColumnTester<TYPE_DATETIME, ArrowTypeId::STRING>::apply(997, datum, type_desc);
}

TEST_F(StarRocksColumnToArrowTest, testConstNullColumn) {
    auto varchar_type_desc = TypeDescriptor::create_varchar_type(10000);
    ConstNullColumnTester<TYPE_VARCHAR, ArrowTypeId::STRING>::apply(997, varchar_type_desc);
    auto decimal128_type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 27, 9);
    ConstNullColumnTester<TYPE_DECIMAL128, ArrowTypeId::DECIMAL>::apply(997, decimal128_type_desc);
}

void convert_to_arrow(const TypeDescriptor& type_desc, const ColumnPtr& column,
                      std::shared_ptr<arrow::DataType>& arrow_type, arrow::MemoryPool* memory_pool,
                      std::shared_ptr<arrow::RecordBatch>* result) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(column, SlotId(0));
    std::vector<const TypeDescriptor*> slot_types{&type_desc};
    std::vector<SlotId> slot_ids{SlotId(0)};

    auto arrow_schema = arrow::schema({arrow::field("col", arrow_type, false)});
    auto st = convert_chunk_to_arrow_batch(chunk.get(), slot_types, slot_ids, arrow_schema, memory_pool, result);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(result->get() != nullptr);
    ASSERT_EQ(1, (*result)->num_columns());
    std::shared_ptr<arrow::Array> array = (*result)->column(0);
    ASSERT_TRUE(array);
    ASSERT_EQ(array->type()->ToString(), arrow_type->ToString());
}

TEST_F(StarRocksColumnToArrowTest, testArrayColumn) {
    auto array_type_desc = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));
    auto column = ColumnHelper::create_column(array_type_desc, false, false, 0, false);

    // [1, 2, 3]
    column->append_datum(DatumArray{1, 2, 3});
    // [4, NULL, 5, 6]
    column->append_datum(DatumArray{4, Datum(), 5, 6});
    // []
    column->append_datum(DatumArray{});
    // [NULL, NULL]
    column->append_datum(DatumArray{Datum(), Datum()});

    auto memory_pool = arrow::MemoryPool::CreateDefault();
    std::shared_ptr<arrow::DataType> arrow_type = arrow::list(arrow::int32());
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow(array_type_desc, column, arrow_type, memory_pool.get(), &result);
    std::shared_ptr<arrow::Array> array = result->column(0);

    auto s = arrow::ipc::internal::json::ArrayFromJSON(arrow_type, "[[1, 2, 3], [4, null, 5, 6], [], [null, null]]");
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(s.ValueUnsafe()->Equals(array));
}

TEST_F(StarRocksColumnToArrowTest, testNullableArrayColumn) {
    auto array_type_desc = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));
    auto column = ColumnHelper::create_column(array_type_desc, true, false, 0, false);

    // [1, 2, 3]
    column->append_datum(DatumArray{1, 2, 3});
    // NULL
    ASSERT_TRUE(column->append_nulls(1));
    // [4, NULL, 5, 6]
    column->append_datum(DatumArray{4, Datum(), 5, 6});
    // []
    column->append_datum(DatumArray{});
    // [NULL, NULL]
    column->append_datum(DatumArray{Datum(), Datum()});

    auto memory_pool = arrow::MemoryPool::CreateDefault();
    std::shared_ptr<arrow::DataType> arrow_type = arrow::list(arrow::int32());
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow(array_type_desc, column, arrow_type, memory_pool.get(), &result);
    std::shared_ptr<arrow::Array> array = result->column(0);

    std::shared_ptr<arrow::Array> expect_array;
    auto s = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                       "[[1, 2, 3], null, [4, null, 5, 6], [], [null, null]]");
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(s.ValueUnsafe()->Equals(array));
}

TEST_F(StarRocksColumnToArrowTest, testStructColumn) {
    std::vector<std::string> field_names{"id", "name"};
    auto struct_type_desc =
            TypeDescriptor::create_struct_type(field_names, {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_CHAR)});
    auto column = ColumnHelper::create_column(struct_type_desc, false, false, 0, false);

    // {1, "test1"}
    column->append_datum(DatumStruct{1, "test1"});
    // {NULL, "test2"}
    column->append_datum(DatumStruct{Datum(), "test2"});
    // {2, NULL}
    column->append_datum(DatumStruct{2, Datum()});
    // {NULL, NULL}
    column->append_datum(DatumStruct{Datum(), Datum()});

    auto memory_pool = arrow::MemoryPool::CreateDefault();
    std::shared_ptr<arrow::DataType> arrow_type =
            arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow(struct_type_desc, column, arrow_type, memory_pool.get(), &result);
    std::shared_ptr<arrow::Array> array = result->column(0);

    auto s = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                       R"([
                        {"id": 1, "name": "test1"},
                        {"id": null, "name": "test2"},
                        {"id": 2, "name": null},
                        {"id": null, "name": null}
                    ])");
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(s.ValueUnsafe()->Equals(array));
}

TEST_F(StarRocksColumnToArrowTest, testNullableStructColumn) {
    std::vector<std::string> field_names{"id", "name"};
    auto struct_type_desc =
            TypeDescriptor::create_struct_type(field_names, {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_CHAR)});
    auto column = ColumnHelper::create_column(struct_type_desc, true, false, 0, false);

    // {1, "test1"}
    column->append_datum(DatumStruct{1, "test1"});
    // NULL
    ASSERT_TRUE(column->append_nulls(1));
    // {NULL, "test2"}
    column->append_datum(DatumStruct{Datum(), "test2"});
    // {2, NULL}
    column->append_datum(DatumStruct{2, Datum()});
    // {NULL, NULL}
    column->append_datum(DatumStruct{Datum(), Datum()});

    auto memory_pool = arrow::MemoryPool::CreateDefault();
    std::shared_ptr<arrow::DataType> arrow_type =
            arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow(struct_type_desc, column, arrow_type, memory_pool.get(), &result);
    std::shared_ptr<arrow::Array> array = result->column(0);

    std::shared_ptr<arrow::Array> expect_array;
    auto s = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                       R"([
                        {"id": 1, "name": "test1"},
                        null,
                        {"id": null, "name": "test2"},
                        {"id": 2, "name": null},
                        {"id": null, "name": null}
                    ])");
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(s.ValueUnsafe()->Equals(array));
}

TEST_F(StarRocksColumnToArrowTest, testMapColumn) {
    auto map_type_desc = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_CHAR));
    auto column = ColumnHelper::create_column(map_type_desc, false, false, 0, false);

    // {1:"test1"},{2,"test2"}
    column->append_datum(DatumMap{{1, (Slice) "test1"}, {2, (Slice) "test2"}});
    // {3:"test3"},{4,"test4"}
    column->append_datum(DatumMap{{3, (Slice) "test3"}, {4, (Slice) "test4"}});
    // {5:Null}
    column->append_datum(DatumMap{{5, Datum()}});
    // empty
    column->append_datum(DatumMap{});

    auto memory_pool = arrow::MemoryPool::CreateDefault();
    std::shared_ptr<arrow::DataType> arrow_type = arrow::map(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow(map_type_desc, column, arrow_type, memory_pool.get(), &result);
    std::shared_ptr<arrow::Array> array = result->column(0);

    std::unique_ptr<arrow::ArrayBuilder> builder;
    ASSERT_OK(arrow::MakeBuilder(memory_pool.get(), arrow_type, &builder));
    arrow::MapBuilder* map_builder = down_cast<arrow::MapBuilder*>(builder.get());
    arrow::Int32Builder* key_builder = down_cast<arrow::Int32Builder*>(map_builder->key_builder());
    arrow::StringBuilder* item_builder = down_cast<arrow::StringBuilder*>(map_builder->item_builder());
    // {1:"test1"},{2,"test2"}
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->AppendValues({1, 2}));
    ASSERT_OK(item_builder->AppendValues({"test1", "test2"}));
    // {3:"test3"},{4,"test4"}
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->AppendValues({3, 4}));
    ASSERT_OK(item_builder->AppendValues({"test3", "test4"}));
    // {5:Null}
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->Append(5));
    ASSERT_OK(item_builder->AppendNull());
    // empty
    ASSERT_OK(map_builder->Append());
    std::shared_ptr<arrow::Array> expect_array;
    ASSERT_OK(map_builder->Finish(&expect_array));
    ASSERT_TRUE(expect_array->Equals(array));
}

TEST_F(StarRocksColumnToArrowTest, testNullableMapColumn) {
    auto map_type_desc = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_CHAR));
    auto column = ColumnHelper::create_column(map_type_desc, true, false, 0, false);

    // {1:"test1"},{2,"test2"}
    column->append_datum(DatumMap{{1, (Slice) "test1"}, {2, (Slice) "test2"}});
    // {3:"test3"},{4,"test4"}
    column->append_datum(DatumMap{{3, (Slice) "test3"}, {4, (Slice) "test4"}});
    // {5:Null}
    column->append_datum(DatumMap{{5, Datum()}});
    // empty
    column->append_datum(DatumMap{});
    // NULL
    column->append_nulls(1);

    auto memory_pool = arrow::MemoryPool::CreateDefault();
    std::shared_ptr<arrow::DataType> arrow_type = arrow::map(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow(map_type_desc, column, arrow_type, memory_pool.get(), &result);
    std::shared_ptr<arrow::Array> array = result->column(0);

    std::unique_ptr<arrow::ArrayBuilder> builder;
    ASSERT_OK(arrow::MakeBuilder(memory_pool.get(), arrow_type, &builder));
    arrow::MapBuilder* map_builder = down_cast<arrow::MapBuilder*>(builder.get());
    arrow::Int32Builder* key_builder = down_cast<arrow::Int32Builder*>(map_builder->key_builder());
    arrow::StringBuilder* item_builder = down_cast<arrow::StringBuilder*>(map_builder->item_builder());
    // {1:"test1"},{2,"test2"}
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->AppendValues({1, 2}));
    ASSERT_OK(item_builder->AppendValues({"test1", "test2"}));
    // {3:"test3"},{4,"test4"}
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->AppendValues({3, 4}));
    ASSERT_OK(item_builder->AppendValues({"test3", "test4"}));
    // {5:Null}
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->Append(5));
    ASSERT_OK(item_builder->AppendNull());
    // empty
    ASSERT_OK(map_builder->Append());
    // NULL
    ASSERT_OK(map_builder->AppendNull());
    std::shared_ptr<arrow::Array> expect_array;
    ASSERT_OK(map_builder->Finish(&expect_array));
    ASSERT_TRUE(expect_array->Equals(array));
}

TEST_F(StarRocksColumnToArrowTest, testNestedArrayStructMap) {
    // ARRAY<STRUCT<INT,MAP<INT,CHAR>>>>
    auto map_type_desc = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_CHAR));
    std::vector<std::string> field_names{"id", "map"};
    auto struct_type_desc = TypeDescriptor::create_struct_type(field_names, {TypeDescriptor(TYPE_INT), map_type_desc});
    auto array_type_desc = TypeDescriptor::create_array_type(struct_type_desc);
    auto column = ColumnHelper::create_column(array_type_desc, true, false, 0, false);
    // [{"id": 1, "map": {11:"test11"},{111:"test111"}}, null]
    column->append_datum(
            DatumArray{DatumStruct{1, DatumMap{{11, (Slice) "test11"}, {111, (Slice) "test111"}}}, Datum()});
    // []
    column->append_datum(DatumArray());
    // [{"id": 2, "map": null}, {"id": null, "map": {33:"test33"},{333:null}}]
    column->append_datum(DatumArray{DatumStruct{2, Datum()},
                                    DatumStruct{Datum(), DatumMap{{33, (Slice) "test33"}, {333, Datum()}}}});
    // [{"id": 4, "map": {}}}]
    column->append_datum(DatumArray{DatumStruct{4, DatumMap{}}});
    // null
    column->append_nulls(1);

    auto memory_pool = arrow::MemoryPool::CreateDefault();
    std::shared_ptr<arrow::DataType> arrow_map_type = arrow::map(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::DataType> arrow_struct_type =
            arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("map", arrow_map_type)});
    std::shared_ptr<arrow::DataType> arrow_type = arrow::list(arrow_struct_type);
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow(array_type_desc, column, arrow_type, memory_pool.get(), &result);
    std::shared_ptr<arrow::Array> array = result->column(0);
    ASSERT_EQ(5, array->length());

    std::unique_ptr<arrow::ArrayBuilder> builder;
    ASSERT_OK(arrow::MakeBuilder(memory_pool.get(), arrow_type, &builder));
    arrow::ListBuilder* list_builder = down_cast<arrow::ListBuilder*>(builder.get());
    arrow::StructBuilder* struct_builder = down_cast<arrow::StructBuilder*>(list_builder->value_builder());
    arrow::Int32Builder* id_builder = down_cast<arrow::Int32Builder*>(struct_builder->field_builder(0));
    arrow::MapBuilder* map_builder = down_cast<arrow::MapBuilder*>(struct_builder->field_builder(1));
    arrow::Int32Builder* key_builder = down_cast<arrow::Int32Builder*>(map_builder->key_builder());
    arrow::StringBuilder* item_builder = down_cast<arrow::StringBuilder*>(map_builder->item_builder());
    // [{"id": 1, "map": {11:"test11"},{111:"test111"}}, null]
    ASSERT_OK(list_builder->Append());
    ASSERT_OK(struct_builder->Append());
    ASSERT_OK(id_builder->Append(1));
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->AppendValues({11, 111}));
    ASSERT_OK(item_builder->AppendValues({"test11", "test111"}));
    ASSERT_OK(struct_builder->AppendNull());
    // []
    ASSERT_OK(list_builder->Append());
    // [{"id": 2, "map": null}, {"id": null, "map": {33:"test33"},{333:null}}]
    ASSERT_OK(list_builder->Append());
    ASSERT_OK(struct_builder->Append());
    ASSERT_OK(id_builder->Append(2));
    ASSERT_OK(map_builder->AppendNull());
    ASSERT_OK(struct_builder->Append());
    ASSERT_OK(id_builder->AppendNull());
    ASSERT_OK(map_builder->Append());
    ASSERT_OK(key_builder->AppendValues({33, 333}));
    ASSERT_OK(item_builder->Append("test33"));
    ASSERT_OK(item_builder->AppendNull());
    // [{"id": 4, "map": {}}}]
    ASSERT_OK(list_builder->Append());
    ASSERT_OK(struct_builder->Append());
    ASSERT_OK(id_builder->Append(4));
    ASSERT_OK(map_builder->Append());
    // null
    ASSERT_OK(list_builder->AppendNull());
    std::shared_ptr<arrow::Array> expect_array;
    ASSERT_OK(builder->Finish(&expect_array));
    ASSERT_TRUE(expect_array->Equals(array));
}

} // namespace starrocks

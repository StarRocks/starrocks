// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/arrow/starrocks_column_to_arrow.h"

#include <gtest/gtest.h>

#include <set>

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

#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <column/chunk.h>
#include <column/type_traits.h>
#include <exec/vectorized/arrow_type_traits.h>

#include "column/column_helper.h"
#include "runtime/large_int_value.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks::vectorized {
struct StarRocksColumnToArrowTest : public testing::Test {};

template <PrimitiveType PT, ArrowTypeId AT>
void compare_arrow_value(const RunTimeCppType<PT>& datum, const ArrowTypeIdToArrayType<AT>* data_array, size_t i) {
    using CppType = RunTimeCppType<PT>;
    ASSERT_TRUE(data_array->IsValid(i));
    if constexpr (pt_is_decimalv2<PT>) {
        auto actual_value = unaligned_load<int128_t>(data_array->Value(i));
        ASSERT_EQ(actual_value, datum.value());
    } else if constexpr (pt_is_decimal<PT>) {
        auto actual_value = unaligned_load<int128_t>(data_array->Value(i));
        int128_t expect_value;
        DecimalV3Cast::to_decimal_trivial<CppType, int128_t, false>(datum, &expect_value);
        ASSERT_EQ(actual_value, expect_value);
    } else if constexpr (pt_is_float<PT> || (pt_is_integer<PT> && !pt_is_largeint<PT>)) {
        ASSERT_EQ(data_array->Value(i), datum);
    } else if constexpr (pt_is_largeint<PT>) {
        ASSERT_EQ(data_array->GetString(i), LargeIntValue::to_string(datum));
    } else if constexpr (pt_is_string<PT> || pt_is_date_or_datetime<PT>) {
        ASSERT_EQ(data_array->GetString(i), datum.to_string());
    } else if constexpr (pt_is_hll<PT>) {
        std::string s;
        raw::make_room(&s, datum->max_serialized_size());
        size_t n = datum->serialize((uint8_t*)&s.front());
        s.resize(n);
        ASSERT_EQ(data_array->GetString(i), s);
    } else if constexpr (pt_is_json<PT>) {
        ASSERT_EQ(data_array->GetString(i), datum->to_string_uncheck());
    }
}

template <PrimitiveType PT, ArrowTypeId AT>
struct NotNullableColumnTester {
    using CppType = RunTimeCppType<PT>;
    using ColumnType = RunTimeColumnType<PT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const std::vector<CppType>& data, const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<PrimitiveType> primitive_types(1, PT);
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
        if constexpr (pt_is_decimalv2<PT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (pt_is_decimal<PT>) {
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
            compare_arrow_value<PT, AT>(datum, data_array, i);
        }
    }
};

template <PrimitiveType PT, ArrowTypeId AT>
struct NullableColumnTester {
    using CppType = RunTimeCppType<PT>;
    using ColumnType = RunTimeColumnType<PT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const std::set<size_t>& null_index, const std::vector<CppType>& data,
                             const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<PrimitiveType> primitive_types(1, PT);
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
        if constexpr (pt_is_decimalv2<PT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (pt_is_decimal<PT>) {
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
                compare_arrow_value<PT, AT>(datum, data_array, i);
            }
        }
    }
};

template <PrimitiveType PT, ArrowTypeId AT>
struct ConstNullColumnTester {
    using CppType = RunTimeCppType<PT>;
    using ColumnType = RunTimeColumnType<PT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<PrimitiveType> primitive_types(1, PT);
        auto column = vectorized::ColumnHelper::create_const_null_column(num_rows);
        chunk->append_column(column, SlotId(0));
        std::shared_ptr<ArrowType> arrow_type;
        if constexpr (pt_is_decimalv2<PT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (pt_is_decimal<PT>) {
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

template <PrimitiveType PT, ArrowTypeId AT>
struct ConstColumnTester {
    using CppType = RunTimeCppType<PT>;
    using ColumnType = RunTimeColumnType<PT>;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    static inline void apply(size_t num_rows, const CppType& datum, const TypeDescriptor& type_desc) {
        auto chunk = std::make_shared<Chunk>();
        std::vector<PrimitiveType> primitive_types(1, PT);
        auto data_column = ColumnType::create();
        std::shared_ptr<ArrowType> arrow_type;
        if constexpr (pt_is_decimalv2<PT>) {
            arrow_type = std::make_shared<ArrowType>(27, 9);
        } else if constexpr (pt_is_decimal<PT>) {
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
            compare_arrow_value<PT, AT>(datum, data_array, i);
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
    std::vector<string> strings{"a", "", "abc", "", "", "abcdefg"};
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
    std::vector<string> strings{"a", "", "abc", "", "", "abcdefg"};
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

} // namespace starrocks::vectorized

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

#include "formats/parquet/statistics_helper.h"

#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <stdexcept>

#include "base/testutil/assert.h"
#include "base/types/int256.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "connector/hive/scanner/hdfs_scanner_context.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/schema.h"
#include "io/string_input_stream.h"
#include "parquet_test_util/util.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

class StatisticsHelperTest : public testing::Test {};

TEST_F(StatisticsHelperTest, DecodeBooleanMinMax) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);

    MutableColumnPtr min_column = ColumnHelper::create_column(type, true);
    MutableColumnPtr max_column = ColumnHelper::create_column(type, true);

    // Parquet stats encode BOOLEAN as a single byte: 0x00 = false, 0x01 = true.
    std::vector<std::string> min_values{std::string("\x00", 1), std::string("\x01", 1)};
    std::vector<std::string> max_values{std::string("\x01", 1), std::string("\x01", 1)};
    std::vector<bool> null_pages{false, false};

    ASSERT_TRUE(
            StatisticsHelper::decode_value_into_column(min_column, min_values, null_pages, type, &field, "UTC").ok());
    ASSERT_TRUE(
            StatisticsHelper::decode_value_into_column(max_column, max_values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(2, min_column->size());
    ASSERT_EQ(2, max_column->size());
    EXPECT_EQ(0, min_column->get(0).get_uint8());
    EXPECT_EQ(1, min_column->get(1).get_uint8());
    EXPECT_EQ(1, max_column->get(0).get_uint8());
    EXPECT_EQ(1, max_column->get(1).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanIgnoresPaddingBits) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    // PLAIN BOOLEAN encoding is bit-packed LSB first, so only bit 0 carries the value.
    // bits 1-7 are unused padding.
    // A byte such as 0x02 or 0xFE must decode to false, and 0x03 or 0xFF to true,
    // regardless of the padding bits.
    std::vector<std::string> values{std::string("\x02", 1), std::string("\x03", 1), std::string("\xFE", 1),
                                    std::string("\xFF", 1)};
    std::vector<bool> null_pages{false, false, false, false};

    ASSERT_TRUE(StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(4, column->size());
    EXPECT_EQ(0, column->get(0).get_uint8());
    EXPECT_EQ(1, column->get(1).get_uint8());
    EXPECT_EQ(0, column->get(2).get_uint8());
    EXPECT_EQ(1, column->get(3).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanNullPage) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    std::vector<std::string> values{std::string(), std::string("\x01", 1)};
    std::vector<bool> null_pages{true, false};

    ASSERT_TRUE(StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(2, column->size());
    EXPECT_TRUE(column->is_null(0));
    EXPECT_FALSE(column->is_null(1));
    EXPECT_EQ(1, column->get(1).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanEmptyValueReturnsCorruption) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    std::vector<std::string> values{std::string()};
    std::vector<bool> null_pages{false};

    auto st = StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC");
    EXPECT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(StatisticsHelperTest, DecodeByteArrayDecimalMinMax) {
    ParquetField field;
    field.physical_column_index = 0;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.precision = 18;
    field.scale = 8;
    const TypeDescriptor type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 18, 8);
    auto min_column = ColumnHelper::create_column(type, true);
    auto max_column = ColumnHelper::create_column(type, true);
    ASSERT_OK(StatisticsHelper::decode_min_max_into_columns(min_column, max_column, {std::string("\xfb", 1), ""},
                                                            {std::string("\x01\x2c", 2), ""}, {false, true}, type,
                                                            &field, "UTC"));
    ASSERT_EQ(2, min_column->size());
    ASSERT_EQ(2, max_column->size());
    EXPECT_EQ(-5, min_column->get(0).get_int64());
    EXPECT_EQ(300, max_column->get(0).get_int64());
    EXPECT_TRUE(min_column->is_null(1));
    EXPECT_TRUE(max_column->is_null(1));

    auto bad_min = ColumnHelper::create_column(type, true);
    auto bad_max = ColumnHelper::create_column(type, true);
    Status st = StatisticsHelper::decode_min_max_into_columns(bad_min, bad_max, {""}, {std::string("\x01", 1)}, {false},
                                                              type, &field, "UTC");
    EXPECT_TRUE(st.is_corruption()) << st.to_string();

    bad_min = ColumnHelper::create_column(type, true);
    bad_max = ColumnHelper::create_column(type, true);
    st = StatisticsHelper::decode_min_max_into_columns(bad_min, bad_max, {std::string(17, '\x01')},
                                                       {std::string("\x01", 1)}, {false}, type, &field, "UTC");
    EXPECT_FALSE(st.ok()) << st.to_string();

    const TypeDescriptor wide_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, -1, 38, 38);
    field.scale = 0;
    bad_min = ColumnHelper::create_column(wide_type, true);
    bad_max = ColumnHelper::create_column(wide_type, true);
    st = StatisticsHelper::decode_min_max_into_columns(bad_min, bad_max,
                                                       {std::string("\x7f", 1) + std::string(15, '\xff')},
                                                       {std::string("\x01", 1)}, {false}, wide_type, &field, "UTC");
    EXPECT_FALSE(st.ok()) << st.to_string();
}

namespace {

std::string encode_magnitude(std::string digits, int width, bool negative) {
    std::string result(width, '\0');
    for (int i = width - 1; i >= 0; --i) {
        int remainder = 0;
        for (char& digit : digits) {
            const int next = remainder * 10 + (digit - '0');
            digit = '0' + next / 256;
            remainder = next % 256;
        }
        result[i] = static_cast<char>(remainder);
    }
    if (digits.find_first_not_of('0') != std::string::npos) throw std::runtime_error("decimal exceeds width");
    if (negative) {
        unsigned carry = 1;
        for (int i = width - 1; i >= 0; --i) {
            unsigned byte = (static_cast<uint8_t>(result[i]) ^ 0xff) + carry;
            result[i] = static_cast<char>(byte & 0xff);
            carry = byte >> 8;
        }
    }
    return result;
}

void read_decimal_file(::parquet::Type::type physical, int width, int precision, int scale) {
    namespace pq = ::parquet;
    SCOPED_TRACE(::testing::Message() << "physical=" << physical << " width=" << width << " precision=" << precision
                                      << " scale=" << scale);
    const std::string limit_digits(precision, '9');
    const int256_t limit = parse_int256(limit_digits);
    const int256_t expected[] = {-limit, int256_t(-1), int256_t(0), int256_t(1), limit};
    const int16_t definitions[] = {1, 1, 0, 1, 1, 1};
    auto node = pq::schema::PrimitiveNode::Make("value", pq::Repetition::OPTIONAL,
                                                pq::LogicalType::Decimal(precision, scale), physical, width);
    auto schema = std::static_pointer_cast<pq::schema::GroupNode>(
            pq::schema::GroupNode::Make("root", pq::Repetition::REQUIRED, {node}));
    auto sink_result = ::arrow::io::BufferOutputStream::Create();
    ASSERT_TRUE(sink_result.ok());
    auto sink = sink_result.ValueOrDie();
    pq::WriterProperties::Builder properties;
    properties.disable_dictionary();
    auto writer = pq::ParquetFileWriter::Open(sink, schema, properties.build());
    auto* column = writer->AppendRowGroup()->NextColumn();
    std::vector<std::string> bytes;
    if (physical == pq::Type::INT32) {
        std::vector<int32_t> values;
        for (auto value : expected) values.push_back(static_cast<int32_t>(static_cast<int128_t>(value)));
        static_cast<pq::Int32Writer*>(column)->WriteBatch(6, definitions, nullptr, values.data());
    } else if (physical == pq::Type::INT64) {
        std::vector<int64_t> values;
        for (auto value : expected) values.push_back(static_cast<int64_t>(static_cast<int128_t>(value)));
        static_cast<pq::Int64Writer*>(column)->WriteBatch(6, definitions, nullptr, values.data());
    } else {
        const std::string magnitudes[] = {limit_digits, "1", "0", "1", limit_digits};
        for (int index = 0; index < 5; ++index) {
            auto encoded = encode_magnitude(magnitudes[index], width, index < 2);
            if (physical == pq::Type::BYTE_ARRAY) {
                while (encoded.size() > 1 &&
                       ((static_cast<uint8_t>(encoded[0]) == 0 && !(static_cast<uint8_t>(encoded[1]) & 0x80)) ||
                        (static_cast<uint8_t>(encoded[0]) == 0xff && (static_cast<uint8_t>(encoded[1]) & 0x80)))) {
                    encoded.erase(0, 1);
                }
            }
            bytes.push_back(std::move(encoded));
        }
        if (physical == pq::Type::BYTE_ARRAY) {
            std::vector<pq::ByteArray> values;
            for (const auto& b : bytes) values.emplace_back(b.size(), reinterpret_cast<const uint8_t*>(b.data()));
            static_cast<pq::ByteArrayWriter*>(column)->WriteBatch(6, definitions, nullptr, values.data());
        } else {
            std::vector<pq::FixedLenByteArray> values;
            for (const auto& b : bytes) values.emplace_back(reinterpret_cast<const uint8_t*>(b.data()));
            static_cast<pq::FixedLenByteArrayWriter*>(column)->WriteBatch(6, definitions, nullptr, values.data());
        }
    }
    writer->Close();
    auto buffer_result = sink->Finish();
    ASSERT_TRUE(buffer_result.ok());
    auto buffer = buffer_result.ValueOrDie();
    if (physical == pq::Type::BYTE_ARRAY || physical == pq::Type::FIXED_LEN_BYTE_ARRAY) {
        auto apache_reader = pq::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
        auto apache_column = apache_reader->RowGroup(0)->Column(0);
        int16_t actual_definitions[6];
        int64_t values_read = 0;
        if (physical == pq::Type::BYTE_ARRAY) {
            pq::ByteArray actual[6];
            ASSERT_EQ(6, static_cast<pq::ByteArrayReader*>(apache_column.get())
                                 ->ReadBatch(6, actual_definitions, nullptr, actual, &values_read));
            ASSERT_EQ(5, values_read);
            for (int i = 0; i < 5; ++i)
                ASSERT_EQ(bytes[i], std::string(reinterpret_cast<const char*>(actual[i].ptr), actual[i].len));
        } else {
            pq::FixedLenByteArray actual[6];
            ASSERT_EQ(6, static_cast<pq::FixedLenByteArrayReader*>(apache_column.get())
                                 ->ReadBatch(6, actual_definitions, nullptr, actual, &values_read));
            ASSERT_EQ(5, values_read);
            for (int i = 0; i < 5; ++i)
                ASSERT_EQ(bytes[i], std::string(reinterpret_cast<const char*>(actual[i].ptr), width));
        }
        for (int i = 0; i < 6; ++i) ASSERT_EQ(definitions[i], actual_definitions[i]);
    }

    ObjectPool pool;
    FormatScannerStats stats{};
    HdfsScannerContext ctx;
    std::atomic<int32_t> lazy_counter{0};
    ctx.format_scan_context.lazy_column_coalesce_counter = &lazy_counter;
    ctx.format_scan_context.stats = &stats;
    ctx.format_scan_context.timezone = "UTC";
    ctx.format_scan_context.predicate_tree = &ctx.predicates.predicate_tree;
    const auto logical =
            precision <= 9 ? TYPE_DECIMAL32
                           : precision <= 18 ? TYPE_DECIMAL64 : precision <= 38 ? TYPE_DECIMAL128 : TYPE_DECIMAL256;
    const auto type = TypeDescriptor::from_logical_type(logical, -1, precision, scale);
    Utils::SlotDesc slots[] = {{"value", type, 0}, {""}};
    auto* tuple = Utils::create_tuple_descriptor(nullptr, &pool, slots);
    Utils::make_column_info_vector(tuple, &ctx.format_scan_context.materialized_columns);
    ctx.slot_descs = tuple->slots();
    ctx.format_scan_context.scan_range_offset = 0;
    ctx.format_scan_context.scan_range_length = buffer->size();
    auto file = std::make_shared<RandomAccessFile>(std::make_shared<io::StringInputStream>(buffer->ToString()),
                                                   "decimal-domain.parquet");
    FileReader reader(2, file.get(), buffer->size());
    ASSERT_OK(reader.init(&ctx.format_scan_context));
    const auto& schema_field = reader.get_file_metadata()->t_metadata().schema[1];
    ASSERT_EQ(static_cast<int>(physical), static_cast<int>(schema_field.type));
    if (physical == pq::Type::FIXED_LEN_BYTE_ARRAY) {
        ASSERT_TRUE(schema_field.__isset.type_length);
        ASSERT_EQ(width, schema_field.type_length);
    }
    ASSERT_TRUE(schema_field.__isset.logicalType && schema_field.logicalType.__isset.DECIMAL);
    ASSERT_EQ(precision, schema_field.logicalType.DECIMAL.precision);
    ASSERT_EQ(scale, schema_field.logicalType.DECIMAL.scale);
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type, true), 0);
    size_t row = 0;
    for (;;) {
        chunk->reset();
        Status status = reader.get_next(&chunk);
        if (status.is_end_of_file()) break;
        ASSERT_OK(status);
        const auto& output = chunk->get_column_by_index(0);
        for (size_t i = 0; i < chunk->num_rows(); ++i, ++row) {
            ASSERT_LT(row, 6);
            ASSERT_EQ(row == 2, output->is_null(i));
            if (row != 2) {
                const auto actual = logical == TYPE_DECIMAL32
                                            ? int256_t(output->get(i).get_int32())
                                            : logical == TYPE_DECIMAL64
                                                      ? int256_t(output->get(i).get_int64())
                                                      : logical == TYPE_DECIMAL128
                                                                ? int256_t(output->get(i).get_int128())
                                                                : output->get(i).get_int256();
                EXPECT_EQ(expected[row < 2 ? row : row - 1], actual);
            }
        }
    }
    ASSERT_EQ(6, row);
}

} // namespace

TEST_F(StatisticsHelperTest, DecimalPhysicalPrecisionScaleDomain) {
    namespace pq = ::parquet;
    const int max_fixed_precision[] = {0,  2,  4,  6,  9,  11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38,
                                       40, 43, 45, 47, 50, 52, 55, 57, 59, 62, 64, 67, 69, 71, 74, 76};
    size_t count = 0;
    for (const auto physical :
         {pq::Type::INT32, pq::Type::INT64, pq::Type::BYTE_ARRAY, pq::Type::FIXED_LEN_BYTE_ARRAY}) {
        const int max_width = physical == pq::Type::FIXED_LEN_BYTE_ARRAY ? 32 : 1;
        for (int w = 1; w <= max_width; ++w) {
            const int width = physical == pq::Type::BYTE_ARRAY ? 32 : w;
            const int max_precision =
                    physical == pq::Type::INT32
                            ? 9
                            : physical == pq::Type::INT64
                                      ? 18
                                      : physical == pq::Type::BYTE_ARRAY ? 76 : max_fixed_precision[w];
            for (int precision = 1; precision <= max_precision; ++precision) {
                for (int scale = 0; scale <= precision; ++scale) {
                    ASSERT_NO_FATAL_FAILURE(read_decimal_file(physical, width, precision, scale));
                    ++count;
                }
            }
        }
    }
    EXPECT_EQ(37268, count);
}

} // namespace starrocks::parquet

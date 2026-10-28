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

#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/column_page.h>
#include <parquet/statistics.h>

#include <cstring>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/types/int256.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/struct_column.h"
#include "common/object_pool.h"
#include "common/util/thrift_util.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/file/scanner/parquet_schema_builder.h"
#include "connector/hive/scanner/hdfs_scanner_context.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/statistics_helper.h"
#include "fs/fs.h"
#include "io/string_input_stream.h"
#include "parquet_test_util/util.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks::parquet {
namespace {
namespace pq = ::parquet;

std::string encode_decimal(int256_t value, int width) {
    std::string bytes(width, '\0');
    for (int i = width - 1; i >= 0; --i) {
        bytes[i] = static_cast<char>(value.low & 0xff);
        value >>= 8;
    }
    return bytes;
}

void check_decimal(pq::Type::type physical, int width, int precision, int source_scale, int target_scale, bool optional,
                   const int256_t& value, const int256_t& expected, bool overflow = false,
                   LogicalType target = TYPE_DECIMAL256, bool invalid_padding = false,
                   pq::Encoding::type encoding = pq::Encoding::PLAIN, bool dictionary = false, bool page_v2 = false,
                   bool legacy_only = false) {
    SCOPED_TRACE(::testing::Message() << "physical=" << physical << " width=" << width << " precision=" << precision
                                      << " scales=" << source_scale << "," << target_scale << " optional=" << optional);
    auto node = pq::schema::PrimitiveNode::Make("value", optional ? pq::Repetition::OPTIONAL : pq::Repetition::REQUIRED,
                                                pq::LogicalType::Decimal(precision, source_scale), physical, width);
    auto schema = std::static_pointer_cast<pq::schema::GroupNode>(
            pq::schema::GroupNode::Make("root", pq::Repetition::REQUIRED, {node}));
    auto sink_result = arrow::io::BufferOutputStream::Create();
    ASSERT_TRUE(sink_result.ok());
    auto sink = sink_result.ValueOrDie();
    pq::WriterProperties::Builder properties;
    if (dictionary) {
        properties.enable_dictionary();
    } else {
        properties.disable_dictionary()->encoding(encoding);
    }
    properties.data_page_version(page_v2 ? pq::ParquetDataPageVersion::V2 : pq::ParquetDataPageVersion::V1);
    auto writer = pq::ParquetFileWriter::Open(sink, schema, properties.build());
    auto* column = writer->AppendRowGroup()->NextColumn();
    const int16_t levels[] = {0, 1, 1, 1};
    const int rows = optional ? 4 : 3;
    const int16_t* defs = optional ? levels : nullptr;
    const int32_t ints32[] = {0, static_cast<int>(value), static_cast<int>(-value)};
    const int64_t ints64[] = {0, static_cast<long>(value), static_cast<long>(-value)};
    std::string bytes[] = {encode_decimal(int256_t(0), width), encode_decimal(value, width),
                           encode_decimal(-value, width)};
    if (invalid_padding) bytes[1][0] = 1;
    if (physical == pq::Type::BYTE_ARRAY && width <= 32) {
        for (auto& data : bytes) {
            while (data.size() > 1 &&
                   ((static_cast<uint8_t>(data[0]) == 0 && static_cast<uint8_t>(data[1]) < 0x80) ||
                    (static_cast<uint8_t>(data[0]) == 0xff && static_cast<uint8_t>(data[1]) >= 0x80))) {
                data.erase(0, 1);
            }
        }
    }
    if (physical == pq::Type::INT32) {
        static_cast<pq::Int32Writer*>(column)->WriteBatch(rows, defs, nullptr, ints32);
    } else if (physical == pq::Type::INT64) {
        static_cast<pq::Int64Writer*>(column)->WriteBatch(rows, defs, nullptr, ints64);
    } else if (physical == pq::Type::BYTE_ARRAY) {
        const pq::ByteArray values[] = {
                {static_cast<uint32_t>(bytes[0].size()), reinterpret_cast<const uint8_t*>(bytes[0].data())},
                {static_cast<uint32_t>(bytes[1].size()), reinterpret_cast<const uint8_t*>(bytes[1].data())},
                {static_cast<uint32_t>(bytes[2].size()), reinterpret_cast<const uint8_t*>(bytes[2].data())}};
        static_cast<pq::ByteArrayWriter*>(column)->WriteBatch(rows, defs, nullptr, values);
    } else {
        const pq::FixedLenByteArray values[] = {
                pq::FixedLenByteArray(reinterpret_cast<const uint8_t*>(bytes[0].data())),
                pq::FixedLenByteArray(reinterpret_cast<const uint8_t*>(bytes[1].data())),
                pq::FixedLenByteArray(reinterpret_cast<const uint8_t*>(bytes[2].data()))};
        static_cast<pq::FixedLenByteArrayWriter*>(column)->WriteBatch(rows, defs, nullptr, values);
    }
    writer->Close();
    auto buffer_result = sink->Finish();
    ASSERT_TRUE(buffer_result.ok());
    auto buffer = buffer_result.ValueOrDie();

    auto apache = pq::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
    const auto* descriptor = apache->metadata()->schema()->Column(0);
    ASSERT_EQ(physical, descriptor->physical_type());
    ASSERT_EQ(precision, descriptor->type_precision());
    ASSERT_EQ(source_scale, descriptor->type_scale());
    ASSERT_EQ(optional ? 1 : 0, descriptor->max_definition_level());
    auto chunk_metadata = apache->metadata()->RowGroup(0)->ColumnChunk(0);
    bool found_encoding = false;
    for (const auto& stat : chunk_metadata->encoding_stats()) {
        if (stat.page_type == pq::PageType::DATA_PAGE || stat.page_type == pq::PageType::DATA_PAGE_V2) {
            EXPECT_EQ(dictionary ? pq::Encoding::RLE_DICTIONARY : encoding, stat.encoding);
            found_encoding = true;
        }
    }
    ASSERT_TRUE(found_encoding);
    auto row_group = apache->RowGroup(0);
    auto pages = row_group->GetColumnPageReader(0);
    bool found_data_page = false;
    while (auto page = pages->NextPage()) {
        if (page->type() == pq::PageType::DATA_PAGE || page->type() == pq::PageType::DATA_PAGE_V2) {
            EXPECT_EQ(page_v2 ? pq::PageType::DATA_PAGE_V2 : pq::PageType::DATA_PAGE, page->type());
            EXPECT_EQ(dictionary ? pq::Encoding::RLE_DICTIONARY : encoding,
                      static_cast<const pq::DataPage*>(page.get())->encoding());
            found_data_page = true;
        }
    }
    ASSERT_TRUE(found_data_page);
    if (precision == 76 && value > int256_t(0) && !invalid_padding &&
        (physical == pq::Type::BYTE_ARRAY || physical == pq::Type::FIXED_LEN_BYTE_ARRAY)) {
        auto stats = apache->metadata()->RowGroup(0)->ColumnChunk(0)->statistics();
        ASSERT_NE(nullptr, stats);
        ASSERT_TRUE(stats->HasMinMax());
        EXPECT_EQ(bytes[2], stats->EncodeMin());
        EXPECT_EQ(bytes[1], stats->EncodeMax());
        ASSERT_TRUE(stats->HasNullCount());
        EXPECT_EQ(optional ? 1 : 0, stats->null_count());
    }
    if (physical == pq::Type::FIXED_LEN_BYTE_ARRAY) {
        ASSERT_EQ(width, descriptor->type_length());
    }
    auto read_column = apache->RowGroup(0)->Column(0);
    int16_t actual_defs[4]{};
    int64_t count = 0;
    if (physical == pq::Type::BYTE_ARRAY) {
        pq::ByteArray actual[4];
        ASSERT_EQ(rows, static_cast<pq::ByteArrayReader*>(read_column.get())
                                ->ReadBatch(rows, actual_defs, nullptr, actual, &count));
        ASSERT_EQ(3, count);
        for (int i = 0; i < 3; ++i) {
            ASSERT_EQ(bytes[i].size(), actual[i].len);
            EXPECT_EQ(0, std::memcmp(bytes[i].data(), actual[i].ptr, actual[i].len));
        }
    } else if (physical == pq::Type::FIXED_LEN_BYTE_ARRAY) {
        pq::FixedLenByteArray actual[4];
        ASSERT_EQ(rows, static_cast<pq::FixedLenByteArrayReader*>(read_column.get())
                                ->ReadBatch(rows, actual_defs, nullptr, actual, &count));
        ASSERT_EQ(3, count);
        for (int i = 0; i < 3; ++i) EXPECT_EQ(0, std::memcmp(bytes[i].data(), actual[i].ptr, width));
    } else if (physical == pq::Type::INT32) {
        int32_t actual[4];
        ASSERT_EQ(rows, static_cast<pq::Int32Reader*>(read_column.get())
                                ->ReadBatch(rows, actual_defs, nullptr, actual, &count));
        ASSERT_EQ(3, count);
        for (int i = 0; i < 3; ++i) EXPECT_EQ(ints32[i], actual[i]);
    } else {
        int64_t actual[4];
        ASSERT_EQ(rows, static_cast<pq::Int64Reader*>(read_column.get())
                                ->ReadBatch(rows, actual_defs, nullptr, actual, &count));
        ASSERT_EQ(3, count);
        for (int i = 0; i < 3; ++i) EXPECT_EQ(ints64[i], actual[i]);
    }
    if (optional) {
        for (int i = 0; i < rows; ++i) EXPECT_EQ(levels[i], actual_defs[i]);
    }

    std::string data = buffer->ToString();
    if (legacy_only) {
        uint32_t footer_length = 0;
        for (int i = 0; i < 4; ++i) {
            footer_length |= static_cast<uint32_t>(static_cast<uint8_t>(data[data.size() - 8 + i])) << (8 * i);
        }
        ASSERT_LE(footer_length, data.size() - 8);
        tparquet::FileMetaData footer;
        ASSERT_OK(
                deserialize_thrift_msg(reinterpret_cast<const uint8_t*>(data.data()) + data.size() - 8 - footer_length,
                                       &footer_length, TProtocolType::COMPACT, &footer));
        ASSERT_EQ(2, footer.schema.size());
        ASSERT_TRUE(footer.schema[1].__isset.converted_type);
        ASSERT_TRUE(footer.schema[1].__isset.logicalType && footer.schema[1].logicalType.__isset.DECIMAL);
        footer.schema[1].__isset.logicalType = false;
        data.resize(data.size() - 8 - footer_length);
        std::string serialized;
        ThriftSerializer serializer(true, 1024);
        ASSERT_OK(serializer.serialize(&footer, &serialized));
        data.append(serialized);
        for (int i = 0; i < 4; ++i) data.push_back(static_cast<char>((serialized.size() >> (8 * i)) & 0xff));
        data.append("PAR1");
    }

    ObjectPool pool;
    auto* ctx = pool.add(new HdfsScannerContext());
    FormatScannerStats stats{};
    std::atomic<int32_t> lazy_counter{0};
    ctx->format_scan_context.lazy_column_coalesce_counter = &lazy_counter;
    ctx->format_scan_context.timezone = "UTC";
    ctx->format_scan_context.stats = &stats;
    ctx->format_scan_context.predicate_tree = &ctx->predicates.predicate_tree;
    const int target_precision = target == TYPE_DECIMAL256
                                         ? 76
                                         : target == TYPE_DECIMAL128
                                                   ? 38
                                                   : target == TYPE_DECIMALV2 ? 27 : target == TYPE_DECIMAL64 ? 18 : 9;
    const auto type = TypeDescriptor::from_logical_type(target, -1, target_precision, target_scale);
    if (precision > 38 && (target == TYPE_DECIMAL32 || target == TYPE_DECIMAL64 || target == TYPE_DECIMALV2)) {
        ParquetField field;
        field.physical_column_index = 0;
        field.physical_type = static_cast<tparquet::Type::type>(physical);
        field.type_length = width;
        field.precision = precision;
        field.scale = source_scale;
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
        auto src = converter->create_src_column();
        ASSERT_TRUE(src->append_nulls(1));
        ASSERT_TRUE(src->append_strings(std::vector<Slice>{Slice(bytes[1])}));
        auto dst = ColumnHelper::create_column(type, true);
        Status status = converter->convert(src.get(), dst.get());
        if (overflow) {
            ASSERT_TRUE(status.is_invalid_argument()) << status.to_string();
        } else {
            ASSERT_OK(status);
            ASSERT_EQ(2, dst->size());
            ASSERT_TRUE(dst->is_null(0));
            ASSERT_FALSE(dst->is_null(1));
            if (target == TYPE_DECIMAL32) {
                EXPECT_EQ(static_cast<int128_t>(expected), dst->get(1).get_int32());
            } else if (target == TYPE_DECIMAL64) {
                EXPECT_EQ(static_cast<int128_t>(expected), dst->get(1).get_int64());
            } else {
                EXPECT_EQ(static_cast<int128_t>(expected), dst->get(1).get_decimal().value());
            }
        }
    }
    Utils::SlotDesc slots[] = {{"value", type, 0}, {""}};
    auto* tuple = Utils::create_tuple_descriptor(nullptr, &pool, slots);
    Utils::make_column_info_vector(tuple, &ctx->format_scan_context.materialized_columns);
    ctx->slot_descs = tuple->slots();
    ctx->format_scan_context.scan_range_offset = 0;
    ctx->format_scan_context.scan_range_length = data.size();
    auto file = std::make_shared<RandomAccessFile>(std::make_shared<io::StringInputStream>(data), "decimal256.parquet");
    FileReader reader(8, file.get(), data.size());
    ASSERT_OK(reader.init(&ctx->format_scan_context));
    const auto& field = reader.get_file_metadata()->t_metadata().schema[1];
    ASSERT_TRUE(field.__isset.converted_type);
    EXPECT_EQ(tparquet::ConvertedType::DECIMAL, field.converted_type);
    ASSERT_TRUE(field.__isset.precision);
    ASSERT_TRUE(field.__isset.scale);
    EXPECT_EQ(precision, field.precision);
    EXPECT_EQ(source_scale, field.scale);
    EXPECT_EQ(!legacy_only, field.__isset.logicalType);
    if (!legacy_only) {
        ASSERT_TRUE(field.logicalType.__isset.DECIMAL);
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(type, true), 0);
    auto status = reader.get_next(&chunk);
    if (overflow) {
        ASSERT_TRUE(status.is_invalid_argument()) << status.to_string();
        return;
    }
    ASSERT_OK(status);
    ASSERT_EQ(rows, chunk->num_rows());
    const auto& result = chunk->get_column_by_index(0);
    if (optional) {
        ASSERT_TRUE(result->is_null(0));
    }
    const int offset = optional ? 1 : 0;
    for (int i = 0; i < 3; ++i) {
        ASSERT_FALSE(result->is_null(offset + i));
        const int256_t want = i == 0 ? int256_t(0) : i == 1 ? expected : -expected;
        if (target == TYPE_DECIMAL256) {
            EXPECT_EQ(want, result->get(offset + i).get_int256());
        } else if (target == TYPE_DECIMAL128) {
            EXPECT_EQ(static_cast<int128_t>(want), result->get(offset + i).get_int128());
        } else if (target == TYPE_DECIMAL64) {
            EXPECT_EQ(static_cast<int128_t>(want), result->get(offset + i).get_int64());
        } else if (target == TYPE_DECIMAL32) {
            EXPECT_EQ(static_cast<int128_t>(want), result->get(offset + i).get_int32());
        } else {
            ASSERT_EQ(TYPE_DECIMALV2, target);
            EXPECT_EQ(static_cast<int128_t>(want), result->get(offset + i).get_decimal().value());
        }
    }
    chunk->reset();
    EXPECT_TRUE(reader.get_next(&chunk).is_end_of_file());
}

TEST(Decimal256ParquetTest, PhysicalEncodings) {
    const int precisions[] = {40, 43, 45, 47, 50, 52, 55, 57, 59, 62, 64, 67, 69, 71, 74, 76};
    for (int width = 17; width <= 32; ++width) {
        int precision = precisions[width - 17];
        int256_t max = parse_int256(std::string(precision, '9'));
        for (bool optional : {false, true}) {
            check_decimal(pq::Type::FIXED_LEN_BYTE_ARRAY, width, precision, 0, 0, optional, max, max);
            check_decimal(pq::Type::BYTE_ARRAY, width, precision, 0, 0, optional, max, max);
        }
    }
    const int256_t max = parse_int256(std::string(76, '9'));
    const std::string positive(
            "\x16\x1b\xcc\xa7\x11\x99\x15\xb5\x07\x64\xb4\xab\xe8\x65\x29\x79"
            "\x77\x75\xa5\xf1\x71\x95\x0f\xff\xff\xff\xff\xff\xff\xff\xff\xff",
            32);
    const std::string negative(
            "\xe9\xe4\x33\x58\xee\x66\xea\x4a\xf8\x9b\x4b\x54\x17\x9a\xd6\x86"
            "\x88\x8a\x5a\x0e\x8e\x6a\xf0\x00\x00\x00\x00\x00\x00\x00\x00\x01",
            32);
    ASSERT_EQ(positive, encode_decimal(max, 32));
    ASSERT_EQ(negative, encode_decimal(-max, 32));
    for (auto physical : {pq::Type::BYTE_ARRAY, pq::Type::FIXED_LEN_BYTE_ARRAY}) {
        check_decimal(physical, 33, 76, 0, 0, true, max, max);
    }
    for (auto physical : {pq::Type::INT32, pq::Type::INT64}) {
        int precision = physical == pq::Type::INT32 ? 9 : 18;
        int256_t max = parse_int256(std::string(precision, '9'));
        for (bool optional : {false, true})
            check_decimal(physical, physical == pq::Type::INT32 ? 4 : 8, precision, 0, 0, optional, max, max);
    }
}

TEST(Decimal256ParquetTest, FilesSchemaInference) {
    for (auto physical : {pq::Type::BYTE_ARRAY, pq::Type::FIXED_LEN_BYTE_ARRAY}) {
        for (int precision : {38, 39, 76, 77}) {
            SCOPED_TRACE(::testing::Message() << "physical=" << physical << " precision=" << precision);
            auto node = pq::schema::PrimitiveNode::Make("value", pq::Repetition::OPTIONAL,
                                                        pq::LogicalType::Decimal(precision, 7), physical, 33);
            TypeDescriptor type;
            auto status = get_parquet_type(node, &type);
            if (precision > 76) {
                EXPECT_TRUE(status.is_invalid_argument()) << status.to_string();
            } else {
                ASSERT_OK(status);
                EXPECT_EQ(precision == 38 ? TYPE_DECIMAL128 : TYPE_DECIMAL256, type.type);
                EXPECT_EQ(precision, type.precision);
                EXPECT_EQ(7, type.scale);
            }
        }
    }
}

TEST(Decimal256ParquetTest, FilesNestedSchemaInference) {
    for (auto physical : {pq::Type::BYTE_ARRAY, pq::Type::FIXED_LEN_BYTE_ARRAY}) {
        for (int precision : {39, 76, 77}) {
            SCOPED_TRACE(::testing::Message() << "physical=" << physical << " precision=" << precision);
            auto leaf = pq::schema::PrimitiveNode::Make("price", pq::Repetition::OPTIONAL,
                                                        pq::LogicalType::Decimal(precision, 7), physical, 33);
            auto group = pq::schema::GroupNode::Make("payload", pq::Repetition::OPTIONAL, {leaf});
            TypeDescriptor type;
            auto status = get_parquet_type(group, &type);
            if (precision > 76) {
                EXPECT_TRUE(status.is_invalid_argument()) << status.to_string();
            } else {
                ASSERT_OK(status);
                ASSERT_EQ(TYPE_STRUCT, type.type);
                ASSERT_EQ(1, type.children.size());
                EXPECT_EQ(TYPE_DECIMAL256, type.children[0].type);
                EXPECT_EQ(precision, type.children[0].precision);
                EXPECT_EQ(7, type.children[0].scale);
            }
        }
    }
}

TEST(Decimal256ParquetTest, FilesUnsupportedNestedFallback) {
    auto element = pq::schema::PrimitiveNode::Make("element", pq::Repetition::REPEATED, pq::Type::INT32);
    auto invalid_list =
            pq::schema::GroupNode::Make("items", pq::Repetition::REPEATED, {element}, pq::ConvertedType::LIST);
    TypeDescriptor child_type;
    EXPECT_TRUE(get_parquet_type(invalid_list, &child_type).is_not_supported());
    auto group = pq::schema::GroupNode::Make("payload", pq::Repetition::OPTIONAL, {invalid_list});
    TypeDescriptor type;
    ASSERT_OK(get_parquet_type(group, &type));
    EXPECT_EQ(TYPE_VARCHAR, type.type);
}

TEST(Decimal256ParquetTest, Statistics) {
    const int256_t max = parse_int256(std::string(76, '9'));
    ParquetField field;
    field.physical_column_index = 0;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.precision = 76;
    field.scale = 0;
    const auto type = TypeDescriptor::from_logical_type(TYPE_DECIMAL256, -1, 76, 0);
    auto min = ColumnHelper::create_column(type, true);
    auto max_column = ColumnHelper::create_column(type, true);
    ASSERT_OK(StatisticsHelper::decode_min_max_into_columns(min, max_column, {encode_decimal(-max, 32), ""},
                                                            {encode_decimal(max, 32), ""}, {false, true}, type, &field,
                                                            "UTC"));
    ASSERT_EQ(2, min->size());
    ASSERT_EQ(2, max_column->size());
    EXPECT_EQ(-max, min->get(0).get_int256());
    EXPECT_EQ(max, max_column->get(0).get_int256());
    EXPECT_TRUE(min->is_null(1));
    EXPECT_TRUE(max_column->is_null(1));

    min = ColumnHelper::create_column(type, true);
    max_column = ColumnHelper::create_column(type, true);
    auto status = StatisticsHelper::decode_min_max_into_columns(min, max_column, {""}, {encode_decimal(max, 32)},
                                                                {false}, type, &field, "UTC");
    EXPECT_TRUE(status.is_corruption()) << status.to_string();
}

TEST(Decimal256ParquetTest, ScaleAndOverflow) {
    const int256_t max76 = parse_int256(std::string(76, '9'));
    const int256_t max38 = parse_int256(std::string(38, '9'));
    for (auto physical : {pq::Type::BYTE_ARRAY, pq::Type::FIXED_LEN_BYTE_ARRAY}) {
        check_decimal(physical, 32, 76, 38, 0, true, max76, max38);
        check_decimal(physical, 32, 76, 38, 38, true, max76, max76);
        check_decimal(physical, 32, 76, 76, 0, true, max76, int256_t(0));
        check_decimal(physical, 32, 76, 0, 1, true, max76, int256_t(0), true);
        check_decimal(physical, 32, 76, 1, 0, true, INT256_MIN, int256_t(0), true);
        check_decimal(physical, 33, 76, 0, 0, true, max76, int256_t(0), true, TYPE_DECIMAL256, true);
        check_decimal(physical, 32, 76, 0, 0, true, max76, int256_t(0), true, TYPE_DECIMAL128);
        check_decimal(physical, 32, 76, 38, 0, true, max76, max38, false, TYPE_DECIMAL128);
    }
    for (auto physical : {pq::Type::INT32, pq::Type::INT64}) {
        int precision = physical == pq::Type::INT32 ? 9 : 18;
        int width = physical == pq::Type::INT32 ? 4 : 8;
        check_decimal(physical, width, precision, 2, 3, true, int256_t(1234), int256_t(12340));
        check_decimal(physical, width, precision, 2, 1, true, int256_t(1234), int256_t(123));
        check_decimal(physical, width, precision, 0, 75, true, int256_t(1), parse_int256("1" + std::string(75, '0')));
    }
}

TEST(Decimal256ParquetTest, EncodingAndPageVersions) {
    struct Case {
        pq::Type::type physical;
        pq::Encoding::type encoding;
        bool dictionary;
    };
    const Case cases[] = {{pq::Type::BYTE_ARRAY, pq::Encoding::PLAIN, false},
                          {pq::Type::BYTE_ARRAY, pq::Encoding::PLAIN, true},
                          {pq::Type::BYTE_ARRAY, pq::Encoding::DELTA_BYTE_ARRAY, false},
                          {pq::Type::BYTE_ARRAY, pq::Encoding::DELTA_LENGTH_BYTE_ARRAY, false},
                          {pq::Type::FIXED_LEN_BYTE_ARRAY, pq::Encoding::PLAIN, false},
                          {pq::Type::FIXED_LEN_BYTE_ARRAY, pq::Encoding::PLAIN, true},
                          {pq::Type::FIXED_LEN_BYTE_ARRAY, pq::Encoding::DELTA_BYTE_ARRAY, false},
                          {pq::Type::FIXED_LEN_BYTE_ARRAY, pq::Encoding::BYTE_STREAM_SPLIT, false}};
    const int256_t max76 = parse_int256(std::string(76, '9'));
    for (const auto& c : cases) {
        for (bool optional : {false, true}) {
            for (bool page_v2 : {false, true}) {
                for (bool legacy_only : {false, true}) {
                    SCOPED_TRACE(::testing::Message() << "encoding=" << c.encoding << " dictionary=" << c.dictionary
                                                      << " page_v2=" << page_v2 << " legacy_only=" << legacy_only);
                    check_decimal(c.physical, 32, 76, 38, 38, optional, max76, max76, false, TYPE_DECIMAL256, false,
                                  c.encoding, c.dictionary, page_v2, legacy_only);
                }
            }
        }
    }
}

TEST(Decimal256ParquetTest, IntegerEncodingAndPageVersions) {
    struct Case {
        pq::Encoding::type encoding;
        bool dictionary;
    };
    const Case cases[] = {{pq::Encoding::PLAIN, false},
                          {pq::Encoding::PLAIN, true},
                          {pq::Encoding::DELTA_BINARY_PACKED, false},
                          {pq::Encoding::BYTE_STREAM_SPLIT, false}};
    for (auto physical : {pq::Type::INT32, pq::Type::INT64}) {
        const int precision = physical == pq::Type::INT32 ? 9 : 18;
        const int width = physical == pq::Type::INT32 ? 4 : 8;
        const int256_t value = parse_int256(std::string(precision, '9'));
        const int256_t expected = parse_int256(std::string(precision, '9') + "0");
        for (const auto& c : cases) {
            for (bool optional : {false, true}) {
                for (bool page_v2 : {false, true}) {
                    for (bool legacy_only : {false, true}) {
                        SCOPED_TRACE(::testing::Message() << "encoding=" << c.encoding << " dictionary=" << c.dictionary
                                                          << " page_v2=" << page_v2 << " legacy_only=" << legacy_only);
                        check_decimal(physical, width, precision, 2, 3, optional, value, expected, false,
                                      TYPE_DECIMAL256, false, c.encoding, c.dictionary, page_v2, legacy_only);
                    }
                }
            }
        }
    }
}

TEST(Decimal256ParquetTest, NarrowWideDecimals) {
    const int256_t max76 = parse_int256(std::string(76, '9'));
    for (auto physical : {pq::Type::BYTE_ARRAY, pq::Type::FIXED_LEN_BYTE_ARRAY}) {
        for (auto [target, scale] :
             {std::pair{TYPE_DECIMAL32, 9}, std::pair{TYPE_DECIMAL64, 18}, std::pair{TYPE_DECIMALV2, 9}}) {
            const int256_t expected = parse_int256(std::string(scale, '9'));
            check_decimal(physical, 32, 76, 76, scale, true, max76, expected, false, target);
            check_decimal(physical, 32, 76, 40, scale, true, max76, int256_t(0), true, target);
            check_decimal(physical, 32, 76, 40, scale, true, -max76, int256_t(0), true, target);
        }
    }
}

TEST(Decimal256ParquetTest, NullableStruct) {
    const int256_t max = parse_int256(std::string(76, '9'));
    const std::string bytes[] = {encode_decimal(max, 32), encode_decimal(-max, 32)};
    auto price = pq::schema::PrimitiveNode::Make("price", pq::Repetition::OPTIONAL, pq::LogicalType::Decimal(76, 0),
                                                 pq::Type::BYTE_ARRAY);
    auto payload = pq::schema::GroupNode::Make("payload", pq::Repetition::OPTIONAL, {price});
    auto schema = std::static_pointer_cast<pq::schema::GroupNode>(
            pq::schema::GroupNode::Make("root", pq::Repetition::REQUIRED, {payload}));
    auto sink_result = arrow::io::BufferOutputStream::Create();
    ASSERT_TRUE(sink_result.ok());
    auto sink = sink_result.ValueOrDie();
    pq::WriterProperties::Builder properties;
    auto writer = pq::ParquetFileWriter::Open(sink, schema, properties.disable_dictionary()->build());
    const int16_t levels[] = {0, 1, 2, 2};
    const pq::ByteArray values[] = {{32, reinterpret_cast<const uint8_t*>(bytes[0].data())},
                                    {32, reinterpret_cast<const uint8_t*>(bytes[1].data())}};
    static_cast<pq::ByteArrayWriter*>(writer->AppendRowGroup()->NextColumn())->WriteBatch(4, levels, nullptr, values);
    writer->Close();
    auto buffer_result = sink->Finish();
    ASSERT_TRUE(buffer_result.ok());
    auto buffer = buffer_result.ValueOrDie();

    auto apache = pq::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
    const auto* descriptor = apache->metadata()->schema()->Column(0);
    ASSERT_EQ(pq::Type::BYTE_ARRAY, descriptor->physical_type());
    ASSERT_EQ(76, descriptor->type_precision());
    ASSERT_EQ(0, descriptor->type_scale());
    ASSERT_EQ(2, descriptor->max_definition_level());
    auto source = apache->RowGroup(0)->Column(0);
    int16_t actual_levels[4];
    pq::ByteArray actual_values[4];
    int64_t count = 0;
    ASSERT_EQ(4, static_cast<pq::ByteArrayReader*>(source.get())
                         ->ReadBatch(4, actual_levels, nullptr, actual_values, &count));
    ASSERT_EQ(2, count);
    for (int i = 0; i < 4; ++i) EXPECT_EQ(levels[i], actual_levels[i]);
    for (int i = 0; i < 2; ++i) {
        ASSERT_EQ(32, actual_values[i].len);
        EXPECT_EQ(0, std::memcmp(bytes[i].data(), actual_values[i].ptr, 32));
    }

    ObjectPool pool;
    auto* state = pool.add(new RuntimeState(TQueryGlobals()));
    FragmentDictState dict_state;
    state->set_fragment_dict_state(&dict_state);
    auto* ctx = pool.add(new HdfsScannerContext());
    FormatScannerStats stats{};
    std::atomic<int32_t> lazy_counter{0};
    ctx->format_scan_context.lazy_column_coalesce_counter = &lazy_counter;
    ctx->format_scan_context.timezone = "UTC";
    ctx->format_scan_context.stats = &stats;
    ctx->format_scan_context.predicate_tree = &ctx->predicates.predicate_tree;
    const auto decimal_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL256, -1, 76, 0);
    const auto struct_type = TypeDescriptor::create_struct_type({"price"}, {decimal_type});
    Utils::SlotDesc slots[] = {{"payload", struct_type, 0}, {""}};
    auto* tuple = Utils::create_tuple_descriptor(state, &pool, slots);
    Utils::make_column_info_vector(tuple, &ctx->format_scan_context.materialized_columns);
    ctx->slot_descs = tuple->slots();
    ctx->format_scan_context.scan_range_offset = 0;
    ctx->format_scan_context.scan_range_length = buffer->size();
    auto file = std::make_shared<RandomAccessFile>(std::make_shared<io::StringInputStream>(buffer->ToString()),
                                                   "nested-decimal256.parquet");
    FileReader reader(8, file.get(), buffer->size());
    ASSERT_OK(reader.init(&ctx->format_scan_context));
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(struct_type, true), 0);
    ASSERT_OK(reader.get_next(&chunk));
    ASSERT_EQ(4, chunk->num_rows());
    const auto& parent = chunk->get_column_by_index(0);
    EXPECT_TRUE(parent->is_null(0));
    for (int i = 1; i < 4; ++i) ASSERT_FALSE(parent->is_null(i));
    auto* structure = down_cast<const StructColumn*>(ColumnHelper::get_data_column(parent.get()));
    auto field = structure->field_column("price");
    ASSERT_TRUE(field.ok());
    ASSERT_TRUE(field.value()->is_null(1));
    ASSERT_FALSE(field.value()->is_null(2));
    ASSERT_FALSE(field.value()->is_null(3));
    EXPECT_EQ(max, field.value()->get(2).get_int256());
    EXPECT_EQ(-max, field.value()->get(3).get_int256());
}

} // namespace
} // namespace starrocks::parquet

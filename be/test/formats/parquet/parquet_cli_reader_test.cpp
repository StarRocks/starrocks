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

#include "formats/parquet/parquet_cli_reader.h"

#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/page_index.h>

#include <filesystem>
#include <optional>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "column/column_helper.h"
#include "common/util/thrift_util.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/hive/scanner/hdfs_scanner_context.h"
#include "formats/parquet/file_reader.h"
#include "io/string_input_stream.h"
#include "parquet_test_util/util.h"
#include "parquet_ut_base.h"
#include "runtime/runtime_state.h"

namespace starrocks::parquet {
class ParquetCLIReaderTest : public testing::Test {
public:
    ParquetCLIReaderTest() = default;
    ~ParquetCLIReaderTest() override = default;

protected:
    void traverse_directory_add_parquet(std::vector<std::string>& paths, const std::filesystem::path& directory,
                                        const std::string& suffix = "") {
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            if (entry.is_directory()) {
                traverse_directory_add_parquet(paths, entry.path(), suffix);
            } else if (entry.is_regular_file()) {
                const std::string& path = entry.path().string();
                if (!suffix.empty() && _end_with(path, suffix)) {
                    paths.emplace_back(path);
                } else if (suffix.empty()) {
                    paths.emplace_back(path);
                }
            }
        }
    }

    void print_res(const std::string& filepath, const Status& status) {
        std::string res;
        if (status.ok()) {
            res = fmt::format("File path: {}, OK", filepath);
        } else {
            res = fmt::format("File path: {}, FAILED: {}", filepath, status.message());
        }
        std::cout << res << std::endl;
    }

private:
    bool _end_with(const std::string& str, const std::string& suffix) {
        if (str.length() < suffix.length()) {
            return false;
        }
        return str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0;
    }
};

TEST_F(ParquetCLIReaderTest, ByteArrayDecimalFullRead) {
    {
        const std::string path = "./be/test/formats/parquet/test_data/column_converter/byte_array.parquet";
        ParquetCLIReader reader{path};
        auto status = reader.init();
        EXPECT_TRUE(status.is_not_supported()) << status.to_string();
        EXPECT_NE(std::string::npos, status.message().find("to `JSON`"));
    }
    {
        const std::string path = "./be/test/formats/parquet/test_data/data_with_page_index_and_bloom_filter.parquet";
        ParquetCLIReader reader{path};
        ASSERT_OK(reader.init());
        auto result = reader.debug(1);
        ASSERT_OK(result.status());
    }
}

TEST_F(ParquetCLIReaderTest, DecimalPhysicalTypesAndEncodings) {
    using Physical = ::parquet::Type;
    using Encoding = ::parquet::Encoding;
    struct Case {
        Physical::type physical;
        Encoding::type encoding;
        bool dictionary;
        int width = 8;
    };
    const std::vector<Case> cases = {
            {Physical::INT32, Encoding::PLAIN, false},
            {Physical::INT32, Encoding::PLAIN, true},
            {Physical::INT32, Encoding::DELTA_BINARY_PACKED, false},
            {Physical::INT32, Encoding::BYTE_STREAM_SPLIT, false},
            {Physical::INT64, Encoding::PLAIN, false},
            {Physical::INT64, Encoding::PLAIN, true},
            {Physical::INT64, Encoding::DELTA_BINARY_PACKED, false},
            {Physical::INT64, Encoding::BYTE_STREAM_SPLIT, false},
            {Physical::FIXED_LEN_BYTE_ARRAY, Encoding::PLAIN, false},
            {Physical::FIXED_LEN_BYTE_ARRAY, Encoding::PLAIN, true},
            {Physical::FIXED_LEN_BYTE_ARRAY, Encoding::DELTA_BYTE_ARRAY, false},
            {Physical::FIXED_LEN_BYTE_ARRAY, Encoding::BYTE_STREAM_SPLIT, false},
            {Physical::FIXED_LEN_BYTE_ARRAY, Encoding::PLAIN, false, 17},
            {Physical::FIXED_LEN_BYTE_ARRAY, Encoding::PLAIN, true, 20},
            {Physical::BYTE_ARRAY, Encoding::PLAIN, false},
            {Physical::BYTE_ARRAY, Encoding::PLAIN, true},
            {Physical::BYTE_ARRAY, Encoding::DELTA_BYTE_ARRAY, false},
            {Physical::BYTE_ARRAY, Encoding::DELTA_LENGTH_BYTE_ARRAY, false},
    };
    for (const auto& c : cases) {
        for (int precision : {9, 18}) {
            if (c.physical == Physical::INT32 && precision == 18) continue;
            for (int scale : {0, 2, precision}) {
                for (bool required : {false, true}) {
                    for (bool legacy : {false, true}) {
                        SCOPED_TRACE(
                                fmt::format("physical={}, encoding={}, dictionary={}, precision={}, scale={}, "
                                            "required={}, legacy={}, width={}",
                                            static_cast<int>(c.physical), static_cast<int>(c.encoding), c.dictionary,
                                            precision, scale, required, legacy, c.width));
                        const int64_t limit = precision == 9 ? 999999999LL : 999999999999999999LL;
                        const std::vector<int64_t> expected = {limit, -limit, 0, 128, -129};
                        const int16_t levels[] = {1, 1, 0, 1, 1, 1};
                        const int num_rows = required ? 5 : 6;
                        const int16_t* defs = required ? nullptr : levels;
                        auto decimal = ::parquet::schema::PrimitiveNode::Make(
                                "value", required ? ::parquet::Repetition::REQUIRED : ::parquet::Repetition::OPTIONAL,
                                ::parquet::LogicalType::Decimal(precision, scale), c.physical, c.width);
                        auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
                                ::parquet::schema::GroupNode::Make("root", ::parquet::Repetition::REQUIRED, {decimal}));
                        auto sink_result = arrow::io::BufferOutputStream::Create();
                        ASSERT_TRUE(sink_result.ok());
                        auto sink = sink_result.ValueOrDie();
                        ::parquet::WriterProperties::Builder properties;
                        if (c.dictionary)
                            properties.enable_dictionary();
                        else
                            properties.disable_dictionary()->encoding(c.encoding);
                        auto writer = ::parquet::ParquetFileWriter::Open(sink, schema, properties.build());
                        auto* column = writer->AppendRowGroup()->NextColumn();
                        if (c.physical == Physical::INT32) {
                            std::vector<int32_t> values(expected.begin(), expected.end());
                            static_cast<::parquet::Int32Writer*>(column)->WriteBatch(num_rows, defs, nullptr,
                                                                                     values.data());
                        } else if (c.physical == Physical::INT64) {
                            static_cast<::parquet::Int64Writer*>(column)->WriteBatch(num_rows, defs, nullptr,
                                                                                     expected.data());
                        } else {
                            std::vector<std::string> bytes;
                            for (int64_t value : expected) {
                                std::string encoded(c.width, value < 0 ? '\xff' : '\0');
                                uint64_t bits = static_cast<uint64_t>(value);
                                for (int i = c.width - 1; i >= c.width - 8; --i) {
                                    encoded[i] = static_cast<char>(bits & 0xff);
                                    bits >>= 8;
                                }
                                if (c.physical == Physical::BYTE_ARRAY) {
                                    while (encoded.size() > 1 && ((static_cast<uint8_t>(encoded[0]) == 0 &&
                                                                   (static_cast<uint8_t>(encoded[1]) & 0x80) == 0) ||
                                                                  (static_cast<uint8_t>(encoded[0]) == 0xff &&
                                                                   (static_cast<uint8_t>(encoded[1]) & 0x80) != 0))) {
                                        encoded.erase(0, 1);
                                    }
                                }
                                bytes.push_back(std::move(encoded));
                            }
                            if (c.physical == Physical::BYTE_ARRAY) {
                                std::vector<::parquet::ByteArray> values;
                                for (const auto& b : bytes)
                                    values.emplace_back(b.size(), reinterpret_cast<const uint8_t*>(b.data()));
                                static_cast<::parquet::ByteArrayWriter*>(column)->WriteBatch(num_rows, defs, nullptr,
                                                                                             values.data());
                            } else {
                                std::vector<::parquet::FixedLenByteArray> values;
                                for (const auto& b : bytes)
                                    values.emplace_back(reinterpret_cast<const uint8_t*>(b.data()));
                                static_cast<::parquet::FixedLenByteArrayWriter*>(column)->WriteBatch(
                                        num_rows, defs, nullptr, values.data());
                            }
                        }
                        writer->Close();
                        auto result = sink->Finish();
                        ASSERT_TRUE(result.ok());
                        auto buffer = result.ValueOrDie();
                        auto apache_reader =
                                ::parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
                        auto metadata = apache_reader->metadata()->RowGroup(0)->ColumnChunk(0);
                        ASSERT_EQ(c.physical, metadata->type());
                        bool found_encoding = false;
                        for (const auto& stat : metadata->encoding_stats()) {
                            if (stat.page_type == ::parquet::PageType::DATA_PAGE ||
                                stat.page_type == ::parquet::PageType::DATA_PAGE_V2) {
                                EXPECT_EQ(c.dictionary ? Encoding::RLE_DICTIONARY : c.encoding, stat.encoding);
                                found_encoding = true;
                            }
                        }
                        ASSERT_TRUE(found_encoding);

                        std::string data = buffer->ToString();
                        uint32_t footer_length = 0;
                        for (int i = 0; i < 4; ++i)
                            footer_length |= static_cast<uint32_t>(static_cast<uint8_t>(data[data.size() - 8 + i]))
                                             << (8 * i);
                        ASSERT_LE(footer_length, data.size() - 8);
                        tparquet::FileMetaData footer;
                        ASSERT_OK(deserialize_thrift_msg(
                                reinterpret_cast<const uint8_t*>(data.data()) + data.size() - 8 - footer_length,
                                &footer_length, TProtocolType::COMPACT, &footer));
                        ASSERT_EQ(2, footer.schema.size());
                        auto& field = footer.schema[1];
                        ASSERT_TRUE(field.__isset.converted_type);
                        ASSERT_TRUE(field.__isset.logicalType && field.logicalType.__isset.DECIMAL);
                        if (legacy) {
                            field.__isset.logicalType = false;
                        } else {
                            field.__isset.converted_type = false;
                            field.__isset.precision = false;
                            field.__isset.scale = false;
                        }
                        data.resize(data.size() - 8 - footer_length);
                        std::string serialized;
                        ThriftSerializer serializer(true, 1024);
                        ASSERT_OK(serializer.serialize(&footer, &serialized));
                        data.append(serialized);
                        for (int i = 0; i < 4; ++i)
                            data.push_back(static_cast<char>((serialized.size() >> (8 * i)) & 0xff));
                        data.append("PAR1");

                        ObjectPool pool;
                        FormatScannerStats stats{};
                        HdfsScannerContext ctx;
                        std::atomic<int32_t> lazy_counter{0};
                        ctx.format_scan_context.lazy_column_coalesce_counter = &lazy_counter;
                        ctx.format_scan_context.stats = &stats;
                        ctx.format_scan_context.timezone = "Asia/Shanghai";
                        ctx.format_scan_context.predicate_tree = &ctx.predicates.predicate_tree;
                        const auto type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 18, scale);
                        Utils::SlotDesc slots[] = {{"value", type, 0}, {""}};
                        auto* tuple = Utils::create_tuple_descriptor(nullptr, &pool, slots);
                        Utils::make_column_info_vector(tuple, &ctx.format_scan_context.materialized_columns);
                        ctx.slot_descs = tuple->slots();
                        ctx.format_scan_context.scan_range_offset = 0;
                        ctx.format_scan_context.scan_range_length = data.size();
                        auto file = std::make_shared<RandomAccessFile>(std::make_shared<io::StringInputStream>(data),
                                                                       "decimal-matrix.parquet");
                        FileReader reader(2, file.get(), data.size());
                        ASSERT_OK(reader.init(&ctx.format_scan_context));
                        const auto& actual_field = reader.get_file_metadata()->t_metadata().schema[1];
                        ASSERT_TRUE(actual_field.__isset.type);
                        EXPECT_EQ(static_cast<int>(c.physical), static_cast<int>(actual_field.type));
                        ASSERT_TRUE(actual_field.__isset.repetition_type);
                        EXPECT_EQ(required ? tparquet::FieldRepetitionType::REQUIRED
                                           : tparquet::FieldRepetitionType::OPTIONAL,
                                  actual_field.repetition_type);
                        if (c.physical == Physical::FIXED_LEN_BYTE_ARRAY) {
                            ASSERT_TRUE(actual_field.__isset.type_length);
                            EXPECT_EQ(c.width, actual_field.type_length);
                        }
                        EXPECT_EQ(legacy, actual_field.__isset.converted_type);
                        EXPECT_EQ(!legacy, actual_field.__isset.logicalType);
                        EXPECT_EQ(legacy, actual_field.__isset.precision);
                        EXPECT_EQ(legacy, actual_field.__isset.scale);
                        if (legacy) {
                            EXPECT_EQ(tparquet::ConvertedType::DECIMAL, actual_field.converted_type);
                            EXPECT_EQ(precision, actual_field.precision);
                            EXPECT_EQ(scale, actual_field.scale);
                        } else {
                            EXPECT_EQ(precision, actual_field.logicalType.DECIMAL.precision);
                            EXPECT_EQ(scale, actual_field.logicalType.DECIMAL.scale);
                        }
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
                                ASSERT_LT(row, num_rows);
                                const bool is_null = !required && row == 2;
                                EXPECT_EQ(is_null, output->is_null(i));
                                if (!is_null && !output->is_null(i)) {
                                    EXPECT_EQ(expected[required || row < 2 ? row : row - 1],
                                              output->get(i).get_int64());
                                }
                            }
                        }
                        EXPECT_EQ(num_rows, row);
                    }
                }
            }
        }
    }
}

TEST_F(ParquetCLIReaderTest, ByteArrayDecimalPredicateStatistics) {
    for (bool dictionary : {false, true}) {
        for (bool statistics : {false, true}) {
            SCOPED_TRACE(fmt::format("dictionary={}, statistics={}", dictionary, statistics));
            auto decimal = ::parquet::schema::PrimitiveNode::Make("value", ::parquet::Repetition::OPTIONAL,
                                                                  ::parquet::LogicalType::Decimal(18, 9),
                                                                  ::parquet::Type::BYTE_ARRAY);
            auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
                    ::parquet::schema::GroupNode::Make("root", ::parquet::Repetition::REQUIRED, {decimal}));
            auto sink_result = arrow::io::BufferOutputStream::Create();
            ASSERT_TRUE(sink_result.ok());
            auto sink = sink_result.ValueOrDie();
            ::parquet::WriterProperties::Builder properties;
            properties.write_batch_size(1)->data_pagesize(1);
            if (dictionary)
                properties.enable_dictionary();
            else
                properties.disable_dictionary();
            if (statistics)
                properties.enable_statistics()->enable_write_page_index();
            else
                properties.disable_statistics();
            auto writer = ::parquet::ParquetFileWriter::Open(sink, schema, properties.build());
            const uint8_t raw[] = {0xfe, 0xff, 0, 1, 2};
            const ::parquet::ByteArray negative[] = {{1, &raw[0]}, {1, &raw[1]}};
            const int16_t negative_defs[] = {1, 1, 0};
            static_cast<::parquet::ByteArrayWriter*>(writer->AppendRowGroup()->NextColumn())
                    ->WriteBatch(3, negative_defs, nullptr, negative);
            const ::parquet::ByteArray mixed[] = {{1, &raw[1]}, {1, &raw[2]}, {1, &raw[3]}, {1, &raw[4]}};
            const int16_t mixed_defs[] = {1, 0, 1, 1, 1};
            static_cast<::parquet::ByteArrayWriter*>(writer->AppendRowGroup()->NextColumn())
                    ->WriteBatch(5, mixed_defs, nullptr, mixed);
            writer->Close();
            auto result = sink->Finish();
            ASSERT_TRUE(result.ok());
            auto buffer = result.ValueOrDie();
            auto apache_reader = ::parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
            ASSERT_EQ(2, apache_reader->metadata()->num_row_groups());
            for (int i = 0; i < 2; ++i) {
                EXPECT_EQ(statistics, apache_reader->metadata()->RowGroup(i)->ColumnChunk(0)->is_stats_set());
            }
            if (statistics) {
                auto page_reader = apache_reader->GetPageIndexReader();
                ASSERT_NE(nullptr, page_reader);
                auto row_group_index = page_reader->RowGroup(1);
                ASSERT_NE(nullptr, row_group_index);
                auto page_index = row_group_index->GetColumnIndex(0);
                ASSERT_NE(nullptr, page_index);
                ASSERT_GT(page_index->null_pages().size(), 1);
            }

            for (int scan_mode : {0, 1, 2}) {
                SCOPED_TRACE(scan_mode);
                ObjectPool pool;
                auto* state = pool.add(new RuntimeState(TQueryGlobals()));
                auto fragment_dict_state = std::make_unique<FragmentDictState>();
                state->set_fragment_dict_state(fragment_dict_state.get());
                FormatScannerStats stats{};
                HdfsScannerContext ctx;
                std::atomic<int32_t> lazy_counter{0};
                ctx.format_scan_context.lazy_column_coalesce_counter = &lazy_counter;
                ctx.format_scan_context.stats = &stats;
                ctx.format_scan_context.timezone = "Asia/Shanghai";
                ctx.format_scan_context.options.parquet_page_index_enable = scan_mode == 2;
                ctx.format_scan_context.predicate_tree = &ctx.predicates.predicate_tree;
                auto type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 27, 9);
                Utils::SlotDesc slots[] = {{"value", type, 0}, {""}};
                auto* tuple = Utils::create_tuple_descriptor(state, &pool, slots);
                Utils::make_column_info_vector(tuple, &ctx.format_scan_context.materialized_columns);
                ctx.slot_descs = tuple->slots();
                ctx.format_scan_context.scan_range_offset = 0;
                ctx.format_scan_context.scan_range_length = buffer->size();
                std::vector<TExpr> conjuncts;
                if (scan_mode != 0) {
                    ParquetUTBase::append_decimal_conjunct(TExprOpcode::GT, 0, "0.000000000", &conjuncts);
                    auto& exprs = ctx.format_scan_context.conjunct_ctxs_by_slot[0];
                    ParquetUTBase::create_conjunct_ctxs(&pool, state, &conjuncts, &exprs);
                    ParquetUTBase::setup_conjuncts_manager(exprs, nullptr, tuple, state, &ctx);
                }
                auto file = std::make_shared<RandomAccessFile>(
                        std::make_shared<io::StringInputStream>(buffer->ToString()), "decimal-predicate.parquet");
                FileReader reader(2, file.get(), buffer->size());
                ASSERT_OK(reader.init(&ctx.format_scan_context));
                auto chunk = std::make_shared<Chunk>();
                chunk->append_column(ColumnHelper::create_column(type, true), 0);
                std::vector<std::optional<int128_t>> observed;
                for (;;) {
                    chunk->reset();
                    Status status = reader.get_next(&chunk);
                    if (status.is_end_of_file()) break;
                    ASSERT_OK(status);
                    const auto& output = chunk->get_column_by_index(0);
                    for (size_t row = 0; row < chunk->num_rows(); ++row) {
                        if (output->is_null(row))
                            observed.emplace_back(std::nullopt);
                        else
                            observed.emplace_back(output->get(row).get_int128());
                    }
                }
                if (scan_mode == 0) {
                    EXPECT_EQ((std::vector<std::optional<int128_t>>{-2, -1, std::nullopt, -1, std::nullopt, 0, 1, 2}),
                              observed);
                } else {
                    EXPECT_EQ((std::vector<std::optional<int128_t>>{1, 2}), observed);
                    if (statistics) {
                        EXPECT_GT(stats.statistics_success_counter, 0);
                    }
                    if (statistics && scan_mode == 2) {
                        EXPECT_GT(stats.page_index_success_counter, 0);
                    }
                    if (scan_mode == 1) {
                        EXPECT_EQ(0, stats.page_index_success_counter);
                    }
                }
            }
        }
    }
}

GROUP_SLOW_TEST_F(ParquetCLIReaderTest, ReadAllParquetFiles) {
    std::vector<std::string> paths;
    // We don't support below files in init phase, but we need to make sure that will not make BE crashed.
    std::set<std::string> unsupported_paths_init;
    {
        // Invalid argument: MAP-annotated group must have a single child.
        // error format built by hudi
        unsupported_paths_init.emplace("./be/test/exec/test_data/parquet_scanner/hudi_array_map.parquet");
        // empty parquet file
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/empty.parquet");
        // parquet column reader: not supported convert from parquet `INT64` to `TIME`
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/column_converter/int32.parquet");
        // parquet column reader: not supported convert from parquet `INT64` to `TIME`
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/column_converter/int64.parquet");
        // parquet column reader: not supported convert from parquet `FIXED_LEN_BYTE_ARRAY` to `TIME`
        unsupported_paths_init.emplace(
                "./be/test/formats/parquet/test_data/column_converter/fixed_len_byte_array.parquet");
        // parquet column reader: not supported convert from parquet `BYTE_ARRAY` to `JSON`
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/column_converter/byte_array.parquet");
        // Invalid argument: Duplicate field name: col1
        unsupported_paths_init.emplace("./be/test/exec/test_data/parquet_data/schema4.parquet");
    }
    // We don't support below files in get_next phase, it's illegal parquet files
    std::set<std::string> unsupported_paths_get_next;
    {
        unsupported_paths_get_next.emplace(
                "./be/test/exec/test_data/parquet_scanner/type_mismatch_decode_min_max.parquet");
        // corrupted file
        unsupported_paths_get_next.emplace("./be/test/exec/test_data/parquet_data/decimal.parquet");
    }
    traverse_directory_add_parquet(paths, "./be/test/exec/test_data/parquet_data", ".parquet");
    traverse_directory_add_parquet(paths, "./be/test/exec/test_data/parquet_scanner", ".parquet");
    traverse_directory_add_parquet(paths, "./be/test/formats/parquet/test_data", ".parquet");

    std::vector<std::pair<int, Status>> results;
    for (const std::string& path : paths) {
        LOG(INFO) << "Testing file: " << path;
        ParquetCLIReader reader{path};
        int stage = 0;
        auto st = reader.init();
        if (!st.ok()) {
            results.emplace_back(stage, st);
            continue;
        }
        stage++;
        auto res = reader.debug(1);
        st = res.status();
        if (!st.ok()) {
            results.emplace_back(stage, st);
            continue;
        }
        stage++;
        results.emplace_back(stage, st);
    }

    ASSERT_EQ(results.size(), paths.size());
    for (int i = 0; i < results.size(); i++) {
        const auto& [stage, status] = results[i];
        const auto& path = paths[i];
        if (stage == 0) {
            EXPECT_TRUE(!status.ok() && unsupported_paths_init.contains(path))
                    << "File path: " << path << ", stage: " << stage << ", status: " << status.to_string();
        } else if (stage == 1) {
            EXPECT_TRUE(!status.ok() && unsupported_paths_get_next.contains(path))
                    << "File path: " << path << ", stage: " << stage << ", status: " << status.to_string();
        } else {
            EXPECT_TRUE(status.ok()) << "File path: " << path << ", stage: " << stage
                                     << ", status: " << status.to_string();
        }
    }
}

GROUP_SLOW_TEST_F(ParquetCLIReaderTest, ReadArrowFuzzingParquetFiles) {
    std::vector<std::string> read_paths;
    {
        traverse_directory_add_parquet(read_paths, "./be/test/formats/parquet/arrow_fuzzing_data/fuzzing/");
        traverse_directory_add_parquet(read_paths,
                                       "./be/test/formats/parquet/arrow_fuzzing_data/generated_simple_numerics/");
        read_paths.emplace_back("./be/test/formats/parquet/arrow_fuzzing_data/ARROW-17100.parquet");
    }

    for (const std::string& path : read_paths) {
        LOG(INFO) << "Testing file: " << path;
        ParquetCLIReader reader{path};
        auto st = reader.init();
        if (st.ok()) {
            auto res = reader.debug(1);
            st = res.status();
        }
    }
}

TEST_F(ParquetCLIReaderTest, ReadDataPageV2ParquetFiles) {
    std::vector<std::string> files = {
            // generated by duckdb: https://duckdb.org/2025/01/22/parquet-encodings.html
            "data_page_v2_float10k.parquet", "data_page_v2_int1m.parquet", "data_page_v2_string4k.parquet",
            // other sources.
            "data_page_v2_test.parquet", "data_page_v2_test2.parquet"};
    for (const auto& file : files) {
        std::string path = "./be/test/formats/parquet/test_data/" + file;
        LOG(INFO) << "Testing file: " << path;
        ParquetCLIReader reader{path};
        auto st = reader.init();
        ASSERT_TRUE(st.ok()) << st.to_string();
        auto res = reader.debug(10);
        st = res.status();
        ASSERT_TRUE(st.ok()) << st.to_string();
        LOG(INFO) << "data rows = " << res.value();
    }
}

} // namespace starrocks::parquet

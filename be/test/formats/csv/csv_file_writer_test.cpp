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

#include "formats/csv/csv_file_writer.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include <filesystem>
#include <iostream>
#include <map>
#include <vector>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"
#include "formats/csv/output_stream_file.h"
#include "fs/fs_memory.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::formats {

class CSVFileWriterTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); };
    void TearDown() override{};

protected:
    std::vector<std::string> _make_type_names(const std::vector<TypeDescriptor>& type_descs) {
        std::vector<std::string> names;
        for (auto& desc : type_descs) {
            names.push_back(desc.debug_string());
        }
        return names;
    }

protected:
    MemoryFileSystem _fs;
    ObjectPool _pool;
    std::string _file_path{"/data.csv"};
    RuntimeState* _runtime_state;
};

TEST_F(CSVFileWriterTest, TestWriteIntergers) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT),
            TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_BIGINT),
    };
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_TINYINT), true);
        std::vector<int8_t> int8_nums{INT8_MIN, INT8_MAX, 0, 1};
        auto count = col0->append_numbers(int8_nums.data(), size(int8_nums) * sizeof(int8_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_SMALLINT), true);
        std::vector<int16_t> int16_nums{INT16_MIN, INT16_MAX, 0, 1};
        count = col1->append_numbers(int16_nums.data(), size(int16_nums) * sizeof(int16_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        count = col2->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col2), chunk->num_columns());

        auto col3 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
        std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col3->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col3), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect =
            "-128,-32768,-2147483648,-9223372036854775808\n127,32767,2147483647,9223372036854775807\n0,0,0,0\n1,1,1,"
            "1\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteBoolean) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_BOOLEAN)};
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BooleanColumn::create();
        std::vector<uint8_t> values = {0, 1, 1, 0};
        data_column->append_numbers(values.data(), values.size() * sizeof(uint8_t));
        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "\\N\ntrue\n\\N\nfalse\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteFloat) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_FLOAT)};

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = FloatColumn::create();
        std::vector<float> values = {0.1, 1.1, 1.2, -99.9};
        data_column->append_numbers(values.data(), values.size() * sizeof(float));
        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {0, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "0.1\n1.1\n\\N\n-99.9\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteDouble) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_DOUBLE)};

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = DoubleColumn::create();
        std::vector<double> values = {0.1, 1.1, 1.2, -99.9};
        data_column->append_numbers(values.data(), values.size() * sizeof(double));
        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {0, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "0.1\n1.1\n\\N\n-99.9\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteDate) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_DATE)};

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = DateColumn::create();
        {
            Datum datum;
            datum.set_date(DateValue::create(1999, 9, 9));
            data_column->append_datum(datum);
            datum.set_date(DateValue::create(1999, 9, 10));
            data_column->append_datum(datum);
            datum.set_date(DateValue::create(1999, 9, 11));
            data_column->append_datum(datum);
            data_column->append_default();
        }

        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {0, 0, 0, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "1999-09-09\n1999-09-10\n1999-09-11\n0000-01-01\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteDatetime) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_DATETIME)};

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = TimestampColumn::create();
        {
            Datum datum;
            datum.set_timestamp(TimestampValue::create(1999, 9, 9, 0, 0, 0));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1999, 9, 10, 1, 1, 1));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1999, 9, 11, 2, 2, 2));
            data_column->append_datum(datum);
            data_column->append_default();
        }

        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {0, 0, 0, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "1999-09-09 00:00:00\n1999-09-10 01:01:01\n1999-09-11 02:02:02\n0000-01-01 00:00:00\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteVarchar) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = BinaryColumn::create();
        data_column->append("hello");
        data_column->append("world");
        data_column->append("starrocks");
        data_column->append("lakehouse");

        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {0, 0, 0, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "hello\nworld\nstarrocks\nlakehouse\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteArray) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_descs.push_back(type_int_array);

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(CSVFileWriterTest, TestWriteMap) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_key = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_value = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(type_int_key);
    type_int_map.children.push_back(type_int_value);
    type_descs.push_back(type_int_map);

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(CSVFileWriterTest, TestWriteNestedArray) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    auto type_int_array_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_int_array_array.children.push_back(type_int_array);
    type_descs.push_back(type_int_array_array);

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(CSVFileWriterTest, TestUnknownCompression) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs.new_writable_file(_file_path).value();
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, nullptr);

    // UNKNOWN_COMPRESSION should fail when creating CompressedAsyncOutputStreamFile
    // We expect this to fail during codec initialization
    ASSERT_DEATH(
        {
            auto compressed_stream = std::make_shared<csv::CompressedAsyncOutputStreamFile>(
                async_stream.get(), CompressionTypePB::UNKNOWN_COMPRESSION, 1024);
        },
        ".*"
    );
}

TEST_F(CSVFileWriterTest, TestFactory) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto fs = std::make_shared<MemoryFileSystem>();
    auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names,
                                                 std::move(column_evaluators), nullptr, nullptr);
    ASSERT_OK(factory.init());
    auto maybe_writer = factory.create("/test.csv");
    ASSERT_OK(maybe_writer.status());
}

// ==================== Compression Tests ====================

// Helper function to decompress gzip data using zlib directly
std::string decompress_gzip(const std::string& compressed_data) {
    z_stream z_strm = {nullptr};
    z_strm.zalloc = Z_NULL;
    z_strm.zfree = Z_NULL;
    z_strm.opaque = Z_NULL;

    // MAX_WBITS + 16 for gzip format
    int ret = inflateInit2(&z_strm, MAX_WBITS + 16);
    EXPECT_EQ(ret, Z_OK);

    z_strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed_data.data()));
    z_strm.avail_in = compressed_data.size();

    size_t max_uncompressed_size = compressed_data.size() * 100; // Conservative estimate
    std::string uncompressed_data;
    uncompressed_data.resize(max_uncompressed_size);

    z_strm.next_out = reinterpret_cast<Bytef*>(uncompressed_data.data());
    z_strm.avail_out = max_uncompressed_size;

    ret = inflate(&z_strm, Z_FINISH);
    EXPECT_TRUE(ret == Z_OK || ret == Z_STREAM_END);

    size_t actual_size = z_strm.total_out;
    inflateEnd(&z_strm);

    uncompressed_data.resize(actual_size);
    return uncompressed_data;
}

TEST_F(CSVFileWriterTest, TestWriteIntegersWithGzipCompression) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT),
            TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_BIGINT),
    };
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, _runtime_state);
    auto csv_output_stream = std::make_shared<csv::CompressedAsyncOutputStreamFile>(
            async_stream.get(), CompressionTypePB::GZIP, 1024 * 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(csv_output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_TINYINT), true);
        std::vector<int8_t> int8_nums{INT8_MIN, INT8_MAX, 0, 1};
        auto count = col0->append_numbers(int8_nums.data(), size(int8_nums) * sizeof(int8_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_SMALLINT), true);
        std::vector<int16_t> int16_nums{INT16_MIN, INT16_MAX, 0, 1};
        count = col1->append_numbers(int16_nums.data(), size(int16_nums) * sizeof(int16_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        count = col2->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col2), chunk->num_columns());

        auto col3 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
        std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col3->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col3), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // Note: writer->commit() already calls finalize() on CompressedAsyncOutputStreamFile
    // which internally closes the async stream, so we don't call async_stream->close() here

    // verify correctness - read compressed data and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    std::string content = decompress_gzip(compressed_content);
    std::string expect =
            "-128,-32768,-2147483648,-9223372036854775808\n127,32767,2147483647,9223372036854775807\n0,0,0,0\n1,1,1,"
            "1\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteVarcharWithGzipCompression) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, _runtime_state);
    auto csv_output_stream = std::make_shared<csv::CompressedAsyncOutputStreamFile>(
            async_stream.get(), CompressionTypePB::GZIP, 1024 * 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(csv_output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BinaryColumn::create();
        data_column->append("hello");
        data_column->append("world");
        data_column->append("starrocks");
        data_column->append("lakehouse");

        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {0, 0, 0, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // Note: writer->commit() already calls finalize() which closes the async stream

    // verify correctness
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    std::string content = decompress_gzip(compressed_content);
    std::string expect = "hello\nworld\nstarrocks\nlakehouse\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteLargeDataWithGzipCompression) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, _runtime_state);
    auto csv_output_stream = std::make_shared<csv::CompressedAsyncOutputStreamFile>(
            async_stream.get(), CompressionTypePB::GZIP, 1024 * 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(csv_output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {});
    ASSERT_OK(writer->init());

    // Write multiple chunks
    const int num_chunks = 10;
    const int rows_per_chunk = 1000;
    int total_rows = 0;

    for (int c = 0; c < num_chunks; c++) {
        auto chunk = std::make_shared<Chunk>();

        auto int_column = Int32Column::create();
        auto str_column = BinaryColumn::create();

        for (int i = 0; i < rows_per_chunk; i++) {
            int_column->append(c * rows_per_chunk + i);
            str_column->append("row_" + std::to_string(c * rows_per_chunk + i));
        }

        chunk->append_column(std::move(int_column), chunk->num_columns());
        chunk->append_column(std::move(str_column), chunk->num_columns());

        ASSERT_OK(writer->write(chunk.get()));
        total_rows += rows_per_chunk;
    }

    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, total_rows);

    // Note: writer->commit() already calls finalize() which closes the async stream

    // Verify compressed file size is smaller than expected uncompressed size
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    // Decompress and verify first and last few lines
    std::string content = decompress_gzip(compressed_content);
    EXPECT_TRUE(content.find("0,row_0\n") != std::string::npos);
    EXPECT_TRUE(content.find(std::to_string(total_rows - 1) + ",row_" + std::to_string(total_rows - 1) + "\n") !=
                std::string::npos);

    // Count lines
    int line_count = std::count(content.begin(), content.end(), '\n');
    EXPECT_EQ(line_count, total_rows);
}

TEST_F(CSVFileWriterTest, TestCompressionRatio) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto column_names = _make_type_names(type_descs);

    // Expected uncompressed content (calculate manually)
    std::string expected_line = "This is a repetitive line that should compress very well\n";
    const int num_lines = 100;  // Use fewer lines to avoid edge cases
    std::string expected_content;
    for (int i = 0; i < num_lines; i++) {
        expected_content += expected_line;
    }
    size_t expected_uncompressed_size = expected_content.size();

    // Create compressed file
    std::string compressed_path = "/data_compressed.csv.gz";
    {
        auto maybe_output_file = _fs.new_writable_file(compressed_path);
        EXPECT_OK(maybe_output_file.status());
        auto output_file = std::move(maybe_output_file.value());
        auto async_stream =
                std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, _runtime_state);
        auto csv_output_stream = std::make_shared<csv::CompressedAsyncOutputStreamFile>(
                async_stream.get(), CompressionTypePB::GZIP, 1024 * 1024);
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
        auto writer_options = std::make_shared<formats::CSVWriterOptions>();
        auto writer = std::make_unique<formats::CSVFileWriter>(
                compressed_path, std::move(csv_output_stream), column_names, type_descs, std::move(column_evaluators),
                writer_options, []() {});
        ASSERT_OK(writer->init());

        auto chunk = std::make_shared<Chunk>();
        auto data_column = BinaryColumn::create();

        // Add repetitive data
        for (int i = 0; i < num_lines; i++) {
            data_column->append("This is a repetitive line that should compress very well");
        }
        chunk->append_column(std::move(data_column), chunk->num_columns());

        ASSERT_OK(writer->write(chunk.get()));
        ASSERT_OK(writer->commit().io_status);
    }

    // Read and verify compressed file
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(compressed_path, &compressed_content));

    // Compressed should be significantly smaller than uncompressed
    EXPECT_LT(compressed_content.size(), expected_uncompressed_size / 2);

    // Verify decompressed content matches expected
    std::string decompressed = decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed.size(), expected_uncompressed_size);
    EXPECT_EQ(decompressed, expected_content);
}

TEST_F(CSVFileWriterTest, TestFactoryWithGzipCompression) {
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_varchar = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    std::vector<TypeDescriptor> type_descs{type_int, type_varchar};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto fs = std::make_shared<MemoryFileSystem>();
    auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::GZIP, {}, column_names,
                                                 std::move(column_evaluators), nullptr, _runtime_state);
    ASSERT_OK(factory.init());

    auto maybe_writer_and_stream = factory.create("/test_compressed.csv.gz");
    ASSERT_OK(maybe_writer_and_stream.status());

    auto& writer_and_stream = maybe_writer_and_stream.value();
    ASSERT_NE(writer_and_stream.writer, nullptr);
    ASSERT_NE(writer_and_stream.stream, nullptr);

    // Write some data
    auto chunk = std::make_shared<Chunk>();
    auto int_col = Int32Column::create();
    auto str_col = BinaryColumn::create();
    int_col->append(1);
    int_col->append(2);
    str_col->append("test1");
    str_col->append("test2");
    chunk->append_column(std::move(int_col), chunk->num_columns());
    chunk->append_column(std::move(str_col), chunk->num_columns());

    ASSERT_OK(writer_and_stream.writer->init());
    ASSERT_OK(writer_and_stream.writer->write(chunk.get()));
    auto result = writer_and_stream.writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // Note: writer->commit() already calls finalize() which closes the stream

    // Verify compressed output
    std::string compressed_content;
    ASSERT_OK(fs->read_file("/test_compressed.csv.gz", &compressed_content));

    std::string content = decompress_gzip(compressed_content);
    EXPECT_TRUE(content.find("1,test1\n") != std::string::npos);
    EXPECT_TRUE(content.find("2,test2\n") != std::string::npos);
}

TEST_F(CSVFileWriterTest, TestCompressionWithCustomDelimiters) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_INT),
                                           TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, _runtime_state);
    auto csv_output_stream = std::make_shared<csv::CompressedAsyncOutputStreamFile>(
            async_stream.get(), CompressionTypePB::GZIP, 1024 * 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->column_terminated_by = "|";
    writer_options->line_terminated_by = ";\n";
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(csv_output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto int_col = Int32Column::create();
        auto str_col = BinaryColumn::create();
        int_col->append(100);
        int_col->append(200);
        str_col->append("value1");
        str_col->append("value2");
        chunk->append_column(std::move(int_col), chunk->num_columns());
        chunk->append_column(std::move(str_col), chunk->num_columns());
    }

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // Note: writer->commit() already calls finalize() which closes the async stream

    // Verify custom delimiters are preserved after compression
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    std::string content = decompress_gzip(compressed_content);
    EXPECT_EQ(content, "100|value1;\n200|value2;\n");
}

} // namespace starrocks::formats

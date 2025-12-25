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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
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

TEST_F(CSVFileWriterTest, TestWriteArrayInt) {
    // type_descs: ARRAY<INT>
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // Create ARRAY<INT> column with data: [1,2,3], [4,5], [], [6]
        auto elements_data = Int32Column::create();
        auto offsets = UInt32Column::create();
        auto null_column = UInt8Column::create();

        // row 0: [1,2,3]
        elements_data->append(1);
        elements_data->append(2);
        elements_data->append(3);
        offsets->append(0);

        // row 1: [4,5]
        elements_data->append(4);
        elements_data->append(5);
        offsets->append(3);

        // row 2: [] (empty array)
        offsets->append(5);

        // row 3: [6]
        elements_data->append(6);
        offsets->append(5);
        offsets->append(6);

        auto array_column =
                ArrayColumn::create(NullableColumn::create(elements_data, UInt8Column::create(6, 0)), offsets);
        null_column->append_numbers(std::vector<uint8_t>{0, 0, 0, 0}.data(), 4);
        auto nullable_column = NullableColumn::create(std::move(array_column), std::move(null_column));
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
    std::string expect = "[1,2,3]\n[4,5]\n[]\n[6]\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteArrayBigInt) {
    // type_descs: ARRAY<BIGINT>
    std::vector<TypeDescriptor> type_descs;
    auto type_bigint = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    auto type_bigint_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_bigint_array.children.push_back(type_bigint);
    type_descs.push_back(type_bigint_array);

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // Create ARRAY<BIGINT> column with data: [100,200], [9223372036854775807]
        auto elements_data = Int64Column::create();
        auto offsets = UInt32Column::create();
        auto null_column = UInt8Column::create();

        // row 0: [100,200]
        elements_data->append(100);
        elements_data->append(200);
        offsets->append(0);

        // row 1: [9223372036854775807] (INT64_MAX)
        elements_data->append(INT64_MAX);
        offsets->append(2);
        offsets->append(3);

        auto array_column =
                ArrayColumn::create(NullableColumn::create(elements_data, UInt8Column::create(3, 0)), offsets);
        null_column->append_numbers(std::vector<uint8_t>{0, 0}.data(), 2);
        auto nullable_column = NullableColumn::create(std::move(array_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "[100,200]\n[9223372036854775807]\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteArrayWithNull) {
    // type_descs: ARRAY<INT> with NULL array and NULL elements
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // Create ARRAY<INT> column with data: [1,null,3], NULL
        auto elements_data = Int32Column::create();
        auto elements_null = UInt8Column::create();
        auto offsets = UInt32Column::create();
        auto array_null_column = UInt8Column::create();

        // row 0: [1,null,3]
        elements_data->append(1);
        elements_null->append(0);
        elements_data->append(0); // placeholder for null
        elements_null->append(1); // null
        elements_data->append(3);
        elements_null->append(0);
        offsets->append(0);

        // row 1: NULL array
        offsets->append(3);
        offsets->append(3);

        auto nullable_elements = NullableColumn::create(elements_data, elements_null);
        auto array_column = ArrayColumn::create(std::move(nullable_elements), offsets);
        array_null_column->append(0); // row 0 is not null
        array_null_column->append(1); // row 1 is null
        auto nullable_column = NullableColumn::create(std::move(array_column), std::move(array_null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "[1,null,3]\n\\N\n";
    ASSERT_EQ(content, expect);
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(CSVFileWriterTest, TestWriteNestedArray) {
    // type_descs: ARRAY<ARRAY<INT>>
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
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // Create ARRAY<ARRAY<INT>> column with data: [[1,2],[3]], [[4]]
        // Inner elements: 1,2,3,4
        auto inner_elements = Int32Column::create();
        inner_elements->append(1);
        inner_elements->append(2);
        inner_elements->append(3);
        inner_elements->append(4);

        // Inner offsets: [0,2,3,4] for inner arrays [1,2], [3], [4]
        auto inner_offsets = UInt32Column::create();
        inner_offsets->append(0);
        inner_offsets->append(2);
        inner_offsets->append(3);
        inner_offsets->append(4);

        auto inner_array =
                ArrayColumn::create(NullableColumn::create(inner_elements, UInt8Column::create(4, 0)), inner_offsets);

        // Outer offsets: [0,2,3] for outer arrays [[1,2],[3]], [[4]]
        auto outer_offsets = UInt32Column::create();
        outer_offsets->append(0);
        outer_offsets->append(2);
        outer_offsets->append(3);

        auto outer_array =
                ArrayColumn::create(NullableColumn::create(inner_array, UInt8Column::create(3, 0)), outer_offsets);

        auto null_column = UInt8Column::create();
        null_column->append(0);
        null_column->append(0);
        auto nullable_column = NullableColumn::create(std::move(outer_array), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "[[1,2],[3]]\n[[4]]\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestUnknownCompression) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs.new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            TCompressionType::UNKNOWN_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
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

} // namespace starrocks::formats

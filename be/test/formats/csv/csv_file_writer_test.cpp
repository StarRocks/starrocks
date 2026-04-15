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
#include "column/const_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"
#include "compression_test_utils.h"
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "[1,null,3]\n\\N\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteHiveArray) {
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
    writer_options->is_hive = true;
    writer_options->collection_delim = '\002';
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "1\0022\0023\n4\0025\n\n6\n";
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
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
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, nullptr);

    // UNKNOWN_COMPRESSION should fail when creating CompressedOutputStream
    // We expect this to return an error status
    auto base_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto result = csv::CompressedOutputStream::create(base_stream, CompressionTypePB::UNKNOWN_COMPRESSION, 1024);
    ASSERT_FALSE(result.ok());
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

// Test factory with all CSV options including header
TEST_F(CSVFileWriterTest, TestFactoryWithOptions) {
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    std::vector<TypeDescriptor> type_descs{type_int};

    std::vector<std::string> column_names = {"id"};
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto fs = std::make_shared<MemoryFileSystem>();

    // Set all CSV options
    std::map<std::string, std::string> options;
    options[formats::CSVWriterOptions::COLUMN_TERMINATED_BY] = "\t";
    options[formats::CSVWriterOptions::LINE_TERMINATED_BY] = "\r\n";
    options[formats::CSVWriterOptions::INCLUDE_HEADER] = "true";

    auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, options, column_names,
                                                 std::move(column_evaluators), nullptr, nullptr);
    ASSERT_OK(factory.init());
    auto maybe_writer = factory.create("/test_with_options.csv");
    ASSERT_OK(maybe_writer.status());
}

// ==================== Header Tests ====================

// Test header row output with include_header option
TEST_F(CSVFileWriterTest, TestWriteWithHeader) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "name"};
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->include_header = true;
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
        std::vector<int32_t> int_nums{1, 2};
        size_t appended = col0->append_numbers(int_nums.data(), size(int_nums) * sizeof(int32_t));
        ASSERT_EQ(appended, size(int_nums));
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto data_column = BinaryColumn::create();
        data_column->append("Alice");
        data_column->append("Bob");
        auto null_column = UInt8Column::create();
        null_column->append_numbers(std::vector<uint8_t>{0, 0}.data(), 2);
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "id,name\n1,Alice\n2,Bob\n";
    ASSERT_EQ(content, expect);
}

// Test header without data
TEST_F(CSVFileWriterTest, TestWriteHeaderOnly) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"col_a", "col_b"};
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->include_header = true;
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    // Commit without writing any data
    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "col_a,col_b\n";
    ASSERT_EQ(content, expect);
}

// Test header with special characters that need escaping
TEST_F(CSVFileWriterTest, TestWriteHeaderWithSpecialChars) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    // Column names with comma, quotes, and newline
    std::vector<std::string> column_names = {"user,id", "user\"name", "user\ninfo"};
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->include_header = true;
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Expected: fields with special chars are quoted, internal quotes are doubled
    std::string expect = "\"user,id\",\"user\"\"name\",\"user\ninfo\"\n";
    ASSERT_EQ(content, expect);
}

// Test header with custom delimiter
TEST_F(CSVFileWriterTest, TestWriteHeaderWithCustomDelimiter) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "name"};
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->include_header = true;
    writer_options->column_terminated_by = "\t";
    writer_options->line_terminated_by = "\r\n";
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "id\tname\r\n";
    ASSERT_EQ(content, expect);
}

// Test header escaping when column name contains the custom delimiter
TEST_F(CSVFileWriterTest, TestWriteHeaderEscapeCustomDelimiter) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    // Column name contains the tab delimiter
    std::vector<std::string> column_names = {"user\tid", "name"};
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->include_header = true;
    writer_options->column_terminated_by = "\t";
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // The column name "user\tid" should be quoted because it contains the delimiter
    std::string expect = "\"user\tid\"\tname\n";
    ASSERT_EQ(content, expect);
}

// Test no header when include_header is false (default)
TEST_F(CSVFileWriterTest, TestWriteWithoutHeader) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_INT)};
    std::vector<std::string> column_names = {"id"};
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->include_header = false; // explicitly false
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
        std::vector<int32_t> int_nums{1, 2};
        size_t appended = col0->append_numbers(int_nums.data(), size(int_nums) * sizeof(int32_t));
        ASSERT_EQ(appended, size(int_nums));
        chunk->append_column(std::move(col0), chunk->num_columns());
    }

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // No header row, only data
    std::string expect = "1\n2\n";
    ASSERT_EQ(content, expect);
}

// ==================== Compression Tests ====================

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
    auto base_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_stream.get(), 1024 * 1024);
    auto csv_output_stream_result =
            csv::CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024 * 1024);
    ASSERT_OK(csv_output_stream_result.status());
    auto csv_output_stream = std::move(csv_output_stream_result.value());
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(csv_output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // Note: writer->close() already calls finalize() on CompressedOutputStream
    // which internally closes the async stream, so we don't call async_stream->close() here

    // verify correctness - read compressed data and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    std::string content = test::decompress_gzip(compressed_content);
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
    auto base_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_stream.get(), 1024 * 1024);
    auto csv_output_stream_result =
            csv::CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024 * 1024);
    ASSERT_OK(csv_output_stream_result.status());
    auto csv_output_stream = std::move(csv_output_stream_result.value());
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(csv_output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // Note: writer->close() already calls finalize() which closes the async stream

    // verify correctness
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    std::string content = test::decompress_gzip(compressed_content);
    std::string expect = "hello\nworld\nstarrocks\nlakehouse\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteLargeDataWithGzipCompression) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_INT),
                                           TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_stream.get(), 1024 * 1024);
    auto csv_output_stream_result =
            csv::CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024 * 1024);
    ASSERT_OK(csv_output_stream_result.status());
    auto csv_output_stream = std::move(csv_output_stream_result.value());
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(csv_output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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

    auto result = writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, total_rows);

    // Note: writer->close() already calls finalize() which closes the async stream

    // Verify compressed file size is smaller than expected uncompressed size
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    // Decompress and verify first and last few lines
    std::string content = test::decompress_gzip(compressed_content);
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
    const int num_lines = 100; // Use fewer lines to avoid edge cases
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
        auto base_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_stream.get(), 1024 * 1024);
        auto csv_output_stream_result =
                csv::CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024 * 1024);
        ASSERT_OK(csv_output_stream_result.status());
        auto csv_output_stream = std::move(csv_output_stream_result.value());
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
        auto writer_options = std::make_shared<formats::CSVWriterOptions>();
        auto writer = std::make_unique<formats::CSVFileWriter>(compressed_path, std::move(csv_output_stream),
                                                               column_names, type_descs, std::move(column_evaluators),
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
        ASSERT_OK(writer->close().io_status);
    }

    // Read and verify compressed file
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(compressed_path, &compressed_content));

    // Compressed should be significantly smaller than uncompressed
    EXPECT_LT(compressed_content.size(), expected_uncompressed_size / 2);

    // Verify decompressed content matches expected
    std::string decompressed = test::decompress_gzip(compressed_content);
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
    auto result = writer_and_stream.writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // Note: writer->close() already calls finalize() which closes the stream

    // Verify compressed output
    std::string compressed_content;
    ASSERT_OK(fs->read_file("/test_compressed.csv.gz", &compressed_content));

    std::string content = test::decompress_gzip(compressed_content);
    EXPECT_TRUE(content.find("1,test1\n") != std::string::npos);
    EXPECT_TRUE(content.find("2,test2\n") != std::string::npos);
}

// ==================== Enclose / Escape Tests ====================

// Helper: build a simple INT + VARCHAR + nullable-VARCHAR chunk for enclose tests.
// Returns writer options pre-configured with given enclose/escape.
namespace {

std::shared_ptr<Chunk> _build_enclose_test_chunk(const std::vector<int32_t>& ids, const std::vector<std::string>& names,
                                                 const std::vector<std::string>& nullable_values,
                                                 const std::vector<uint8_t>& nullable_nulls) {
    auto chunk = std::make_shared<Chunk>();

    // Column 0: non-nullable INT
    auto id_col = Int32Column::create();
    for (auto v : ids) {
        id_col->append(v);
    }
    chunk->append_column(std::move(id_col), chunk->num_columns());

    // Column 1: nullable VARCHAR (never-null values)
    auto name_data = BinaryColumn::create();
    for (auto& s : names) {
        name_data->append(s);
    }
    auto name_null = UInt8Column::create();
    name_null->append_numbers(std::vector<uint8_t>(names.size(), 0).data(), names.size());
    auto name_col = NullableColumn::create(std::move(name_data), std::move(name_null));
    chunk->append_column(std::move(name_col), chunk->num_columns());

    // Column 2: nullable VARCHAR (with actual NULLs)
    auto nullable_data = BinaryColumn::create();
    for (auto& s : nullable_values) {
        nullable_data->append(s);
    }
    auto nullable_null = UInt8Column::create();
    nullable_null->append_numbers(nullable_nulls.data(), nullable_nulls.size());
    auto nullable_col = NullableColumn::create(std::move(nullable_data), std::move(nullable_null));
    chunk->append_column(std::move(nullable_col), chunk->num_columns());

    return chunk;
}

} // namespace

// Case: enclose == escape == '"'  (RFC 4180 doubling style)
TEST_F(CSVFileWriterTest, TestEncloseEscapeRFC4180) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "name", "val"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->enclose = '"';
    writer_options->escape = '"';

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    // Rows:
    //   (1, "hello",        "not null")    -- plain
    //   (2, "hello,world",  NULL)          -- comma + NULL
    //   (3, "say \"hi\"",   "val")         -- internal double quotes
    auto chunk = _build_enclose_test_chunk({1, 2, 3}, {"hello", "hello,world", "say \"hi\""}, {"not null", "", "val"},
                                           {0, 1, 0});
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->close().io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Expected:
    //   "1","hello","not null"\n
    //   "2","hello,world",\N\n
    //   "3","say ""hi""","val"\n
    std::string expect =
            "\"1\",\"hello\",\"not null\"\n"
            "\"2\",\"hello,world\",\\N\n"
            "\"3\",\"say \"\"hi\"\"\",\"val\"\n";
    ASSERT_EQ(content, expect);
}

// Case: enclose == '"', escape == '\\'  (backslash escape style)
// Verifies:
//   - internal enclose char ('"') is escaped with '\\'
//   - internal escape char ('\\') itself is also escaped (for reader roundtrip)
//   - NULL stays as \N (unenclosed)
TEST_F(CSVFileWriterTest, TestEncloseEscapeBackslash) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "name", "val"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->enclose = '"';
    writer_options->escape = '\\';

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    // Rows:
    //   (1, "say \"hi\"", "not null")     -- enclose char inside
    //   (2, "path\\to",   NULL)           -- escape char inside
    //   (3, "end\\",      "val")          -- trailing escape char
    //   (4, "a\\\"b",     "x")            -- both escape and enclose chars
    auto chunk = _build_enclose_test_chunk({1, 2, 3, 4}, {"say \"hi\"", "path\\to", "end\\", "a\\\"b"},
                                           {"not null", "", "val", "x"}, {0, 1, 0, 0});
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->close().io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Expected (C++ escaped):
    //   "1","say \"hi\"","not null"\n
    //   "2","path\\to",\N\n
    //   "3","end\\","val"\n
    //   "4","a\\\"b","x"\n
    std::string expect =
            "\"1\",\"say \\\"hi\\\"\",\"not null\"\n"
            "\"2\",\"path\\\\to\",\\N\n"
            "\"3\",\"end\\\\\",\"val\"\n"
            "\"4\",\"a\\\\\\\"b\",\"x\"\n";
    ASSERT_EQ(content, expect);
}

// Case: enclose set but escape not set (escape == 0).
// Fields are wrapped but internal enclose chars are NOT escaped (caller's responsibility).
TEST_F(CSVFileWriterTest, TestEncloseWithoutEscape) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    std::vector<std::string> column_names = {"name"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->enclose = '"';
    // escape intentionally left as 0

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    auto data = BinaryColumn::create();
    data->append("hello");
    data->append("a,b");
    auto nulls = UInt8Column::create();
    nulls->append_numbers(std::vector<uint8_t>{0, 0}.data(), 2);
    chunk->append_column(NullableColumn::create(std::move(data), std::move(nulls)), chunk->num_columns());

    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->close().io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Enclose only, no escape applied
    std::string expect = "\"hello\"\n\"a,b\"\n";
    ASSERT_EQ(content, expect);
}

// Case: enclose with include_header=true.
// Header column names are also enclosed & escaped using the same rules.
TEST_F(CSVFileWriterTest, TestEncloseWithHeader) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    // Column name with enclose char inside
    std::vector<std::string> column_names = {"id", "my\"col"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->enclose = '"';
    writer_options->escape = '"';
    writer_options->include_header = true;

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    auto id_col = Int32Column::create();
    id_col->append(1);
    chunk->append_column(std::move(id_col), chunk->num_columns());

    auto name_data = BinaryColumn::create();
    name_data->append("alice");
    auto name_nulls = UInt8Column::create();
    name_nulls->append(0);
    chunk->append_column(NullableColumn::create(std::move(name_data), std::move(name_nulls)), chunk->num_columns());

    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->close().io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Header: "id","my""col"\n
    // Row:    "1","alice"\n
    std::string expect = "\"id\",\"my\"\"col\"\n\"1\",\"alice\"\n";
    ASSERT_EQ(content, expect);
}

// Case: enclose disabled (default). Behavior must be unchanged.
TEST_F(CSVFileWriterTest, TestEncloseDisabled) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "name"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    // enclose / escape default to 0

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    auto id_col = Int32Column::create();
    id_col->append(1);
    id_col->append(2);
    chunk->append_column(std::move(id_col), chunk->num_columns());

    auto name_data = BinaryColumn::create();
    name_data->append("alice");
    name_data->append("bob");
    auto name_nulls = UInt8Column::create();
    name_nulls->append_numbers(std::vector<uint8_t>{0, 0}.data(), 2);
    chunk->append_column(NullableColumn::create(std::move(name_data), std::move(name_nulls)), chunk->num_columns());

    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->close().io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Original raw output
    std::string expect = "1,alice\n2,bob\n";
    ASSERT_EQ(content, expect);
}

// Regression: enclose path must tolerate ConstColumn(Nullable) inputs, which can
// arise from expression evaluation (e.g. `SELECT NULL ...`). An unconditional
// down_cast<NullableColumn*> previously asserted here. With the polymorphic
// is_null() check, the writer should treat the row as NULL and emit \N.
TEST_F(CSVFileWriterTest, TestEncloseConstNullableColumn) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "const_col"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->enclose = '"';
    writer_options->escape = '"';

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    constexpr size_t kNumRows = 3;

    // Column 0: regular INT column.
    auto id_col = Int32Column::create();
    id_col->append(1);
    id_col->append(2);
    id_col->append(3);
    chunk->append_column(std::move(id_col), chunk->num_columns());

    // Column 1: ConstColumn wrapping a NullableColumn that holds a single NULL.
    // This mimics what ExprContext produces for a `SELECT NULL` literal.
    auto data_col = BinaryColumn::create();
    data_col->append(""); // placeholder; null bitmap marks it null
    auto null_col = UInt8Column::create();
    null_col->append(1);
    auto nullable_inner = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto const_null_col = ConstColumn::create(std::move(nullable_inner), kNumRows);
    chunk->append_column(std::move(const_null_col), chunk->num_columns());

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Each row: enclosed id, unenclosed \N for the const null column.
    std::string expect = "\"1\",\\N\n\"2\",\\N\n\"3\",\\N\n";
    ASSERT_EQ(content, expect);
}

// Regression: even with enclose disabled (the default / pre-existing path),
// the writer must tolerate ConstColumn inputs. Before the
// unpack_and_duplicate_const_column fix, every converter (BinaryConverter,
// NullableConverter, etc.) would crash on its own down_cast.
TEST_F(CSVFileWriterTest, TestConstColumnWithoutEnclose) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "const_str"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    // enclose/escape left at default (disabled)

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    constexpr size_t kNumRows = 2;

    auto id_col = Int32Column::create();
    id_col->append(10);
    id_col->append(20);
    chunk->append_column(std::move(id_col), chunk->num_columns());

    auto data_col = BinaryColumn::create();
    data_col->append("hello");
    auto const_str_col = ConstColumn::create(std::move(data_col), kNumRows);
    chunk->append_column(std::move(const_str_col), chunk->num_columns());

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "10,hello\n20,hello\n";
    ASSERT_EQ(content, expect);
}

// Regression: enclose path must tolerate non-nullable ConstColumn(BinaryColumn)
// inputs, which can arise from constant literal expressions (e.g.
// `SELECT 'hello,world' ...`). Previously such columns would bypass the writer
// NULL check and hit a NullableColumn/BinaryColumn down_cast inside the
// converter. With unpack_and_duplicate_const_column at the top of write(), the
// converter always sees a concrete column type.
TEST_F(CSVFileWriterTest, TestEncloseConstStringColumn) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_VARCHAR),
    };
    std::vector<std::string> column_names = {"id", "const_str"};
    auto output_file = std::move(_fs.new_writable_file(_file_path).value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->enclose = '"';
    writer_options->escape = '"';

    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    constexpr size_t kNumRows = 3;

    // Column 0: regular INT.
    auto id_col = Int32Column::create();
    id_col->append(1);
    id_col->append(2);
    id_col->append(3);
    chunk->append_column(std::move(id_col), chunk->num_columns());

    // Column 1: ConstColumn wrapping a non-null BinaryColumn — a value that also
    // contains the column separator and the enclose char, to exercise escaping.
    auto data_col = BinaryColumn::create();
    data_col->append("hello,\"world\"");
    auto const_str_col = ConstColumn::create(std::move(data_col), kNumRows);
    chunk->append_column(std::move(const_str_col), chunk->num_columns());

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();
    ASSERT_OK(result.io_status);

    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    // Each row: enclosed id, enclosed constant string with doubled quotes.
    std::string expect =
            "\"1\",\"hello,\"\"world\"\"\"\n"
            "\"2\",\"hello,\"\"world\"\"\"\n"
            "\"3\",\"hello,\"\"world\"\"\"\n";
    ASSERT_EQ(content, expect);
}

// Factory: verify enclose/escape options are parsed from map.
TEST_F(CSVFileWriterTest, TestFactoryWithEncloseEscapeOptions) {
    auto type_varchar = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    std::vector<TypeDescriptor> type_descs{type_varchar};
    std::vector<std::string> column_names = {"name"};
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto fs = std::make_shared<MemoryFileSystem>();

    std::map<std::string, std::string> options;
    options[formats::CSVWriterOptions::ENCLOSE] = "\"";
    options[formats::CSVWriterOptions::ESCAPE] = "\\";

    auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, options, column_names,
                                                 std::move(column_evaluators), nullptr, nullptr);
    ASSERT_OK(factory.init());
    auto maybe_writer = factory.create("/test_enclose.csv");
    ASSERT_OK(maybe_writer.status());
}

TEST_F(CSVFileWriterTest, TestCompressionWithCustomDelimiters) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_INT),
                                           TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(output_file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_stream.get(), 1024 * 1024);
    auto csv_output_stream_result =
            csv::CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024 * 1024);
    ASSERT_OK(csv_output_stream_result.status());
    auto csv_output_stream = std::move(csv_output_stream_result.value());
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    writer_options->column_terminated_by = "|";
    writer_options->line_terminated_by = ";\n";
    auto writer =
            std::make_unique<formats::CSVFileWriter>(_file_path, std::move(csv_output_stream), column_names, type_descs,
                                                     std::move(column_evaluators), writer_options, []() {});
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
    auto result = writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);

    // Note: writer->close() already calls finalize() which closes the async stream

    // Verify custom delimiters are preserved after compression
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(_file_path, &compressed_content));

    std::string content = test::decompress_gzip(compressed_content);
    EXPECT_EQ(content, "100|value1;\n200|value2;\n");
}

} // namespace starrocks::formats

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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_TINYINT), true);
        std::vector<int8_t> int8_nums{INT8_MIN, INT8_MAX, 0, 1};
        auto count = col0->append_numbers(int8_nums.data(), size(int8_nums) * sizeof(int8_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col0, chunk->num_columns());

        auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_SMALLINT), true);
        std::vector<int16_t> int16_nums{INT16_MIN, INT16_MAX, 0, 1};
        count = col1->append_numbers(int16_nums.data(), size(int16_nums) * sizeof(int16_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col1, chunk->num_columns());

        auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        count = col2->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col2, chunk->num_columns());

        auto col3 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
        std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col3->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col3, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BooleanColumn::create();
        std::vector<uint8_t> values = {0, 1, 1, 0};
        data_column->append_numbers(values.data(), values.size() * sizeof(uint8_t));
        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
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
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
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
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
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
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
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
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
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
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
    ASSERT_OK(writer->init());

    // [1], NULL, [], [2, NULL, 3]
    auto chunk = std::make_shared<Chunk>();
    {
        auto elements_data_col = Int32Column::create();
        std::vector<int32_t> nums{1, 2, -99, 3};
        elements_data_col->append_numbers(nums.data(), sizeof(int32_t) * nums.size());
        auto elements_null_col = UInt8Column::create();
        std::vector<uint8_t> nulls{0, 0, 1, 0};
        elements_null_col->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto elements_col = NullableColumn::create(elements_data_col, elements_null_col);

        auto offsets_col = UInt32Column::create();
        std::vector<uint32_t> offsets{0, 1, 1, 1, 4};
        offsets_col->append_numbers(offsets.data(), sizeof(uint32_t) * offsets.size());
        auto array_col = ArrayColumn::create(elements_col, offsets_col);

        std::vector<uint8_t> _nulls{0, 1, 0, 0};
        auto null_col = UInt8Column::create();
        null_col->append_numbers(_nulls.data(), sizeof(uint8_t) * _nulls.size());
        auto nullable_col = NullableColumn::create(array_col, null_col);

        chunk->append_column(nullable_col, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "[1]\n\\N\n[]\n[2,null,3]\n"; // TODO(letian-jiang): check if this is valid
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
    ASSERT_OK(writer->init());

    // [1 -> 1], NULL, [], [2 -> 2, 3 -> NULL, 4 -> 4]
    auto chunk = std::make_shared<Chunk>();
    {
        auto key_data_col = Int32Column::create();
        std::vector<int32_t> key_nums{1, 2, 3, 4};
        key_data_col->append_numbers(key_nums.data(), sizeof(int32_t) * key_nums.size());
        auto key_null_col = UInt8Column::create();
        std::vector<uint8_t> key_nulls{0, 0, 0, 0};
        key_null_col->append_numbers(key_nulls.data(), sizeof(uint8_t) * key_nulls.size());
        auto key_col = NullableColumn::create(key_data_col, key_null_col);

        auto value_data_col = Int32Column::create();
        std::vector<int32_t> value_nums{1, 2, -99, 4};
        value_data_col->append_numbers(value_nums.data(), sizeof(int32_t) * value_nums.size());
        auto value_null_col = UInt8Column::create();
        std::vector<uint8_t> value_nulls{0, 0, 1, 0};
        value_null_col->append_numbers(value_nulls.data(), sizeof(uint8_t) * value_nulls.size());
        auto value_col = NullableColumn::create(value_data_col, value_null_col);

        auto offsets_col = UInt32Column::create();
        std::vector<uint32_t> offsets{0, 1, 1, 1, 4};
        offsets_col->append_numbers(offsets.data(), sizeof(uint32_t) * offsets.size());
        auto map_col = MapColumn::create(key_col, value_col, offsets_col);

        std::vector<uint8_t> _nulls{0, 1, 0, 0};
        auto null_col = UInt8Column::create();
        null_col->append_numbers(_nulls.data(), sizeof(uint8_t) * _nulls.size());
        auto nullable_col = NullableColumn::create(map_col, null_col);

        chunk->append_column(nullable_col, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "{1:1}\n\\N\n{}\n{2:2,3:null,4:4}\n"; // TODO(letian-jiang): check if this is valid
    ASSERT_EQ(content, expect);
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
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, nullptr);
    ASSERT_OK(writer->init());

    // [[1], NULL, [], [2, NULL, 3]], [[4, 5], [6]], NULL
    auto chunk = std::make_shared<Chunk>();
    {
        auto int_data_col = Int32Column::create();
        std::vector<int32_t> nums{1, 2, -99, 3, 4, 5, 6};
        int_data_col->append_numbers(nums.data(), sizeof(int32_t) * nums.size());
        auto int_null_col = UInt8Column::create();
        std::vector<uint8_t> nulls{0, 0, 1, 0, 0, 0, 0};
        int_null_col->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto int_col = NullableColumn::create(int_data_col, int_null_col);

        auto offsets_col = UInt32Column::create();
        std::vector<uint32_t> offsets{0, 1, 1, 1, 4, 6, 7};
        offsets_col->append_numbers(offsets.data(), sizeof(uint32_t) * offsets.size());
        auto array_data_col = ArrayColumn::create(int_col, offsets_col);

        std::vector<uint8_t> _nulls{0, 1, 0, 0, 0, 0};
        auto array_null_col = UInt8Column::create();
        array_null_col->append_numbers(_nulls.data(), sizeof(uint8_t) * _nulls.size());
        auto array_col = NullableColumn::create(array_data_col, array_null_col);

        auto array_array_offsets_col = UInt32Column::create();
        std::vector<uint32_t> array_array_offsets{0, 4, 6, 6};
        array_array_offsets_col->append_numbers(array_array_offsets.data(),
                                                sizeof(uint32_t) * array_array_offsets.size());
        auto array_array_data_col = ArrayColumn::create(array_col, array_array_offsets_col);

        std::vector<uint8_t> outer_nulls{0, 0, 1};
        auto array_array_null_col = UInt8Column::create();
        array_array_null_col->append_numbers(outer_nulls.data(), sizeof(uint8_t) * outer_nulls.size());
        auto array_array_col = NullableColumn::create(array_array_data_col, array_array_null_col);

        chunk->append_column(array_array_col, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "[[1],null,[],[2,null,3]]\n[[4,5],[6]]\n\\N\n"; // TODO(letian-jiang): check if this is valid
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestWriteWithExecutors) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

    auto column_names = _make_type_names(type_descs);
    auto maybe_output_file = _fs.new_writable_file(_file_path);
    EXPECT_OK(maybe_output_file.status());
    auto output_file = std::move(maybe_output_file.value());
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(output_file), 1024);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::CSVWriterOptions>();
    auto executors = PriorityThreadPool("test", 1, 1);
    auto writer = std::make_unique<formats::CSVFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            writer_options, []() {}, &executors);
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BooleanColumn::create();
        std::vector<uint8_t> values = {0, 1, 1, 0};
        data_column->append_numbers(values.data(), values.size() * sizeof(uint8_t));
        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk).get());
    auto result = writer->commit().get();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // verify correctness
    std::string content;
    ASSERT_OK(_fs.read_file(_file_path, &content));
    std::string expect = "\\N\ntrue\n\\N\nfalse\n";
    ASSERT_EQ(content, expect);
}

TEST_F(CSVFileWriterTest, TestFactory) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto fs = std::make_shared<MemoryFileSystem>();
    auto factory = formats::CSVFileWriterFactory(fs, {}, column_names, std::move(column_evaluators));
    auto maybe_writer = factory.create("/test.csv");
    ASSERT_OK(maybe_writer.status());
    auto writer = maybe_writer.value();
    ASSERT_OK(writer->init());
}

} // namespace starrocks::formats

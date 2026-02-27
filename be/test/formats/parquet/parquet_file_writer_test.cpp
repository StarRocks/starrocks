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

#include "formats/parquet/parquet_file_writer.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "testutil/column_test_helper.h"

namespace starrocks::formats {

class ParquetFileWriterTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
        _writer_options = std::make_shared<ParquetWriterOptions>();
        auto output_file = _fs.new_writable_file(_file_path).value();
        _output_stream = std::make_unique<parquet::ParquetOutputStream>(std::move(output_file));
    }

    void TearDown() override {}

protected:
    HdfsScannerContext* _create_scan_context(const std::vector<TypeDescriptor>& type_descs) {
        auto ctx = _pool.add(new HdfsScannerContext());
        ctx->lazy_column_coalesce_counter = &_lazy_column_coalesce_counter;

        std::vector<parquet::Utils::SlotDesc> slot_descs;
        for (auto& type_desc : type_descs) {
            auto type_name = type_desc.debug_string();
            slot_descs.push_back({type_name, type_desc});
        }
        slot_descs.push_back({""});

        TupleDescriptor* tuple_desc =
                parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs.data());
        parquet::Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        ctx->scan_range = _create_scan_range(_file_path, file_size);
        ctx->timezone = "Asia/Shanghai";
        ctx->stats = &_hdfs_scan_stats;

        return ctx;
    }

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t file_length) {
        auto* scan_range = _pool.add(new THdfsScanRange());
        scan_range->relative_path = file_path;
        scan_range->file_length = file_length;
        scan_range->offset = 4;
        scan_range->length = file_length;

        return scan_range;
    }

    static std::vector<std::string> _make_type_names(const std::vector<TypeDescriptor>& type_descs) {
        std::vector<std::string> names;
        for (auto& desc : type_descs) {
            names.push_back(desc.debug_string());
        }
        return names;
    }

    ChunkPtr _read_chunk(const std::vector<TypeDescriptor>& type_descs) {
        auto ctx = _create_scan_context(type_descs);
        ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);

        auto st = file_reader->init(ctx);
        if (!st.ok()) {
            std::cout << st.to_string() << std::endl;
            return nullptr;
        }

        auto read_chunk = std::make_shared<Chunk>();
        for (auto type_desc : type_descs) {
            auto col = ColumnHelper::create_column(type_desc, true);
            read_chunk->append_column(std::move(col), read_chunk->num_columns());
        }

        EXPECT_OK(file_reader->get_next(&read_chunk));
        return read_chunk;
    }

    StatusOr<std::unique_ptr<ParquetFileWriter>> _create_writer(const std::vector<TypeDescriptor>& type_descs,
                                                                std::vector<bool> nullable = {},
                                                                std::vector<std::string> column_names = {});

    HdfsScanStats _hdfs_scan_stats;
    MemoryFileSystem _fs;
    std::string _file_path{"/dummy_file.parquet"};
    std::unique_ptr<parquet::ParquetOutputStream> _output_stream;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
    std::atomic<int> _lazy_column_coalesce_counter = 0;
    std::shared_ptr<ParquetWriterOptions> _writer_options;
    TCompressionType::type _compression_type = TCompressionType::NO_COMPRESSION;
};

StatusOr<std::unique_ptr<ParquetFileWriter>> ParquetFileWriterTest::_create_writer(
        const std::vector<TypeDescriptor>& type_descs, std::vector<bool> nullable,
        std::vector<std::string> column_names) {
    if (column_names.size() == 0) {
        column_names = _make_type_names(type_descs);
    }
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<ParquetFileWriter>(
            _file_path, std::move(_output_stream), std::move(column_names), type_descs, std::move(column_evaluators),
            _compression_type, _writer_options, [] {}, std::move(nullable));
    if (Status st = writer->init(); st.ok()) {
        return writer;
    } else {
        return st;
    }
}

TEST_F(ParquetFileWriterTest, TestWriteIntegralTypes) {
    std::vector type_descs{TYPE_TINYINT_DESC, TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnTestHelper::build_nullable_column<int8_t>({INT8_MIN, INT8_MAX, 0, 1});
        auto col1 = ColumnTestHelper::build_nullable_column<int16_t>({INT16_MIN, INT16_MAX, 0, 1});
        auto col2 = ColumnTestHelper::build_nullable_column<int32_t>({INT32_MIN, INT32_MAX, 0, 1});
        auto col3 = ColumnTestHelper::build_nullable_column<int64_t>({INT64_MIN, INT64_MAX, 0, 1});

        chunk->append_column(std::move(col0), 0);
        chunk->append_column(std::move(col1), 1);
        chunk->append_column(std::move(col2), 2);
        chunk->append_column(std::move(col3), 3);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDecimal) {
    std::vector type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 10),
    };
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(type_descs[0], true);
        std::vector<int32_t> int32_nums{-999999, 999999, 0, 1};
        ASSERT_EQ(col0->append_numbers(int32_nums.data(), int32_nums.size() * sizeof(int32_t)), 4);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(type_descs[1], true);
        std::vector<int64_t> int64_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col1->append_numbers(int64_nums.data(), int64_nums.size() * sizeof(int64_t)), 4);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(type_descs[2], true);
        std::vector<int128_t> int128_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col2->append_numbers(int128_nums.data(), int128_nums.size() * sizeof(int128_t)), 4);
        chunk->append_column(std::move(col2), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDecimalCompatibleWithHiveReader) {
    std::vector type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 1),
    };
    _writer_options->use_legacy_decimal_encoding = true;
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(type_descs[0], true);
        std::vector<int32_t> int32_nums{-999999, 999999, 0, 1};
        ASSERT_EQ(col0->append_numbers(int32_nums.data(), int32_nums.size() * sizeof(int32_t)), 4);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(type_descs[1], true);
        std::vector<int64_t> int64_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col1->append_numbers(int64_nums.data(), int64_nums.size() * sizeof(int64_t)), 4);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(type_descs[2], true);
        std::vector<int128_t> int128_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col2->append_numbers(int128_nums.data(), int128_nums.size() * sizeof(int128_t)), 4);
        chunk->append_column(std::move(col2), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteBoolean) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<uint8_t>({0, 1, 1, 0}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteFloat) {
    std::vector type_descs{TYPE_FLOAT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<float>({0.1, 1.1, 1.2, -99.9}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDouble) {
    std::vector type_descs{TYPE_DOUBLE_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<double>({0.1, 1.1, 1.2, -99.9}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDate) {
    std::vector type_descs{TYPE_DATE_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

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

        auto null_column = ColumnTestHelper::build_column<uint8_t>({1, 0, 1, 0});
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDatetime) {
    std::vector type_descs{TYPE_DATETIME_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = TimestampColumn::create();
        {
            Datum datum;
            datum.set_timestamp(TimestampValue::create(2023, 9, 9, 23, 59, 59));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1999, 9, 10, 1, 1, 1));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1970, 1, 1, 0, 0, 0));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1970, 1, 1, 1, 1, 1));
            data_column->append_datum(datum);
        }

        auto null_column = ColumnTestHelper::build_column<uint8_t>({0, 0, 0, 0});
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDatetimeCompatibleWithHiveReader) {
    std::vector type_descs{TYPE_DATETIME_DESC};
    _writer_options->use_int96_timestamp_encoding = true;
    _writer_options->time_zone = "Asia/Shanghai";
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

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
            // Daylight Saving Time value
            datum.set_timestamp(TimestampValue::create(1986, 8, 25, 0, 0, 0));
            data_column->append_datum(datum);
            data_column->append_default();
        }

        auto null_column = ColumnTestHelper::build_column<uint8_t>({1, 0, 1, 0, 0});
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 5);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 5);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteVarchar) {
    std::vector type_descs{TYPE_VARCHAR_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<Slice>({"hello", "world", "starrocks", "lakehouse"},
                                                                  {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteArray) {
    std::vector<TypeDescriptor> type_descs;
    type_descs.push_back(TYPE_INT_ARRAY_DESC);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [1], NULL, [], [2, NULL, 3]
    auto chunk = std::make_shared<Chunk>();
    {
        auto elements_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4});
        auto array_col = ArrayColumn::create(std::move(elements_col), std::move(offsets_col));
        auto null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0});
        auto nullable_col = NullableColumn::create(std::move(array_col), std::move(null_col));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteStruct) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto nullable_col_a = ColumnTestHelper::build_nullable_column<int16_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto nullable_col_b = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto nullable_col_c = ColumnTestHelper::build_nullable_column<int64_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto null_column = ColumnTestHelper::build_column<uint8_t>({0, 0, 1, 0});

        Columns fields{std::move(nullable_col_a), std::move(nullable_col_b), std::move(nullable_col_c)};
        auto struct_column = StructColumn::create(std::move(fields), std::move(type_int_struct.field_names));
        auto nullable_col = NullableColumn::create(std::move(struct_column), std::move(null_column));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteMap) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_map);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [1 -> 1], NULL, [], [2 -> 2, 3 -> NULL, 4 -> 4]
    auto chunk = std::make_shared<Chunk>();
    {
        auto key_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4}, {0, 0, 0, 0});
        auto value_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 4}, {0, 0, 1, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4});
        auto null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0});

        auto map_col = MapColumn::create(std::move(key_col), std::move(value_col), std::move(offsets_col));
        auto nullable_col = NullableColumn::create(std::move(map_col), std::move(null_col));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteMapOfNullKey) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_map);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [NULL -> 1], NULL, [], [2 -> 2, 3 -> NULL, 4 -> 4]
    auto chunk = std::make_shared<Chunk>();
    {
        auto key_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4}, {1, 0, 0, 0});
        auto value_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 4}, {0, 0, 1, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4});
        auto null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0});

        auto map_col = MapColumn::create(std::move(key_col), std::move(value_col), std::move(offsets_col));
        auto nullable_col = NullableColumn::create(std::move(map_col), std::move(null_col));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_ERROR(writer->write(chunk.get()));
}

TEST_F(ParquetFileWriterTest, TestWriteNestedArray) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_array_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array_array.children.push_back(TYPE_INT_ARRAY_DESC);
    type_descs.push_back(type_int_array_array);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [[1], NULL, [], [2, NULL, 3]], [[4, 5], [6]], NULL
    auto chunk = std::make_shared<Chunk>();
    {
        auto int_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 3, 4, 5, 6}, {0, 0, 1, 0, 0, 0, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4, 6, 7});
        auto array_data_col = ArrayColumn::create(std::move(int_col), std::move(offsets_col));
        auto array_null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0, 0, 0});
        auto array_col = NullableColumn::create(std::move(array_data_col), std::move(array_null_col));

        auto array_array_offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 4, 6, 6});
        auto array_array_data_col = ArrayColumn::create(std::move(array_col), std::move(array_array_offsets_col));

        auto array_array_null_col = ColumnTestHelper::build_column<uint8_t>({0, 0, 1});
        auto array_array_col = NullableColumn::create(std::move(array_array_data_col), std::move(array_array_null_col));

        chunk->append_column(std::move(array_array_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteVarbinary) {
    std::vector type_descs{TYPE_VARBINARY_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<Slice>({"hello", "world", "starrocks", "lakehouse"},
                                                                  {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestAllocatedBytes) {
    std::vector type_descs{TYPE_VARBINARY_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<Slice>({"hello", "world", "starrocks", "lakehouse"},
                                                                  {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_TRUE(writer->get_allocated_bytes() > 0);
    auto result = writer->close();
    ASSERT_TRUE(writer->get_allocated_bytes() == 0);

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestFlushRowgroup) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    _writer_options->rowgroup_size = 1;
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<uint8_t>({0, 1, 1, 0}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk twice
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 8);
}

TEST_F(ParquetFileWriterTest, TestWriteWithFieldID) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    _writer_options->column_ids = {FileColumnId{1, {}}};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto nullable_column = ColumnTestHelper::build_nullable_column<uint8_t>({0, 1, 1, 0}, {1, 0, 1, 0});
        chunk->append_column(std::move(nullable_column), 0);
    }

    // write chunk twice
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 8);
}

TEST_F(ParquetFileWriterTest, TestUnknownCompression) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    ASSERT_ERROR(_create_writer(type_descs));
}

TEST_F(ParquetFileWriterTest, TestFactory) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(
            ColumnSlotIdEvaluator::from_types(type_descs));
    auto fs = std::make_shared<MemoryFileSystem>();
    auto factory = ParquetFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, column_evaluators,
                                            std::nullopt, nullptr, nullptr);
    ASSERT_OK(factory.init());
    auto maybe_writer = factory.create(_file_path);
    ASSERT_OK(maybe_writer.status());
}

TEST_F(ParquetFileWriterTest, TestWriteJson) {
    std::vector type_descs{TYPE_JSON_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // nullable column
    auto data_column = JsonColumn::create();
    auto json1 = JsonValue::parse(R"({"a": 1, "b": 2})");
    data_column->append(&json1.value());
    data_column->append(&json1.value());
    auto json2 = JsonValue::parse(R"({"a": 3})");
    data_column->append(&json2.value());

    auto null_column = ColumnTestHelper::build_column<uint8_t>({1, 0, 1});
    auto nullable_column = NullableColumn::create(data_column, null_column);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(nullable_column, 0);

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // _read_chunk does not support read json
}

TEST_F(ParquetFileWriterTest, TestNullableColumnsAllRequired) {
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {false, false}));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR) - non-null
        auto col0 = ColumnTestHelper::build_column<Slice>({"file1.parquet", "file2.parquet", "file3.parquet"});
        chunk->append_column(std::move(col0), 0);

        // pos column (BIGINT) - non-null
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 300});
        chunk->append_column(std::move(col1), 1);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has REQUIRED columns
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Check that both columns are REQUIRED
    for (int i = 0; i < file_metadata->schema().get_fields_size(); i++) {
        auto field = file_metadata->schema().get_stored_column_by_field_idx(i);
        auto repetition = field->schema_element.repetition_type;
        EXPECT_EQ(repetition, tparquet::FieldRepetitionType::REQUIRED)
                << "Column " << i << " should be REQUIRED but is " << repetition;
    }
}

TEST_F(ParquetFileWriterTest, TestNullableColumnsMixed) {
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};

    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {true, false}));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR) - nullable
        auto nullable_col0 =
                ColumnTestHelper::build_nullable_column<Slice>({"file1.parquet", "file2.parquet", ""}, {0, 0, 1});
        chunk->append_column(std::move(nullable_col0), chunk->num_columns());

        // pos column (BIGINT) - non-null
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 300});
        chunk->append_column(std::move(col1), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has correct Repetition types
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Check first column is OPTIONAL
    auto col0_field = file_metadata->schema().get_stored_column_by_field_idx(0);
    auto repetition0 = col0_field->schema_element.repetition_type;
    EXPECT_EQ(repetition0, tparquet::FieldRepetitionType::OPTIONAL)
            << "Column 0 should be OPTIONAL but is " << repetition0;

    // Check second column is REQUIRED
    auto col1_field = file_metadata->schema().get_stored_column_by_field_idx(1);
    auto repetition1 = col1_field->schema_element.repetition_type;
    EXPECT_EQ(repetition1, tparquet::FieldRepetitionType::REQUIRED)
            << "Column 1 should be REQUIRED but is " << repetition1;
}

TEST_F(ParquetFileWriterTest, TestNullableColumnsDefaultEmpty) {
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR)
        auto col0 = ColumnTestHelper::build_column<Slice>({"file1.parquet", "file2.parquet", "file3.parquet"});
        chunk->append_column(std::move(col0), 0);

        // pos column (BIGINT)
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 300});
        chunk->append_column(std::move(col1), 1);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has OPTIONAL columns (backward compatibility)
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Check that both columns are OPTIONAL (default behavior)
    for (int i = 0; i < file_metadata->schema().get_fields_size(); i++) {
        auto field = file_metadata->schema().get_stored_column_by_field_idx(i);
        auto repetition = field->schema_element.repetition_type;
        EXPECT_EQ(repetition, tparquet::FieldRepetitionType::OPTIONAL)
                << "Column " << i << " should be OPTIONAL (default) but is " << repetition;
    }
}

TEST_F(ParquetFileWriterTest, TestIcebergDeleteFileColumnsRequired) {
    // Test specific use case: Iceberg position delete file with file_path and pos columns as REQUIRED
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    std::vector nullable = {false, false};
    std::vector<std::string> column_names = {"file_path", "pos"};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, nullable, column_names));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR) - REQUIRED for Iceberg delete files
        auto col0 = ColumnTestHelper::build_column<Slice>({"s3://bucket/table/data/file1.parquet",
                                                           "s3://bucket/table/data/file1.parquet",
                                                           "s3://bucket/table/data/file2.parquet"});
        chunk->append_column(std::move(col0), 0);

        // pos column (BIGINT) - REQUIRED for Iceberg delete files
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 50});
        chunk->append_column(std::move(col1), 1);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has REQUIRED columns for Iceberg delete file
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Verify column names
    EXPECT_EQ(file_metadata->schema().get_stored_column_by_field_idx(0)->name, "file_path");
    EXPECT_EQ(file_metadata->schema().get_stored_column_by_field_idx(1)->name, "pos");

    // Verify both columns are REQUIRED (Iceberg spec requirement)
    for (int i = 0; i < file_metadata->schema().get_fields_size(); i++) {
        auto field = file_metadata->schema().get_stored_column_by_field_idx(i);
        auto repetition = field->schema_element.repetition_type;
        EXPECT_EQ(repetition, tparquet::FieldRepetitionType::REQUIRED)
                << "Column " << field->name << " should be REQUIRED for Iceberg delete files but is " << repetition;
    }
}

} // namespace starrocks::formats

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

#include "formats/parquet/file_writer.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/statusor.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gutil/casts.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"

namespace starrocks::parquet {

static HdfsScanStats g_hdfs_scan_stats;
using starrocks::HdfsScannerContext;

class FileWriterTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }
    void TearDown() override {}

protected:
    HdfsScannerContext* _create_scan_context(const std::vector<TypeDescriptor>& type_descs) {
        auto ctx = _pool.add(new HdfsScannerContext());
        auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
        ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;

        std::vector<Utils::SlotDesc> slot_descs;
        for (auto& type_desc : type_descs) {
            auto type_name = type_desc.debug_string();
            slot_descs.push_back({type_name, type_desc});
        }
        slot_descs.push_back({""});

        ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs.data());
        Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_path, file_size));
        ctx->timezone = "Asia/Shanghai";
        ctx->stats = &g_hdfs_scan_stats;

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

    std::vector<std::string> _make_type_names(const std::vector<TypeDescriptor>& type_descs) {
        std::vector<std::string> names;
        for (auto& desc : type_descs) {
            names.push_back(desc.debug_string());
        }
        return names;
    }

    std::shared_ptr<::parquet::schema::GroupNode> _make_schema(const std::vector<TypeDescriptor>& type_descs) {
        auto type_names = _make_type_names(type_descs);
        auto ret =
                ParquetBuildHelper::make_schema(type_names, type_descs, std::vector<FileColumnId>(type_descs.size()));
        if (!ret.ok()) {
            return nullptr;
        }
        auto schema = ret.ValueOrDie();
        return schema;
    }

    Status _write_chunk(const ChunkPtr& chunk, const std::vector<TypeDescriptor>& type_descs,
                        const std::shared_ptr<::parquet::schema::GroupNode>& schema) {
        ASSIGN_OR_ABORT(auto file, _fs.new_writable_file(_file_path));
        auto properties = ParquetBuildHelper::make_properties(ParquetBuilderOptions());
        auto file_writer = std::make_shared<SyncFileWriter>(std::move(file), properties, schema, type_descs);
        file_writer->init();
        auto st = file_writer->write(chunk.get());
        if (!st.ok()) {
            std::cout << st.to_string() << std::endl;
            return st;
        }
        return file_writer->close();
    }

    ChunkPtr _read_chunk(const std::vector<TypeDescriptor>& type_descs) {
        auto ctx = _create_scan_context(type_descs);
        ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), file_size);

        auto st = file_reader->init(ctx);
        if (!st.ok()) {
            std::cout << st.to_string() << std::endl;
            return nullptr;
        }

        auto read_chunk = std::make_shared<Chunk>();
        for (auto type_desc : type_descs) {
            auto col = ColumnHelper::create_column(type_desc, true);
            read_chunk->append_column(col, read_chunk->num_columns());
        }

        file_reader->get_next(&read_chunk);
        return read_chunk;
    }

    MemoryFileSystem _fs;
    std::string _file_path{"/dummy_file.parquet"};
    RuntimeState* _runtime_state;
    ObjectPool _pool;
};

TEST_F(FileWriterTest, TestWriteIntegralTypes) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT),
            TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_BIGINT),
    };

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
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteDecimal) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 10),
    };

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(type_descs[0], true);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        auto count = col0->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col0, chunk->num_columns());

        auto col1 = ColumnHelper::create_column(type_descs[1], true);
        std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col1->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col1, chunk->num_columns());

        auto col2 = ColumnHelper::create_column(type_descs[2], true);
        std::vector<int128_t> int128_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col2->append_numbers(int128_nums.data(), size(int128_nums) * sizeof(int128_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col2, chunk->num_columns());
    }

    // write chunk
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteBoolean) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

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
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteFloat) {
    auto type_float = TypeDescriptor::from_logical_type(TYPE_FLOAT);
    std::vector<TypeDescriptor> type_descs{type_float};

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = FloatColumn::create();
        std::vector<float> values = {0.1, 1.1, 1.2, -99.9};
        data_column->append_numbers(values.data(), values.size() * sizeof(float));
        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteDouble) {
    auto type_float = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    std::vector<TypeDescriptor> type_descs{type_float};

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = DoubleColumn::create();
        std::vector<double> values = {0.1, 1.1, 1.2, -99.9};
        data_column->append_numbers(values.data(), values.size() * sizeof(double));
        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteDate) {
    auto type_date = TypeDescriptor::from_logical_type(TYPE_DATE);
    std::vector<TypeDescriptor> type_descs{type_date};

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
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteDatetime) {
    auto type_datetime = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    std::vector<TypeDescriptor> type_descs{type_datetime};

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
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteVarchar) {
    auto type_varchar = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    std::vector<TypeDescriptor> type_descs{type_varchar};

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = BinaryColumn::create();
        data_column->append("hello");
        data_column->append("world");
        data_column->append("starrocks");
        data_column->append("lakehouse");

        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    // write chunk
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteArray) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_descs.push_back(type_int_array);

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
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteStruct) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_a = TypeDescriptor::from_logical_type(TYPE_SMALLINT);
    auto type_int_b = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_c = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {type_int_a, type_int_b, type_int_c};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    auto chunk = std::make_shared<Chunk>();
    {
        std::vector<uint8_t> nulls{0, 0, 1, 0};

        auto data_col_a = Int16Column::create();
        std::vector<int16_t> nums_a{1, 2, -99, 3};
        data_col_a->append_numbers(nums_a.data(), sizeof(int16_t) * nums_a.size());
        auto null_col_a = UInt8Column::create();
        null_col_a->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col_a = NullableColumn::create(data_col_a, null_col_a);

        auto data_col_b = Int32Column::create();
        std::vector<int32_t> nums_b{1, 2, -99, 3};
        data_col_b->append_numbers(nums_b.data(), sizeof(int32_t) * nums_b.size());
        auto null_col_b = UInt8Column::create();
        null_col_b->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col_b = NullableColumn::create(data_col_b, null_col_b);

        auto data_col_c = Int64Column::create();
        std::vector<int64_t> nums_c{1, 2, -99, 3};
        data_col_c->append_numbers(nums_c.data(), sizeof(int64_t) * nums_c.size());
        auto null_col_c = UInt8Column::create();
        null_col_c->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col_c = NullableColumn::create(data_col_c, null_col_c);

        Columns fields{nullable_col_a, nullable_col_b, nullable_col_c};
        auto struct_column = StructColumn::create(fields, type_int_struct.field_names);
        auto null_column = UInt8Column::create();
        null_column->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col = NullableColumn::create(struct_column, null_column);

        chunk->append_column(nullable_col, chunk->num_columns());
    }

    // write chunk
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteMap) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_key = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_value = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(type_int_key);
    type_int_map.children.push_back(type_int_value);
    type_descs.push_back(type_int_map);

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
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestWriteNestedArray) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    auto type_int_array_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_int_array_array.children.push_back(type_int_array);
    type_descs.push_back(type_int_array_array);

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
    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema != nullptr);
    auto st = _write_chunk(chunk, type_descs, schema);
    ASSERT_OK(st);

    // read chunk and assert equality
    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(FileWriterTest, TestVarbinaryNotSupport) {
    auto type_varbinary = TypeDescriptor::from_logical_type(TYPE_VARBINARY);
    std::vector<TypeDescriptor> type_descs{type_varbinary};

    auto schema = _make_schema(type_descs);
    ASSERT_TRUE(schema == nullptr);
}

TEST_F(FileWriterTest, TestFieldIdWithStruct) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    auto type_int_a = TypeDescriptor::from_logical_type(TYPE_SMALLINT);
    auto type_int_b = TypeDescriptor::from_logical_type(TYPE_INT);

    type_int_struct.children = {type_int_a, type_int_b};
    type_int_struct.field_names = {"a", "b"};
    type_descs.push_back(type_int_struct);

    FileColumnId group_file_id;
    std::vector<FileColumnId> children_file_ids = {{.field_id = 22}, {.field_id = 33}};
    group_file_id.field_id = 11;
    group_file_id.children = children_file_ids;

    auto schema = ParquetBuildHelper::make_schema(std::vector<std::string>{"column"}, type_descs,
                                                  std::vector<FileColumnId>{group_file_id});
    auto root_group_node = schema.ValueOrDie();
    ASSERT_TRUE(root_group_node->is_group());
    ASSERT_EQ(root_group_node->field_count(), 1);

    auto struct_node = root_group_node->field(0);
    ASSERT_TRUE(struct_node->is_group());
    ASSERT_EQ(struct_node->field_id(), 11);
    ASSERT_EQ(struct_node->name(), "column");

    auto struct_group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(struct_node);
    ASSERT_EQ(struct_group_node->field_count(), 2);
    ASSERT_EQ(struct_group_node->field(0)->field_id(), 22);
    ASSERT_EQ(struct_group_node->field(1)->field_id(), 33);
    ASSERT_EQ(struct_group_node->field(0)->name(), "a");
    ASSERT_EQ(struct_group_node->field(1)->name(), "b");
}

} // namespace starrocks::parquet

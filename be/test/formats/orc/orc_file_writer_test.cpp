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

#include "formats/orc/orc_file_writer.h"

#include <gtest/gtest.h>

#include <ctime>
#include <filesystem>
#include <iostream>
#include <map>
#include <vector>

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"
#include "formats/orc/orc_chunk_reader.h"
#include "fs/fs_memory.h"
#include "fs/fs_posix.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::formats {

static void assert_equal_chunk(const Chunk* expected, const Chunk* actual) {
    if (expected->debug_columns() != actual->debug_columns()) {
        std::cout << expected->debug_columns() << std::endl;
        std::cout << actual->debug_columns() << std::endl;
    }
    ASSERT_EQ(expected->debug_columns(), actual->debug_columns());
    for (size_t i = 0; i < expected->num_columns(); i++) {
        const auto& expected_col = expected->get_column_by_index(i);
        const auto& actual_col = actual->get_column_by_index(i);
        if (expected_col->debug_string() != actual_col->debug_string()) {
            std::cout << expected_col->debug_string() << std::endl;
            std::cout << actual_col->debug_string() << std::endl;
        }
        ASSERT_EQ(expected_col->debug_string(), actual_col->debug_string());
    }
}

class OrcFileWriterTest : public testing::Test {
public:
    void SetUp() override {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = config::vector_chunk_size;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();
        _fs = new_fs_posix();
    };
    void TearDown() override { _fs->delete_file(_file_path); };

protected:
    std::vector<std::string> _make_type_names(const std::vector<TypeDescriptor>& type_descs) {
        std::vector<std::string> names;
        for (auto& desc : type_descs) {
            names.push_back(desc.debug_string());
        }
        return names;
    }

    void _create_tuple_descriptor(RuntimeState* state, ObjectPool* pool, const std::vector<std::string>& column_names,
                                  const std::vector<TypeDescriptor>& type_descs, TupleDescriptor** tuple_desc,
                                  bool nullable) {
        TDescriptorTableBuilder table_desc_builder;

        TTupleDescriptorBuilder tuple_desc_builder;
        for (size_t i = 0; i < column_names.size(); i++) {
            TSlotDescriptorBuilder b2;
            b2.column_name(column_names[i]).type(type_descs[i]).id(i).nullable(nullable);
            tuple_desc_builder.add_slot(b2.build());
        }
        tuple_desc_builder.build(&table_desc_builder);

        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(state, pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size);

        RowDescriptor* row_desc = pool->add(new RowDescriptor(*tbl, row_tuples));
        *tuple_desc = row_desc->tuple_descriptors()[0];
        return;
    }

    void _create_slot_descriptors(RuntimeState* state, ObjectPool* pool, std::vector<SlotDescriptor*>* res,
                                  const std::vector<std::string>& column_names,
                                  const std::vector<TypeDescriptor>& type_descs, bool nullable) {
        TupleDescriptor* tuple_desc;
        _create_tuple_descriptor(state, pool, column_names, type_descs, &tuple_desc, nullable);
        *res = tuple_desc->slots();
        return;
    }

    Status _read_chunk(ChunkPtr& chunk, const std::vector<std::string>& column_names,
                       const std::vector<TypeDescriptor>& type_descs, bool nullable) {
        std::vector<SlotDescriptor*> src_slot_descs;
        _create_slot_descriptors(_runtime_state.get(), &_pool, &src_slot_descs, column_names, type_descs, nullable);
        OrcChunkReader reader(_runtime_state->chunk_size(), src_slot_descs);
        auto input_stream = orc::readLocalFile(_file_path);
        EXPECT_OK(reader.init(std::move(input_stream)));
        auto st = reader.read_next();
        DCHECK(st.ok()) << st.message();

        auto chunk_read = reader.create_chunk();
        st = reader.fill_chunk(&chunk_read);
        DCHECK(st.ok()) << st.message();

        chunk = reader.cast_chunk(&chunk_read);
        return Status::OK();
    }

protected:
    std::unique_ptr<FileSystem> _fs;
    std::string _file_path{"./be/test/exec/test_data/orc_scanner/tmp.orc"};
    ObjectPool _pool;
    std::shared_ptr<RuntimeState> _runtime_state;
};

TEST_F(OrcFileWriterTest, TestWriteIntergersNullable) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT),
            TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_BIGINT),
    };
    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
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

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteIntergersNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT),
            TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_BIGINT),
    };
    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_TINYINT), false);
        std::vector<int8_t> int8_nums{INT8_MIN, INT8_MAX, 0, 1};
        auto count = col0->append_numbers(int8_nums.data(), size(int8_nums) * sizeof(int8_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_SMALLINT), false);
        std::vector<int16_t> int16_nums{INT16_MIN, INT16_MAX, 0, 1};
        count = col1->append_numbers(int16_nums.data(), size(int16_nums) * sizeof(int16_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), false);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        count = col2->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col2), chunk->num_columns());

        auto col3 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), false);
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

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, false));
    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteFloat) {
    auto type_float = TypeDescriptor::from_logical_type(TYPE_FLOAT);
    std::vector<TypeDescriptor> type_descs{type_float};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
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

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));
    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteDouble) {
    auto type_float = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    std::vector<TypeDescriptor> type_descs{type_float};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
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

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));
    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteBooleanNullable) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};
    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
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

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteBooleanNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};
    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BooleanColumn::create();
        std::vector<uint8_t> values = {0, 1, 1, 0};
        data_column->append_numbers(values.data(), values.size() * sizeof(uint8_t));
        chunk->append_column(std::move(data_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, false));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteStringsNullable) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_varchar = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    auto type_char = TypeDescriptor::from_logical_type(TYPE_CHAR);
    std::vector<TypeDescriptor> type_descs{type_varchar, type_char};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BinaryColumn::create();
        data_column->append("hello");
        data_column->append("world");
        data_column->append("starrocks");
        data_column->append("lakehouse");

        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());

        auto data_column2 = BinaryColumn::create();
        data_column2->append("hello");
        data_column2->append("world");
        data_column2->append("abc");
        data_column2->append("def");

        auto null_column2 = UInt8Column::create();
        std::vector<uint8_t> nulls2 = {0, 0, 1, 1};
        null_column2->append_numbers(nulls2.data(), nulls2.size());
        auto nullable_column2 = NullableColumn::create(std::move(data_column2), std::move(null_column2));
        chunk->append_column(std::move(nullable_column2), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteStringsNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_varchar = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    auto type_char = TypeDescriptor::from_logical_type(TYPE_CHAR);
    std::vector<TypeDescriptor> type_descs{type_varchar, type_char};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BinaryColumn::create();
        data_column->append("hello");
        data_column->append("world");
        data_column->append("starrocks");
        data_column->append("lakehouse");

        chunk->append_column(std::move(data_column), chunk->num_columns());

        auto data_column2 = BinaryColumn::create();
        data_column2->append("hello");
        data_column2->append("world");
        data_column2->append("abc");
        data_column2->append("def");

        chunk->append_column(std::move(data_column2), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, false));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteDecimal) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 10),
    };

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(type_descs[0], true);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        auto count = col0->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(type_descs[1], true);
        std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col1->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(type_descs[2], true);
        std::vector<int128_t> int128_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col2->append_numbers(int128_nums.data(), size(int128_nums) * sizeof(int128_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col2), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteDecimalNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 10),
    };

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col1 = ColumnHelper::create_column(type_descs[0], false);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        auto count = col1->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(type_descs[1], false);
        std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col2->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col2), chunk->num_columns());

        auto col3 = ColumnHelper::create_column(type_descs[2], false);
        std::vector<int128_t> int128_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col3->append_numbers(int128_nums.data(), size(int128_nums) * sizeof(int128_t));
        ASSERT_EQ(4, count);
        chunk->append_column(std::move(col3), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, false));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteDate) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_date = TypeDescriptor::from_logical_type(TYPE_DATE);
    std::vector<TypeDescriptor> type_descs{type_date};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
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
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteDateNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_date = TypeDescriptor::from_logical_type(TYPE_DATE);
    std::vector<TypeDescriptor> type_descs{type_date};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
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

        chunk->append_column(std::move(data_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, false));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestAllocatedBytes) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_datetime = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    std::vector<TypeDescriptor> type_descs{type_datetime};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
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
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_TRUE(writer->get_allocated_bytes() > 0);
    auto result = writer->commit();
    ASSERT_TRUE(writer->get_allocated_bytes() == 0);
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteTimestamp) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_datetime = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    std::vector<TypeDescriptor> type_descs{type_datetime};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
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

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, true));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteTimestampNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    auto type_datetime = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    std::vector<TypeDescriptor> type_descs{type_datetime};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
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

        chunk->append_column(std::move(data_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    // read chunk
    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, false));

    // verify correctness
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(OrcFileWriterTest, TestWriteStructNullable) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_a = TypeDescriptor::from_logical_type(TYPE_SMALLINT);
    auto type_int_b = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_c = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {type_int_a, type_int_b, type_int_c};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteStructNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_a = TypeDescriptor::from_logical_type(TYPE_SMALLINT);
    auto type_int_b = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_c = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {type_int_a, type_int_b, type_int_c};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteMapNullable) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_key = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_value = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(type_int_key);
    type_int_map.children.push_back(type_int_value);
    type_descs.push_back(type_int_map);

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteMapNotNull) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_key = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_value = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(type_int_key);
    type_int_map.children.push_back(type_int_value);
    type_descs.push_back(type_int_map);

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteArrayNullable) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_descs.push_back(type_int_array);

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteArrayNotNull) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_descs.push_back(type_int_array);

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteNestedArray) {
    ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    auto type_int_array_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_int_array_array.children.push_back(type_int_array);
    type_descs.push_back(type_int_array_array);

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestUnknownCompression) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(
            _file_path, std::move(output_stream), column_names, type_descs, std::move(column_evaluators),
            TCompressionType::UNKNOWN_COMPRESSION, writer_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestFactory) {
    auto type_bool = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    std::vector<TypeDescriptor> type_descs{type_bool};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto fs = std::make_shared<MemoryFileSystem>();
    auto factory = formats::ORCFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names,
                                                 std::move(column_evaluators), nullptr, nullptr);
    ASSERT_OK(factory.init());
    auto maybe_writer = factory.create("/test.orc");
    ASSERT_OK(maybe_writer.status());
}

TEST_F(OrcFileWriterTest, TestOrcPatchedBaseWrongBaseWidth) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_BIGINT)};
    auto column_names = _make_type_names(type_descs);
    auto output_file = _fs->new_writable_file(_file_path).value();
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(output_file));
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ORCWriterOptions>();
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(output_stream), column_names,
                                                           type_descs, std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, writer_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto intColumn = Int64Column::create();
        std::vector<int64_t> intValues = {12, -5400, -5400, 12,    -5400, 12, 12,    -5400, -5400, 24363720,  -2654,
                                          12, -5400, 12,    -5400, -5400, 12, -5400, 12,    12,    -14000000, 4};
        intColumn->append_numbers(intValues.data(), intValues.size() * sizeof(int64_t));
        chunk->append_column(std::move(intColumn), chunk->num_columns());
    }

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->commit();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 22);

    ChunkPtr read_chunk;
    ASSERT_OK(_read_chunk(read_chunk, column_names, type_descs, false));
    assert_equal_chunk(chunk.get(), read_chunk.get());
}

} // namespace starrocks::formats

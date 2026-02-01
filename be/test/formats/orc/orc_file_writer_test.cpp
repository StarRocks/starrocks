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

#include <iostream>
#include <vector>

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"
#include "formats/column_evaluator.h"
#include "formats/orc/orc_chunk_reader.h"
#include "fs/fs_memory.h"
#include "fs/fs_posix.h"
#include "gen_cpp/Exprs_types.h"
#include "io/async_flush_output_stream.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"

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
        ASSERT_OK(ignore_not_found(_fs->delete_file(_file_path)));
        _file = _fs->new_writable_file(_file_path).value();
        _stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(_file), nullptr, _runtime_state.get());
        _orc_stream = std::make_unique<AsyncOrcOutputStream>(_stream.get());
        _write_options = std::make_shared<formats::ORCWriterOptions>();
    };
    void TearDown() override { (void)_fs->delete_file(_file_path); };

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
    std::unique_ptr<WritableFile> _file;
    std::unique_ptr<io::AsyncFlushOutputStream> _stream;
    std::unique_ptr<AsyncOrcOutputStream> _orc_stream;
    std::shared_ptr<formats::ORCWriterOptions> _write_options;
};

TEST_F(OrcFileWriterTest, TestWriteIntergersNullable) {
    std::vector<TypeDescriptor> type_descs{TYPE_TINYINT_DESC, TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnTestHelper::build_nullable_column<int8_t>({INT8_MIN, INT8_MAX, 0, 1});
        auto col1 = ColumnTestHelper::build_nullable_column<int16_t>({INT16_MIN, INT16_MAX, 0, 1});
        auto col2 = ColumnTestHelper::build_nullable_column<int32_t>({INT32_MIN, INT32_MAX, 0, 1});
        auto col3 = ColumnTestHelper::build_nullable_column<int64_t>({INT64_MIN, INT64_MAX, 0, 1});

        chunk->append_column(std::move(col0), chunk->num_columns());
        chunk->append_column(std::move(col1), chunk->num_columns());
        chunk->append_column(std::move(col2), chunk->num_columns());
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
    std::vector<TypeDescriptor> type_descs{TYPE_TINYINT_DESC, TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnTestHelper::build_column<int8_t>({INT8_MIN, INT8_MAX, 0, 1});
        auto col1 = ColumnTestHelper::build_column<int16_t>({INT16_MIN, INT16_MAX, 0, 1});
        auto col2 = ColumnTestHelper::build_column<int32_t>({INT32_MIN, INT32_MAX, 0, 1});
        auto col3 = ColumnTestHelper::build_column<int64_t>({INT64_MIN, INT64_MAX, 0, 1});

        chunk->append_column(std::move(col0), chunk->num_columns());
        chunk->append_column(std::move(col1), chunk->num_columns());
        chunk->append_column(std::move(col2), chunk->num_columns());
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
    std::vector<TypeDescriptor> type_descs{TYPE_FLOAT_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto nullable_column = ColumnTestHelper::build_nullable_column<float>({0.1, 1.1, 1.2, -99.9}, {1, 0, 1, 0});
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
    std::vector<TypeDescriptor> type_descs{TYPE_DOUBLE_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto nullable_column = ColumnTestHelper::build_nullable_column<double>({0.1, 1.1, 1.2, -99.9}, {1, 0, 1, 0});
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
    std::vector<TypeDescriptor> type_descs{TYPE_BOOLEAN_DESC};
    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto nullable_column = ColumnTestHelper::build_nullable_column<uint8_t>({0, 1, 1, 0}, {1, 0, 1, 0});
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
    std::vector<TypeDescriptor> type_descs{TYPE_BOOLEAN_DESC};
    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = ColumnTestHelper::build_column<uint8_t>({0, 1, 1, 0});
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
    std::vector<TypeDescriptor> type_descs{TYPE_VARCHAR_DESC, TYPE_CHAR_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto nullable_column = ColumnTestHelper::build_nullable_column<Slice>(
                {"hello", "world", "starrocks", "lakehouse"}, {1, 0, 1, 0});
        chunk->append_column(std::move(nullable_column), chunk->num_columns());

        auto nullable_column2 =
                ColumnTestHelper::build_nullable_column<Slice>({"hello", "world", "abc", "def"}, {0, 0, 1, 1});
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
    std::vector<TypeDescriptor> type_descs{TYPE_VARCHAR_DESC, TYPE_CHAR_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = ColumnTestHelper::build_column<Slice>({"hello", "world", "starrocks", "lakehouse"});
        chunk->append_column(std::move(data_column), chunk->num_columns());

        auto data_column2 = ColumnTestHelper::build_column<Slice>({"hello", "world", "abc", "def"});
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
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 10),
    };

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 10),
    };

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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
    std::vector<TypeDescriptor> type_descs{TYPE_DATE_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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
    std::vector<TypeDescriptor> type_descs{TYPE_DATE_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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
    std::vector<TypeDescriptor> type_descs{TYPE_DATETIME_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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
    std::vector<TypeDescriptor> type_descs{TYPE_DATETIME_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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
    std::vector<TypeDescriptor> type_descs{TYPE_DATETIME_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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
    std::vector<TypeDescriptor> type_descs;
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteStructNotNull) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteMapNullable) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_map);

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());
    ASSERT_OK(writer->commit().io_status);
}

TEST_F(OrcFileWriterTest, TestWriteMapNotNull) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_map);

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_OK(writer->init());
    ASSERT_OK(writer->commit().io_status);
}

TEST_F(OrcFileWriterTest, TestWriteArrayNullable) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_array);

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteArrayNotNull) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_array);

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestWriteNestedArray) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_array_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array_array.children.push_back(TYPE_INT_ARRAY_DESC);
    type_descs.push_back(type_int_array_array);

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestUnknownCompression) {
    std::vector<TypeDescriptor> type_descs{TYPE_BOOLEAN_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(
            _file_path, std::move(_orc_stream), column_names, type_descs, std::move(column_evaluators),
            TCompressionType::UNKNOWN_COMPRESSION, _write_options, []() {});
    ASSERT_ERROR(writer->init());
}

TEST_F(OrcFileWriterTest, TestFactory) {
    std::vector<TypeDescriptor> type_descs{TYPE_BOOLEAN_DESC};

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
    std::vector<TypeDescriptor> type_descs{TYPE_BIGINT_DESC};
    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<formats::ORCFileWriter>(_file_path, std::move(_orc_stream), column_names, type_descs,
                                                           std::move(column_evaluators),
                                                           TCompressionType::NO_COMPRESSION, _write_options, []() {});
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

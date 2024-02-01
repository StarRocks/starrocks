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
#include "formats/parquet/parquet_file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gutil/casts.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"

namespace starrocks::formats {

static HdfsScanStats g_hdfs_scan_stats;
using starrocks::HdfsScannerContext;

class ParquetFileWriterTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }

    void TearDown() override {}

protected:
    HdfsScannerContext* _create_scan_context(const std::vector<TypeDescriptor>& type_descs) {
        auto ctx = _pool.add(new HdfsScannerContext());
        auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
        ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;

        std::vector<parquet::Utils::SlotDesc> slot_descs;
        for (auto& type_desc : type_descs) {
            auto type_name = type_desc.debug_string();
            slot_descs.push_back({type_name, type_desc});
        }
        slot_descs.push_back({""});

        ctx->tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs.data());
        parquet::Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
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

    ChunkPtr _read_chunk(const std::vector<TypeDescriptor>& type_descs) {
        auto ctx = _create_scan_context(type_descs);
        ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size, 0);

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

        EXPECT_OK(file_reader->get_next(&read_chunk));
        return read_chunk;
    }

    MemoryFileSystem _fs;
    std::string _file_path{"/dummy_file.parquet"};
    RuntimeState* _runtime_state;
    ObjectPool _pool;
};

TExpr create_primitive_slot_ref(SlotId slot_id, TPrimitiveType::type type) {
    TExprNode node;
    node.node_type = TExprNodeType::SLOT_REF;
    node.type = gen_type_desc(type);
    node.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node.__set_slot_ref(t_slot_ref);
    node.is_nullable = true;

    TExpr t_expr;
    t_expr.nodes = {node};
    return t_expr;
}

TExpr create_array_slot_ref(SlotId slot_id, TPrimitiveType::type type) {
    // ARRAY<int>
    TTypeDesc array_ttype;
    {
        array_ttype.__isset.types = true;
        array_ttype.types.resize(2);
        array_ttype.types[0].__set_type(TTypeNodeType::ARRAY);
        array_ttype.types[1].__set_type(TTypeNodeType::SCALAR);
        array_ttype.types[1].__set_scalar_type(TScalarType());
        array_ttype.types[1].scalar_type.__set_type(type);
        array_ttype.types[1].scalar_type.__set_len(0);
    }

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = array_ttype;
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    TExpr t_expr;
    t_expr.nodes = {node1};
    return t_expr;
}

TEST_F(ParquetFileWriterTest, TestWriteIntegralTypes) {
    auto output_stream = _fs.new_writable_file(_file_path).value();
    auto parquet_output_stream = std::make_unique<parquet::ParquetOutputStream>(std::move(output_stream));
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT),
            TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            TypeDescriptor::from_logical_type(TYPE_INT),
            TypeDescriptor::from_logical_type(TYPE_BIGINT),
    };
    std::vector<std::string> column_names = _make_type_names(type_descs);

    std::vector<std::unique_ptr<ColumnEvaluator>> column_evaluators;
    for (int i = 0; i < 4; i++) {
        column_evaluators.push_back(std::make_unique<ColumnSlotIdEvaluator>(i));
    }

    auto writer_options = std::make_shared<ParquetFileWriter::ParquetWriterOptions>();
    auto writer = std::make_unique<ParquetFileWriter>(
            std::move(parquet_output_stream), column_names, type_descs, std::move(column_evaluators), writer_options,
            []() {}, nullptr);
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
    ASSERT_TRUE(writer->write(chunk).get().ok());
    auto result = writer->commit().get();

    ASSERT_TRUE(result.io_status.ok());
    ASSERT_EQ(result.file_metrics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

} // namespace starrocks::formats

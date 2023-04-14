//
// Created by Letian Jiang on 2023/4/10.
//

#include "formats/parquet/file_writer.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "column/chunk.h"
#include "common/statusor.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gutil/casts.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"

namespace starrocks {
namespace parquet {

static HdfsScanStats g_hdfs_scan_stats;
using starrocks::HdfsScannerContext;

class FileWriterTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }
    void TearDown() override {}

protected:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path);

    HdfsScannerContext* _create_scan_context();

    HdfsScannerContext* _create_file1_base_context();
    HdfsScannerContext* _create_context_for_partition();
    HdfsScannerContext* _create_context_for_not_exist();

    HdfsScannerContext* _create_file2_base_context();
    HdfsScannerContext* _create_context_for_min_max();
    HdfsScannerContext* _create_context_for_filter_file();
    HdfsScannerContext* _create_context_for_dict_filter();
    HdfsScannerContext* _create_context_for_other_filter();
    HdfsScannerContext* _create_context_for_skip_group();

    HdfsScannerContext* _create_file3_base_context();
    HdfsScannerContext* _create_context_for_multi_filter();
    HdfsScannerContext* _create_context_for_late_materialization();

    HdfsScannerContext* _create_file4_base_context();
    HdfsScannerContext* _create_context_for_struct_column();
    HdfsScannerContext* _create_context_for_upper_pred();

    HdfsScannerContext* _create_file5_base_context();
    HdfsScannerContext* _create_file6_base_context();
    HdfsScannerContext* _create_file_map_char_key_context();
    HdfsScannerContext* _create_file_map_base_context();
    HdfsScannerContext* _create_file_map_partial_materialize_context();

    void _create_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, int value,
                                   std::vector<ExprContext*>* conjunct_ctxs);
    void _create_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                      std::vector<ExprContext*>* conjunct_ctxs);

    static ChunkPtr _create_chunk();
    static ChunkPtr _create_multi_page_chunk();
    static ChunkPtr _create_struct_chunk();
    static ChunkPtr _create_required_array_chunk();
    static ChunkPtr _create_chunk_for_partition();
    static ChunkPtr _create_chunk_for_not_exist();
    static void _append_column_for_chunk(LogicalType column_type, ChunkPtr* chunk);

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length);

    //    std::unique_ptr<MemoryFileSystem> _fs;
    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
};

HdfsScannerContext* FileWriterTest::_create_scan_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());
    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;
    return ctx;
}

THdfsScanRange* FileWriterTest::_create_scan_range(const std::string& file_path, size_t file_length) {
    auto* scan_range = _pool.add(new THdfsScanRange());

    scan_range->relative_path = file_path;
    scan_range->file_length = file_length;
    scan_range->offset = 4;
    scan_range->length = file_length;

    return scan_range;
}

TEST_F(FileWriterTest, TestWriteIntegralTypes) {
    MemoryFileSystem fs;
    const std::string file_path = "/data.parquet";
    uint64_t file_size{0};

    std::vector<LogicalType> integral_types{TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT};
    std::vector<TypeDescriptor> type_descs;
    std::vector<std::string> type_names;
    for (auto type : integral_types) {
        auto type_desc = TypeDescriptor::from_logical_type(type);
        auto type_name = type_desc.debug_string();

        type_descs.push_back(type_desc);
        type_names.push_back(type_name);
    }

    auto ret = ParquetBuildHelper::make_schema(type_names, type_descs);
    ASSERT_TRUE(ret.ok());
    auto schema = ret.ValueOrDie();
    auto properties = ParquetBuildHelper::make_properties(ParquetBuilderOptions());

    // populate chunk to write
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
    {
        ASSIGN_OR_ABORT(auto file, fs.new_writable_file(file_path));
        auto file_writer = std::make_shared<SyncFileWriter>(std::move(file), properties, schema, type_descs);
        file_writer->init();
        file_writer->write(chunk.get());
        auto st = file_writer->close();
        ASSERT_TRUE(st.ok());
        ASSIGN_OR_ABORT(file_size, fs.get_file_size(file_path));
    }

    // scanner context
    auto ctx = _create_scan_context();
    {
        std::vector<SlotDesc> slot_descs;
        for (auto type : integral_types) {
            auto type_desc = TypeDescriptor::from_logical_type(type);
            auto type_name = type_desc.debug_string();

            slot_descs.push_back({type_name, type_desc});
        }
        slot_descs.push_back({""});

        ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs.data());
        make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ctx->scan_ranges.emplace_back(_create_scan_range(file_path, file_size));
    }

    // read chunk and check identity
    {
        ASSIGN_OR_ABORT(auto file, fs.new_random_access_file(file_path));
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), file_size);

        auto st = file_reader->init(ctx);
        ASSERT_TRUE(st.ok());

        auto read_chunk = std::make_shared<Chunk>();

        for (auto type_desc : type_descs) {
            auto col = ColumnHelper::create_column(type_desc, true);
            read_chunk->append_column(col, read_chunk->num_columns());
        }

        file_reader->get_next(&read_chunk);
        assert_identical_chunk(chunk.get(), read_chunk.get());
    }
}

} // namespace parquet
} // namespace starrocks

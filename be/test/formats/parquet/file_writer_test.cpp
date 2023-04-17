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
#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/nullable_column.h"

namespace starrocks {
namespace parquet {

static HdfsScanStats g_hdfs_scan_stats;
using starrocks::HdfsScannerContext;

class FileWriterTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
    }
    void TearDown() override {}

protected:
    HdfsScannerContext* _create_scan_context();
    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length);

    MemoryFileSystem _fs;
    std::string _file_path{"/dummy_file.parquet"};
    RuntimeState* _runtime_state;
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
        ASSIGN_OR_ABORT(auto file, _fs.new_writable_file(_file_path));
        auto file_writer = std::make_shared<SyncFileWriter>(std::move(file), properties, schema, type_descs);
        file_writer->init();
        file_writer->write(chunk.get());
        auto st = file_writer->close();
        ASSERT_TRUE(st.ok());
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
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_path, file_size));
    }

    // read chunk and assert equality
    {
        ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), file_size);

        auto st = file_reader->init(ctx);
        ASSERT_TRUE(st.ok());

        auto read_chunk = std::make_shared<Chunk>();

        for (auto type_desc : type_descs) {
            auto col = ColumnHelper::create_column(type_desc, true);
            read_chunk->append_column(col, read_chunk->num_columns());
        }

        file_reader->get_next(&read_chunk);
        assert_equal_chunk(chunk.get(), read_chunk.get());
    }
}

TEST_F(FileWriterTest, TestWriteArray) {
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);

    std::vector<TypeDescriptor> type_descs{type_int_array};
    std::vector<std::string> type_names{type_int_array.debug_string()};

    auto ret = ParquetBuildHelper::make_schema({"array_int"}, {type_int_array});
    ASSERT_TRUE(ret.ok());
    auto schema = ret.ValueOrDie();
    auto properties = ParquetBuildHelper::make_properties(ParquetBuilderOptions());

    // populate chunk to write
    // [1], NULL, [], [2, NULL, 3]
    auto chunk = std::make_shared<Chunk>();
    {
        auto elements_data_col = Int32Column::create();
        std::vector<int32_t> nums{1, 2, -99, 3};
        elements_data_col->append_numbers(nums.data(), sizeof(int32_t)*nums.size());
        auto elements_null_col = UInt8Column::create();
        std::vector<uint8_t> nulls{0, 0, 1, 0};
        elements_null_col->append_numbers(nulls.data(), sizeof(uint8_t)*nulls.size());
        auto elements_col = NullableColumn::create(elements_data_col, elements_null_col);

        auto offsets_col = UInt32Column::create();
        std::vector<uint32_t> offsets{0, 1, 1, 1, 4};
        offsets_col->append_numbers(offsets.data(), sizeof(uint32_t)*offsets.size());
        auto array_col = ArrayColumn::create(elements_col, offsets_col);

        std::vector<uint8_t> _nulls{0, 1, 0, 0};
        auto null_col = UInt8Column::create();
        null_col->append_numbers(_nulls.data(), sizeof(uint8_t) * _nulls.size());
        auto nullable_col = NullableColumn::create(array_col, null_col);

        chunk->append_column(nullable_col, chunk->num_columns());
    }

    // write chunk
    {
        ASSIGN_OR_ABORT(auto file, _fs.new_writable_file(_file_path));
        auto file_writer = std::make_shared<SyncFileWriter>(std::move(file), properties, schema, type_descs);
        file_writer->init();
        file_writer->write(chunk.get());
        auto st = file_writer->close();
        ASSERT_TRUE(st.ok());
    }

    // scanner context
    auto ctx = _create_scan_context();
    {
        std::vector<SlotDesc> slot_descs;
        for (auto type : type_descs) {
            auto type_name = type.debug_string();
            slot_descs.push_back({type_name, type});
        }
        slot_descs.push_back({""});

        ctx->tuple_desc = create_tuple_descriptor(_runtime_state, &_pool, slot_descs.data());
        make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        ctx->scan_ranges.emplace_back(_create_scan_range(_file_path, file_size));
    }

    // read chunk and assert equality
    {
        ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), file_size);

        auto st = file_reader->init(ctx);
        ASSERT_TRUE(st.ok());

        auto read_chunk = std::make_shared<Chunk>();

        for (auto type_desc : type_descs) {
            auto col = ColumnHelper::create_column(type_desc, true);
            read_chunk->append_column(col, read_chunk->num_columns());
        }

        file_reader->get_next(&read_chunk);
        assert_equal_chunk(chunk.get(), read_chunk.get());
    }
}

} // namespace parquet
} // namespace starrocks

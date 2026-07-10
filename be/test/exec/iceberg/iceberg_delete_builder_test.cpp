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

#include "exec/iceberg/iceberg_delete_builder.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class IcebergDeleteBuilderTest : public testing::Test {
protected:
    const std::string _parquet_delete_path = "/iceberg_position_delete.parquet";
    const std::string _parquet_data_path = "parquet_data_file.parquet";
    MemoryFileSystem _fs;
};

TEST_F(IcebergDeleteBuilderTest, TestParquetBuilder) {
    TQueryGlobals query_globals;
    query_globals.__set_time_zone("UTC");
    RuntimeState runtime_state(query_globals);
    RuntimeProfile runtime_profile("IcebergDeleteBuilderTest");

    std::vector type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                           TypeDescriptor::from_logical_type(TYPE_BIGINT)};
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer_options = std::make_shared<formats::ParquetWriterOptions>();
    writer_options->column_ids = {formats::FileColumnId{IcebergDeleteFileMeta::get_delete_file_path_slot().id(), {}},
                                  formats::FileColumnId{IcebergDeleteFileMeta::get_delete_file_pos_slot().id(), {}}};
    ASSIGN_OR_ABORT(auto writable_file, _fs.new_writable_file(_parquet_delete_path));
    auto output_stream = std::make_shared<parquet::ParquetOutputStream>(std::move(writable_file));
    formats::ParquetFileWriter writer(_parquet_delete_path, std::move(output_stream), {"file_path", "pos"}, type_descs,
                                      std::move(column_evaluators), TCompressionType::NO_COMPRESSION,
                                      std::move(writer_options), [] {}, {false, false});
    ASSERT_OK(writer.init());

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnTestHelper::build_column<Slice>(
                                 {_parquet_data_path, "another_data_file.parquet", _parquet_data_path}),
                         0);
    chunk->append_column(ColumnTestHelper::build_column<int64_t>({7, 9, 11}), 1);
    ASSERT_OK(writer.write(chunk.get()));
    ASSERT_OK(writer.close().io_status);

    HdfsScannerContext scanner_ctx;
    scanner_ctx.fs = &_fs;
    scanner_ctx.file_path = _parquet_data_path;
    scanner_ctx.profile.runtime_profile = &runtime_profile;

    ASSIGN_OR_ABORT(const int64_t delete_file_size, scanner_ctx.fs->get_file_size(_parquet_delete_path));
    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(_parquet_delete_path);
    delete_file.__set_length(delete_file_size);

    auto skip_rows_ctx = std::make_shared<SkipRowsContext>();
    IcebergDeleteBuilder builder(skip_rows_ctx, &runtime_state, scanner_ctx);

    ASSERT_OK(builder.build_parquet(delete_file));
    ASSERT_NE(nullptr, skip_rows_ctx->deletion_bitmap);
    EXPECT_EQ(2, skip_rows_ctx->deletion_bitmap->get_cardinality());
    std::vector<uint64_t> deleted_rowids(skip_rows_ctx->deletion_bitmap->get_cardinality());
    skip_rows_ctx->deletion_bitmap->to_array(deleted_rowids);
    EXPECT_EQ((std::vector<uint64_t>{7, 11}), deleted_rowids);
}

} // namespace starrocks

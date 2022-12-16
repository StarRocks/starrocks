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

#include "column/datum_tuple.h"
#include "fs/fs_util.h"
#include "storage/binlog_manager.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class BinlogReaderTest : public testing::Test {
public:
    void SetUp() override {
        fs::remove_all(_binlog_file_dir);
        fs::create_directories(_binlog_file_dir);
        create_tablet_schema();
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

protected:
    ColumnPB create_column_pb(int32_t id, string name, string type, int length, bool is_key) {
        ColumnPB col;
        col.set_unique_id(id);
        col.set_name(name);
        col.set_type(type);
        col.set_is_key(is_key);
        col.set_is_nullable(false);
        col.set_length(length);
        col.set_index_length(4);
        col.set_is_bf_column(false);
        col.set_has_bitmap_index(false);
        return col;
    }

    void create_tablet_schema() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(2);
        schema_pb.set_num_rows_per_row_block(5);
        schema_pb.set_next_column_unique_id(4);

        auto* col1 = schema_pb.add_column();
        *col1 = create_column_pb(1, "col1", "INT", 4, true);
        auto* col2 = schema_pb.add_column();
        *col2 = create_column_pb(2, "col2", "INT", 4, true);
        auto* col3 = schema_pb.add_column();
        *col3 = create_column_pb(3, "col3", "VARCHAR", 20, false);

        _tablet_schema = std::make_unique<TabletSchema>(schema_pb);
        _schema = ChunkHelper::convert_schema_to_format_v2(*_tablet_schema);
    }

    void create_rowset_writer_context(RowsetId& rowset_id, int64_t version,
                                      RowsetWriterContext* rowset_writer_context) {
        RowsetId rid;
        rowset_id.init(rowset_id.to_string());
        rowset_writer_context->rowset_id = rid;
        rowset_writer_context->tablet_id = tablet_id;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->version = Version(version, 0);
        rowset_writer_context->rowset_path_prefix = _binlog_file_dir;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = _tablet_schema.get();
        rowset_writer_context->version.first = 0;
        rowset_writer_context->version.second = 0;
        rowset_writer_context->writer_type = kHorizontal;
    }

    void build_segment(int32_t start_key, int32_t num_rows, RowsetWriter* rowset_writer, SegmentPB* seg_info) {
        std::vector<uint32_t> column_indexes{0, 1, 2};
        auto chunk = ChunkHelper::new_chunk(_schema, num_rows);
        for (int i = start_key; i < num_rows + start_key; i++) {
            auto& cols = chunk->columns();
            cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(vectorized::Datum(std::to_string(i)));
        }
        ASSERT_OK(rowset_writer->flush_chunk(*chunk, seg_info));
    }

    void build_rowset(int version, RowsetId rowset_id, int32_t* start_key, std::vector<int32_t> rows_per_segment,
                      RowsetSharedPtr& rowset) {
        RowsetWriterContext writer_context;
        create_rowset_writer_context(rowset_id, version, &writer_context);
        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_OK(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer));

        int32_t total_rows = 0;
        std::vector<std::unique_ptr<SegmentPB>> seg_infos;
        for (int32_t num_rows : rows_per_segment) {
            seg_infos.emplace_back(std::make_unique<SegmentPB>());
            build_segment(*start_key, num_rows, rowset_writer.get(), seg_infos.back().get());
            start_key += num_rows;
            total_rows += num_rows;
        }

        rowset = rowset_writer->build().value();
        ASSERT_EQ(total_rows, rowset->rowset_meta()->num_rows());
        ASSERT_EQ(rows_per_segment.size(), rowset->rowset_meta()->num_segments());
    }

protected:
    std::unique_ptr<TabletSchema> _tablet_schema;
    vectorized::VectorizedSchema _schema;
    int64_t tablet_id = 100;
    std::string _binlog_file_dir = "binlog_reader_test";
};

void verify_binlog_reader(std::shared_ptr<BinlogManager> binlog_manager, vectorized::VectorizedSchema& schema,
                          std::vector<int32_t>& num_rows_vector, int32_t start_key, int64_t seek_version,
                          int64_t seek_seq_id) {
    std::shared_ptr<BinlogReader> binlog_reader = binlog_manager->create_reader(schema, 100);
    ASSERT_OK(binlog_reader->seek(seek_version, seek_seq_id));
    vectorized::ChunkPtr chunk = ChunkHelper::new_chunk(schema, 100);
    int num_rows = start_key;
    int next_version = 1;
    while (num_rows < num_rows_vector.back()) {
        ASSERT_EQ(next_version, binlog_reader->next_version());
        ASSERT_EQ(num_rows - num_rows_vector[next_version - 1], binlog_reader->next_version());
        chunk->reset();
        ASSERT_OK(binlog_reader->get_next(&chunk, 4));
        ASSERT_GE(0, chunk->num_rows());
        for (int32_t i = 0; i < chunk->num_rows(); i++) {
            ASSERT_EQ(i + num_rows, chunk->get(i)[0].get_int32());
            ASSERT_EQ(i + num_rows, chunk->get(i)[0].get_int32());
            ASSERT_EQ(i + num_rows, chunk->get(i)[0].get_int32());
        }
        num_rows += chunk->num_rows();
        if (num_rows >= num_rows_vector[next_version]) {
            next_version += 1;
        }
    }
    chunk->reset();
    ASSERT_TRUE(binlog_reader->get_next(&chunk, 4).is_end_of_file());
    ASSERT_EQ(4, binlog_reader->next_version());
    ASSERT_EQ(0, binlog_reader->next_version());
    binlog_reader->close();
}

TEST_F(BinlogReaderTest, test_basic) {
    RowsetId rowset_id;
    int32_t start_key = 0;

    std::vector<RowsetSharedPtr> rowsets;
    rowsets.emplace_back(RowsetSharedPtr());
    rowset_id.init(2, 1, 2, 3);
    build_rowset(1, rowset_id, &start_key, {1000, 10, 20}, rowsets.back());

    rowsets.emplace_back(RowsetSharedPtr());
    rowset_id.init(2, 2, 2, 3);
    build_rowset(2, rowset_id, &start_key, {5, 90, 1}, rowsets.back());

    rowsets.emplace_back(RowsetSharedPtr());
    rowset_id.init(2, 3, 2, 3);
    build_rowset(3, rowset_id, &start_key, {10, 40, 10}, rowsets.back());

    std::shared_ptr<BinlogManager> binlog_manager =
            std::make_shared<BinlogManager>(_binlog_file_dir, 100, 20, LZ4_FRAME);
    for (const auto& rowset : rowsets) {
        ASSERT_OK(binlog_manager->add_insert_rowset(rowset));
    }

    std::vector<int32_t> num_rows_vector{0};
    for (const auto& rowset : rowsets) {
        num_rows_vector.emplace_back(num_rows_vector.back() + rowset->num_rows());
    }

    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 0, 1, 0);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 500, 1, 500);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 999, 1, 999);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1000, 1, 1000);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1005, 1, 1005);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1009, 1, 1009);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1010, 1, 1010);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1020, 1, 1020);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1029, 1, 1029);

    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1030, 2, 1030);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1032, 2, 1032);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1034, 2, 1034);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1035, 2, 1035);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1100, 2, 1100);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1124, 2, 1124);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1125, 2, 1125);

    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1126, 3, 1126);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1131, 3, 1131);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1135, 3, 1135);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1136, 3, 1136);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1156, 3, 1156);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1175, 3, 1175);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1180, 3, 1180);
    verify_binlog_reader(binlog_manager, _schema, num_rows_vector, 1184, 3, 1184);

    std::shared_ptr<BinlogReader> binlog_reader = binlog_manager->create_reader(_schema, 100);
    ASSERT_TRUE(binlog_reader->seek(4, 0).is_not_found());
    binlog_reader->close();
}

} // namespace starrocks

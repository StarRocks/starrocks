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

class OutputVerifier;

class BinlogReaderTest : public testing::Test {
public:
    void SetUp() override {
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
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

    vectorized::VectorizedFieldPtr make_field(ColumnId cid, const std::string& cname, LogicalType type) {
        return std::make_shared<vectorized::VectorizedField>(cid, cname, get_type_info(type), false);
    }

    void create_rowset_writer_context(RowsetId& rowset_id, int64_t version,
                                      RowsetWriterContext* rowset_writer_context) {
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = _tablet_id;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->version = Version(version, 0);
        rowset_writer_context->rowset_path_prefix = _binlog_file_dir;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = _tablet_schema.get();
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
            *start_key += num_rows;
            total_rows += num_rows;
        }

        rowset = rowset_writer->build().value();
        ASSERT_EQ(total_rows, rowset->rowset_meta()->num_rows());
        ASSERT_EQ(rows_per_segment.size(), rowset->rowset_meta()->num_segments());
    }

    void test_reader(vectorized::VectorizedSchema& output_schema, OutputVerifier& verifier);

protected:
    std::unique_ptr<TabletSchema> _tablet_schema;
    vectorized::VectorizedSchema _schema;
    int64_t _tablet_id = 100;
    std::string _binlog_file_dir = "binlog_reader_test";
    std::shared_ptr<BinlogManager> _binlog_manager;
};

struct VersionInfo {
    int64_t version;
    int64_t create_time_in_us;
    int64_t culmulitive_num_rows;
};

struct ExpectRowInfo {
    int32_t key;
    int8_t op;
    int64_t version;
    int64_t seq_id;
    int64_t timestamp;
};

class OutputVerifier {
public:
    virtual void verify(vectorized::DatumTuple& actual_tuple, ExpectRowInfo& row_info) = 0;
};

void verify_binlog_reader(std::shared_ptr<BinlogManager> binlog_manager, vectorized::VectorizedSchema& output_schema,
                          std::vector<VersionInfo>& version_infos, int64_t seek_version, int64_t seek_seq_id,
                          OutputVerifier* verifier) {
    BinlogReaderParams params;
    params.chunk_size = 100;
    params.output_schema = output_schema;
    std::shared_ptr<BinlogReader> binlog_reader = binlog_manager->create_reader(params);
    ASSERT_OK(binlog_reader->init());
    ASSERT_OK(binlog_reader->seek(seek_version, seek_seq_id));
    vectorized::ChunkPtr chunk = ChunkHelper::new_chunk(output_schema, 100);
    int32_t num_rows = version_infos[seek_version - 1].culmulitive_num_rows + seek_seq_id;
    int64_t next_version = seek_version;
    int64_t next_seq_id = seek_seq_id;
    ExpectRowInfo row_info;
    row_info.op = 0;
    while (num_rows < version_infos.back().culmulitive_num_rows) {
        ASSERT_EQ(next_version, binlog_reader->next_version());
        ASSERT_EQ(next_seq_id, binlog_reader->next_seq_id());
        chunk->reset();
        ASSERT_OK(binlog_reader->get_next(&chunk, 4));
        ASSERT_EQ(output_schema.num_fields(), chunk->num_columns());
        for (int32_t i = 0; i < chunk->num_rows(); i++) {
            row_info.key = num_rows;
            row_info.op = 0;
            row_info.version = next_version;
            row_info.seq_id = next_seq_id;
            row_info.timestamp = version_infos[next_version].create_time_in_us;
            vectorized::DatumTuple actual_row = chunk->get(i);
            verifier->verify(actual_row, row_info);
            num_rows += 1;
            next_seq_id += 1;
        }
        if (num_rows >= version_infos[next_version].culmulitive_num_rows) {
            next_version += 1;
            next_seq_id = 0;
        }
    }
    chunk->reset();
    ASSERT_TRUE(binlog_reader->get_next(&chunk, 4).is_end_of_file());
    ASSERT_EQ(4, binlog_reader->next_version());
    ASSERT_EQ(0, binlog_reader->next_seq_id());
    ASSERT_EQ(num_rows, version_infos.back().culmulitive_num_rows);
    binlog_reader->close();
}

void BinlogReaderTest::test_reader(vectorized::VectorizedSchema& output_schema, OutputVerifier& verifier) {
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
    // TODO init tablet
    for (const auto& rowset : rowsets) {
        ASSERT_OK(rowset->load());
        ASSERT_OK(binlog_manager->add_insert_rowset(rowset));
    }
    // ensure there are multiple binlog files
    ASSERT_EQ(6, binlog_manager->num_binlog_files());

    std::vector<VersionInfo> version_infos;
    version_infos.emplace_back();
    for (const auto& rowset : rowsets) {
        VersionInfo info;
        info.version = rowset->start_version();
        info.create_time_in_us = rowset->creation_time() * 1000000;
        info.culmulitive_num_rows = version_infos.back().culmulitive_num_rows + rowset->num_rows();
        version_infos.emplace_back(info);
    }

    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 0, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 500, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 999, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 1000, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 1005, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 1009, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 1010, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 1020, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 1, 1029, &verifier);

    verify_binlog_reader(binlog_manager, output_schema, version_infos, 2, 0, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 2, 3, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 2, 4, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 2, 5, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 2, 50, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 2, 94, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 2, 95, &verifier);

    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 0, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 5, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 9, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 10, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 25, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 49, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 50, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 55, &verifier);
    verify_binlog_reader(binlog_manager, output_schema, version_infos, 3, 59, &verifier);

    BinlogReaderParams params;
    params.chunk_size = 100;
    params.output_schema = output_schema;
    std::shared_ptr<BinlogReader> binlog_reader = binlog_manager->create_reader(params);
    ASSERT_OK(binlog_reader->init());
    ASSERT_TRUE(binlog_reader->seek(4, 0).is_not_found());
    binlog_reader->close();
}

// verifier for the output including both data and meta columns
class FullColumnsVerifier : public OutputVerifier {
public:
    void verify(vectorized::DatumTuple& actual_tuple, ExpectRowInfo& row_info) override {
        ASSERT_EQ(row_info.key, actual_tuple[0].get_int32());
        ASSERT_EQ(row_info.key, actual_tuple[1].get_int32());
        ASSERT_EQ(std::to_string(row_info.key), actual_tuple[2].get_slice());
        ASSERT_EQ(row_info.op, actual_tuple[3].get_int8());
        ASSERT_EQ(row_info.version, actual_tuple[4].get_int64());
        ASSERT_EQ(row_info.seq_id, actual_tuple[5].get_int64());
        ASSERT_EQ(row_info.timestamp, actual_tuple[6].get_int64());
    }
};

TEST_F(BinlogReaderTest, test_read_full_columns) {
    vectorized::VectorizedSchema schema;
    vectorized::VectorizedFieldPtr op = make_field(4, BINLOG_OP, TYPE_TINYINT);
    vectorized::VectorizedFieldPtr version = make_field(5, BINLOG_VERSION, TYPE_BIGINT);
    vectorized::VectorizedFieldPtr seq_id = make_field(6, BINLOG_SEQ_ID, TYPE_BIGINT);
    vectorized::VectorizedFieldPtr timestamp = make_field(7, BINLOG_TIMESTAMP, TYPE_BIGINT);
    schema.append(_schema.field(0));
    schema.append(_schema.field(1));
    schema.append(_schema.field(2));
    schema.append(op);
    schema.append(version);
    schema.append(seq_id);
    schema.append(timestamp);

    FullColumnsVerifier verifier;
    test_reader(schema, verifier);
}

// verifier for the output only including data columns
class DataColumnsVerifier : public OutputVerifier {
public:
    void verify(vectorized::DatumTuple& actual_tuple, ExpectRowInfo& row_info) override {
        ASSERT_EQ(row_info.key, actual_tuple[0].get_int32());
        ASSERT_EQ(row_info.key, actual_tuple[1].get_int32());
        ASSERT_EQ(std::to_string(row_info.key), actual_tuple[2].get_slice());
    }
};

TEST_F(BinlogReaderTest, test_read_data_columns) {
    DataColumnsVerifier verifier;
    test_reader(_schema, verifier);
}

// verify the result for BinlogReaderTest#_output_partial_schema
class RandomColumnsVerifier : public OutputVerifier {
public:
    void verify(vectorized::DatumTuple& actual_tuple, ExpectRowInfo& row_info) override {
        ASSERT_EQ(row_info.op, actual_tuple[0].get_int8());
        ASSERT_EQ(std::to_string(row_info.key), actual_tuple[1].get_slice());
        ASSERT_EQ(row_info.key, actual_tuple[2].get_int32());
    }
};

// verifier for the output including random columns
TEST_F(BinlogReaderTest, test_read_random_columns) {
    vectorized::VectorizedSchema schema;
    vectorized::VectorizedFieldPtr op = make_field(4, BINLOG_OP, TYPE_TINYINT);
    schema.append(op);
    schema.append(_schema.field(2));
    schema.append(_schema.field(0));

    RandomColumnsVerifier verifier;
    test_reader(schema, verifier);
}

} // namespace starrocks

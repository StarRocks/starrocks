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

#include "storage/binlog_reader.h"

#include <gtest/gtest.h>

#include "column/datum_tuple.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class RowsetInfo;
class OutputVerifier;

class BinlogReaderTest : public testing::Test {
public:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        _tablet = create_tablet(rand(), rand());
        _schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

protected:
    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TBinlogConfig binlog_config;
        binlog_config.version = 1;
        binlog_config.binlog_enable = true;
        binlog_config.binlog_ttl_second = 30 * 60;
        binlog_config.binlog_max_size = INT64_MAX;
        request.__set_binlog_config(binlog_config);

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "k2";
        k2.__set_is_key(true);
        k2.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k2);

        TColumn v1;
        v1.column_name = "v1";
        v1.__set_is_key(false);
        v1.column_type.type = TPrimitiveType::VARCHAR;
        v1.column_type.len = INT32_MAX;
        request.tablet_schema.columns.push_back(v1);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    FieldPtr make_field(ColumnId cid, const std::string& cname, LogicalType type) {
        return std::make_shared<Field>(cid, cname, get_type_info(type), false);
    }

    void create_rowset(int32_t* start_key, RowsetInfo& rowset_info, RowsetSharedPtr* rowset);

    void ingestion_rowsets(std::vector<std::vector<RowsetInfo>>& rowset_infos);

    void test_reader(Schema& output_schema, OutputVerifier& verifier);

protected:
    TabletSharedPtr _tablet;
    Schema _schema;
};

struct RowsetInfo {
    int64_t version;
    int64_t create_time_in_us;
    int64_t total_rows;
    int64_t num_segments;
};

struct ExpectRowInfo {
    int32_t key;
    int8_t op;
    int64_t version;
    int64_t seq_id;
    int64_t timestamp;
};

void BinlogReaderTest::create_rowset(int32_t* start_key, RowsetInfo& rowset_info, RowsetSharedPtr* rowset) {
    RowsetWriterContext writer_context;
    RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
    writer_context.rowset_id = rowset_id;
    writer_context.tablet_id = _tablet->tablet_id();
    writer_context.tablet_schema_hash = _tablet->schema_hash();
    writer_context.partition_id = 10;
    writer_context.rowset_path_prefix = _tablet->schema_hash_path();
    writer_context.rowset_state = COMMITTED;
    writer_context.tablet_schema = _tablet->tablet_schema();
    writer_context.version.first = 0;
    writer_context.version.second = 0;
    writer_context.segments_overlap = NONOVERLAPPING;

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_OK(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer));
    for (int64_t seg_index = 0; seg_index < rowset_info.num_segments; seg_index++) {
        int64_t avg_rows = rowset_info.total_rows / rowset_info.num_segments;
        int64_t extra_rows = seg_index < rowset_info.total_rows % rowset_info.num_segments;
        int64_t num_rows = avg_rows + extra_rows;
        std::vector<uint32_t> column_indexes{0, 1, 2};
        auto chunk = ChunkHelper::new_chunk(_schema, num_rows);
        for (int i = *start_key; i < num_rows + *start_key; i++) {
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(Slice(std::to_string(i))));
        }
        ASSERT_OK(rowset_writer->flush_chunk(*chunk));
        *start_key += num_rows;
    }

    auto status_or = rowset_writer->build();
    ASSERT_OK(status_or.status());
    *rowset = status_or.value();
}

void BinlogReaderTest::ingestion_rowsets(std::vector<std::vector<RowsetInfo>>& rowsets_per_binlog_file) {
    int32_t start_key = 0;
    BinlogManager* binlog_manager = _tablet->binlog_manager();
    for (auto& rowset_infos : rowsets_per_binlog_file) {
        for (auto& rowset_info : rowset_infos) {
            RowsetSharedPtr rowset;
            create_rowset(&start_key, rowset_info, &rowset);
            ASSERT_OK(_tablet->add_inc_rowset(rowset, rowset_info.version));
            // the creation time of the rowset will be set to the visible time in add_inc_rowset(), and
            // the binlog timestamp actually use the creation time after rowset is visible, so should
            // get the creation time after add_inc_rowset() to verify the correctness of binlog timestamp
            rowset_info.create_time_in_us = rowset->creation_time() * 1000000;
        }
        binlog_manager->close_active_writer();
    }
    ASSERT_EQ(rowsets_per_binlog_file.size(), binlog_manager->alive_binlog_files().size());
}

class OutputVerifier {
public:
    virtual void verify(DatumTuple& actual_tuple, ExpectRowInfo& row_info) = 0;
};

void verify_binlog_reader(TabletSharedPtr tablet, std::vector<RowsetInfo>& rowset_infos, int32_t rowset_index,
                          int64_t row_index, Schema& output_schema, OutputVerifier* verifier) {
    int64_t next_key = 0;
    for (int i = 0; i < rowset_index; i++) {
        next_key += rowset_infos[i].total_rows;
    }
    next_key += row_index;
    int64_t next_rowset_index = rowset_index;
    int64_t next_row_index = row_index;
    BinlogReaderParams params;
    params.chunk_size = 100;
    params.output_schema = output_schema;
    BinlogReaderSharedPtr binlog_reader = std::make_shared<BinlogReader>(tablet, params);
    ASSERT_OK(binlog_reader->init());
    ASSERT_OK(binlog_reader->seek(rowset_infos[rowset_index].version, row_index));
    ASSERT_EQ(rowset_infos[next_rowset_index].version, binlog_reader->next_version());
    ASSERT_EQ(next_row_index, binlog_reader->next_seq_id());
    ChunkPtr chunk = ChunkHelper::new_chunk(output_schema, 100);
    Status st;
    while (true) {
        chunk->reset();
        st = binlog_reader->get_next(&chunk, rowset_infos.back().version + 1);
        if (!st.ok()) {
            break;
        }
        ASSERT_EQ(output_schema.num_fields(), chunk->num_columns());
        ExpectRowInfo row_info;
        int32_t next_tuple = 0;
        while (next_tuple < chunk->num_rows()) {
            while (next_rowset_index < rowset_infos.size() &&
                   rowset_infos[next_rowset_index].total_rows == next_row_index) {
                next_rowset_index += 1;
                next_row_index = 0;
            }
            ASSERT_TRUE(next_rowset_index < rowset_infos.size());
            row_info.key = next_key;
            row_info.op = 0;
            row_info.version = rowset_infos[next_rowset_index].version;
            row_info.seq_id = next_row_index;
            row_info.timestamp = rowset_infos[next_rowset_index].create_time_in_us;
            DatumTuple actual_row = chunk->get(next_tuple);
            verifier->verify(actual_row, row_info);
            next_tuple += 1;
            next_key += 1;
            next_row_index += 1;
        }
        ASSERT_EQ(chunk->num_rows(), next_tuple);
        if (next_rowset_index < rowset_infos.size() && rowset_infos[next_rowset_index].total_rows == next_row_index) {
            next_rowset_index += 1;
            next_row_index = 0;
        }
        int64_t next_version = next_rowset_index == rowset_infos.size() ? rowset_infos.back().version + 1
                                                                        : rowset_infos[next_rowset_index].version;
        ASSERT_EQ(next_version, binlog_reader->next_version());
        ASSERT_EQ(next_row_index, binlog_reader->next_seq_id());
    }
    ASSERT_TRUE(st.is_end_of_file());
    while (next_rowset_index < rowset_infos.size()) {
        ASSERT_EQ(0, rowset_infos[next_rowset_index].total_rows);
        next_rowset_index += 1;
    }
    ASSERT_EQ(rowset_infos.size(), next_rowset_index);
    ASSERT_EQ(rowset_infos.back().version + 1, binlog_reader->next_version());
    ASSERT_EQ(0, binlog_reader->next_seq_id());
    binlog_reader->close();
}

void BinlogReaderTest::test_reader(Schema& output_schema, OutputVerifier& verifier) {
    std::vector<std::vector<RowsetInfo>> rowsets_per_binlog_file;
    for (int i = 2; i < 15; i++) {
        RowsetInfo rowset_info;
        rowset_info.version = i;
        rowset_info.total_rows = std::rand() % 10000;
        rowset_info.num_segments = std::min(rowset_info.total_rows, (int64_t)std::rand() % 10 + 1);
        bool share_one_file = (std::rand() % 10) < 7;
        if (i == 2 || !share_one_file) {
            rowsets_per_binlog_file.push_back(std::vector<RowsetInfo>());
        }
        rowsets_per_binlog_file.back().push_back(rowset_info);
    }
    ingestion_rowsets(rowsets_per_binlog_file);

    std::vector<RowsetInfo> rowset_infos;
    for (auto& vec : rowsets_per_binlog_file) {
        rowset_infos.insert(rowset_infos.end(), vec.begin(), vec.end());
    }

    for (int32_t rowset_index = 0; rowset_index < rowset_infos.size(); rowset_index++) {
        RowsetInfo& rowset_info = rowset_infos[rowset_index];
        verify_binlog_reader(_tablet, rowset_infos, rowset_index, 0, output_schema, &verifier);
        if (rowset_info.total_rows > 1) {
            verify_binlog_reader(_tablet, rowset_infos, rowset_index, rowset_info.total_rows - 1, output_schema,
                                 &verifier);
            verify_binlog_reader(_tablet, rowset_infos, rowset_index, rowset_info.total_rows / 2, output_schema,
                                 &verifier);
        }
    }

    BinlogReaderParams params;
    params.chunk_size = 100;
    params.output_schema = output_schema;
    BinlogReaderSharedPtr binlog_reader = std::make_shared<BinlogReader>(_tablet, params);
    ASSERT_OK(binlog_reader->init());
    ASSERT_TRUE(binlog_reader->seek(rowset_infos.back().version + 1, 0).is_not_found());
    binlog_reader->close();
}

class FullColumnsVerifier : public OutputVerifier {
public:
    void verify(DatumTuple& actual_tuple, ExpectRowInfo& row_info) override {
        ASSERT_EQ(row_info.key, actual_tuple[0].get_int32());
        ASSERT_EQ(row_info.key, actual_tuple[1].get_int32());
        ASSERT_EQ(std::to_string(row_info.key), actual_tuple[2].get_slice());
        ASSERT_EQ(row_info.op, actual_tuple[3].get_int8());
        ASSERT_EQ(row_info.version, actual_tuple[4].get_int64());
        ASSERT_EQ(row_info.seq_id, actual_tuple[5].get_int64());
        ASSERT_EQ(row_info.timestamp, actual_tuple[6].get_int64());
    }
};

// verify that for the output including both meta and data columns
TEST_F(BinlogReaderTest, test_read_full_columns) {
    Schema schema;
    FieldPtr op = make_field(4, BINLOG_OP, TYPE_TINYINT);
    FieldPtr version = make_field(5, BINLOG_VERSION, TYPE_BIGINT);
    FieldPtr seq_id = make_field(6, BINLOG_SEQ_ID, TYPE_BIGINT);
    FieldPtr timestamp = make_field(7, BINLOG_TIMESTAMP, TYPE_BIGINT);
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

class DataColumnsVerifier : public OutputVerifier {
public:
    void verify(DatumTuple& actual_tuple, ExpectRowInfo& row_info) override {
        ASSERT_EQ(row_info.key, actual_tuple[0].get_int32());
        ASSERT_EQ(row_info.key, actual_tuple[1].get_int32());
        ASSERT_EQ(std::to_string(row_info.key), actual_tuple[2].get_slice());
    }
};

// verify that for the output only including data columns
TEST_F(BinlogReaderTest, test_read_data_columns) {
    DataColumnsVerifier verifier;
    test_reader(_schema, verifier);
}

class RandomColumnsVerifier : public OutputVerifier {
public:
    void verify(DatumTuple& actual_tuple, ExpectRowInfo& row_info) override {
        ASSERT_EQ(row_info.op, actual_tuple[0].get_int8());
        ASSERT_EQ(std::to_string(row_info.key), actual_tuple[1].get_slice());
        ASSERT_EQ(row_info.key, actual_tuple[2].get_int32());
    }
};

// verify that for the output including random columns
TEST_F(BinlogReaderTest, test_read_random_columns) {
    Schema schema;
    FieldPtr op = make_field(4, BINLOG_OP, TYPE_TINYINT);
    schema.append(op);
    schema.append(_schema.field(2));
    schema.append(_schema.field(0));

    RandomColumnsVerifier verifier;
    test_reader(schema, verifier);
}

} // namespace starrocks

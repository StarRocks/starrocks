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

#include "storage/binlog_manager.h"

#include <gtest/gtest.h>

#include "fs/fs_util.h"
#include "storage/binlog_test_base.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class BinlogManagerTest : public BinlogTestBase {
public:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(_binlog_file_dir));
        create_tablet_schema();
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

protected:
    void create_tablet_schema() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(5);
        schema_pb.set_next_column_unique_id(2);

        ColumnPB& col = *(schema_pb.add_column());
        col.set_unique_id(1);
        col.set_name("col1");
        col.set_type("INT");
        col.set_is_key(true);
        col.set_is_nullable(false);
        col.set_length(4);
        col.set_index_length(4);
        col.set_is_bf_column(false);
        col.set_has_bitmap_index(false);

        _tablet_schema = std::make_unique<TabletSchema>(schema_pb);
        _schema = ChunkHelper::convert_schema(*_tablet_schema);
    }

    void create_rowset(int version, std::vector<int32_t> rows_per_segment, RowsetSharedPtr& rowset) {
        RowsetId rowset_id;
        rowset_id.init(2, ++_next_rowset_uid, 1, 1);
        RowsetWriterContext writer_context;
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = 1000;
        writer_context.tablet_schema_hash = 1111;
        writer_context.partition_id = 10;
        writer_context.version = Version(version, 0);
        writer_context.rowset_path_prefix = _binlog_file_dir;
        writer_context.rowset_state = VISIBLE;
        writer_context.tablet_schema = _tablet_schema.get();
        writer_context.writer_type = kHorizontal;

        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_OK(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer));

        std::vector<std::unique_ptr<SegmentPB>> seg_infos;
        int32_t total_rows = 0;
        for (int32_t num_rows : rows_per_segment) {
            std::unique_ptr<SegmentPB> segment;
            auto chunk = ChunkHelper::new_chunk(_schema, num_rows);
            for (int i = total_rows; i < num_rows + total_rows; i++) {
                auto& cols = chunk->columns();
                cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            }
            ASSERT_OK(rowset_writer->flush_chunk(*chunk, segment.get()));
            total_rows += num_rows;
            seg_infos.emplace_back(std::move(segment));
        }

        rowset = rowset_writer->build().value();
    }

protected:
    int64_t _next_rowset_uid;
    std::unique_ptr<TabletSchema> _tablet_schema;
    Schema _schema;
    std::shared_ptr<FileSystem> _fs;
    std::string _binlog_file_dir = "binlog_manager_test";
};

class MockRowsetFetcher : public RowsetFetcher {
public:
    RowsetSharedPtr get_rowset(int64_t uid) override {
        auto iter = _rowsets.find(uid);
        if (iter == _rowsets.end()) {
            return nullptr;
        }
        return iter->second;
    }

    void add_rowset(int64_t uid, RowsetSharedPtr rowset) {
        _rowsets[uid] = rowset;
        _total_rowset_disk_size += rowset->data_disk_size();
    }

    int64_t total_rowset_disk_size() { return _total_rowset_disk_size; }

private:
    int64_t _total_rowset_disk_size = 0;
    std::unordered_map<int64_t, RowsetSharedPtr> _rowsets;
};

using LsnMap = std::map<int128_t, BinlogFileMetaPBPtr>;
using RowsetCountMap = std::unordered_map<int64_t, int32_t>;

TEST_F(BinlogManagerTest, test_ingestion_commit) {
    int64_t max_file_size = 10000;
    int32_t max_page_size = 20;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);
    LsnMap& lsn_map = binlog_manager->file_metas();
    RowsetCountMap& rowset_count_map = binlog_manager->rowset_count_map();
    std::map<int64_t, BinlogFileMetaPBPtr> expect_file_metas;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> version_to_file_ids;
    RowsetSharedPtr mock_rowset;
    create_rowset(0, {1, 2, 3}, mock_rowset);

    std::shared_ptr<BinlogFileWriter> last_file_writer;
    std::shared_ptr<BinlogFileMetaPB> last_file_meta;
    for (int64_t version = 1, next_file_id = 0; version < 1000; version++) {
        rowset_fetcher->add_rowset(version, mock_rowset);
        ASSERT_EQ(next_file_id, binlog_manager->next_file_id());
        ASSERT_EQ(-1, binlog_manager->ingestion_version());
        ASSERT_TRUE(binlog_manager->build_result() == nullptr);

        StatusOr<BinlogBuilderParamsPtr> status_or = binlog_manager->begin_ingestion(version);
        ASSERT_TRUE(status_or.ok());
        ASSERT_EQ(version, binlog_manager->ingestion_version());
        ASSERT_TRUE(binlog_manager->build_result() == nullptr);

        BinlogBuilderParamsPtr param = status_or.value();
        ASSERT_EQ(_binlog_file_dir, param->binlog_storage_path);
        ASSERT_EQ(max_file_size, param->max_file_size);
        ASSERT_EQ(max_page_size, param->max_page_size);
        ASSERT_EQ(compress_type, param->compression_type);
        ASSERT_EQ(next_file_id, param->start_file_id);
        ASSERT_EQ(last_file_writer.get(), param->active_file_writer.get());
        ASSERT_EQ(last_file_meta.get(), param->active_file_meta.get());

        BinlogBuildResultPtr result = std::make_shared<BinlogBuildResult>();
        result->params = param;
        if (last_file_meta != nullptr) {
            BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
            file_meta->CopyFrom(*last_file_meta);
            file_meta->set_file_size(last_file_meta->id() * version + 1);
            file_meta->add_rowsets(version);
            result->metas.push_back(file_meta);
            result->next_file_id = param->start_file_id + (std::rand() % 3);
            expect_file_metas[file_meta->id()] = file_meta;
        } else {
            // at least one new file if start without an active writer
            result->next_file_id = param->start_file_id + (std::rand() % 3) + 1;
        }
        next_file_id = result->next_file_id;

        for (int64_t file_id = param->start_file_id; file_id < result->next_file_id; file_id++) {
            BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
            file_meta->set_id(file_id);
            file_meta->set_start_version(version);
            file_meta->set_start_seq_id(file_id);
            file_meta->set_file_size(file_id * version + 1);
            file_meta->add_rowsets(version);
            result->metas.push_back(file_meta);
            expect_file_metas[file_id] = file_meta;
        }

        for (auto& meta : result->metas) {
            version_to_file_ids[version].emplace(meta->id());
        }

        // whether there is active writer for the next build
        bool has_active_writer = (std::rand() % 3) == 0;
        if (has_active_writer) {
            int64_t file_id = result->next_file_id - 1;
            std::string path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
            result->active_writer = std::make_shared<BinlogFileWriter>(file_id, path, max_page_size, compress_type);
            last_file_writer = result->active_writer;
            last_file_meta = expect_file_metas[file_id];
        } else {
            result->active_writer = nullptr;
            last_file_writer = nullptr;
            last_file_meta = nullptr;
        }

        binlog_manager->precommit_ingestion(version, result);
        ASSERT_EQ(result.get(), binlog_manager->build_result());

        binlog_manager->commit_ingestion(version);
        ASSERT_EQ(result->next_file_id, binlog_manager->next_file_id());
        ASSERT_EQ(-1, binlog_manager->ingestion_version());
        ASSERT_TRUE(binlog_manager->build_result() == nullptr);
        ASSERT_EQ(result->active_writer.get(), binlog_manager->active_binlog_writer());
        ASSERT_EQ(expect_file_metas.size(), lsn_map.size());
        int64_t expect_binlog_file_size = 0;
        for (auto it : expect_file_metas) {
            BinlogFileMetaPBPtr meta = it.second;
            int128_t lsn = BinlogUtil::get_lsn(meta->start_version(), meta->start_seq_id());
            ASSERT_EQ(1, lsn_map.count(lsn));
            ASSERT_EQ(meta.get(), lsn_map[lsn].get());
            expect_binlog_file_size += meta->file_size();
        }
        ASSERT_EQ(expect_binlog_file_size, binlog_manager->total_binlog_file_disk_size());
        ASSERT_EQ(version_to_file_ids.size(), rowset_count_map.size());
        for (auto it : version_to_file_ids) {
            int64_t file_id = it.first;
            ASSERT_EQ(1, rowset_count_map.count(file_id));
            ASSERT_EQ(it.second.size(), rowset_count_map[file_id]);
        }
        ASSERT_EQ(rowset_fetcher->total_rowset_disk_size(), binlog_manager->total_rowset_disk_size());
    }
}

TEST_F(BinlogManagerTest, test_ingestion_abort) {
    int64_t max_file_size = 10000;
    int32_t max_page_size = 20;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);
    LsnMap& lsn_map = binlog_manager->file_metas();
    RowsetCountMap& rowset_count_map = binlog_manager->rowset_count_map();
    std::map<int64_t, BinlogFileMetaPBPtr> expect_file_metas;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> version_to_file_ids;
    RowsetSharedPtr mock_rowset;
    create_rowset(0, {1, 2, 3}, mock_rowset);

    int64_t version_1 = 1;
    rowset_fetcher->add_rowset(version_1, mock_rowset);
    StatusOr<BinlogBuilderParamsPtr> status_or = binlog_manager->begin_ingestion(version_1);
    ASSERT_TRUE(status_or.ok());
    ASSERT_EQ(version_1, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);
    BinlogBuilderParamsPtr param_1 = status_or.value();
    BinlogBuildResultPtr result_1 = std::make_shared<BinlogBuildResult>();
    result_1->params = param_1;
    result_1->next_file_id = param_1->start_file_id + 5;
    for (int64_t file_id = param_1->start_file_id; file_id < result_1->next_file_id; file_id++) {
        BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
        file_meta->set_id(file_id);
        file_meta->set_start_version(version_1);
        file_meta->set_start_seq_id(file_id);
        file_meta->set_file_size(file_id * version_1 + 1);
        file_meta->add_rowsets(version_1);
        result_1->metas.push_back(file_meta);
        expect_file_metas[file_id] = file_meta;
        version_to_file_ids[version_1].emplace(file_meta->id());
    }

    binlog_manager->precommit_ingestion(version_1, result_1);
    ASSERT_EQ(result_1.get(), binlog_manager->build_result());
    binlog_manager->commit_ingestion(version_1);

    int64_t version_2 = 2;
    status_or = binlog_manager->begin_ingestion(version_2);
    ASSERT_TRUE(status_or.ok());
    ASSERT_EQ(version_2, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);
    BinlogBuilderParamsPtr param_2 = status_or.value();
    BinlogBuildResultPtr result_2 = std::make_shared<BinlogBuildResult>();
    result_2->params = param_2;
    result_2->next_file_id = param_2->start_file_id + 3;
    int64_t file_id = param_2->start_file_id - 1;
    std::string path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
    result_2->active_writer = std::make_shared<BinlogFileWriter>(file_id, path, max_page_size, compress_type);
    binlog_manager->abort_ingestion(version_2, result_2);
    ASSERT_EQ(result_2->next_file_id, binlog_manager->next_file_id());
    ASSERT_EQ(-1, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);
    ASSERT_EQ(result_2->active_writer.get(), binlog_manager->active_binlog_writer());
    ASSERT_EQ(expect_file_metas.size(), lsn_map.size());
    int64_t expect_binlog_file_size = 0;
    for (auto it : expect_file_metas) {
        BinlogFileMetaPBPtr meta = it.second;
        int128_t lsn = BinlogUtil::get_lsn(meta->start_version(), meta->start_seq_id());
        ASSERT_EQ(1, lsn_map.count(lsn));
        ASSERT_EQ(meta.get(), lsn_map[lsn].get());
        expect_binlog_file_size += meta->file_size();
    }
    ASSERT_EQ(expect_binlog_file_size, binlog_manager->total_binlog_file_disk_size());

    ASSERT_EQ(version_to_file_ids.size(), rowset_count_map.size());
    for (auto it : version_to_file_ids) {
        int64_t fid = it.first;
        ASSERT_EQ(1, rowset_count_map.count(fid));
        ASSERT_EQ(it.second.size(), rowset_count_map[fid]);
    }
    ASSERT_EQ(rowset_fetcher->total_rowset_disk_size(), binlog_manager->total_rowset_disk_size());
}

TEST_F(BinlogManagerTest, test_ingestion_delete) {
    int64_t max_file_size = 10000;
    int32_t max_page_size = 20;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);
    LsnMap& lsn_map = binlog_manager->file_metas();
    RowsetCountMap& rowset_count_map = binlog_manager->rowset_count_map();
    std::map<int64_t, BinlogFileMetaPBPtr> expect_file_metas;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> version_to_file_ids;
    RowsetSharedPtr mock_rowset;
    create_rowset(0, {1, 2, 3}, mock_rowset);

    int64_t version_1 = 1;
    rowset_fetcher->add_rowset(version_1, mock_rowset);
    StatusOr<BinlogBuilderParamsPtr> status_or = binlog_manager->begin_ingestion(version_1);
    ASSERT_TRUE(status_or.ok());
    ASSERT_EQ(version_1, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);
    BinlogBuilderParamsPtr param_1 = status_or.value();
    BinlogBuildResultPtr result_1 = std::make_shared<BinlogBuildResult>();
    result_1->params = param_1;
    result_1->next_file_id = param_1->start_file_id + 5;
    for (int64_t file_id = param_1->start_file_id; file_id < result_1->next_file_id; file_id++) {
        BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
        file_meta->set_id(file_id);
        file_meta->set_start_version(version_1);
        file_meta->set_start_seq_id(file_id);
        file_meta->set_file_size(file_id * version_1 + 1);
        file_meta->add_rowsets(version_1);
        result_1->metas.push_back(file_meta);
        expect_file_metas[file_id] = file_meta;
        version_to_file_ids[version_1].emplace(file_meta->id());
    }
    binlog_manager->precommit_ingestion(version_1, result_1);
    ASSERT_EQ(result_1.get(), binlog_manager->build_result());
    binlog_manager->commit_ingestion(version_1);

    int64_t version_2 = 2;
    status_or = binlog_manager->begin_ingestion(version_2);
    ASSERT_TRUE(status_or.ok());
    ASSERT_EQ(version_2, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);
    BinlogBuilderParamsPtr param_2 = status_or.value();
    BinlogBuildResultPtr result_2 = std::make_shared<BinlogBuildResult>();
    result_2->params = param_2;
    result_2->next_file_id = param_2->start_file_id + 3;
    for (int64_t file_id = param_2->start_file_id; file_id < result_2->next_file_id; file_id++) {
        BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
        file_meta->set_id(file_id);
        file_meta->set_start_version(version_2);
        file_meta->set_start_seq_id(file_id);
        file_meta->set_file_size(file_id * version_2 + 1);
        file_meta->add_rowsets(version_2);
        result_2->metas.push_back(file_meta);
    }
    binlog_manager->precommit_ingestion(version_2, result_2);
    ASSERT_EQ(version_2, binlog_manager->ingestion_version());
    ASSERT_EQ(result_2.get(), binlog_manager->build_result());
    binlog_manager->delete_ingestion(version_2);
    ASSERT_EQ(result_2->next_file_id, binlog_manager->next_file_id());
    ASSERT_EQ(-1, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);

    int64_t expect_binlog_file_size = 0;
    for (auto it : expect_file_metas) {
        BinlogFileMetaPBPtr meta = it.second;
        int128_t lsn = BinlogUtil::get_lsn(meta->start_version(), meta->start_seq_id());
        ASSERT_EQ(1, lsn_map.count(lsn));
        ASSERT_EQ(meta.get(), lsn_map[lsn].get());
        expect_binlog_file_size += meta->file_size();
    }
    ASSERT_EQ(expect_binlog_file_size, binlog_manager->total_binlog_file_disk_size());
    ASSERT_EQ(version_to_file_ids.size(), rowset_count_map.size());
    for (auto it : version_to_file_ids) {
        int64_t fid = it.first;
        ASSERT_EQ(1, rowset_count_map.count(fid));
        ASSERT_EQ(it.second.size(), rowset_count_map[fid]);
    }
    ASSERT_EQ(rowset_fetcher->total_rowset_disk_size(), binlog_manager->total_rowset_disk_size());
}

} // namespace starrocks

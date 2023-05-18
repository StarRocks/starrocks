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

class MockRowsetFetcher;
class BinlogFileInfo;
class PartialRowsetInfo;

class BinlogManagerTest : public BinlogTestBase {
public:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(_binlog_file_dir));
        _next_rowset_uid = 0;
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
    void generate_random_binlog(BinlogManager* binlog_manager, MockRowsetFetcher* rowset_fetcher,
                                std::vector<std::shared_ptr<BinlogFileInfo>>& binlog_files, int32_t num_version,
                                int32_t num_binlog_files);
    void test_binlog_delete(bool expired, bool overcapacity);
    void generate_binlog_file(int64_t file_id, std::vector<PartialRowsetInfo>& rowset_datas,
                              std::vector<BinlogFileMetaPBPtr>* metas_for_each_page);

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
        if (_rowsets.count(uid) > 0) {
            return;
        }

        _rowsets[uid] = rowset;
        _total_rowset_data_size += rowset->data_disk_size();
    }

    int64_t total_rowset_num() { return _rowsets.size(); }
    int64_t total_rowset_data_size() { return _total_rowset_data_size; }

private:
    int64_t _total_rowset_data_size = 0;
    std::unordered_map<int64_t, RowsetSharedPtr> _rowsets;
};

using LsnMap = std::map<BinlogLsn, BinlogFilePtr>;
using RowsetCountMap = std::unordered_map<int64_t, int32_t>;

TEST_F(BinlogManagerTest, test_ingestion_commit) {
    int64_t max_file_size = 10000;
    int32_t max_page_size = 20;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);
    std::map<int64_t, BinlogFileMetaPBPtr> expect_file_metas;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> version_to_file_ids;
    RowsetSharedPtr mock_rowset;
    create_rowset(0, {1, 2, 3}, mock_rowset);

    std::shared_ptr<BinlogFileWriter> last_file_writer;
    std::shared_ptr<BinlogFileMetaPB> last_file_meta;
    for (int64_t version = 1, next_file_id = 1; version < 1000; version++) {
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

        LsnMap& lsn_map = binlog_manager->alive_binlog_files();
        RowsetCountMap& rowset_count_map = binlog_manager->alive_rowset_count_map();
        ASSERT_EQ(expect_file_metas.size(), lsn_map.size());
        int64_t expect_binlog_file_size = 0;
        for (auto it : expect_file_metas) {
            BinlogFileMetaPBPtr meta = it.second;
            BinlogLsn lsn(meta->start_version(), meta->start_seq_id());
            ASSERT_EQ(1, lsn_map.count(lsn));
            ASSERT_EQ(meta.get(), lsn_map[lsn]->file_meta().get());
            expect_binlog_file_size += meta->file_size();
        }
        ASSERT_EQ(expect_binlog_file_size, binlog_manager->total_alive_binlog_file_size());
        ASSERT_EQ(version_to_file_ids.size(), rowset_count_map.size());
        for (auto it : version_to_file_ids) {
            int64_t ver = it.first;
            ASSERT_EQ(1, rowset_count_map.count(ver));
            ASSERT_EQ(it.second.size(), rowset_count_map[ver]);
        }
        ASSERT_EQ(rowset_fetcher->total_rowset_data_size(), binlog_manager->total_alive_rowset_data_size());
    }
}

TEST_F(BinlogManagerTest, test_ingestion_abort) {
    int64_t max_file_size = 10000;
    int32_t max_page_size = 20;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);
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

    LsnMap& lsn_map = binlog_manager->alive_binlog_files();
    RowsetCountMap& rowset_count_map = binlog_manager->alive_rowset_count_map();
    ASSERT_EQ(expect_file_metas.size(), lsn_map.size());
    int64_t expect_binlog_file_size = 0;
    for (auto it : expect_file_metas) {
        BinlogFileMetaPBPtr meta = it.second;
        BinlogLsn lsn(meta->start_version(), meta->start_seq_id());
        ASSERT_EQ(1, lsn_map.count(lsn));
        ASSERT_EQ(meta.get(), lsn_map[lsn]->file_meta().get());
        expect_binlog_file_size += meta->file_size();
    }
    ASSERT_EQ(expect_binlog_file_size, binlog_manager->total_alive_binlog_file_size());

    ASSERT_EQ(version_to_file_ids.size(), rowset_count_map.size());
    for (auto it : version_to_file_ids) {
        int64_t ver = it.first;
        ASSERT_EQ(1, rowset_count_map.count(ver));
        ASSERT_EQ(it.second.size(), rowset_count_map[ver]);
    }
    ASSERT_EQ(rowset_fetcher->total_rowset_data_size(), binlog_manager->total_alive_rowset_data_size());
}

TEST_F(BinlogManagerTest, test_ingestion_delete) {
    int64_t max_file_size = 10000;
    int32_t max_page_size = 20;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);
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

    LsnMap& lsn_map = binlog_manager->alive_binlog_files();
    RowsetCountMap& rowset_count_map = binlog_manager->alive_rowset_count_map();
    int64_t expect_binlog_file_size = 0;
    for (auto it : expect_file_metas) {
        BinlogFileMetaPBPtr meta = it.second;
        BinlogLsn lsn(meta->start_version(), meta->start_seq_id());
        ASSERT_EQ(1, lsn_map.count(lsn));
        ASSERT_EQ(meta.get(), lsn_map[lsn]->file_meta().get());
        expect_binlog_file_size += meta->file_size();
    }
    ASSERT_EQ(expect_binlog_file_size, binlog_manager->total_alive_binlog_file_size());
    ASSERT_EQ(version_to_file_ids.size(), rowset_count_map.size());
    for (auto it : version_to_file_ids) {
        int64_t ver = it.first;
        ASSERT_EQ(1, rowset_count_map.count(ver));
        ASSERT_EQ(it.second.size(), rowset_count_map[ver]);
    }
    ASSERT_EQ(rowset_fetcher->total_rowset_data_size(), binlog_manager->total_alive_rowset_data_size());
}

struct RowsetPartition {
    int64_t rowset_id;
    int64_t version;
    int64_t timestamp_in_us;
    int64_t start_seq_id;
    int64_t end_seq_id;
    int64_t num_pages;
    bool last_partition;
};

struct BinlogFileInfo {
    int64_t file_id;
    int64_t file_size;
    std::vector<RowsetPartition> rowsets;
};
using BinlogFileInfoPtr = std::shared_ptr<BinlogFileInfo>;

// parameters to make a file expired or overcapacity.
// see BinlogManager#check_expire_and_capacity
struct ExpireAndCapacityParams {
    int64_t current_second;
    int64_t binlog_ttl_second;
    int64_t binlog_max_size;
};

BinlogFileMetaPBPtr build_binlog_file_meta(BinlogFileInfoPtr file_info) {
    BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
    file_meta->set_id(file_info->file_id);

    RowsetPartition& start_rowset = file_info->rowsets.front();
    file_meta->set_start_version(start_rowset.version);
    file_meta->set_start_seq_id(start_rowset.start_seq_id);
    file_meta->set_start_timestamp_in_us(start_rowset.timestamp_in_us);

    RowsetPartition& end_rowset = file_info->rowsets.back();
    file_meta->set_end_version(end_rowset.version);
    file_meta->set_end_seq_id(end_rowset.end_seq_id);
    file_meta->set_end_timestamp_in_us(end_rowset.timestamp_in_us);
    file_meta->set_version_eof(end_rowset.last_partition);

    int64_t num_pages = 0;
    for (RowsetPartition& partition : file_info->rowsets) {
        num_pages += partition.num_pages;
        file_meta->add_rowsets(partition.rowset_id);
    }
    file_meta->set_num_pages(num_pages);
    file_meta->set_file_size(file_info->file_size);

    return file_meta;
}

void BinlogManagerTest::generate_random_binlog(BinlogManager* binlog_manager, MockRowsetFetcher* rowset_fetcher,
                                               std::vector<BinlogFileInfoPtr>& binlog_file_infos, int32_t num_version,
                                               int32_t num_binlog_files) {
    // mock rowsets and only use them to get data size Rowset#data_disk_size()
    std::vector<RowsetSharedPtr> mock_rowsets;
    for (int i = 1; i <= 5; i++) {
        RowsetSharedPtr rowset;
        int num_rows = std::rand() % 10000 + 1;
        create_rowset(i, {num_rows}, rowset);
        mock_rowsets.push_back(rowset);
    }

    // construct alive files
    int64_t next_file_id = 0;
    for (int32_t version = 2; version < num_version + 2 || binlog_file_infos.size() < num_binlog_files; version++) {
        int64_t rowset_id = version;
        RowsetSharedPtr rowset = mock_rowsets[std::rand() % mock_rowsets.size()];
        rowset_fetcher->add_rowset(rowset_id, rowset);

        int32_t num_partition = (std::rand() % 5) + 1;
        bool share_with_prev = (std::rand() % 3) > 0;
        int64_t next_seq_id = 0;
        std::vector<BinlogFileInfoPtr> rowset_binlog_files;
        for (int32_t j = 0; j < num_partition; j++) {
            int32_t num_rows = std::rand() % 10000 + 1;
            RowsetPartition part;
            part.rowset_id = rowset_id;
            part.version = version;
            part.timestamp_in_us = version * 10 * 1000 * 1000;
            part.start_seq_id = next_seq_id;
            part.end_seq_id = next_seq_id + num_rows - 1;
            part.num_pages = std::rand() % 5 + 1;
            part.last_partition = j + 1 == num_partition;
            next_seq_id += num_rows;

            BinlogFileInfoPtr file_info;
            if (share_with_prev && !binlog_file_infos.empty()) {
                file_info = binlog_file_infos.back();
                file_info->file_size += std::rand() % 1000 + 1;
                share_with_prev = false;
            } else {
                file_info = std::make_shared<BinlogFileInfo>();
                file_info->file_id = next_file_id;
                file_info->file_size = std::rand() % 10000 + 1;
                next_file_id += 1;
                binlog_file_infos.push_back(file_info);
            }
            file_info->rowsets.push_back(part);
            rowset_binlog_files.push_back(file_info);
        }

        ASSIGN_OR_ABORT(auto params, binlog_manager->begin_ingestion(version));
        BinlogBuildResultPtr build_result = std::make_shared<BinlogBuildResult>();
        build_result->params = params;
        build_result->next_file_id = rowset_binlog_files.back()->file_id + 1;
        for (BinlogFileInfoPtr& file_info : rowset_binlog_files) {
            build_result->metas.push_back(build_binlog_file_meta(file_info));
            std::string file_path = binlog_manager->get_binlog_file_path(file_info->file_id);
            std::shared_ptr<FileSystem> fs;
            ASSIGN_OR_ABORT(fs, FileSystem::CreateSharedFromString(file_path));
            auto st = fs->path_exists(file_path);
            if (st.is_not_found()) {
                WritableFileOptions write_option;
                write_option.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
                ASSIGN_OR_ABORT(auto file, fs->new_writable_file(write_option, file_path));
                ASSERT_OK(fs->path_exists(file_path));
            } else {
                ASSERT_OK(st);
            }
        }

        binlog_manager->precommit_ingestion(version, build_result);
        binlog_manager->commit_ingestion(version);

        ASSERT_EQ(binlog_file_infos.size(), binlog_manager->alive_binlog_files().size());
    }
}

void generate_params(BinlogManager* binlog_manager, MockRowsetFetcher* rowset_fetcher,
                     std::vector<BinlogFileInfoPtr>& binlog_file_infos, std::vector<ExpireAndCapacityParams>& params) {
    int64_t total_binlog_file_size = binlog_manager->total_alive_binlog_file_size();
    int64_t total_rowset_data_size = binlog_manager->total_alive_rowset_data_size();
    RowsetCountMap rowset_count;
    for (auto& file_info : binlog_file_infos) {
        for (auto& rowset : file_info->rowsets) {
            rowset_count[rowset.rowset_id] += 1;
        }
    }

    int64_t ttl_second = 1;
    for (auto& file_info : binlog_file_infos) {
        params.push_back(ExpireAndCapacityParams());
        ExpireAndCapacityParams& param = params.back();
        int64_t max_time = file_info->rowsets.back().timestamp_in_us / 1000 / 1000;
        param.current_second = max_time + ttl_second + 1;
        param.binlog_ttl_second = ttl_second;
        param.binlog_max_size = total_binlog_file_size + total_rowset_data_size - 1;

        total_binlog_file_size -= file_info->file_size;
        for (RowsetPartition& part : file_info->rowsets) {
            int64_t rowset_id = part.rowset_id;
            rowset_count[rowset_id] -= 1;
            if (rowset_count[rowset_id] == 0) {
                total_rowset_data_size -= rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
            }
        }
    }
}

void verify_alive_binlog_files(BinlogManager* binlog_manager, RowsetFetcher* rowset_fetcher,
                               std::vector<BinlogFileInfoPtr>& binlog_file_infos, int32_t first_alive_index) {
    int32_t num_files = binlog_file_infos.size() - first_alive_index;
    LsnMap& alive_binlog_files = binlog_manager->alive_binlog_files();
    RowsetCountMap& rowset_count_map = binlog_manager->alive_rowset_count_map();
    ASSERT_EQ(num_files, alive_binlog_files.size());

    RowsetCountMap expect_rowset_count;
    int64_t expect_total_binlog_file_size = 0;
    int64_t expect_total_rowset_data_size = 0;
    for (int i = first_alive_index; i < binlog_file_infos.size(); i++) {
        auto& file_info = binlog_file_infos[i];
        RowsetPartition& first_rowset = file_info->rowsets.front();
        BinlogLsn lsn(first_rowset.version, first_rowset.start_seq_id);
        ASSERT_EQ(1, alive_binlog_files.count(lsn));
        ASSERT_EQ(file_info->file_id, alive_binlog_files[lsn]->file_meta()->id());
        expect_total_binlog_file_size += file_info->file_size;

        for (auto& rowset : file_info->rowsets) {
            expect_rowset_count[rowset.rowset_id] += 1;
            if (expect_rowset_count[rowset.rowset_id] == 1) {
                expect_total_rowset_data_size += rowset_fetcher->get_rowset(rowset.rowset_id)->data_disk_size();
            }
        }
    }

    ASSERT_EQ(expect_rowset_count.size(), rowset_count_map.size());
    for (auto it = expect_rowset_count.begin(); it != expect_rowset_count.end(); it++) {
        ASSERT_EQ(1, rowset_count_map.count(it->first));
        ASSERT_EQ(it->second, rowset_count_map[it->first]);
    }

    ASSERT_EQ(expect_total_binlog_file_size, binlog_manager->total_alive_binlog_file_size());
    ASSERT_EQ(expect_total_rowset_data_size, binlog_manager->total_alive_rowset_data_size());
}

void verify_wait_reader_binlog_files(BinlogManager* binlog_manager, RowsetFetcher* rowset_fetcher,
                                     std::vector<BinlogFileInfoPtr>& binlog_file_infos, int32_t first_index,
                                     int32_t last_index) {
    int32_t num_files = last_index - first_index + 1;
    std::deque<BinlogFilePtr>& actual_binlog_files = binlog_manager->wait_reader_binlog_files();
    RowsetCountMap& rowset_count_map = binlog_manager->wait_reader_rowset_count_map();
    ASSERT_EQ(num_files, actual_binlog_files.size());

    RowsetCountMap expect_rowset_count;
    int64_t expect_total_binlog_file_size = 0;
    int64_t expect_total_rowset_data_size = 0;
    int32_t index = first_index;
    for (auto& binlog_file : actual_binlog_files) {
        auto& file_info = binlog_file_infos[index];
        ASSERT_EQ(file_info->file_id, binlog_file->file_meta()->id());
        expect_total_binlog_file_size += file_info->file_size;

        for (auto& rowset : file_info->rowsets) {
            expect_rowset_count[rowset.rowset_id] += 1;
            if (expect_rowset_count[rowset.rowset_id] == 1) {
                expect_total_rowset_data_size += rowset_fetcher->get_rowset(rowset.rowset_id)->data_disk_size();
            }
        }
        index += 1;
    }

    ASSERT_EQ(expect_rowset_count.size(), rowset_count_map.size());
    for (auto it = expect_rowset_count.begin(); it != expect_rowset_count.end(); it++) {
        ASSERT_EQ(1, rowset_count_map.count(it->first));
        ASSERT_EQ(it->second, rowset_count_map[it->first]);
    }

    ASSERT_EQ(expect_total_binlog_file_size, binlog_manager->total_wait_reader_binlog_file_size());
    ASSERT_EQ(expect_total_rowset_data_size, binlog_manager->total_wait_reader_rowset_data_size());
}

void verify_unused_binlog_files(BinlogManager* binlog_manager, std::vector<BinlogFileInfoPtr>& binlog_file_infos,
                                int32_t first_index, int32_t last_index) {
    int32_t num_files = last_index - first_index + 1;
    ASSERT_EQ(num_files, binlog_manager->unused_binlog_file_ids().get_size());
    for (int32_t index = first_index; index <= last_index; index++) {
        auto& file_info = binlog_file_infos[index];
        std::string file_path = binlog_manager->get_binlog_file_path(file_info->file_id);
        std::shared_ptr<FileSystem> fs;
        ASSIGN_OR_ABORT(fs, FileSystem::CreateSharedFromString(file_path));
        ASSERT_OK(fs->path_exists(file_path));
    }
    binlog_manager->delete_unused_binlog();
    ASSERT_EQ(0, binlog_manager->unused_binlog_file_ids().get_size());
    for (int32_t index = first_index; index <= last_index; index++) {
        auto& file_info = binlog_file_infos[index];
        std::string file_path = binlog_manager->get_binlog_file_path(file_info->file_id);
        std::shared_ptr<FileSystem> fs;
        ASSIGN_OR_ABORT(fs, FileSystem::CreateSharedFromString(file_path));
        ASSERT_TRUE(fs->path_exists(file_path).is_not_found());
    }
}

void BinlogManagerTest::test_binlog_delete(bool expired, bool overcapacity) {
    int64_t max_file_size = 100 * 1024 * 1024;
    int32_t max_page_size = 1024 * 1024;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);

    std::vector<BinlogFileInfoPtr> binlog_file_infos;
    generate_random_binlog(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 100, 20);
    verify_alive_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 0);
    verify_wait_reader_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 0, -1);
    verify_unused_binlog_files(binlog_manager.get(), binlog_file_infos, 0, -1);

    std::vector<ExpireAndCapacityParams> params;
    generate_params(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, params);
    bool is_alive = !expired && !overcapacity;
    for (int i = 0; i < params.size(); i++) {
        auto& param = params[i];
        // skip the file if it's timestamp is no less than that of the next file
        if (expired && i + 1 < params.size()) {
            int64_t t1 = binlog_file_infos[i]->rowsets.back().timestamp_in_us;
            int64_t t2 = binlog_file_infos[i + 1]->rowsets.back().timestamp_in_us;
            if (t1 >= t2) {
                continue;
            }
        }

        int64_t current_second = expired ? param.current_second : param.binlog_ttl_second;
        int64_t max_size = overcapacity ? param.binlog_max_size : INT64_MAX;
        bool expired_or_overcapacity =
                binlog_manager->check_expire_and_capacity(current_second, param.binlog_ttl_second, max_size);
        if (is_alive) {
            verify_alive_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 0);
            ASSERT_FALSE(expired_or_overcapacity);
        } else {
            verify_alive_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, i + 1);
            ASSERT_TRUE(expired_or_overcapacity);
        }
        verify_wait_reader_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, i, i - 1);
    }

    if (is_alive) {
        verify_unused_binlog_files(binlog_manager.get(), binlog_file_infos, 0, -1);
    } else {
        verify_unused_binlog_files(binlog_manager.get(), binlog_file_infos, 0, params.size() - 1);
    }
}

TEST_F(BinlogManagerTest, test_binlog_all_alive) {
    test_binlog_delete(false, false);
}

TEST_F(BinlogManagerTest, test_binlog_expired) {
    test_binlog_delete(true, false);
}

TEST_F(BinlogManagerTest, test_binlog_overcapacity) {
    test_binlog_delete(false, true);
}

TEST_F(BinlogManagerTest, test_binlog_expired_and_overcapacity) {
    test_binlog_delete(true, true);
}

TEST_F(BinlogManagerTest, test_wait_reader) {
    int64_t max_file_size = 100 * 1024 * 1024;
    int32_t max_page_size = 1024 * 1024;
    CompressionTypePB compress_type = LZ4_FRAME;
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, max_file_size, max_page_size, compress_type, rowset_fetcher);

    std::vector<BinlogFileInfoPtr> binlog_file_infos;
    generate_random_binlog(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 100, 20);

    std::vector<ExpireAndCapacityParams> params;
    generate_params(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, params);

    bool expired_or_overcapacity = false;
    auto& param_4 = params[4];
    expired_or_overcapacity = binlog_manager->check_expire_and_capacity(
            param_4.binlog_ttl_second, param_4.binlog_ttl_second, param_4.binlog_max_size);
    ASSERT_TRUE(expired_or_overcapacity);
    verify_alive_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 5);
    verify_wait_reader_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 0, -1);
    ASSERT_EQ(5, binlog_manager->unused_binlog_file_ids().get_size());

    auto& file_info_5 = binlog_file_infos[5];
    auto st_5 = binlog_manager->find_binlog_file(file_info_5->rowsets.front().version,
                                                 file_info_5->rowsets.front().start_seq_id);
    ASSERT_OK(st_5.status());
    auto& param_9 = params[9];
    expired_or_overcapacity = binlog_manager->check_expire_and_capacity(
            param_9.binlog_ttl_second, param_9.binlog_ttl_second, param_9.binlog_max_size);
    ASSERT_TRUE(expired_or_overcapacity);
    verify_alive_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 10);
    verify_wait_reader_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 5, 9);
    ASSERT_EQ(5, binlog_manager->unused_binlog_file_ids().get_size());

    auto& param_14 = params[14];
    expired_or_overcapacity = binlog_manager->check_expire_and_capacity(
            param_14.binlog_ttl_second, param_14.binlog_ttl_second, param_14.binlog_max_size);
    ASSERT_TRUE(expired_or_overcapacity);
    verify_alive_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 15);
    verify_wait_reader_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 5, 14);
    ASSERT_EQ(5, binlog_manager->unused_binlog_file_ids().get_size());

    st_5.value().reset();
    auto& param_last = params.back();
    expired_or_overcapacity = binlog_manager->check_expire_and_capacity(
            param_last.binlog_ttl_second, param_last.binlog_ttl_second, param_last.binlog_max_size);
    ASSERT_TRUE(expired_or_overcapacity);
    verify_alive_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, params.size());
    verify_wait_reader_binlog_files(binlog_manager.get(), rowset_fetcher.get(), binlog_file_infos, 0, -1);
    ASSERT_EQ(params.size(), binlog_manager->unused_binlog_file_ids().get_size());

    verify_unused_binlog_files(binlog_manager.get(), binlog_file_infos, 0, params.size() - 1);
}

struct PartialRowsetInfo {
    int64_t version;
    int64_t rowset_id;
    int32_t start_seg_index;
    int32_t end_seg_index;
    bool last_part;
    int32_t num_rows_per_seg;
    int64_t timestamp;

    static PartialRowsetInfo create(int64_t version, int64_t rowset_id, int32_t start_seg_index, int32_t end_seg_index,
                                    bool last_part, int32_t num_rows_per_seg) {
        PartialRowsetInfo info;
        info.version = version;
        info.rowset_id = rowset_id;
        info.start_seg_index = start_seg_index;
        info.end_seg_index = end_seg_index;
        info.last_part = last_part;
        info.num_rows_per_seg = num_rows_per_seg;
        info.timestamp = info.version * 1000000;
        return info;
    }
};

void BinlogManagerTest::generate_binlog_file(int64_t file_id, std::vector<PartialRowsetInfo>& rowset_datas,
                                             std::vector<BinlogFileMetaPBPtr>* metas_for_each_page) {
    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, 1024 * 1024 * 1024, LZ4_FRAME);
    ASSERT_OK(file_writer->init());
    BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
    std::unordered_set<int64_t> rowset_ids;
    for (PartialRowsetInfo& rowset_data : rowset_datas) {
        int64_t start_seq_id = rowset_data.start_seg_index * rowset_data.num_rows_per_seg;
        ASSERT_OK(file_writer->begin(rowset_data.version, start_seq_id, rowset_data.timestamp));
        if (metas_for_each_page->empty()) {
            file_meta->set_id(file_id);
            file_meta->set_start_version(rowset_data.version);
            file_meta->set_start_seq_id(start_seq_id);
            file_meta->set_start_timestamp_in_us(rowset_data.timestamp);
            file_meta->set_num_pages(0);
        }
        file_meta->add_rowsets(rowset_data.rowset_id);
        for (int32_t seg_index = rowset_data.start_seg_index; seg_index <= rowset_data.end_seg_index; seg_index++) {
            RowsetSegInfo info(rowset_data.rowset_id, seg_index);
            ASSERT_OK(file_writer->add_insert_range(info, 0, rowset_data.num_rows_per_seg));
            rowset_ids.insert(rowset_data.rowset_id);
            if (seg_index < rowset_data.end_seg_index) {
                ASSERT_OK(file_writer->force_flush_page(rowset_data.last_part));
            } else {
                ASSERT_OK(file_writer->commit(rowset_data.last_part));
            }
            file_meta->set_end_version(rowset_data.version);
            file_meta->set_end_seq_id((seg_index + 1) * rowset_data.num_rows_per_seg - 1);
            file_meta->set_end_timestamp_in_us(rowset_data.timestamp);
            file_meta->set_version_eof(rowset_data.last_part);
            file_meta->set_num_pages(file_meta->num_pages() + 1);
            file_meta->set_file_size(_fs->get_file_size(file_path).value());
            std::shared_ptr<BinlogFileMetaPB> meta = std::make_shared<BinlogFileMetaPB>();
            meta->CopyFrom(*file_meta);
            metas_for_each_page->push_back(meta);
        }
    }
    ASSERT_OK(file_writer->close(true));
}

TEST_F(BinlogManagerTest, test_init) {
    // mock rowsets and only use them to get data size Rowset#data_disk_size()
    std::vector<RowsetSharedPtr> mock_rowsets;
    for (int i = 1; i <= 5; i++) {
        RowsetSharedPtr rowset;
        int num_rows = std::rand() % 10000 + 1;
        create_rowset(i, {num_rows}, rowset);
        mock_rowsets.push_back(rowset);
    }

    std::shared_ptr<MockRowsetFetcher> rowset_fetcher = std::make_shared<MockRowsetFetcher>();
    std::shared_ptr<BinlogManager> binlog_manager = std::make_shared<BinlogManager>(
            std::rand(), _binlog_file_dir, 100 * 1024 * 1024, 1024 * 1024, LZ4_FRAME, rowset_fetcher);

    std::vector<BinlogFileMetaPBPtr> expect_metas;
    std::vector<int64_t> valid_versions;

    // file1: all data is valid
    std::vector<PartialRowsetInfo> binlog_file_info_1;
    binlog_file_info_1.push_back(PartialRowsetInfo::create(1, 1, 0, 4, true, 100));
    rowset_fetcher->add_rowset(1, mock_rowsets[1 % mock_rowsets.size()]);
    binlog_file_info_1.push_back(PartialRowsetInfo::create(2, 2, 0, 3, false, 100));
    rowset_fetcher->add_rowset(2, mock_rowsets[2 % mock_rowsets.size()]);
    std::vector<BinlogFileMetaPBPtr> binlog_page_metas_1;
    generate_binlog_file(1, binlog_file_info_1, &binlog_page_metas_1);
    expect_metas.push_back(binlog_page_metas_1.back());
    valid_versions.push_back(1);
    valid_versions.push_back(2);

    // file2: only version 2 is valid
    std::vector<PartialRowsetInfo> binlog_file_info_2;
    binlog_file_info_2.push_back(PartialRowsetInfo::create(2, 2, 4, 6, true, 100));
    binlog_file_info_2.push_back(PartialRowsetInfo::create(3, 3, 0, 4, false, 100));
    rowset_fetcher->add_rowset(3, mock_rowsets[3 % mock_rowsets.size()]);
    std::vector<BinlogFileMetaPBPtr> binlog_page_metas_2;
    generate_binlog_file(2, binlog_file_info_2, &binlog_page_metas_2);
    expect_metas.push_back(binlog_page_metas_2[2]);

    // file3: all data is valid
    std::vector<PartialRowsetInfo> binlog_file_info_3;
    binlog_file_info_3.push_back(PartialRowsetInfo::create(3, 3, 0, 6, true, 100));
    binlog_file_info_3.push_back(PartialRowsetInfo::create(4, 4, 0, 2, true, 100));
    rowset_fetcher->add_rowset(4, mock_rowsets[4 % mock_rowsets.size()]);
    std::vector<BinlogFileMetaPBPtr> binlog_page_metas_3;
    generate_binlog_file(3, binlog_file_info_3, &binlog_page_metas_3);
    expect_metas.push_back(binlog_page_metas_3.back());
    valid_versions.push_back(3);
    valid_versions.push_back(4);

    // file4: no data is valid
    std::vector<PartialRowsetInfo> binlog_file_info_4;
    binlog_file_info_4.push_back(PartialRowsetInfo::create(5, 5, 0, 2, false, 100));
    std::vector<BinlogFileMetaPBPtr> binlog_page_metas_4;
    generate_binlog_file(4, binlog_file_info_4, &binlog_page_metas_4);

    // file5: no data is valid
    std::vector<PartialRowsetInfo> binlog_file_info_5;
    binlog_file_info_5.push_back(PartialRowsetInfo::create(5, 5, 0, 1, false, 100));
    std::vector<BinlogFileMetaPBPtr> binlog_page_metas_5;
    generate_binlog_file(5, binlog_file_info_5, &binlog_page_metas_5);

    // file6: all data is valid
    std::vector<PartialRowsetInfo> binlog_file_info_6;
    binlog_file_info_6.push_back(PartialRowsetInfo::create(5, 5, 0, 6, true, 100));
    rowset_fetcher->add_rowset(5, mock_rowsets[5 % mock_rowsets.size()]);
    binlog_file_info_6.push_back(PartialRowsetInfo::create(6, 6, 0, 1, true, 100));
    rowset_fetcher->add_rowset(6, mock_rowsets[6 % mock_rowsets.size()]);
    std::vector<BinlogFileMetaPBPtr> binlog_page_metas_6;
    generate_binlog_file(6, binlog_file_info_6, &binlog_page_metas_6);
    expect_metas.push_back(binlog_page_metas_6.back());
    valid_versions.push_back(5);
    valid_versions.push_back(6);

    BinlogLsn minLsn(1, 0);
    ASSERT_OK(binlog_manager->init(minLsn, valid_versions));

    LsnMap& alive_binlog_files = binlog_manager->alive_binlog_files();
    RowsetCountMap& rowset_count_map = binlog_manager->alive_rowset_count_map();
    ASSERT_EQ(expect_metas.size(), alive_binlog_files.size());
    RowsetCountMap expect_rowset_count;
    int64_t expect_total_binlog_file_size = 0;
    int64_t expect_total_rowset_data_size = 0;
    for (auto& meta : expect_metas) {
        BinlogLsn lsn(meta->start_version(), meta->start_seq_id());
        ASSERT_EQ(1, alive_binlog_files.count(lsn));
        verify_file_meta(meta.get(), alive_binlog_files[lsn]->file_meta());
        expect_total_binlog_file_size += meta->file_size();

        for (auto rowset_id : meta->rowsets()) {
            expect_rowset_count[rowset_id] += 1;
            if (expect_rowset_count[rowset_id] == 1) {
                expect_total_rowset_data_size += rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
            }
        }
    }

    ASSERT_EQ(expect_rowset_count.size(), rowset_count_map.size());
    for (auto it = expect_rowset_count.begin(); it != expect_rowset_count.end(); it++) {
        ASSERT_EQ(1, rowset_count_map.count(it->first));
        ASSERT_EQ(it->second, rowset_count_map[it->first]);
    }

    ASSERT_EQ(expect_total_binlog_file_size, binlog_manager->total_alive_binlog_file_size());
    ASSERT_EQ(expect_total_rowset_data_size, binlog_manager->total_alive_rowset_data_size());
}

} // namespace starrocks

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

#include "storage/lake/local_pk_index_manager.h"

#include <gtest/gtest.h>

#include "column/schema.h"
#include "common/config.h"
#include "fs/fs.h"
#include "storage/chunk_helper.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"

namespace starrocks::lake {

class LocalPkIndexManagerTest : public TestBase {
public:
    LocalPkIndexManagerTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        _tablet_metadata->set_enable_persistent_index(true);

        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            c1->set_aggregation("REPLACE");
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }

protected:
    constexpr static const char* const kTestGroupPath = "test_local_pk_index_gc";

    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
};

TEST_F(LocalPkIndexManagerTest, test_gc) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("is_tablet_in_worker:1", [](void* arg) { *(bool*)arg = false; });
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    Chunk chunk0({c0, c1}, _schema);
    auto rowset_txn_meta = std::make_unique<RowsetTxnMetaPB>();

    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    std::shared_ptr<const TabletSchema> const_schema = _tablet_schema;
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());

    // write segment #1
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    // write txnlog
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_metadata->id());
    txn_log->set_txn_id(txn_id);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : writer->files()) {
        op_write->mutable_rowset()->add_segments(std::move(f.path));
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);

    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    writer->close();
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), 2, txn_id).status());
    auto stores = StorageEngine::instance()->get_stores();
    ASSERT_TRUE(stores.size() > 0);
    ASSERT_OK(FileSystem::Default()->path_exists(stores[0]->get_persistent_index_path() + "/" +
                                                 std::to_string(_tablet_metadata->id())));

    auto* data_dir = stores[0];
    auto pk_path = data_dir->get_persistent_index_path();
    std::set<std::string> tablet_ids;
    ASSERT_OK(fs::list_dirs_files(pk_path, &tablet_ids, nullptr));
    LocalPkIndexManager::gc(ExecEnv::GetInstance()->lake_update_manager(), data_dir, tablet_ids);

    ASSERT_ERROR(FileSystem::Default()->path_exists(stores[0]->get_persistent_index_path() + "/" +
                                                    std::to_string(_tablet_metadata->id())));
    SyncPoint::GetInstance()->ClearCallBack("is_tablet_in_worker:1");
    SyncPoint::GetInstance()->DisableProcessing();

    txn_id = next_id();
    ASSIGN_OR_ABORT(writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());

    // write segment #2
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    // write txnlog
    txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_metadata->id());
    txn_log->set_txn_id(txn_id);
    op_write = txn_log->mutable_op_write();
    for (auto& f : writer->files()) {
        op_write->mutable_rowset()->add_segments(std::move(f.path));
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    writer->close();
    // publish again should be successful, persistent index will be reloaded.
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), 2, txn_id).status());
}

TEST_F(LocalPkIndexManagerTest, test_evict) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("LocalPkIndexManager::evict:1", [](void* arg) { *(bool*)arg = true; });
    SyncPoint::GetInstance()->SetCallBack("LocalPkIndexManager::evict:2", [](void* arg) { *(bool*)arg = true; });
    SyncPoint::GetInstance()->SetCallBack("LocalPkIndexManager::evict:3", [](void* arg) { *(bool*)arg = true; });
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    Chunk chunk0({c0, c1}, _schema);
    auto rowset_txn_meta = std::make_unique<RowsetTxnMetaPB>();

    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    std::shared_ptr<const TabletSchema> const_schema = _tablet_schema;
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());

    // write segment #1
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    // write txnlog
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_metadata->id());
    txn_log->set_txn_id(txn_id);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : writer->files()) {
        op_write->mutable_rowset()->add_segments(std::move(f.path));
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);

    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    writer->close();
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), 2, txn_id).status());
    auto stores = StorageEngine::instance()->get_stores();
    ASSERT_TRUE(stores.size() > 0);
    ASSERT_OK(FileSystem::Default()->path_exists(stores[0]->get_persistent_index_path() + "/" +
                                                 std::to_string(_tablet_metadata->id())));

    auto* data_dir = stores[0];
    auto pk_path = data_dir->get_persistent_index_path();
    std::set<std::string> tablet_ids;
    ASSERT_OK(fs::list_dirs_files(pk_path, &tablet_ids, nullptr));
    LocalPkIndexManager::evict(ExecEnv::GetInstance()->lake_update_manager(), data_dir, tablet_ids);

    ASSERT_ERROR(FileSystem::Default()->path_exists(stores[0]->get_persistent_index_path() + "/" +
                                                    std::to_string(_tablet_metadata->id())));
    SyncPoint::GetInstance()->ClearCallBack("LocalPkIndexManager::evict:1");
    SyncPoint::GetInstance()->ClearCallBack("LocalPkIndexManager::evict:2");
    SyncPoint::GetInstance()->ClearCallBack("LocalPkIndexManager::evict:3");
    SyncPoint::GetInstance()->DisableProcessing();

    txn_id = next_id();
    ASSIGN_OR_ABORT(writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());

    // write segment #2
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    // write txnlog
    txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_metadata->id());
    txn_log->set_txn_id(txn_id);
    op_write = txn_log->mutable_op_write();
    for (auto& f : writer->files()) {
        op_write->mutable_rowset()->add_segments(std::move(f.path));
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    writer->close();
    // publish again should be successful, persistent index will be reloaded.
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), 2, txn_id).status());
}

} // namespace starrocks::lake

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

#include "service/service_be/lake_service.h"

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "fs/fs_util.h"
#include "gutil/strings/util.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"

namespace starrocks {

class LakeServiceTest : public testing::Test {
public:
    LakeServiceTest()
            : _tablet_id(next_id()),
              _location_provider(new lake::FixedLocationProvider(kRootLocation)),
              _tablet_mgr(ExecEnv::GetInstance()->lake_tablet_manager()),
              _lake_service(ExecEnv::GetInstance(), ExecEnv::GetInstance()->lake_tablet_manager()) {
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName));
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName));
        FileSystem::Default()->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName));
    }

    ~LakeServiceTest() override {
        CHECK_OK(fs::remove_all(kRootLocation));
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        delete _location_provider;
    }

    void create_tablet() {
        _tablet_id = next_id();
        auto metadata = std::make_shared<lake::TabletMetadata>();
        metadata->set_id(_tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(0);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(1);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
        ASSERT_TRUE(tablet_mgr != nullptr);
        ASSERT_OK(tablet_mgr->put_tablet_metadata(metadata));
    }

    void SetUp() override { create_tablet(); }

    void TearDown() override {}

protected:
    // Return the new generated segment file name
    std::string generate_segment_file(int64_t txn_id) {
        auto seg_name = lake::gen_segment_filename(txn_id);
        auto seg_path = _tablet_mgr->segment_location(_tablet_id, seg_name);
        ASSIGN_OR_ABORT(auto f, fs::new_writable_file(seg_path));
        CHECK_OK(f->append(seg_path));
        CHECK_OK(f->close());
        return seg_name;
    }

    lake::TxnLog generate_write_txn_log(int num_segments, int64_t num_rows, int64_t data_size) {
        auto txn_id = next_id();
        lake::TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        for (int i = 0; i < num_segments; i++) {
            log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
        }
        log.mutable_op_write()->mutable_rowset()->set_data_size(data_size);
        log.mutable_op_write()->mutable_rowset()->set_num_rows(num_rows);
        log.mutable_op_write()->mutable_rowset()->set_overlapped(num_segments > 1);
        return log;
    }

    constexpr static const char* const kRootLocation = "./lake_service_test";
    int64_t _tablet_id;
    lake::LocationProvider* _location_provider;
    lake::TabletManager* _tablet_mgr;
    lake::LocationProvider* _backup_location_provider;
    LakeServiceImpl _lake_service;
};

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_missing_tablet_ids) {
    brpc::Controller cntl;
    lake::PublishVersionRequest request;
    lake::PublishVersionResponse response;
    request.set_base_version(1);
    request.set_new_version(2);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_missing_txn_ids) {
    brpc::Controller cntl;
    lake::PublishVersionRequest request;
    lake::PublishVersionResponse response;
    request.set_base_version(1);
    request.set_new_version(2);
    request.add_tablet_ids(_tablet_id);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing txn_ids", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_missing_base_version) {
    brpc::Controller cntl;
    lake::PublishVersionRequest request;
    lake::PublishVersionResponse response;
    request.set_new_version(2);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing base version", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_missing_new_version) {
    brpc::Controller cntl;
    lake::PublishVersionRequest request;
    lake::PublishVersionResponse response;
    request.set_base_version(1);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing new version", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    lake::PublishVersionRequest request;
    lake::PublishVersionResponse response;
    request.set_base_version(1);
    request.set_new_version(2);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1000);
    _lake_service.publish_version(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(1, response.failed_tablets_size());
    ASSERT_EQ(_tablet_id, response.failed_tablets(0));
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_for_write) {
    std::vector<lake::TxnLog> logs;
    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // TxnLog with 2 segments
    logs.emplace_back(generate_write_txn_log(2, 101, 4096));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Publish version request for the first transaction
    lake::PublishVersionRequest publish_request_1000;
    publish_request_1000.set_base_version(1);
    publish_request_1000.set_new_version(2);
    publish_request_1000.add_tablet_ids(_tablet_id);
    publish_request_1000.add_txn_ids(logs[0].txn_id());

    // Publish txn failed: get base tablet metadata failed
    {
        _tablet_mgr->prune_metacache();

        TEST_ENABLE_ERROR_POINT("TabletManager::load_tablet_metadata",
                                Status::IOError("injected get tablet metadata error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::load_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // Publish failed: get txn log failed
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::load_txn_log", Status::IOError("injected get txn log error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::load_txn_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // Publish txn success
    {
        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    // publish version request for the second transaction
    lake::PublishVersionRequest publish_request_1;
    publish_request_1.set_base_version(2);
    publish_request_1.set_new_version(3);
    publish_request_1.add_tablet_ids(_tablet_id);
    publish_request_1.add_txn_ids(logs[1].txn_id());
    publish_request_1.set_commit_time(987654321);

    // Publish txn put tablet metadata failed
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::put_tablet_metadata",
                                Status::IOError("injected put tablet metadata error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::put_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }

    // Publish txn success
    {
        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    {
        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
        ASSERT_EQ(3, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(3, metadata->next_rowset_id());
        ASSERT_EQ(1, metadata->rowsets_size());
        ASSERT_EQ(1, metadata->rowsets(0).id());
        ASSERT_EQ(2, metadata->rowsets(0).segments_size());
        ASSERT_TRUE(metadata->rowsets(0).overlapped());
        ASSERT_EQ(logs[1].op_write().rowset().num_rows(), metadata->rowsets(0).num_rows());
        ASSERT_EQ(logs[1].op_write().rowset().data_size(), metadata->rowsets(0).data_size());
        ASSERT_EQ(logs[1].op_write().rowset().segments(0), metadata->rowsets(0).segments(0));
        ASSERT_EQ(logs[1].op_write().rowset().segments(1), metadata->rowsets(0).segments(1));
        EXPECT_EQ(987654321, metadata->commit_time());
    }
    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // TxnLog`s should have been deleted
    ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
    ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().is_not_found());

    // Send publish version request again.
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs[1].txn_id());
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_EQ(1, response.compaction_scores_size());
    }
    // Send publish version request again with an non-exist tablet
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_tablet_ids(9999);
        request.add_txn_ids(logs[1].txn_id());
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(9999, response.failed_tablets(0));
        ASSERT_EQ(1, response.compaction_scores_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    // Send publish version request again with an non-exist txnlog
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(3);
        request.set_new_version(4);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1111);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        ASSERT_EQ(0, response.compaction_scores_size());
    }
    // Delete old version metadata then send publish version again
    ASSERT_OK(tablet.delete_metadata(1));
    ASSERT_OK(tablet.delete_metadata(2));
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs[1].txn_id());
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }

    // Empty TxnLog
    {
        logs.emplace_back(generate_write_txn_log(0, 0, 0));
        ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));
    }
    // Publish txn
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(3);
        request.set_new_version(4);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(logs[2].txn_id());
        request.set_commit_time(0);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(4));
        EXPECT_EQ(0, metadata->commit_time());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_for_write_batch) {
    // Empty TxnLog
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1002);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    // TxnLog with 2 segments
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1003);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("1.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("2.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }

    // Publish txn 1002 and txn 1003
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1002);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
    ASSERT_EQ(3, metadata->version());
    ASSERT_EQ(_tablet_id, metadata->id());
    ASSERT_EQ(3, metadata->next_rowset_id());
    ASSERT_EQ(1, metadata->rowsets_size());
    ASSERT_EQ(1, metadata->rowsets(0).id());
    ASSERT_EQ(2, metadata->rowsets(0).segments_size());
    ASSERT_TRUE(metadata->rowsets(0).overlapped());
    ASSERT_EQ(101, metadata->rowsets(0).num_rows());
    ASSERT_EQ(4096, metadata->rowsets(0).data_size());
    ASSERT_EQ("1.dat", metadata->rowsets(0).segments(0));
    ASSERT_EQ("2.dat", metadata->rowsets(0).segments(1));

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // TxnLog should't have been deleted
    ASSERT_TRUE(tablet.get_txn_log(1002).status().ok());
    ASSERT_TRUE(tablet.get_txn_log(1003).status().ok());

    // Send publish version request again.
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_EQ(1, response.compaction_scores_size());
    }
    // Send publish version request again with an non-exist tablet
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_tablet_ids(9999);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(9999, response.failed_tablets(0));
        ASSERT_EQ(1, response.compaction_scores_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    // Send publish version request again with an non-exist txnlog
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(3);
        request.set_new_version(4);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1111);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
        ASSERT_EQ(0, response.compaction_scores_size());
    }
    // Delete old version metadata then send publish version again
    ASSERT_OK(tablet.delete_metadata(1));
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1002);
        request.add_txn_ids(1003);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
}

TEST_F(LakeServiceTest, test_publish_version_transform_single_to_batch) {
    std::vector<lake::TxnLog> logs;
    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // TxnLog with 2 segments
    logs.emplace_back(generate_write_txn_log(2, 101, 4096));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Publish version request for the first transaction
    lake::PublishVersionRequest publish_request_1000;
    publish_request_1000.set_base_version(1);
    publish_request_1000.set_new_version(2);
    publish_request_1000.add_tablet_ids(_tablet_id);
    publish_request_1000.add_txn_ids(logs[0].txn_id());

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));

    // Publish txn single
    {
        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
    }

    // Publish version request for the two transactions
    lake::PublishVersionRequest publish_request_1001;
    publish_request_1001.set_base_version(1);
    publish_request_1001.set_new_version(4);
    publish_request_1001.add_tablet_ids(_tablet_id);
    publish_request_1001.add_txn_ids(logs[0].txn_id());
    publish_request_1001.add_txn_ids(logs[1].txn_id());
    publish_request_1001.add_txn_ids(logs[2].txn_id());

    // publish txn batch with previous txns which have been published
    {
        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1001, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog of logs[0] should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
        // the other txn_logs should't have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().ok());
        ASSERT_TRUE(tablet.get_txn_log(logs[2].txn_id()).status().ok());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(4));
        ASSERT_EQ(4, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(3, metadata->next_rowset_id());
        ASSERT_EQ(1, metadata->rowsets_size());
        ASSERT_EQ(1, metadata->rowsets(0).id());
        ASSERT_EQ(2, metadata->rowsets(0).segments_size());
        ASSERT_TRUE(metadata->rowsets(0).overlapped());
        ASSERT_EQ(101, metadata->rowsets(0).num_rows());
        ASSERT_EQ(4096, metadata->rowsets(0).data_size());

        // middle tablet meta should't exist
        ASSERT_FALSE(tablet.get_metadata(3).status().ok());
    }
}

TEST_F(LakeServiceTest, test_publish_version_transform_batch_to_single) {
    std::vector<lake::TxnLog> logs;
    // Empty TxnLog
    logs.emplace_back(generate_write_txn_log(0, 0, 0));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // TxnLog with 2 segments
    logs.emplace_back(generate_write_txn_log(2, 101, 4096));
    ASSERT_OK(_tablet_mgr->put_txn_log(logs.back()));

    // Publish version request
    lake::PublishVersionRequest publish_request_1000;
    publish_request_1000.set_base_version(1);
    publish_request_1000.set_new_version(3);
    publish_request_1000.add_tablet_ids(_tablet_id);
    publish_request_1000.add_txn_ids(logs[0].txn_id());
    publish_request_1000.add_txn_ids(logs[1].txn_id());

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));

    // Publish txn batch
    {
        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1000, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog should't have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().ok());
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().ok());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
        ASSERT_EQ(3, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(101, metadata->rowsets(0).num_rows());
        ASSERT_EQ(4096, metadata->rowsets(0).data_size());
    }

    // Publish single
    lake::PublishVersionRequest publish_request_1001;
    publish_request_1001.set_base_version(1);
    publish_request_1001.set_new_version(2);
    publish_request_1001.add_tablet_ids(_tablet_id);
    publish_request_1001.add_txn_ids(logs[0].txn_id());

    // publish first txn
    {
        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1001, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog of logs[0] should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[0].txn_id()).status().is_not_found());
        // TxnLog of logs[1] should't have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().ok());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(2));
        ASSERT_EQ(2, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(0, metadata->rowsets_size());
    }

    // Publish single
    lake::PublishVersionRequest publish_request_1002;
    publish_request_1002.set_base_version(2);
    publish_request_1002.set_new_version(3);
    publish_request_1002.add_tablet_ids(_tablet_id);
    publish_request_1002.add_txn_ids(logs[1].txn_id());

    // publish second txn
    {
        lake::PublishVersionResponse response;
        _lake_service.publish_version(nullptr, &publish_request_1002, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // TxnLog of logs[1] should have been deleted
        ASSERT_TRUE(tablet.get_txn_log(logs[1].txn_id()).status().is_not_found());

        ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(3));
        ASSERT_EQ(3, metadata->version());
        ASSERT_EQ(_tablet_id, metadata->id());
        ASSERT_EQ(101, metadata->rowsets(0).num_rows());
        ASSERT_EQ(4096, metadata->rowsets(0).data_size());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_abort) {
    std::vector<lake::TxnLog> logs;

    // Empty TxnLog
    {
        auto txn_id = next_id();
        lake::TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }

    // Write txn log
    {
        auto txn_id = next_id();
        lake::TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_write()->mutable_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        log.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        log.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }
    // Compaction txn log
    {
        auto txn_id = next_id();
        lake::TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        log.mutable_op_compaction()->mutable_output_rowset()->set_overlapped(false);
        log.mutable_op_compaction()->mutable_output_rowset()->set_num_rows(101);
        log.mutable_op_compaction()->mutable_output_rowset()->set_data_size(4096);
        log.mutable_op_compaction()->mutable_output_rowset()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_compaction()->mutable_output_rowset()->add_segments(generate_segment_file(txn_id));
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }
    // Schema change txn log
    {
        auto txn_id = next_id();
        lake::TxnLog log;
        log.set_tablet_id(_tablet_id);
        log.set_txn_id(txn_id);
        log.mutable_op_schema_change()->add_rowsets()->add_segments(generate_segment_file(txn_id));
        log.mutable_op_schema_change()->add_rowsets()->add_segments(generate_segment_file(txn_id));
        ASSERT_OK(_tablet_mgr->put_txn_log(log));

        logs.emplace_back(log);
    }

    lake::AbortTxnRequest request;
    request.add_tablet_ids(_tablet_id);
    for (auto&& log : logs) {
        request.add_txn_ids(log.txn_id());
    }

    {
        TEST_ENABLE_ERROR_POINT("TabletManager::load_txn_log", Status::IOError("injected get txn log error"));
        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::load_txn_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }
    {
        lake::AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

    // TxnLog`s and segments should have been deleted
    for (auto&& log : logs) {
        for (auto&& s : log.op_write().rowset().segments()) {
            EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
        }
        for (auto&& s : log.op_compaction().output_rowset().segments()) {
            EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
        }
        for (auto&& r : log.op_schema_change().rowsets()) {
            for (auto&& s : r.segments()) {
                EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, s)));
            }
        }
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, log.txn_id())));
    }

    // Send AbortTxn request again
    {
        lake::AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }
    // Thread pool is full
    {
        SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::AbortTxnResponse response;
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_delete_tablet) {
    brpc::Controller cntl;
    lake::DeleteTabletRequest request;
    lake::DeleteTabletResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.delete_tablet(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(0, response.failed_tablets_size());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_delete_tablet_dir_not_exit) {
    ASSERT_OK(fs::remove_all(kRootLocation));
    brpc::Controller cntl;
    lake::DeleteTabletRequest request;
    lake::DeleteTabletResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.delete_tablet(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(0, response.failed_tablets_size());
    EXPECT_EQ(0, response.status().status_code()) << response.status().error_msgs(0);
    // restore test directory
    ASSERT_OK(fs::create_directories(kRootLocation));
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_compact) {
    auto compact = [this](::google::protobuf::RpcController* cntl, const lake::CompactRequest* request,
                          lake::CompactResponse* response) {
        CountDownLatch latch(1);
        auto cb = ::google::protobuf::NewCallback(&latch, &CountDownLatch::count_down);
        _lake_service.compact(cntl, request, response, cb);
        latch.wait();
    };

    auto txn_id = next_id();
    // missing tablet_ids
    {
        brpc::Controller cntl;
        lake::CompactRequest request;
        lake::CompactResponse response;
        // request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
    }
    // missing txn_id
    {
        brpc::Controller cntl;
        lake::CompactRequest request;
        lake::CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        //request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing txn_id", cntl.ErrorText());
    }
    // missing version
    {
        brpc::Controller cntl;
        lake::CompactRequest request;
        lake::CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        //request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing version", cntl.ErrorText());
    }
    // tablet not exist
    {
        brpc::Controller cntl;
        lake::CompactRequest request;
        lake::CompactResponse response;
        request.add_tablet_ids(_tablet_id + 1);
        request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id + 1, response.failed_tablets(0));
    }
    // compact
    {
        brpc::Controller cntl;
        lake::CompactRequest request;
        lake::CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        compact(&cntl, &request, &response);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
    }
    // publish version
    {
        brpc::Controller cntl;
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(txn_id);
        request.set_base_version(1);
        request.set_new_version(2);
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_drop_table) {
    ASSERT_OK(FileSystem::Default()->path_exists(kRootLocation));
    lake::DropTableRequest request;
    lake::DropTableResponse response;

    brpc::Controller cntl;
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing tablet_id", cntl.ErrorText());

    cntl.Reset();
    request.set_tablet_id(_tablet_id);
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    auto st = FileSystem::Default()->path_exists(kRootLocation);
    ASSERT_TRUE(st.is_not_found()) << st;
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_log_version) {
    auto txn_id = next_id();
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(txn_id);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("1.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("2.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    {
        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
    }
    {
        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing txn_id", cntl.ErrorText());
    }
    {
        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing version", cntl.ErrorText());
    }
    for (auto inject_error : {Status::InternalError("injected"), Status::NotFound("injected")}) {
        std::cerr << "Injected error: " << inject_error << '\n';
        TEST_ENABLE_ERROR_POINT("fs::copy_file", inject_error);
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("fs::copy_file");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, txn_id)));
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 10)));
    }
    {
        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, txn_id)));
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 10)));
    }
    // duplicate request
    {
        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 10)));
    }
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_log_version_batch) {
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1001);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("1.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("2.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));

        lake::TxnLog txnlog2;
        txnlog2.set_tablet_id(_tablet_id);
        txnlog2.set_txn_id(1002);
        txnlog2.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog2.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog2.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog2.mutable_op_write()->mutable_rowset()->add_segments("3.dat");
        txnlog2.mutable_op_write()->mutable_rowset()->add_segments("4.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog2));
    }
    {
        lake::PublishLogVersionBatchRequest request;
        lake::PublishLogVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing tablet_ids", cntl.ErrorText());
    }
    {
        lake::PublishLogVersionBatchRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing txn_ids", cntl.ErrorText());
    }
    {
        lake::PublishLogVersionBatchRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing versions", cntl.ErrorText());
    }
    {
        lake::PublishLogVersionBatchRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
        request.add_txn_ids(1002);
        request.add_versions(10);
        request.add_versions(11);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        _tablet_mgr->prune_metacache();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1001).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1001).status();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1002).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1002).status();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_vlog(_tablet_id, 10));
        ASSERT_EQ(_tablet_id, txn_log->tablet_id());
        ASSERT_EQ(1001, txn_log->txn_id());

        ASSIGN_OR_ABORT(auto txn_log2, _tablet_mgr->get_txn_vlog(_tablet_id, 11));
        ASSERT_EQ(_tablet_id, txn_log2->tablet_id());
        ASSERT_EQ(1002, txn_log2->txn_id());
    }
    // duplicate request
    {
        lake::PublishLogVersionBatchRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
        request.add_txn_ids(1002);
        request.add_versions(10);
        request.add_versions(11);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        _tablet_mgr->prune_metacache();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1001).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1001).status();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_vlog(_tablet_id, 10));
        ASSERT_EQ(_tablet_id, txn_log->tablet_id());
        ASSERT_EQ(1001, txn_log->txn_id());

        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1002).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1002).status();

        ASSIGN_OR_ABORT(auto txn_log2, _tablet_mgr->get_txn_vlog(_tablet_id, 11));
        ASSERT_EQ(_tablet_id, txn_log2->tablet_id());
        ASSERT_EQ(1002, txn_log2->txn_id());
    }

    // not existing txnId
    {
        lake::PublishLogVersionBatchRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1111);
        brpc::Controller cntl;
        _lake_service.publish_log_version_batch(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_for_schema_change) {
    // write 1 rowset when schema change
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1000);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(4);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(14);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("4.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("5.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("6.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));

        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(1000);
        request.set_version(4);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    // schema change with 2 rowsets
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1001);
        auto op_schema_change = txnlog.mutable_op_schema_change();
        op_schema_change->set_alter_version(3);
        auto rowset0 = op_schema_change->add_rowsets();
        rowset0->set_id(1);
        rowset0->set_overlapped(true);
        rowset0->set_num_rows(2);
        rowset0->set_data_size(12);
        rowset0->add_segments("1.dat");
        rowset0->add_segments("2.dat");
        auto rowset1 = op_schema_change->add_rowsets();
        rowset1->set_id(3);
        rowset1->set_overlapped(false);
        rowset1->set_num_rows(3);
        rowset1->set_data_size(13);
        rowset1->add_segments("3.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }

    lake::PublishVersionRequest request;
    request.set_base_version(1);
    request.set_new_version(5);
    request.add_tablet_ids(_tablet_id);
    request.add_txn_ids(1001);

    // fail to get txn vlog
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::get_txn_vlog", Status::InternalError("injected internal error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::get_txn_vlog");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // txn vlog does not exit
    {
        TEST_ENABLE_ERROR_POINT("TabletManager::get_txn_vlog", Status::NotFound("injected not found"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::get_txn_vlog");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // apply schema change log failed
    {
        TEST_ENABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_schema_change_log",
                                Status::InternalError("injected apply error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_schema_change_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // apply write log failed
    {
        TEST_ENABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_write_log",
                                Status::InternalError("injected apply error"));
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_write_log");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        lake::PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(_tablet_id, response.failed_tablets(0));
    }
    // apply success
    {
        lake::PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    _tablet_mgr->prune_metacache();
    // publish again
    {
        lake::PublishVersionResponse response;
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(5));
    ASSERT_EQ(5, metadata->version());
    ASSERT_EQ(3, metadata->rowsets().size());
    const auto& rowset0 = metadata->rowsets(0);
    ASSERT_TRUE(rowset0.overlapped());
    ASSERT_EQ(2, rowset0.num_rows());
    ASSERT_EQ(12, rowset0.data_size());
    ASSERT_EQ(2, rowset0.segments_size());
    const auto& rowset1 = metadata->rowsets(1);
    ASSERT_FALSE(rowset1.overlapped());
    ASSERT_EQ(3, rowset1.num_rows());
    ASSERT_EQ(13, rowset1.data_size());
    ASSERT_EQ(1, rowset1.segments_size());
    const auto& rowset2 = metadata->rowsets(2);
    ASSERT_FALSE(rowset2.overlapped());
    ASSERT_EQ(4, rowset2.num_rows());
    ASSERT_EQ(14, rowset2.data_size());
    ASSERT_EQ(3, rowset2.segments_size());

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, 1000)));
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_id, 1001)));
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_id, 4)));
}

TEST_F(LakeServiceTest, test_abort_compaction) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->LoadDependency(
            {{"CompactionScheduler::compact:return", "LakeServiceImpl::abort_compaction:enter"},
             {"LakeServiceImpl::abort_compaction:aborted", "CompactionScheduler::do_compaction:before_execute_task"}});

    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    auto txn_id = next_id();

    auto compaction_thread = std::thread([&]() {
        brpc::Controller cntl;
        lake::CompactRequest request;
        lake::CompactResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(1);
        CountDownLatch latch(1);
        auto cb = ::google::protobuf::NewCallback(&latch, &CountDownLatch::count_down);
        _lake_service.compact(&cntl, &request, &response, cb);
        latch.wait();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(1, response.failed_tablets_size());
        ASSERT_EQ(TStatusCode::ABORTED, response.status().status_code());
    });

    {
        brpc::Controller cntl;
        lake::AbortCompactionRequest request;
        lake::AbortCompactionResponse response;
        request.set_txn_id(txn_id);
        _lake_service.abort_compaction(&cntl, &request, &response, nullptr);
        ASSERT_EQ(TStatusCode::OK, response.status().status_code());
    }

    compaction_thread.join();

    {
        brpc::Controller cntl;
        lake::AbortCompactionRequest request;
        lake::AbortCompactionResponse response;
        request.set_txn_id(txn_id);
        _lake_service.abort_compaction(&cntl, &request, &response, nullptr);
        ASSERT_EQ(TStatusCode::NOT_FOUND, response.status().status_code());
    }
}

// https://github.com/StarRocks/starrocks/issues/28244
// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_publish_version_issue28244) {
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(102301);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("xxxxx.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }

    SyncPoint::GetInstance()->SetCallBack("publish_version:delete_txn_log",
                                          [](void* st) { *(Status*)st = Status::InternalError("injected"); });
    SyncPoint::GetInstance()->LoadDependency(
            {{"LakeServiceImpl::publish_version:return", "publish_version:delete_txn_log"}});
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("publish_version:delete_txn_log");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(2);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(102301);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 102301).status().is_not_found());
}

TEST_F(LakeServiceTest, test_get_tablet_stats) {
    lake::TabletStatRequest request;
    lake::TabletStatResponse response;
    auto* info = request.add_tablet_infos();
    info->set_tablet_id(_tablet_id);
    info->set_version(1);
    _lake_service.get_tablet_stats(nullptr, &request, &response, nullptr);
    ASSERT_EQ(1, response.tablet_stats_size());
    ASSERT_EQ(_tablet_id, response.tablet_stats(0).tablet_id());
    ASSERT_EQ(0, response.tablet_stats(0).num_rows());
    ASSERT_EQ(0, response.tablet_stats(0).data_size());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_drop_table_no_thread_pool) {
    ASSERT_OK(FileSystem::Default()->path_exists(kRootLocation));

    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    lake::DropTableRequest request;
    lake::DropTableResponse response;
    request.set_tablet_id(100);
    brpc::Controller cntl;
    _lake_service.drop_table(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("no thread pool to run task", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_delete_tablet_no_thread_pool) {
    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    lake::DeleteTabletRequest request;
    lake::DeleteTabletResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.delete_tablet(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("no thread pool to run task", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_vacuum_null_thread_pool) {
    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    lake::VacuumRequest request;
    lake::VacuumResponse response;
    request.add_tablet_ids(_tablet_id);
    request.set_partition_id(next_id());
    _lake_service.vacuum(&cntl, &request, &response, nullptr);
    ASSERT_EQ("vacuum thread pool is null", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_vacuum_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    lake::VacuumRequest request;
    lake::VacuumResponse response;
    request.add_tablet_ids(_tablet_id);
    request.set_partition_id(next_id());
    _lake_service.vacuum(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed());
    EXPECT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code()) << response.status().status_code();
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_vacuum_full_null_thread_pool) {
    SyncPoint::GetInstance()->SetCallBack("AgentServer::Impl::get_thread_pool:1",
                                          [](void* arg) { *(ThreadPool**)arg = nullptr; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("AgentServer::Impl::get_thread_pool:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    lake::VacuumFullRequest request;
    lake::VacuumFullResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.vacuum_full(&cntl, &request, &response, nullptr);
    ASSERT_EQ("full vacuum thread pool is null", cntl.ErrorText());
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_vacuum_full_thread_pool_full) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    brpc::Controller cntl;
    lake::VacuumFullRequest request;
    lake::VacuumFullResponse response;
    request.add_tablet_ids(_tablet_id);
    _lake_service.vacuum_full(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(TStatusCode::SERVICE_UNAVAILABLE, response.status().status_code()) << response.status().status_code();
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_duplicated_vacuum_request) {
    SyncPoint::GetInstance()->LoadDependency({{"LakeServiceImpl::vacuum:1", "LakeServiceImpl::vacuum:2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    auto duplicate = false;
    auto partition_id = next_id();

    auto t = std::thread([&]() {
        brpc::Controller cntl;
        lake::VacuumRequest request;
        lake::VacuumResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_partition_id(partition_id);
        _lake_service.vacuum(&cntl, &request, &response, nullptr);
        if (cntl.ErrorText() == fmt::format("duplicated vacuum request of partition {}", partition_id)) {
            duplicate = true;
        }
    });

    {
        brpc::Controller cntl;
        lake::VacuumRequest request;
        lake::VacuumResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_partition_id(partition_id);
        _lake_service.vacuum(&cntl, &request, &response, nullptr);
        if (cntl.ErrorText() == fmt::format("duplicated vacuum request of partition {}", partition_id)) {
            duplicate = true;
        }
    }

    t.join();

    ASSERT_TRUE(duplicate);
}

TEST_F(LakeServiceTest, test_lock_and_unlock_tablet_metadata) {
    {
        lake::LockTabletMetadataRequest request;
        lake::LockTabletMetadataResponse response;
        request.set_tablet_id(10);
        request.set_version(5);
        brpc::Controller cntl;
        _lake_service.lock_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }
    {
        lake::UnlockTabletMetadataRequest request;
        lake::UnlockTabletMetadataResponse response;
        request.set_tablet_id(10);
        request.set_version(13);
        request.set_expire_time(10000);
        brpc::Controller cntl;
        _lake_service.unlock_tablet_metadata(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }
}

TEST_F(LakeServiceTest, test_abort_txn2) {
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));

    auto load_mgr = ExecEnv::GetInstance()->load_channel_mgr();
    auto db_id = next_id();
    auto table_id = next_id();
    auto partition_id = next_id();
    auto index_id = metadata->schema().id();
    auto txn_id = next_id();
    PUniqueId load_id;
    load_id.set_hi(next_id());
    load_id.set_lo(next_id());
    // Open load channel
    {
        PTabletWriterOpenRequest request;
        PTabletWriterOpenResult response;
        request.set_is_lake_tablet(true);
        request.mutable_id()->CopyFrom(load_id);
        request.set_table_id(table_id);
        request.set_index_id(index_id);
        request.set_txn_id(txn_id);
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        request.set_load_channel_timeout_s(10000000);
        request.set_is_vectorized(true);
        request.set_table_id(next_id());
        request.mutable_schema()->set_db_id(db_id);
        request.mutable_schema()->set_table_id(table_id);
        request.mutable_schema()->set_version(1);
        auto index = request.mutable_schema()->add_indexes();
        index->set_id(index_id);
        index->set_schema_hash(0);
        for (int i = 0, sz = metadata->schema().column_size(); i < sz; i++) {
            const auto& column = metadata->schema().column(i);
            auto slot = request.mutable_schema()->add_slot_descs();
            slot->set_id(i);
            slot->set_byte_offset(i * sizeof(int) /*unused*/);
            slot->set_col_name(column.name() /*unused*/);
            slot->set_slot_idx(i);
            slot->set_is_materialized(true);
            ASSERT_EQ("INT", column.type());
            slot->mutable_slot_type()->add_types()->mutable_scalar_type()->set_type(TYPE_INT);

            index->add_columns(metadata->schema().column(i).name());
        }
        request.mutable_schema()->mutable_tuple_desc()->set_id(1);
        request.mutable_schema()->mutable_tuple_desc()->set_byte_size(8 /*unused*/);
        request.mutable_schema()->mutable_tuple_desc()->set_num_null_bytes(0 /*unused*/);
        request.mutable_schema()->mutable_tuple_desc()->set_table_id(10 /*unused*/);

        auto ptablet = request.add_tablets();
        ptablet->set_partition_id(partition_id);
        ptablet->set_tablet_id(metadata->id());

        load_mgr->open(nullptr, request, &response, nullptr);
        ASSERT_EQ(TStatusCode::OK, response.status().status_code()) << response.status().error_msgs(0);
    }

    auto tablet_schema = TabletSchema::create(metadata->schema());
    auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));

    auto generate_data = [=](int64_t chunk_size) -> Chunk {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::iota(v0.begin(), v0.end(), 0);
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        Chunk chunk({c0, c1}, schema);
        chunk.set_slot_id_to_index(0, 0);
        chunk.set_slot_id_to_index(1, 1);
        return chunk;
    };

    auto do_write = [&]() {
        auto chunk_size = 10;
        auto chunk = generate_data(chunk_size);
        bool cancelled = false;
        for (int64_t i = 0; i < 1000; i++) {
            PTabletWriterAddChunkRequest add_chunk_request;
            PTabletWriterAddBatchResult add_chunk_response;
            add_chunk_request.mutable_id()->CopyFrom(load_id);
            add_chunk_request.set_index_id(index_id);
            add_chunk_request.set_sender_id(0);
            add_chunk_request.set_eos(false);
            add_chunk_request.set_packet_seq(i);

            for (int j = 0; j < chunk_size; j++) {
                add_chunk_request.add_tablet_ids(_tablet_id);
                add_chunk_request.add_partition_ids(partition_id);
            }

            ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
            add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

            load_mgr->add_chunk(add_chunk_request, &add_chunk_response);
            if (add_chunk_response.status().status_code() != TStatusCode::OK) {
                std::cerr << add_chunk_response.status().error_msgs(0) << '\n';
                cancelled = MatchPattern(add_chunk_response.status().error_msgs(0), "*has been closed*");
                break;
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        ASSERT_TRUE(cancelled);
    };

    auto t1 = std::thread(do_write);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    {
        lake::AbortTxnRequest request;
        lake::AbortTxnResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(txn_id);
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }

    t1.join();
}

} // namespace starrocks

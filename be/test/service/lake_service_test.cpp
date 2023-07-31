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

#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "storage/lake/filenames.h"
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

namespace starrocks {

class LakeServiceTest : public testing::Test {
public:
    LakeServiceTest() : _lake_service(ExecEnv::GetInstance()), _tablet_id(next_id()) {
        _location_provider = new lake::FixedLocationProvider(kRootLocation);
        _tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
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

    void create_tablet() const {
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

    void SetUp() override {
        ASSERT_OK(_tablet_mgr->delete_tablet(_tablet_id));
        create_tablet();
    }

    void TearDown() override { (void)_tablet_mgr->delete_tablet(_tablet_id); }

protected:
    constexpr static const char* const kRootLocation = "./lake_service_test";
    LakeServiceImpl _lake_service;
    int64_t _tablet_id;
    lake::TabletManager* _tablet_mgr;
    lake::LocationProvider* _location_provider;
    lake::LocationProvider* _backup_location_provider;
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
TEST_F(LakeServiceTest, test_publish_version_for_write) {
    // Empty TxnLog
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1000);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    // TxnLog with 2 segments
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
    }

    // Publish txn 1000
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(2);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1000);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
    }
    // Publish txn 1001
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
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

    ExecEnv::GetInstance()->vacuum_thread_pool()->wait();
    // TxnLog`s should have been deleted
    ASSERT_TRUE(tablet.get_txn_log(1000).status().is_not_found());
    ASSERT_TRUE(tablet.get_txn_log(1001).status().is_not_found());

    // Send publish version request again.
    {
        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(2);
        request.set_new_version(3);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
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
        request.add_txn_ids(1001);
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
        request.add_txn_ids(1001);
        _lake_service.publish_version(nullptr, &request, &response, nullptr);
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_abort) {
    // Empty TxnLog
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1000);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    // TxnLog with 2 segments
    {
        ASSIGN_OR_ABORT(auto seg1, fs::new_writable_file(_tablet_mgr->segment_location(_tablet_id, "1.dat")));
        ASSIGN_OR_ABORT(auto seg2, fs::new_writable_file(_tablet_mgr->segment_location(_tablet_id, "2.dat")));
        ASSERT_OK(seg1->append("xx"));
        ASSERT_OK(seg2->append("yy"));
        ASSERT_OK(seg1->close());
        ASSERT_OK(seg2->close());

        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1001);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(true);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(101);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(4096);
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("1.dat");
        txnlog.mutable_op_write()->mutable_rowset()->add_segments("2.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    // TxnLog with 2 segments generated by compaction task
    {
        ASSIGN_OR_ABORT(auto seg1, fs::new_writable_file(_tablet_mgr->segment_location(_tablet_id, "3.dat")));
        ASSIGN_OR_ABORT(auto seg2, fs::new_writable_file(_tablet_mgr->segment_location(_tablet_id, "4.dat")));
        ASSERT_OK(seg1->append("xx"));
        ASSERT_OK(seg2->append("yy"));
        ASSERT_OK(seg1->close());
        ASSERT_OK(seg2->close());

        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1002);
        txnlog.mutable_op_compaction()->mutable_output_rowset()->set_overlapped(true);
        txnlog.mutable_op_compaction()->mutable_output_rowset()->set_num_rows(101);
        txnlog.mutable_op_compaction()->mutable_output_rowset()->set_data_size(4096);
        txnlog.mutable_op_compaction()->mutable_output_rowset()->add_segments("3.dat");
        txnlog.mutable_op_compaction()->mutable_output_rowset()->add_segments("4.dat");
        ASSERT_OK(_tablet_mgr->put_txn_log(txnlog));
    }
    // Send AbortTxn request
    {
        lake::AbortTxnRequest request;
        lake::AbortTxnResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1000);
        request.add_txn_ids(1001);
        request.add_txn_ids(1002);
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));

    // TxnLog`s and segments should have been deleted
    ASSERT_TRUE(tablet.get_txn_log(1000).status().is_not_found());
    ASSERT_TRUE(tablet.get_txn_log(1001).status().is_not_found());
    ASSERT_TRUE(tablet.get_txn_log(1002).status().is_not_found());
    ASSERT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, "1.dat")));
    ASSERT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, "2.dat")));
    ASSERT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, "3.dat")));
    ASSERT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_tablet_id, "4.dat")));

    // Send AbortTxn request again
    {
        lake::AbortTxnRequest request;
        lake::AbortTxnResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1000);
        request.add_txn_ids(1001);
        request.add_txn_ids(1002);
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
    ASSERT_FALSE(cntl.Failed());
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
    ASSERT_FALSE(cntl.Failed());
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
        request.set_txn_id(1001);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ("missing version", cntl.ErrorText());
    }
    {
        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(1001);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        _tablet_mgr->prune_metacache();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1001).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1001).status();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_vlog(_tablet_id, 10));
        ASSERT_EQ(_tablet_id, txn_log->tablet_id());
        ASSERT_EQ(1001, txn_log->txn_id());
    }
    // duplicate request
    {
        lake::PublishLogVersionRequest request;
        lake::PublishLogVersionResponse response;
        request.add_tablet_ids(_tablet_id);
        request.set_txn_id(1001);
        request.set_version(10);
        brpc::Controller cntl;
        _lake_service.publish_log_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());

        _tablet_mgr->prune_metacache();
        ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 1001).status().is_not_found())
                << _tablet_mgr->get_txn_log(_tablet_id, 1001).status();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_vlog(_tablet_id, 10));
        ASSERT_EQ(_tablet_id, txn_log->tablet_id());
        ASSERT_EQ(1001, txn_log->txn_id());
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

        lake::PublishVersionRequest request;
        lake::PublishVersionResponse response;
        request.set_base_version(1);
        request.set_new_version(5);
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1001);
        brpc::Controller cntl;
        _lake_service.publish_version(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(0, response.failed_tablets_size());
        ASSERT_TRUE(response.compaction_scores().contains(_tablet_id));
    }

    _tablet_mgr->prune_metacache();
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
}

// NOLINTNEXTLINE
TEST_F(LakeServiceTest, test_lock_unlock_tablet_metadata) {
    ASSERT_OK(FileSystem::Default()->path_exists(kRootLocation));
    lake::LockTabletMetadataRequest lock_request;
    lake::LockTabletMetadataResponse lock_response;

    brpc::Controller cntl;
    _lake_service.lock_tablet_metadata(&cntl, &lock_request, &lock_response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing version", cntl.ErrorText());

    cntl.Reset();
    lock_request.set_tablet_id(_tablet_id);
    lock_request.set_version(1);
    lock_request.set_expire_time(1);
    _lake_service.lock_tablet_metadata(&cntl, &lock_request, &lock_response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    std::string tablet_metadata_lock_path = _location_provider->tablet_metadata_lock_location(_tablet_id, 1, 1);
    ASSERT_OK(FileSystem::Default()->path_exists(tablet_metadata_lock_path));

    cntl.Reset();
    lake::UnlockTabletMetadataRequest unlock_request;
    lake::UnlockTabletMetadataResponse unlock_response;
    _lake_service.unlock_tablet_metadata(&cntl, &unlock_request, &unlock_response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ("missing version", cntl.ErrorText());

    cntl.Reset();
    unlock_request.set_tablet_id(_tablet_id);
    unlock_request.set_expire_time(1);
    unlock_request.set_version(1);
    _lake_service.unlock_tablet_metadata(&cntl, &unlock_request, &unlock_response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    auto st = FileSystem::Default()->path_exists(tablet_metadata_lock_path);
    ASSERT_TRUE(st.is_not_found()) << st;
}

TEST_F(LakeServiceTest, test_abort_compaction) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->LoadDependency(
            {{"CompactionScheduler::compact:return", "LakeServiceImpl::abort_compaction:enter"},
             {"LakeServiceImpl::abort_compaction:aborted", "CompactionScheduler::do_compaction:before_execute_task"}});

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

    SyncPoint::GetInstance()->ClearCallBack("publish_version:delete_txn_log");
    SyncPoint::GetInstance()->DisableProcessing();
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

    ExecEnv::GetInstance()->vacuum_thread_pool()->wait();
    ASSERT_TRUE(_tablet_mgr->get_txn_log(_tablet_id, 102301).status().is_not_found());

    SyncPoint::GetInstance()->ClearCallBack("publish_version:delete_txn_log");
    SyncPoint::GetInstance()->DisableProcessing();
}
} // namespace starrocks

// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "service/service_be/lake_service.h"

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include "runtime/exec_env.h"
#include "storage/lake/group_assigner.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "testutil/assert.h"

namespace starrocks {

class LakeServiceTest : public testing::Test {
public:
    LakeServiceTest() : _lake_service(ExecEnv::GetInstance()), _tablet_id(54321) {
        std::set<std::string> groups;
        auto group_assigner = ExecEnv::GetInstance()->lake_group_assigner();
        (void)group_assigner->list_group(&groups);
        CHECK(!groups.empty());
        _group = *groups.begin();
        _tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    }

    void create_tablet() {
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
        schema->set_id(10);
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        schema->set_compress_kind(COMPRESS_LZ4);
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
        ASSERT_OK(tablet_mgr->put_tablet_metadata(_group, metadata));
    }

    void SetUp() override {
        _tablet_mgr->drop_tablet(_tablet_id);
        create_tablet();
    }

    void TearDown() override { _tablet_mgr->drop_tablet(_tablet_id); }

protected:
    LakeServiceImpl _lake_service;
    int64_t _tablet_id;
    std::string _group;
    lake::TabletManager* _tablet_mgr;
};

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

TEST_F(LakeServiceTest, test_publish_version_for_write) {
    // Empty TxnLog
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1000);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(_group, txnlog));
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
        ASSERT_OK(_tablet_mgr->put_txn_log(_group, txnlog));
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
    }
    // Delete old version metadata then send publish version again
    tablet.delete_metadata(1);
    tablet.delete_metadata(2);
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
}

TEST_F(LakeServiceTest, test_abort) {
    // Empty TxnLog
    {
        lake::TxnLog txnlog;
        txnlog.set_tablet_id(_tablet_id);
        txnlog.set_txn_id(1000);
        txnlog.mutable_op_write()->mutable_rowset()->set_num_rows(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_data_size(0);
        txnlog.mutable_op_write()->mutable_rowset()->set_overlapped(false);
        ASSERT_OK(_tablet_mgr->put_txn_log(_group, txnlog));
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
        ASSERT_OK(_tablet_mgr->put_txn_log(_group, txnlog));
    }
    // Send AbortTxn request
    {
        lake::AbortTxnRequest request;
        lake::AbortTxnResponse response;
        request.add_tablet_ids(_tablet_id);
        request.add_txn_ids(1000);
        request.add_txn_ids(1001);
        _lake_service.abort_txn(nullptr, &request, &response, nullptr);
    }

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_id));

    // TxnLog`s should have been deleted
    ASSERT_TRUE(tablet.get_txn_log(1000).status().is_not_found());
    ASSERT_TRUE(tablet.get_txn_log(1001).status().is_not_found());
}

TEST_F(LakeServiceTest, test_drop_tablet) {
    // 
    {
        brpc::Controller cntl;
        lake::DropTabletRequest request;
        lake::DropTabletResponse response;
        request.add_tablet_ids(_tablet_id);
        _lake_service.drop_tablet(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }
}

} // namespace starrocks

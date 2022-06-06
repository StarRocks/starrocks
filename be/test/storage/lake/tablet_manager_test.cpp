// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include <fmt/format.h>
#include <fs/fs_starlet.h>
#include <gtest/gtest.h>
#include <starlet.h>

#include <fstream>

#include "common/config.h"
#include "gen_cpp/AgentService_types.h"
#include "gutil/strings/join.h"
#include "service/staros_worker.h"
#include "storage/lake/tablet.h"
#include "testutil/assert.h"

namespace starrocks {

extern staros::starlet::Starlet* g_starlet;
extern std::shared_ptr<StarOSWorker> g_worker;

class LakeTabletManagerTest : public testing::Test {
public:
    LakeTabletManagerTest() = default;
    ~LakeTabletManagerTest() override = default;
    void SetUp() override {
        g_worker = std::make_shared<StarOSWorker>();
        _starlet = new staros::starlet::Starlet(g_worker);
        _starlet->init(8888);
        _tabletManager = new starrocks::lake::TabletManager(_starlet, 1024);
        g_starlet = _starlet;
    }
    std::string group_path(std::string_view path) {
        if (path.front() == '/') {
            return fmt::format("s3://{}.s3.{}.{}{}", kBucket, kRegion, kDomain, path);
        } else {
            return fmt::format("s3://{}.s3.{}.{}/{}", kBucket, kRegion, kDomain, path);
        }
    }

    void TearDown() override {
        g_starlet = nullptr;
        g_worker.reset();
        delete _starlet;
        delete _tabletManager;
    }

    starrocks::lake::TabletManager* _tabletManager;

private:
    staros::starlet::Starlet* _starlet;
    constexpr static const char* kBucket = "starlet-test";
    constexpr static const char* kRegion = "oss-cn-hangzhou";
    constexpr static const char* kDomain = "aliyuncs.com";
};

TEST_F(LakeTabletManagerTest, tablet_meta_write_and_read) {
    starrocks::lake::TabletMetadata metadata;
    metadata.set_id(12345);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    auto group = group_path("shard1");
    EXPECT_OK(_tabletManager->put_tablet_metadata(group, metadata));
    auto res = _tabletManager->get_tablet_metadata(group, 12345, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->id(), 12345);
    EXPECT_EQ(res.value()->version(), 2);
    EXPECT_OK(_tabletManager->delete_tablet_metadata(group, 12345, 2));
}

TEST_F(LakeTabletManagerTest, txnlog_write_and_read) {
    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(12345);
    txnLog.set_txn_id(2);
    auto group = group_path("shard1");
    EXPECT_OK(_tabletManager->put_txn_log(group, txnLog));
    auto res = _tabletManager->get_txn_log(group, 12345, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->tablet_id(), 12345);
    EXPECT_EQ(res.value()->txn_id(), 2);
    EXPECT_OK(_tabletManager->delete_txn_log(group, 12345, 2));
}

TEST_F(LakeTabletManagerTest, create_and_drop_tablet) {
    staros::starlet::ShardInfo shard_info;
    shard_info.id = 65535;
    shard_info.obj_store_info.uri = group_path("shard2");
    g_worker->add_shard(shard_info);
    TCreateTabletReq req;
    req.tablet_id = 65535;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.schema_hash = 270068375;
    req.tablet_schema.short_key_column_count = 2;
    EXPECT_OK(_tabletManager->create_tablet(req));
    auto res = _tabletManager->get_tablet(65535);
    EXPECT_TRUE(res.ok());
    EXPECT_OK(_tabletManager->drop_tablet(65535));
}
} // namespace starrocks

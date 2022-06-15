// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include <fmt/format.h>
#include <fs/fs_starlet.h>
#include <gtest/gtest.h>

#include <fstream>

#include "common/config.h"
#include "gen_cpp/AgentService_types.h"
#include "gutil/strings/join.h"
#include "service/staros_worker.h"
#include "storage/lake/group_assigner.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/options.h"
#include "testutil/assert.h"
#include "util/filesystem_util.h"

namespace starrocks {

class LocalGroupAssigner : public lake::GroupAssigner {
public:
    LocalGroupAssigner(const std::string& dir) : _local_dir(dir){};

    StatusOr<std::string> get_group(int64_t tablet_id) override { return _local_dir; };

    Status list_group(std::vector<std::string>* groups) override {
        return Status::NotSupported("LocalGroupAssigner::list_group");
    };

private:
    std::string _local_dir;
};

class LakeTabletManagerTest : public testing::Test {
public:
    LakeTabletManagerTest() = default;
    ~LakeTabletManagerTest() override = default;
    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths);
        _test_dir = paths[0].path + "/lake";
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_test_dir));
        _group_assigner = new LocalGroupAssigner(_test_dir);
        _tabletManager = new starrocks::lake::TabletManager(_group_assigner, 16384);
    }
    std::string tablet_group(std::string_view prefix, int64_t tablet_id) {
        ASSIGN_OR_ABORT(auto group_path, _group_assigner->get_group(tablet_id));
        auto tablet_group = fmt::format("{}/{}", group_path, prefix);
        FileSystemUtil::create_directory(tablet_group);
        return tablet_group;
    }

    void TearDown() override {
        delete _tabletManager;
        delete _group_assigner;
        (void)FileSystem::Default()->delete_dir_recursive(_test_dir);
    }

    starrocks::lake::TabletManager* _tabletManager;

private:
    std::string _test_dir;
    LocalGroupAssigner* _group_assigner;
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
    auto group = tablet_group("shard1", 12345);
    EXPECT_OK(_tabletManager->put_tablet_metadata(group, metadata));
    auto res = _tabletManager->get_tablet_metadata(group, 12345, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->id(), 12345);
    EXPECT_EQ(res.value()->version(), 2);
    EXPECT_OK(_tabletManager->delete_tablet_metadata(group, 12345, 2));
    res = _tabletManager->get_tablet_metadata(group, 12345, 2);
    EXPECT_TRUE(res.status().is_not_found());
}

TEST_F(LakeTabletManagerTest, txnlog_write_and_read) {
    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(12345);
    txnLog.set_txn_id(2);
    auto group = tablet_group("shard1", 12345);
    EXPECT_OK(_tabletManager->put_txn_log(group, txnLog));
    auto res = _tabletManager->get_txn_log(group, 12345, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->tablet_id(), 12345);
    EXPECT_EQ(res.value()->txn_id(), 2);
    EXPECT_OK(_tabletManager->delete_txn_log(group, 12345, 2));
    res = _tabletManager->get_txn_log(group, 12345, 2);
    EXPECT_TRUE(res.status().is_not_found());
}

TEST_F(LakeTabletManagerTest, create_and_drop_tablet) {
    TCreateTabletReq req;
    req.tablet_id = 65535;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.schema_hash = 270068375;
    req.tablet_schema.short_key_column_count = 2;
    EXPECT_OK(_tabletManager->create_tablet(req));
    auto res = _tabletManager->get_tablet(65535);
    EXPECT_TRUE(res.ok());

    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(65535);
    txnLog.set_txn_id(2);
    auto group = res.value().group();
    EXPECT_OK(_tabletManager->put_txn_log(group, txnLog));
    EXPECT_OK(_tabletManager->drop_tablet(65535));

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(group));
    auto st = fs->path_exists(fmt::format("{}/tbl_{:016X}_{:016X}", group, 65535, 1));
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(fmt::format("{}/txn_{:016X}_{:016X}", group, 65535, 2));
    EXPECT_TRUE(st.is_not_found());
}

TEST_F(LakeTabletManagerTest, list_tablet_meta) {
    starrocks::lake::TabletMetadata metadata;
    metadata.set_id(12345);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    auto group = tablet_group("shard1", 12345);
    EXPECT_OK(_tabletManager->put_tablet_metadata(group, metadata));

    metadata.set_version(3);
    EXPECT_OK(_tabletManager->put_tablet_metadata(group, metadata));

    metadata.set_id(23456);
    metadata.set_version(2);
    EXPECT_OK(_tabletManager->put_tablet_metadata(group, metadata));

    ASSIGN_OR_ABORT(auto metaIter, _tabletManager->list_tablet_metadata(group));

    std::vector<std::string> objects;
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto tabletmeta_ptr, metaIter.next());
        objects.emplace_back(fmt::format("tbl_{:016X}_{:016X}", tabletmeta_ptr->id(), tabletmeta_ptr->version()));
    }

    EXPECT_EQ(objects.size(), 3);
    auto iter = std::find(objects.begin(), objects.end(), "tbl_0000000000003039_0000000000000002");
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), "tbl_0000000000003039_0000000000000003");
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), "tbl_0000000000005BA0_0000000000000002");
    EXPECT_TRUE(iter != objects.end());

    ASSIGN_OR_ABORT(metaIter, _tabletManager->list_tablet_metadata(group, 12345));

    objects.clear();
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto tabletmeta_ptr, metaIter.next());
        objects.emplace_back(fmt::format("tbl_{:016X}_{:016X}", tabletmeta_ptr->id(), tabletmeta_ptr->version()));
    }

    EXPECT_EQ(objects.size(), 2);
    iter = std::find(objects.begin(), objects.end(), "tbl_0000000000003039_0000000000000002");
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), "tbl_0000000000003039_0000000000000003");
    EXPECT_TRUE(iter != objects.end());
}

TEST_F(LakeTabletManagerTest, list_txn_log) {
    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(12345);
    txnLog.set_txn_id(2);
    auto group = tablet_group("shard1", 12345);
    EXPECT_OK(_tabletManager->put_txn_log(group, txnLog));

    txnLog.set_txn_id(3);
    EXPECT_OK(_tabletManager->put_txn_log(group, txnLog));

    txnLog.set_tablet_id(23456);
    txnLog.set_txn_id(3);
    EXPECT_OK(_tabletManager->put_txn_log(group, txnLog));

    ASSIGN_OR_ABORT(auto metaIter, _tabletManager->list_txn_log(group));

    std::vector<std::string> txnlogs;
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto txnlog_ptr, metaIter.next());
        txnlogs.emplace_back(fmt::format("txn_{:016X}_{:016X}", txnlog_ptr->tablet_id(), txnlog_ptr->txn_id()));
    }

    EXPECT_EQ(txnlogs.size(), 3);
    auto iter = std::find(txnlogs.begin(), txnlogs.end(), "txn_0000000000003039_0000000000000002");
    EXPECT_TRUE(iter != txnlogs.end());
    iter = std::find(txnlogs.begin(), txnlogs.end(), "txn_0000000000003039_0000000000000003");
    EXPECT_TRUE(iter != txnlogs.end());
    iter = std::find(txnlogs.begin(), txnlogs.end(), "txn_0000000000005BA0_0000000000000003");
    EXPECT_TRUE(iter != txnlogs.end());

    ASSIGN_OR_ABORT(metaIter, _tabletManager->list_txn_log(group, 12345));

    txnlogs.clear();
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto txnlog_ptr, metaIter.next());
        txnlogs.emplace_back(fmt::format("txn_{:016X}_{:016X}", txnlog_ptr->tablet_id(), txnlog_ptr->txn_id()));
    }

    EXPECT_EQ(txnlogs.size(), 2);
    iter = std::find(txnlogs.begin(), txnlogs.end(), "txn_0000000000003039_0000000000000002");
    EXPECT_TRUE(iter != txnlogs.end());
    iter = std::find(txnlogs.begin(), txnlogs.end(), "txn_0000000000003039_0000000000000003");
    EXPECT_TRUE(iter != txnlogs.end());
}

TEST_F(LakeTabletManagerTest, put_get_tabletmetadata_witch_cache_evict) {
    starrocks::lake::TabletMetadata metadata;
    metadata.set_id(23456);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);

    auto group = tablet_group("shard1", 12345);

    // we set meta cache capacity to 16K, and each meta here cost 232 bytes,putting 64 tablet meta will fill up the cache space.
    for (int i = 0; i < 64; ++i) {
        metadata.set_version(2 + i);
        EXPECT_OK(_tabletManager->put_tablet_metadata(group, metadata));
    }

    // get version 4 to update lru list in cache
    {
        metadata.set_version(4);
        auto res = _tabletManager->get_tablet_metadata(group, metadata.id(), metadata.version());
        EXPECT_TRUE(res.ok());
    }

    // put another 32 tablet meta to trigger cache eviction.
    for (int i = 0; i < 32; ++i) {
        metadata.set_version(66 + i);
        EXPECT_OK(_tabletManager->put_tablet_metadata(group, metadata));
    }

    // test eviction result;
    {
        // version 4 expect not evicted
        metadata.set_version(4);
        auto key_path = fmt::format("tbl_{:016X}_{:016X}", metadata.id(), metadata.version());
        auto ptr = _tabletManager->get_from_metacache(key_path);
        EXPECT_TRUE(ptr != nullptr);
        auto res = _tabletManager->get_tablet_metadata(group, metadata.id(), metadata.version());
        EXPECT_TRUE(res.ok());
    }
    {
        // version 2 expect evicted
        metadata.set_version(2);
        auto key_path = fmt::format("tbl_{:016X}_{:016X}", metadata.id(), metadata.version());
        auto ptr = _tabletManager->get_from_metacache(key_path);
        EXPECT_TRUE(ptr == nullptr);
        auto res = _tabletManager->get_tablet_metadata(group, metadata.id(), metadata.version());
        EXPECT_TRUE(res.ok());
    }
    {
        // version 6 expect evicted
        metadata.set_version(6);
        auto key_path = fmt::format("tbl_{:016X}_{:016X}", metadata.id(), metadata.version());
        auto ptr = _tabletManager->get_from_metacache(key_path);
        EXPECT_TRUE(ptr == nullptr);
        auto res = _tabletManager->get_tablet_metadata(group, metadata.id(), metadata.version());
        EXPECT_TRUE(res.ok());
    }
}

} // namespace starrocks

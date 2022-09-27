// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <fstream>

#include "common/config.h"
#include "fs/fs.h"
#include "gen_cpp/AgentService_types.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet.h"
#include "storage/options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "util/filesystem_util.h"
#include "util/lru_cache.h"

namespace starrocks {

class LakeTabletManagerTest : public testing::Test {
public:
    LakeTabletManagerTest() : _tablet_manager(nullptr), _test_dir(), _location_provider(nullptr){};

    ~LakeTabletManagerTest() override = default;

    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths);
        _test_dir = paths[0].path + "/lake";
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_test_dir));
        _location_provider = new lake::FixedLocationProvider(_test_dir);
        _tablet_manager = new starrocks::lake::TabletManager(_location_provider, 16384);
    }

    std::string tablet_root_location(int64_t tablet_id) const {
        auto root = _location_provider->root_location(tablet_id);
        FileSystemUtil::create_directory(root);
        return root;
    }

    void TearDown() override {
        delete _tablet_manager;
        delete _location_provider;
        (void)FileSystem::Default()->delete_dir_recursive(_test_dir);
    }

    starrocks::lake::TabletManager* _tablet_manager;
    std::string _test_dir;
    lake::LocationProvider* _location_provider;
};

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, tablet_meta_write_and_read) {
    starrocks::lake::TabletMetadata metadata;
    metadata.set_id(12345);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    auto root = tablet_root_location(12345);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    auto res = _tablet_manager->get_tablet_metadata(12345, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->id(), 12345);
    EXPECT_EQ(res.value()->version(), 2);
    EXPECT_OK(_tablet_manager->delete_tablet_metadata(12345, 2));
    res = _tablet_manager->get_tablet_metadata(12345, 2);
    EXPECT_TRUE(res.status().is_not_found());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, txnlog_write_and_read) {
    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(12345);
    txnLog.set_txn_id(2);
    auto root = tablet_root_location(12345);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));
    auto res = _tablet_manager->get_txn_log(12345, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->tablet_id(), 12345);
    EXPECT_EQ(res.value()->txn_id(), 2);
    EXPECT_OK(_tablet_manager->delete_txn_log(12345, 2));
    res = _tablet_manager->get_txn_log(12345, 2);
    EXPECT_TRUE(res.status().is_not_found());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_and_drop_tablet) {
    TCreateTabletReq req;
    req.tablet_id = 65535;
    req.__set_version(1);
    req.tablet_schema.schema_hash = 270068375;
    req.tablet_schema.short_key_column_count = 2;
    EXPECT_OK(_tablet_manager->create_tablet(req));
    auto res = _tablet_manager->get_tablet(65535);
    EXPECT_TRUE(res.ok());

    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(65535);
    txnLog.set_txn_id(2);
    auto root = res.value().root_location();
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));
    EXPECT_OK(_tablet_manager->drop_tablet(65535));

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(root));
    auto st = fs->path_exists(fmt::format("{}/tbl_{:016X}_{:016X}", root, 65535, 1));
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(fmt::format("{}/txn_{:016X}_{:016X}", root, 65535, 2));
    EXPECT_TRUE(st.is_not_found());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, list_tablet_meta) {
    starrocks::lake::TabletMetadata metadata;
    metadata.set_id(12345);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    auto root = tablet_root_location(12345);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(3);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_id(23456);
    metadata.set_version(2);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    ASSIGN_OR_ABORT(auto metaIter, _tablet_manager->list_tablet_metadata(23456, false));

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

    ASSIGN_OR_ABORT(metaIter, _tablet_manager->list_tablet_metadata(12345, true));

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

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, list_txn_log) {
    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(12345);
    txnLog.set_txn_id(2);
    auto root = tablet_root_location(12345);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));

    txnLog.set_txn_id(3);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));

    txnLog.set_tablet_id(23456);
    txnLog.set_txn_id(3);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));

    ASSIGN_OR_ABORT(auto metaIter, _tablet_manager->list_txn_log(23456, false));

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

    ASSIGN_OR_ABORT(metaIter, _tablet_manager->list_txn_log(12345, true));

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

// TODO: enable this test.
// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, DISABLED_put_get_tabletmetadata_witch_cache_evict) {
    int64_t tablet_id = 23456;
    std::vector<lake::TabletMetadataPtr> vec;

    auto root = tablet_root_location(tablet_id);

    // we set meta cache capacity to 16K, and each meta here cost 232 bytes,putting 64 tablet meta will fill up the cache space.
    for (int i = 0; i < 64; ++i) {
        auto metadata = std::make_shared<lake::TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(2 + i);
        auto rowset_meta_pb = metadata->add_rowsets();
        rowset_meta_pb->set_id(2);
        rowset_meta_pb->set_overlapped(false);
        rowset_meta_pb->set_data_size(1024);
        rowset_meta_pb->set_num_rows(5);
        EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
        vec.emplace_back(metadata);
    }

    // get version 4 from cache
    {
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 4);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 4);
    }

    // put another 32 tablet meta to trigger cache eviction.
    for (int i = 0; i < 32; ++i) {
        auto metadata = std::make_shared<lake::TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(66 + i);
        auto rowset_meta_pb = metadata->add_rowsets();
        rowset_meta_pb->set_id(2);
        rowset_meta_pb->set_overlapped(false);
        rowset_meta_pb->set_data_size(1024);
        rowset_meta_pb->set_num_rows(5);
        EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    }

    // test eviction result;
    {
        // version 4 expect not evicted
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 4);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 4);
        EXPECT_EQ(res.value().get(), vec[2].get());
    }
    {
        // version 6 expect evicted
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 6);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 6);
        EXPECT_NE(res.value().get(), vec[4].get());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, tablet_schema_load) {
    starrocks::lake::TabletMetadata metadata;
    metadata.set_id(12345);
    metadata.set_version(2);

    auto schema = metadata.mutable_schema();
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
    auto root = tablet_root_location(12345);
    _tablet_manager->put_tablet_metadata(metadata);

    const TabletSchema* ptr = nullptr;

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(12345));
    {
        auto st = tablet.get_schema();
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(st.value()->id(), 10);
        EXPECT_EQ(st.value()->num_columns(), 2);
        EXPECT_EQ(st.value()->column(0).name(), "c0");
        EXPECT_EQ(st.value()->column(1).name(), "c1");
        ptr = st.value().get();
    }
    {
        auto st = tablet.get_schema();
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(st.value()->id(), 10);
        EXPECT_EQ(st.value()->num_columns(), 2);
        EXPECT_EQ(st.value()->column(0).name(), "c0");
        EXPECT_EQ(st.value()->column(1).name(), "c1");
        EXPECT_EQ(ptr, st.value().get());
    }
}

} // namespace starrocks

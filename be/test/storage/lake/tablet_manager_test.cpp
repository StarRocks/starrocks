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

#include "storage/lake/tablet_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/update_manager.h"
#include "storage/options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/filesystem_util.h"

// NOTE: intend to put the following header to the end of the include section
// so that our `gutil/dynamic_annotations.h` takes precedence of the absl's.
// NOLINTNEXTLINE
#include "service/staros_worker.h"

namespace starrocks {

class LakeTabletManagerTest : public testing::Test {
public:
    LakeTabletManagerTest() : _test_dir(){};

    ~LakeTabletManagerTest() override = default;

    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/lake";
        _location_provider = new lake::FixedLocationProvider(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = new starrocks::lake::TabletManager(_location_provider, _update_manager.get(), 16384);
    }

    void TearDown() override {
        delete _tablet_manager;
        delete _location_provider;
        (void)FileSystem::Default()->delete_dir_recursive(_test_dir);
    }

    starrocks::lake::TabletManager* _tablet_manager{nullptr};
    std::string _test_dir;
    lake::LocationProvider* _location_provider{nullptr};
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
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
TEST_F(LakeTabletManagerTest, create_and_delete_tablet) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    EXPECT_TRUE(_tablet_manager->get_tablet(tablet_id).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());

    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(tablet_id);
    txnLog.set_txn_id(2);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));
    EXPECT_OK(_tablet_manager->delete_tablet(tablet_id));

    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).is_not_found());
    EXPECT_TRUE(fs->path_exists(_location_provider->txn_log_location(tablet_id, 2)).is_not_found());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_tablet_without_schema_file) {
    auto fs = FileSystem::Default();

    for (auto create_schema_file : {false, true}) {
        auto tablet_id = next_id();
        auto schema_id = next_id();

        TCreateTabletReq req;
        req.tablet_id = tablet_id;
        req.__set_version(1);
        req.__set_version_hash(0);
        req.tablet_schema.__set_id(schema_id);
        req.tablet_schema.__set_schema_hash(270068375);
        req.tablet_schema.__set_short_key_column_count(2);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
        req.__set_create_schema_file(create_schema_file);
        EXPECT_OK(_tablet_manager->create_tablet(req));
        EXPECT_TRUE(_tablet_manager->get_tablet(tablet_id).ok());
        EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
        auto st = fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id));
        if (create_schema_file) {
            EXPECT_TRUE(st.ok()) << st;
        } else {
            EXPECT_TRUE(st.is_not_found()) << st;
        }
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_and_delete_pk_tablet) {
    TCreateTabletReq req;
    req.tablet_id = 65535;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(next_id());
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::PRIMARY_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    auto res = _tablet_manager->get_tablet(65535);
    EXPECT_TRUE(res.ok());

    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(65535);
    txnLog.set_txn_id(2);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));
    EXPECT_OK(_tablet_manager->delete_tablet(65535));

    auto st = FileSystem::Default()->path_exists(_location_provider->tablet_metadata_location(65535, 1));
    EXPECT_TRUE(st.is_not_found());
    st = FileSystem::Default()->path_exists(_location_provider->tablet_metadata_location(65535, 2));
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
        objects.emplace_back(fmt::format("{:016X}_{:016X}.meta", tabletmeta_ptr->id(), tabletmeta_ptr->version()));
    }

    EXPECT_EQ(objects.size(), 3);
    auto iter = std::find(objects.begin(), objects.end(), "0000000000003039_0000000000000002.meta");
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), "0000000000003039_0000000000000003.meta");
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), "0000000000005BA0_0000000000000002.meta");
    EXPECT_TRUE(iter != objects.end());

    ASSIGN_OR_ABORT(metaIter, _tablet_manager->list_tablet_metadata(12345, true));

    objects.clear();
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto tabletmeta_ptr, metaIter.next());
        objects.emplace_back(fmt::format("{:016X}_{:016X}.meta", tabletmeta_ptr->id(), tabletmeta_ptr->version()));
    }

    EXPECT_EQ(objects.size(), 2);
    iter = std::find(objects.begin(), objects.end(), "0000000000003039_0000000000000002.meta");
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), "0000000000003039_0000000000000003.meta");
    EXPECT_TRUE(iter != objects.end());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, list_txn_log) {
    starrocks::lake::TxnLog txnLog;
    txnLog.set_tablet_id(12345);
    txnLog.set_txn_id(2);
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
        txnlogs.emplace_back(fmt::format("{:016X}_{:016X}.log", txnlog_ptr->tablet_id(), txnlog_ptr->txn_id()));
    }

    EXPECT_EQ(txnlogs.size(), 3);
    auto iter = std::find(txnlogs.begin(), txnlogs.end(), "0000000000003039_0000000000000002.log");
    EXPECT_TRUE(iter != txnlogs.end());
    iter = std::find(txnlogs.begin(), txnlogs.end(), "0000000000003039_0000000000000003.log");
    EXPECT_TRUE(iter != txnlogs.end());
    iter = std::find(txnlogs.begin(), txnlogs.end(), "0000000000005BA0_0000000000000003.log");
    EXPECT_TRUE(iter != txnlogs.end());

    ASSIGN_OR_ABORT(metaIter, _tablet_manager->list_txn_log(12345, true));

    txnlogs.clear();
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto txnlog_ptr, metaIter.next());
        txnlogs.emplace_back(fmt::format("{:016X}_{:016X}.log", txnlog_ptr->tablet_id(), txnlog_ptr->txn_id()));
    }

    EXPECT_EQ(txnlogs.size(), 2);
    iter = std::find(txnlogs.begin(), txnlogs.end(), "0000000000003039_0000000000000002.log");
    EXPECT_TRUE(iter != txnlogs.end());
    iter = std::find(txnlogs.begin(), txnlogs.end(), "0000000000003039_0000000000000003.log");
    EXPECT_TRUE(iter != txnlogs.end());
}

// TODO: enable this test.
// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, DISABLED_put_get_tabletmetadata_witch_cache_evict) {
    int64_t tablet_id = 23456;
    std::vector<lake::TabletMetadataPtr> vec;

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
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));

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

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_from_base_tablet) {
    // Create base tablet:
    //  - c0 BIGINT KEY
    //  - c1 INT
    //  - c2 FLOAT
    {
        TCreateTabletReq req;
        req.tablet_id = 65535;
        req.__set_version(1);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c1 = req.tablet_schema.columns.emplace_back();
        c1.column_name = "c1";
        c1.is_key = false;
        c1.column_type.type = TPrimitiveType::INT;
        c1.is_allow_null = false;
        c1.default_value = "10";

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65535));
        ASSIGN_OR_ABORT(auto schema, tablet.get_schema());
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->next_column_unique_id());
    }
    // Add a new column "c3" based on tablet 65535
    {
        TCreateTabletReq req;
        req.tablet_id = 65536;
        req.__set_version(1);
        req.__set_base_tablet_id(65535);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c3 = req.tablet_schema.columns.emplace_back();
        c3.column_name = "c3";
        c3.is_key = false;
        c3.column_type.type = TPrimitiveType::DOUBLE;
        c3.is_allow_null = false;

        auto& c1 = req.tablet_schema.columns.emplace_back();
        c1.column_name = "c1";
        c1.is_key = false;
        c1.column_type.type = TPrimitiveType::INT;
        c1.is_allow_null = false;
        c1.default_value = "10";

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65536));
        ASSIGN_OR_ABORT(auto schema, tablet.get_schema());
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->column(3).unique_id());
        ASSERT_EQ(4, schema->next_column_unique_id());

        ASSERT_EQ("c0", schema->column(0).name());
        ASSERT_EQ("c3", schema->column(1).name());
        ASSERT_EQ("c1", schema->column(2).name());
        ASSERT_EQ("c2", schema->column(3).name());
    }
    // Drop column "c1" based on tablet 65536
    {
        TCreateTabletReq req;
        req.tablet_id = 65537;
        req.__set_version(1);
        req.__set_base_tablet_id(65536);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c3 = req.tablet_schema.columns.emplace_back();
        c3.column_name = "c3";
        c3.is_key = false;
        c3.column_type.type = TPrimitiveType::DOUBLE;
        c3.is_allow_null = false;

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65537));
        ASSIGN_OR_ABORT(auto schema, tablet.get_schema());
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->next_column_unique_id());

        ASSERT_EQ("c0", schema->column(0).name());
        ASSERT_EQ("c3", schema->column(1).name());
        ASSERT_EQ("c2", schema->column(2).name());
    }
}

namespace {
class PartitionedLocationProvider : public lake::LocationProvider {
public:
    PartitionedLocationProvider(std::string root_dir, int num_partition)
            : _root_dir(std::move(root_dir)), _num_partition(num_partition) {
        for (int i = 0; i < _num_partition; i++) {
            auto dir = lake::join_path(_root_dir, std::to_string(i));
            CHECK_OK(fs::create_directories(lake::join_path(dir, lake::kMetadataDirectoryName)));
        }
    }

    std::string root_location(int64_t tablet_id) const override {
        return lake::join_path(_root_dir, std::to_string(tablet_id % _num_partition));
    }

private:
    std::string _root_dir;
    const int _num_partition;
};

TCreateTabletReq build_create_tablet_request(int64_t tablet_id, int64_t index_id) {
    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.tablet_schema.__set_id(index_id);
    req.tablet_schema.__set_schema_hash(0);
    req.tablet_schema.__set_short_key_column_count(1);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    auto& c0 = req.tablet_schema.columns.emplace_back();
    c0.column_name = "c0";
    c0.is_key = true;
    c0.column_type.type = TPrimitiveType::BIGINT;
    c0.is_allow_null = false;

    return req;
}

} // namespace

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, test_multi_partition_schema_file) {
    const static int kNumPartition = 4;
    const static int64_t kIndexId = 123454321;
    auto lp = std::make_unique<PartitionedLocationProvider>(_test_dir, kNumPartition);
    _tablet_manager->TEST_set_location_provider(lp.get());
    for (int i = 0; i < 10; i++) {
        auto req = build_create_tablet_request(next_id(), kIndexId);
        ASSERT_OK(_tablet_manager->create_tablet(req));
    }
    for (int i = 0; i < kNumPartition; i++) {
        auto partition_dir = lake::join_path(_test_dir, std::to_string(i));
        auto schema_file_path = lake::join_path(partition_dir, lake::schema_filename(kIndexId));
        EXPECT_TRUE(fs::path_exist(schema_file_path)) << schema_file_path;
    }
}

#ifdef USE_STAROS
class MockStarOSWorker : public StarOSWorker {
public:
    MOCK_METHOD((absl::StatusOr<staros::starlet::ShardInfo>), _fetch_shard_info_from_remote,
                (staros::starlet::ShardId id));
};

TEST_F(LakeTabletManagerTest, tablet_schema_load_from_remote) {
    int64_t tablet_id = 12345;
    int64_t schema_id = 10086;

    TabletSchemaPB schema_pb;
    schema_pb.set_id(10);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    {
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto c1 = schema_pb.add_column();
    {
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
    }
    // prepare and set schema info in global_schema_cache
    auto schema_ptr = TabletSchema::create(schema_pb);
    _tablet_manager->TEST_set_global_schema_cache(schema_id, schema_ptr);

    // prepare fake shard_info returned by mocked `_fetch_shard_info_from_remote()`
    staros::starlet::ShardInfo shard_info;
    shard_info.id = tablet_id;
    shard_info.properties.emplace("indexId", std::to_string(schema_id));

    // preserve original g_worker value, and reset it to our MockedWorker
    std::shared_ptr<StarOSWorker> origin_worker = g_worker;
    g_worker.reset(new MockStarOSWorker());
    DeferOp op([origin_worker] { g_worker = origin_worker; });

    // set mock function excepted call
    MockStarOSWorker* worker = dynamic_cast<MockStarOSWorker*>(g_worker.get());
    EXPECT_CALL(*worker, _fetch_shard_info_from_remote(tablet_id)).WillOnce(::testing::Return(shard_info));

    // fire the testing
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    auto st = tablet.get_schema();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(st.value()->id(), 10);
    EXPECT_EQ(st.value()->num_columns(), 2);
    EXPECT_EQ(st.value()->column(0).name(), "c0");
    EXPECT_EQ(st.value()->column(1).name(), "c1");
}
#endif // USE_STAROS

} // namespace starrocks

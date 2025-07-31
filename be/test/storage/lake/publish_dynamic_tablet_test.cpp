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

#include "storage/lake/publish_dynamic_tablet.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/filesystem_util.h"

namespace starrocks {

class LakePublishDynamicTabletTest : public testing::Test {
public:
    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/lake";
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
    }

    void TearDown() override {
        auto status = fs::remove_all(config::storage_root_path);
        EXPECT_TRUE(status.ok() || status.is_not_found()) << status;
    }

protected:
    std::unique_ptr<starrocks::lake::TabletManager> _tablet_manager;
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
};

TEST_F(LakePublishDynamicTabletTest, test_publish_splitting_tablet) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->add_segments("test.dat");
    rowset_meta_pb->add_del_files()->set_name("test.del");
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);

    FileMetaPB file_meta;
    file_meta.set_name("test.delvec");
    metadata.mutable_delvec_meta()->mutable_version_to_file()->insert({2, file_meta});

    DeltaColumnGroupVerPB dcg;
    dcg.add_column_files("test.dcg");
    metadata.mutable_dcg_meta()->mutable_dcgs()->insert({2, dcg});

    metadata.mutable_sstable_meta()->add_sstables()->set_filename("test.sst");

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    SplittingTabletInfoPB splitting_tablet;
    splitting_tablet.set_old_tablet_id(tablet_id);
    splitting_tablet.add_new_tablet_ids(next_id());
    splitting_tablet.add_new_tablet_ids(next_id());

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::vector<std::string> distribution_columns = {std::string("test_key")};
    auto res = lake::publish_splitting_tablet(_tablet_manager.get(), std::span(distribution_columns), splitting_tablet,
                                              metadata.version(), metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(3, res.value().size());

    IdenticalTabletInfoPB identical_tablet;
    identical_tablet.set_old_tablet_id(tablet_id);
    identical_tablet.set_new_tablet_id(next_id());
    res = lake::publish_identical_tablet(_tablet_manager.get(), identical_tablet, metadata.version(),
                                         metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(2, res.value().size());

    res = lake::publish_splitting_tablet(_tablet_manager.get(), std::span(distribution_columns), splitting_tablet,
                                         metadata.version(), metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(3, res.value().size());

    res = lake::publish_identical_tablet(_tablet_manager.get(), identical_tablet, metadata.version(),
                                         metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(2, res.value().size());

    _tablet_manager->prune_metacache();

    res = lake::publish_splitting_tablet(_tablet_manager.get(), std::span(distribution_columns), splitting_tablet,
                                         metadata.version(), metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(3, res.value().size());

    res = lake::publish_identical_tablet(_tablet_manager.get(), identical_tablet, metadata.version(),
                                         metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(2, res.value().size());

    EXPECT_OK(_tablet_manager->delete_tablet_metadata(metadata.id(), metadata.version()));

    res = lake::publish_splitting_tablet(_tablet_manager.get(), std::span(distribution_columns), splitting_tablet,
                                         metadata.version(), metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(3, res.value().size());

    res = lake::publish_identical_tablet(_tablet_manager.get(), identical_tablet, metadata.version(),
                                         metadata.version() + 1, txn_info, false);
    EXPECT_OK(res.status());
    EXPECT_EQ(2, res.value().size());
}

} // namespace starrocks

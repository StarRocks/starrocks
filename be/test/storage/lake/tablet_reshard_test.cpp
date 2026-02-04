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

#include "storage/lake/tablet_reshard.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/variant_tuple.h"
#include "util/filesystem_util.h"

namespace starrocks {

class LakeTabletReshardTest : public testing::Test {
public:
    static TuplePB generate_sort_key(int value) {
        DatumVariant variant(get_type_info(LogicalType::TYPE_INT), Datum(value));
        VariantTuple tuple;
        tuple.append(variant);
        TuplePB tuple_pb;
        tuple.to_proto(&tuple_pb);
        return tuple_pb;
    }

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

TEST_F(LakeTabletReshardTest, test_tablet_splitting) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->add_segments("test.dat");
    rowset_meta_pb->add_segment_size(1024);
    auto* segment_meta = rowset_meta_pb->add_segment_metas();
    segment_meta->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta->mutable_sort_key_max()->CopyFrom(generate_sort_key(100));
    segment_meta->set_num_rows(5);
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

    ReshardingTabletInfoPB resharding_tablet_for_splitting;
    auto& splitting_tablet = *resharding_tablet_for_splitting.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(tablet_id);
    splitting_tablet.add_new_tablet_ids(next_id());
    splitting_tablet.add_new_tablet_ids(next_id());

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res =
            lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                            metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    ReshardingTabletInfoPB resharding_tablet_for_identical;
    auto& identical_tablet = *resharding_tablet_for_identical.mutable_identical_tablet_info();
    identical_tablet.set_old_tablet_id(tablet_id);
    identical_tablet.set_new_tablet_id(next_id());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());

    _tablet_manager->prune_metacache();

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());

    EXPECT_OK(_tablet_manager->delete_tablet_metadata(metadata.id(), metadata.version()));

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());
}

} // namespace starrocks

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

#include "storage/lake/meta_file.h"

#include <gtest/gtest.h>

#include <ctime>
#include <set>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/del_vector.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/uid_util.h"

namespace starrocks::lake {

class TestLocationProvider : public LocationProvider {
public:
    explicit TestLocationProvider(LocationProvider* lp) : _lp(lp) {}

    std::string root_location(int64_t tablet_id) const override {
        if (_owned_shards.count(tablet_id) > 0) {
            return _lp->root_location(tablet_id);
        } else {
            return "/path/to/nonexist/directory/";
        }
    }

    Status list_root_locations(std::set<std::string>* roots) const override { return _lp->list_root_locations(roots); }

    std::set<int64_t> _owned_shards;
    LocationProvider* _lp;
};

class MetaFileTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        CHECK_OK(fs::create_directories(join_path(kTestDir, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kSegmentDirectoryName)));

        s_location_provider = std::make_unique<FixedLocationProvider>(kTestDir);
        s_update_manager = std::make_unique<lake::UpdateManager>(s_location_provider.get());
        s_tablet_manager =
                std::make_unique<lake::TabletManager>(s_location_provider.get(), s_update_manager.get(), 1638400000);
    }

    static void TearDownTestCase() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_meta_test";
    inline static std::unique_ptr<lake::LocationProvider> s_location_provider;
    inline static std::unique_ptr<TabletManager> s_tablet_manager;
    inline static std::unique_ptr<UpdateManager> s_update_manager;
};

TEST_F(MetaFileTest, test_meta_rw) {
    // 1. generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(s_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    // 2. write to pk meta file
    MetaFileBuilder builder(*tablet, metadata);
    Status st = builder.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 3. read meta from meta file
    MetaFileReader reader(s_tablet_manager->tablet_metadata_location(tablet_id, 10), false);
    EXPECT_TRUE(reader.load().ok());
    auto meta_st = reader.get_meta();
    EXPECT_TRUE(meta_st.ok());
    EXPECT_EQ((*meta_st)->id(), tablet_id);
    EXPECT_EQ((*meta_st)->version(), 10);
}

TEST_F(MetaFileTest, test_delvec_rw) {
    // 1. generate metadata
    const int64_t tablet_id = 10002;
    const uint32_t segment_id = 1234;
    const int64_t version = 11;
    const int64_t version2 = 12;
    auto tablet = std::make_shared<Tablet>(s_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // 2. write pk meta & delvec
    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    EXPECT_TRUE(dv.empty());

    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 3, 5, 7, 90000};
    dv.add_dels_as_new_version(dels, version, &ndv);
    EXPECT_FALSE(ndv->empty());
    std::string before_delvec = ndv->save();
    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 3. read delvec
    MetaFileReader reader(s_tablet_manager->tablet_metadata_location(tablet_id, version), false);
    EXPECT_TRUE(reader.load().ok());
    DelVector after_delvec;
    EXPECT_TRUE(reader.get_del_vec(s_tablet_manager.get(), segment_id, &after_delvec).ok());
    EXPECT_EQ(before_delvec, after_delvec.save());

    // 4. read meta
    MetaFileReader reader2(s_tablet_manager->tablet_metadata_location(tablet_id, version), false);
    EXPECT_TRUE(reader2.load().ok());
    auto meta_st = reader2.get_meta();
    EXPECT_TRUE(meta_st.ok());
    auto iter = (*meta_st)->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != (*meta_st)->delvec_meta().delvecs().end());
    auto delvec_pagepb = iter->second;
    EXPECT_EQ(delvec_pagepb.version(), version);

    // 5. update delvec
    metadata->set_version(version2);
    MetaFileBuilder builder2(*tablet, metadata);
    DelVector dv2;
    dv2.set_empty();
    EXPECT_TRUE(dv2.empty());
    std::shared_ptr<DelVector> ndv2;
    std::vector<uint32_t> dels2 = {1, 3, 5, 9, 90000};
    dv2.add_dels_as_new_version(dels2, version2, &ndv2);
    builder2.append_delvec(ndv2, segment_id);
    st = builder2.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 6. read again
    MetaFileReader reader3(s_tablet_manager->tablet_metadata_location(tablet_id, version2), false);
    EXPECT_TRUE(reader3.load().ok());
    meta_st = reader3.get_meta();
    EXPECT_TRUE(meta_st.ok());

    iter = (*meta_st)->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != (*meta_st)->delvec_meta().delvecs().end());
    auto delvecpb = iter->second;
    EXPECT_EQ(delvecpb.version(), version2);

    // 7. test reclaim delvec version to file name record
    MetaFileReader reader4(s_tablet_manager->tablet_metadata_location(tablet_id, version2), false);
    EXPECT_TRUE(reader4.load().ok());
    meta_st = reader4.get_meta();
    EXPECT_TRUE(meta_st.ok());

    // clear all delvec meta element so that all element in
    // version_to_file map will also be removed
    // in this case, delvecs meta map has only one element [key=(segment=1234, value=(version=12, offset=0, size=35)]
    // delvec_to_file has also one element [key=(version=12), value=(delvec_file=xxx)]
    // after clearing,  delvecs meta map will have nothing, and element in delvec_to_file will also be useless
    (*meta_st)->mutable_delvec_meta()->mutable_delvecs()->clear();

    // insert a new delvec record into delvecs meta map with new version 13
    // we expect the old element in delvec_to_file map (version 12) will be removed
    auto new_version = version2 + 1;
    MetaFileBuilder builder3(*tablet, *meta_st);
    (*meta_st)->set_version(new_version);
    DelVector dv3;
    dv3.set_empty();
    EXPECT_TRUE(dv3.empty());
    std::shared_ptr<DelVector> ndv3;
    std::vector<uint32_t> dels3 = {1, 3, 5, 9, 90000};
    dv3.add_dels_as_new_version(dels3, new_version, &ndv3);
    builder3.append_delvec(ndv3, segment_id + 1);
    st = builder3.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // validate delvec file record with version 12 been removed
    MetaFileReader reader5(s_tablet_manager->tablet_metadata_location(tablet_id, new_version), false);
    EXPECT_TRUE(reader5.load().ok());
    auto version_to_file_map = (*meta_st)->delvec_meta().version_to_file();
    EXPECT_EQ(version_to_file_map.size(), 1);

    auto iter2 = version_to_file_map.find(version2);
    EXPECT_TRUE(iter2 == version_to_file_map.end());

    iter2 = version_to_file_map.find(new_version);
    EXPECT_TRUE(iter2 != version_to_file_map.end());
}

TEST_F(MetaFileTest, test_delvec_read_meta_cache) {
    // 1. generate metadata
    const int64_t tablet_id = 10003;
    const uint32_t segment_id = 1234;
    const int64_t version = 11;
    auto tablet = std::make_shared<Tablet>(s_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // 2. write pk meta & delvec
    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    EXPECT_TRUE(dv.empty());

    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 3, 5, 7, 90000};
    dv.add_dels_as_new_version(dels, version, &ndv);
    EXPECT_FALSE(ndv->empty());
    std::string before_delvec = ndv->save();
    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 3. read delvec
    MetaFileReader reader(s_tablet_manager->tablet_metadata_location(tablet_id, version), false);
    auto tablet_meta_ptr =
            s_tablet_manager->lookup_tablet_metadata(s_tablet_manager->tablet_metadata_location(tablet_id, version));
    EXPECT_TRUE(tablet_meta_ptr != nullptr);
    EXPECT_EQ(tablet_meta_ptr->id(), tablet_id);
    // call load_by cache for test
    EXPECT_TRUE(
            reader.load_by_cache(s_tablet_manager->tablet_metadata_location(tablet_id, version), s_tablet_manager.get())
                    .ok());
    DelVector after_delvec;
    EXPECT_TRUE(reader.get_del_vec(s_tablet_manager.get(), segment_id, &after_delvec).ok());
    EXPECT_EQ(before_delvec, after_delvec.save());
}

TEST_F(MetaFileTest, test_delvec_read_loop) {
    // 1. generate metadata
    const int64_t tablet_id = 10002;
    const int64_t version = 11;
    auto tablet = std::make_shared<Tablet>(s_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // 2. test delvec
    auto test_delvec = [&](uint32_t segment_id) {
        MetaFileBuilder builder(*tablet, metadata);
        DelVector dv;
        dv.set_empty();
        EXPECT_TRUE(dv.empty());

        std::shared_ptr<DelVector> ndv;
        std::vector<uint32_t> dels;
        for (int i = 0; i < 10; i++) {
            dels.push_back(rand() % 1000);
        }
        dv.add_dels_as_new_version(dels, version, &ndv);
        EXPECT_FALSE(ndv->empty());
        std::string before_delvec = ndv->save();
        builder.append_delvec(ndv, segment_id);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());

        // 3. read delvec
        MetaFileReader reader(s_tablet_manager->tablet_metadata_location(tablet_id, version), false);
        EXPECT_TRUE(reader.load().ok());
        DelVector after_delvec;
        EXPECT_TRUE(reader.get_del_vec(s_tablet_manager.get(), segment_id, &after_delvec).ok());
        EXPECT_EQ(before_delvec, after_delvec.save());
    };
    for (uint32_t segment_id = 1000; segment_id < 1200; segment_id++) {
        test_delvec(segment_id);
    }
    // test twice
    for (uint32_t segment_id = 1000; segment_id < 1200; segment_id++) {
        test_delvec(segment_id);
    }
}

} // namespace starrocks::lake

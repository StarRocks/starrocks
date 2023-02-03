// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/meta_file.h"

#include <gtest/gtest.h>

#include <ctime>
#include <set>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/del_vector.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/gc.h"
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
        s_tablet_manager = std::make_unique<lake::TabletManager>(s_location_provider.get(), nullptr, 16384);
        s_update_manager = std::make_unique<lake::UpdateManager>(s_location_provider.get());
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
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    // 2. write to pk meta file
    MetaFileBuilder builder(metadata, s_update_manager.get());
    Status st = builder.finalize(s_location_provider.get());
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
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    // 2. write pk meta & delvec
    MetaFileBuilder builder(metadata, s_update_manager.get());
    DelVector dv;
    dv.set_empty();
    EXPECT_TRUE(dv.empty());
    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 3, 5, 7, 90000};
    dv.add_dels_as_new_version(dels, version, &ndv);
    EXPECT_FALSE(ndv->empty());
    std::string before_delvec = ndv->save();
    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(s_location_provider.get());
    EXPECT_TRUE(st.ok());
    // 3. read delvec
    MetaFileReader reader(s_tablet_manager->tablet_metadata_location(tablet_id, version), false);
    EXPECT_TRUE(reader.load().ok());
    DelVector after_delvec;
    int64_t latest_version;
    EXPECT_TRUE(reader.get_del_vec(s_tablet_manager.get(), segment_id, &after_delvec, &latest_version).ok());
    EXPECT_EQ(before_delvec, after_delvec.save());
    EXPECT_EQ(version, latest_version);
    // 4. read meta
    MetaFileReader reader2(s_tablet_manager->tablet_metadata_location(tablet_id, version), false);
    EXPECT_TRUE(reader2.load().ok());
    auto meta_st = reader2.get_meta();
    EXPECT_TRUE(meta_st.ok());
    DelvecPagePB delvec_pagepb = (*meta_st)->delvec_meta().delvecs()[segment_id];
    EXPECT_EQ(delvec_pagepb.version(), version);
    // 5. update delvec
    metadata->set_version(version2);
    MetaFileBuilder builder2(metadata, s_update_manager.get());
    DelVector dv2;
    dv2.set_empty();
    EXPECT_TRUE(dv2.empty());
    std::shared_ptr<DelVector> ndv2;
    std::vector<uint32_t> dels2 = {1, 3, 5, 9, 90000};
    dv2.add_dels_as_new_version(dels2, version2, &ndv2);
    builder2.append_delvec(ndv2, segment_id);
    st = builder2.finalize(s_location_provider.get());
    EXPECT_TRUE(st.ok());
    // 6. read again
    MetaFileReader reader3(s_tablet_manager->tablet_metadata_location(tablet_id, version2), false);
    EXPECT_TRUE(reader3.load().ok());
    meta_st = reader3.get_meta();
    EXPECT_TRUE(meta_st.ok());
    delvecpb = (*meta_st)->delvec_meta().delvecs(0);
    EXPECT_EQ(delvecpb.segment_id(), segment_id);
    EXPECT_EQ(delvecpb.page().version(), version2);
}

} // namespace starrocks::lake

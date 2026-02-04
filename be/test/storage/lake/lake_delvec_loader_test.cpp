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

#include "storage/lake/lake_delvec_loader.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "fs/fs_util.h"
#include "storage/del_vector.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"

namespace starrocks::lake {

class LakeDelvecLoaderTest : public ::testing::Test {
public:
    void SetUp() override {
        CHECK_OK(fs::create_directories(join_path(kTestDir, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kSegmentDirectoryName)));

        _location_provider = std::make_shared<FixedLocationProvider>(kTestDir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1638400000);
    }

    void TearDown() override { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_delvec_loader_test";
    std::shared_ptr<LocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<UpdateManager> _update_manager;
};

// Test load_from_meta with empty pdelvec pointer
TEST_F(LakeDelvecLoaderTest, test_load_from_meta_with_empty_pdelvec) {
    const int64_t tablet_id = 10001;
    const uint32_t segment_id = 100;
    const int64_t version = 10;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);

    // 1. Create metadata and write delvec
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 2, 3, 5, 8, 13};
    dv.add_dels_as_new_version(dels, version, &ndv);
    std::string expected_delvec = ndv->save();

    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    ASSERT_TRUE(st.ok());

    // 2. Read metadata and get delvec page
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version));
    auto iter = metadata2->delvec_meta().delvecs().find(segment_id);
    ASSERT_TRUE(iter != metadata2->delvec_meta().delvecs().end());
    DelvecPagePB delvec_page = iter->second;

    // 3. Test load_from_meta with empty pdelvec pointer
    DelVectorPtr pdelvec;  // This is an empty shared_ptr
    ASSERT_FALSE(pdelvec); // Verify it's null

    LakeIOOptions lake_io_opts;
    LakeDelvecLoader loader(_tablet_manager.get(), nullptr, true, lake_io_opts);

    // This should not crash even when pdelvec is empty, because the fix initializes it
    st = loader.load_from_meta(metadata2, delvec_page, &pdelvec);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(pdelvec != nullptr); // Should be initialized now
    EXPECT_EQ(expected_delvec, pdelvec->save());
}

// Test load_from_meta with normal usage
TEST_F(LakeDelvecLoaderTest, test_load_from_meta_normal) {
    const int64_t tablet_id = 10002;
    const uint32_t segment_id = 200;
    const int64_t version = 11;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);

    // 1. Create metadata and write delvec
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {10, 20, 30, 40, 50};
    dv.add_dels_as_new_version(dels, version, &ndv);
    std::string expected_delvec = ndv->save();

    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    ASSERT_TRUE(st.ok());

    // 2. Read metadata and get delvec page
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version));
    auto iter = metadata2->delvec_meta().delvecs().find(segment_id);
    ASSERT_TRUE(iter != metadata2->delvec_meta().delvecs().end());
    DelvecPagePB delvec_page = iter->second;

    // 3. Test load_from_meta
    DelVectorPtr pdelvec;
    LakeIOOptions lake_io_opts;
    LakeDelvecLoader loader(_tablet_manager.get(), nullptr, true, lake_io_opts);

    st = loader.load_from_meta(metadata2, delvec_page, &pdelvec);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(pdelvec != nullptr);
    EXPECT_EQ(expected_delvec, pdelvec->save());
}

// Test load_from_file with empty pdelvec pointer
TEST_F(LakeDelvecLoaderTest, test_load_from_file_with_empty_pdelvec) {
    const int64_t tablet_id = 10003;
    const uint32_t segment_id = 300;
    const int64_t version = 12;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);

    // 1. Create metadata and write delvec
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {100, 200, 300};
    dv.add_dels_as_new_version(dels, version, &ndv);
    std::string expected_delvec = ndv->save();

    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    ASSERT_TRUE(st.ok());

    // 2. Test load_from_file with empty pdelvec
    DelVectorPtr pdelvec; // Empty shared_ptr
    ASSERT_FALSE(pdelvec);

    TabletSegmentId tsid;
    tsid.tablet_id = tablet_id;
    tsid.segment_id = segment_id;

    LakeIOOptions lake_io_opts;
    LakeDelvecLoader loader(_tablet_manager.get(), nullptr, true, lake_io_opts);

    st = loader.load_from_file(tsid, version, &pdelvec);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(pdelvec != nullptr);
    EXPECT_EQ(expected_delvec, pdelvec->save());
}

// Test load_from_file with normal usage
TEST_F(LakeDelvecLoaderTest, test_load_from_file_normal) {
    const int64_t tablet_id = 10004;
    const uint32_t segment_id = 400;
    const int64_t version = 13;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);

    // 1. Create metadata and write delvec
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 3, 5, 7, 9, 11};
    dv.add_dels_as_new_version(dels, version, &ndv);
    std::string expected_delvec = ndv->save();

    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    ASSERT_TRUE(st.ok());

    // 2. Test load_from_file
    DelVectorPtr pdelvec;
    TabletSegmentId tsid;
    tsid.tablet_id = tablet_id;
    tsid.segment_id = segment_id;

    LakeIOOptions lake_io_opts;
    LakeDelvecLoader loader(_tablet_manager.get(), nullptr, true, lake_io_opts);

    st = loader.load_from_file(tsid, version, &pdelvec);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(pdelvec != nullptr);
    EXPECT_EQ(expected_delvec, pdelvec->save());
}

// Test load with pk_builder
TEST_F(LakeDelvecLoaderTest, test_load_with_pk_builder) {
    const int64_t tablet_id = 10005;
    const uint32_t segment_id = 500;
    const int64_t version = 14;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);

    // 1. Create metadata and write delvec
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {2, 4, 6, 8};
    dv.add_dels_as_new_version(dels, version, &ndv);
    std::string expected_delvec = ndv->save();

    // Add delvec to builder
    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    ASSERT_TRUE(st.ok());

    // 2. Create a new version with MetaFileBuilder
    metadata->set_version(version + 1);
    MetaFileBuilder builder2(*tablet, metadata);

    // 3. Test load with pk_builder (MetaFileBuilder)
    DelVectorPtr pdelvec;
    TabletSegmentId tsid;
    tsid.tablet_id = tablet_id;
    tsid.segment_id = segment_id;

    LakeIOOptions lake_io_opts;
    LakeDelvecLoader loader(_tablet_manager.get(), &builder2, true, lake_io_opts);

    st = loader.load(tsid, version, &pdelvec);
    // The load should succeed (either found in builder or loaded from file)
    ASSERT_TRUE(st.ok());
}

} // namespace starrocks::lake

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

#include "storage/lake/gc.h"

#include <gtest/gtest.h>

#include <ctime>
#include <set>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/del_vector.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/uid_util.h"

namespace starrocks::lake {

class TestLocationProvider : public LocationProvider {
public:
    explicit TestLocationProvider(std::string dir) : _dir(dir) {}

    std::set<int64_t> owned_tablets() const override { return _owned_shards; }

    std::string root_location(int64_t tablet_id) const override { return _dir; }

    Status list_root_locations(std::set<std::string>* roots) const override {
        roots->insert(_dir);
        return Status::OK();
    }

    std::set<int64_t> _owned_shards;
    std::string _dir;
};

class GCTest : public ::testing::Test {
public:
    void SetUp() override {
        CHECK_OK(fs::create_directories(join_path(kTestDir, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kSegmentDirectoryName)));
        config::lake_gc_metadata_max_versions = 4;
    }

    void TearDown() override { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_gc_test";
};

// NOLINTNEXTLINE
TEST_F(GCTest, test_metadata_gc) {
    auto fs = FileSystem::Default();
    auto tablet_id_1 = next_id();
    auto tablet_id_2 = next_id();

    // LocationProvider and TabletManager of worker A
    auto location_provider_1 = std::make_unique<TestLocationProvider>(kTestDir);
    auto update_manager_1 = std::make_unique<lake::UpdateManager>(location_provider_1.get());
    auto tablet_manager_1 =
            std::make_unique<lake::TabletManager>(location_provider_1.get(), update_manager_1.get(), 16384);
    // tablet_id_1 owned by worker A
    location_provider_1->_owned_shards.insert(tablet_id_1);

    // LocationProvider and TabletManager of worker B
    auto location_provider_2 = std::make_unique<TestLocationProvider>(kTestDir);
    auto update_manager_2 = std::make_unique<lake::UpdateManager>(location_provider_2.get());
    auto tablet_manager_2 =
            std::make_unique<lake::TabletManager>(location_provider_2.get(), update_manager_2.get(), 16384);
    // tablet_id_2 owned by worker B
    location_provider_2->_owned_shards.insert(tablet_id_2);

    // Save metadata on woker A
    auto version_count = config::lake_gc_metadata_max_versions + 4;
    for (int i = 0; i < version_count; i++) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id_1);
        metadata->set_version(i + 1);
        metadata->set_next_rowset_id(i + 1);
        ASSERT_OK(tablet_manager_1->put_tablet_metadata(metadata));
    }
    for (int i = 0; i < 5; i++) {
        auto txn_log = std::make_shared<lake::TxnLog>();
        txn_log->set_tablet_id(tablet_id_1);
        txn_log->set_txn_id(i);
        ASSERT_OK(tablet_manager_1->put_txn_log(txn_log));
    }

    // Save metadata on woker B
    for (int i = 0; i < version_count; i++) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id_2);
        metadata->set_version(i + 1);
        metadata->set_next_rowset_id(i + 1);
        ASSERT_OK(tablet_manager_2->put_tablet_metadata(metadata));
    }
    for (int i = 0; i < 5; i++) {
        auto txn_log = std::make_shared<lake::TxnLog>();
        txn_log->set_tablet_id(tablet_id_2);
        txn_log->set_txn_id(i);
        ASSERT_OK(tablet_manager_2->put_txn_log(txn_log));
    }

    // Doing GC on worker A
    ASSERT_OK(metadata_gc(kTestDir, tablet_manager_1.get(), 1000));

    // Woker B should only delete expired metadata of tablet_id_1
    for (int i = 0; i < version_count - config::lake_gc_metadata_max_versions; i++) {
        auto location = tablet_manager_1->tablet_metadata_location(tablet_id_1, i + 1);
        auto st = fs->path_exists(location);
        if (i < version_count - config::lake_gc_metadata_max_versions) {
            ASSERT_TRUE(st.is_not_found()) << st;
        } else {
            ASSERT_TRUE(st.ok()) << st;
        }

        location = tablet_manager_2->tablet_metadata_location(tablet_id_2, i + 1);
        st = fs->path_exists(location);
        ASSERT_TRUE(st.ok()) << st;
    }
    for (int i = 0; i < 5; i++) {
        auto txn = tablet_manager_1->txn_log_location(tablet_id_1, i);
        auto st = fs->path_exists(txn);
        ASSERT_TRUE(st.is_not_found()) << st;

        txn = tablet_manager_2->txn_log_location(tablet_id_2, i);
        st = fs->path_exists(txn);
        ASSERT_TRUE(st.ok()) << st;
    }

    // Doing GC on worker B
    ASSERT_OK(metadata_gc(kTestDir, tablet_manager_2.get(), 1000));
    for (int i = 0; i < version_count - config::lake_gc_metadata_max_versions; i++) {
        auto location = tablet_manager_2->tablet_metadata_location(tablet_id_2, i + 1);
        auto st = fs->path_exists(location);
        if (i < version_count - config::lake_gc_metadata_max_versions) {
            ASSERT_TRUE(st.is_not_found()) << st;
        } else {
            ASSERT_TRUE(st.ok()) << st;
        }
    }
    for (int i = 0; i < 5; i++) {
        auto txn = tablet_manager_2->txn_log_location(tablet_id_2, i);
        auto st = fs->path_exists(txn);
        ASSERT_TRUE(st.is_not_found()) << st;
    }
}

// NOLINTNEXTLINE
TEST_F(GCTest, test_datafile_gc) {
    auto fs = FileSystem::Default();
    auto tablet_id_1 = next_id();
    auto tablet_id_2 = next_id();
    // LocationProvider and TabletManager of worker A
    auto location_provider_1 = std::make_unique<TestLocationProvider>(kTestDir);
    auto update_manager_1 = std::make_unique<lake::UpdateManager>(location_provider_1.get());
    auto tablet_manager_1 =
            std::make_unique<lake::TabletManager>(location_provider_1.get(), update_manager_1.get(), 16384);
    // tablet_id_1 owned by worker A
    location_provider_1->_owned_shards.insert(tablet_id_1);

    // LocationProvider and TabletManager of worker B
    auto location_provider_2 = std::make_unique<TestLocationProvider>(kTestDir);
    auto update_manager_2 = std::make_unique<lake::UpdateManager>(location_provider_2.get());
    auto tablet_manager_2 =
            std::make_unique<lake::TabletManager>(location_provider_2.get(), update_manager_2.get(), 16384);
    // tablet_id_2 owned by worker B
    location_provider_2->_owned_shards.insert(tablet_id_2);

    auto segments = std::vector<std::string>();
    for (int i = 0; i < 10; i++) {
        segments.emplace_back(random_segment_filename());
        auto location = tablet_manager_1->segment_location(tablet_id_1, segments.back());
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(location));
        ASSERT_OK(wf->close());
    }
    auto valid_segment_cnt = 0;

    // segment referenced by tablet_id_1
    {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id_1);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        auto rowset = metadata->add_rowsets();
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(tablet_manager_1->put_tablet_metadata(metadata));
    }
    // segment referenced by tablet_id_2
    {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id_2);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        auto rowset = metadata->add_rowsets();
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(tablet_manager_2->put_tablet_metadata(metadata));
    }
    // segment referenced by txn log of tablet_id_1
    {
        auto log_write = std::make_shared<TxnLog>();
        log_write->set_tablet_id(tablet_id_1);
        log_write->set_txn_id(next_id());

        auto rowset = log_write->mutable_op_write()->mutable_rowset();
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(tablet_manager_1->put_txn_log(log_write));
    }
    // segment referenced by txn log of tablet_id_1
    {
        auto log_compaction = std::make_shared<TxnLog>();
        log_compaction->set_tablet_id(tablet_id_1);
        log_compaction->set_txn_id(next_id());

        auto rowset = log_compaction->mutable_op_compaction()->mutable_output_rowset();
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(tablet_manager_1->put_txn_log(log_compaction));
    }
    // segment referenced by txn log of tablet_id_1
    {
        auto log_schema_change = std::make_shared<TxnLog>();
        log_schema_change->set_tablet_id(tablet_id_1);
        log_schema_change->set_txn_id(next_id());

        auto rowset = log_schema_change->mutable_op_schema_change()->add_rowsets();
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(tablet_manager_1->put_txn_log(log_schema_change));
    }
    ASSERT_LT(valid_segment_cnt, segments.size());

    // Orphan segments have not timed out yet
    config::lake_gc_segment_expire_seconds = 600;
    ASSERT_OK(datafile_gc(kTestDir, tablet_manager_1.get()));
    for (const auto& seg : segments) {
        auto location = join_path(join_path(kTestDir, kSegmentDirectoryName), seg);
        ASSERT_OK(fs->path_exists(location));
    }

    // Segment GC on tablet_manager_2 should not delete any file
    config::lake_gc_segment_expire_seconds = 0;
    ASSERT_OK(datafile_gc(kTestDir, tablet_manager_2.get()));
    for (const auto& seg : segments) {
        auto location = join_path(join_path(kTestDir, kSegmentDirectoryName), seg);
        ASSERT_OK(fs->path_exists(location));
    }

    // Segment GC on tablet_manager_1
    ASSERT_OK(datafile_gc(kTestDir, tablet_manager_1.get()));
    for (int i = 0, sz = segments.size(); i < sz; i++) {
        auto location = join_path(join_path(kTestDir, kSegmentDirectoryName), segments[i]);
        if (i < valid_segment_cnt) {
            ASSERT_OK(fs->path_exists(location));
        } else {
            auto st = fs->path_exists(location);
            ASSERT_TRUE(st.is_not_found()) << st;
        }
    }
    {
        auto orphan_list = join_path(kTestDir, kGCFileName);
        auto st = fs->path_exists(orphan_list);
        ASSERT_OK(st);
    }
}

TEST_F(GCTest, test_dels_gc) {
    auto fs = FileSystem::Default();
    auto tablet_id_1 = next_id();
    // LocationProvider and TabletManager of worker A
    auto location_provider_1 = std::make_unique<TestLocationProvider>(kTestDir);
    auto update_manager_1 = std::make_unique<lake::UpdateManager>(location_provider_1.get());
    auto tablet_manager_1 =
            std::make_unique<lake::TabletManager>(location_provider_1.get(), update_manager_1.get(), 16384);
    // tablet_id_1 owned by worker A
    location_provider_1->_owned_shards.insert(tablet_id_1);
    {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id_1);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        ASSERT_OK(tablet_manager_1->put_tablet_metadata(metadata));
    }

    auto dels = std::vector<std::string>();
    for (int i = 0; i < 10; i++) {
        auto name = fmt::format("{}.del", generate_uuid_string());
        dels.emplace_back(name);
        auto location = tablet_manager_1->segment_location(tablet_id_1, dels.back());
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(location));
        ASSERT_OK(wf->close());
    }

    // add 5 del files to txn logs
    auto log_write = std::make_shared<TxnLog>();
    log_write->set_tablet_id(tablet_id_1);
    log_write->set_txn_id(next_id());
    for (int i = 0; i < 5; i++) {
        log_write->mutable_op_write()->add_dels(dels[i]);
    }
    ASSERT_OK(tablet_manager_1->put_txn_log(log_write));

    // Orphan dels have not timed out yet
    config::lake_gc_segment_expire_seconds = 600;
    ASSERT_OK(datafile_gc(kTestDir, tablet_manager_1.get()));
    for (const auto& del : dels) {
        auto location = tablet_manager_1->segment_location(tablet_id_1, del);
        ASSERT_OK(fs->path_exists(location));
    }

    // Orphan dels have been deleted
    config::lake_gc_segment_expire_seconds = 0;
    ASSERT_OK(datafile_gc(kTestDir, tablet_manager_1.get()));
    for (int i = 0; i < dels.size(); i++) {
        auto location = tablet_manager_1->segment_location(tablet_id_1, dels[i]);
        if (i < 5) {
            ASSERT_OK(fs->path_exists(location));
        } else {
            ASSERT_ERROR(fs->path_exists(location));
        }
    }
}

TEST_F(GCTest, test_delvec_gc) {
    auto fs = FileSystem::Default();
    auto tablet_id_1 = next_id();
    // LocationProvider and TabletManager of worker A
    auto location_provider_1 = std::make_unique<TestLocationProvider>(kTestDir);
    auto update_manager_1 = std::make_unique<lake::UpdateManager>(location_provider_1.get());
    auto tablet_manager_1 =
            std::make_unique<lake::TabletManager>(location_provider_1.get(), update_manager_1.get(), 16384);
    // tablet_id_1 owned by worker A
    location_provider_1->_owned_shards.insert(tablet_id_1);
    {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id_1);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        for (int i = 1; i < 5; i++) {
            (*metadata->mutable_delvec_meta()->mutable_delvecs())[i].set_version(i);
        }
        ASSERT_OK(tablet_manager_1->put_tablet_metadata(metadata));
    }
    // create delvec files
    for (int i = 1; i < 10; i++) {
        auto location = location_provider_1->tablet_delvec_location(tablet_id_1, i);
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(location));
        ASSERT_OK(wf->close());
    }
    // gc
    config::lake_gc_segment_expire_seconds = 0;
    ASSERT_OK(datafile_gc(kTestDir, tablet_manager_1.get()));
    for (int i = 1; i < 10; i++) {
        auto location = location_provider_1->tablet_delvec_location(tablet_id_1, i);
        if (i < 5) {
            ASSERT_OK(fs->path_exists(location));
        } else {
            ASSERT_ERROR(fs->path_exists(location));
        }
    }
}

TEST_F(GCTest, test_concurrent_gc) {
    auto fs = FileSystem::Default();
    auto tablet_id_1 = next_id();

    config::lake_gc_segment_expire_seconds = 0;
    config::lake_gc_metadata_max_versions = 1;

    // LocationProvider and TabletManager of worker A
    auto lp = std::make_unique<TestLocationProvider>(kTestDir);
    auto um = std::make_unique<lake::UpdateManager>(lp.get());
    auto tablet_mgr = std::make_unique<lake::TabletManager>(lp.get(), um.get(), 0);
    lp->_owned_shards.insert(tablet_id_1);

    auto segments = std::vector<std::string>();
    for (int i = 0; i < 2; i++) {
        segments.emplace_back(random_segment_filename());
        auto location = tablet_mgr->segment_location(tablet_id_1, segments.back());
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(location));
        ASSERT_OK(wf->append("content"));
        ASSERT_OK(wf->close());
    }

    // Generate a metadata of version 1 and segments[0] is referenced in the metadata
    {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id_1);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        auto rowset = metadata->add_rowsets();
        rowset->add_segments(segments[0]);
        ASSERT_OK(tablet_mgr->put_tablet_metadata(metadata));
    }

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->LoadDependency({
            {"CloudNative::GC::find_orphan_datafiles:finished_list_meta", "GCTest::test_concurrent_gc:begin_write"},
            {"GCTest::test_concurrent_gc:finish_write", "CloudNative::GC::delete_tablet_metadata:enter"},
            {"CloudNative::GC::delete_tablet_metadata:return", "CloudNative::GC::find_orphan_datafiles:check_meta"},
    });

    // This thread will generate a metadata of version 2 and both segments[0] and segments[1] are referenced in
    // the metadata
    auto write_thread = std::thread([&]() {
        TEST_SYNC_POINT("GCTest::test_concurrent_gc:begin_write");
        auto metadata_v2 = std::make_shared<TabletMetadata>();
        metadata_v2->set_id(tablet_id_1);
        metadata_v2->set_version(2);
        metadata_v2->set_next_rowset_id(3);
        metadata_v2->add_rowsets()->add_segments(segments[0]);
        metadata_v2->add_rowsets()->add_segments(segments[1]);
        ASSERT_OK(tablet_mgr->put_tablet_metadata(metadata_v2));
        TEST_SYNC_POINT("GCTest::test_concurrent_gc:finish_write");
    });

    // This thread can only see the metadata of version 1 and the version 1 will be removed during the execution
    auto datagc_thread = std::thread([&]() { (void)datafile_gc(kTestDir, tablet_mgr.get()); });

    // This thread is used to simulate the shard been balanced to another node.
    // This thread will remove the metadata of version 1 but keep the metadata of version 2
    auto metagc_thread = std::thread([&]() { CHECK_OK(metadata_gc(kTestDir, tablet_mgr.get(), 0)); });

    write_thread.join();
    datagc_thread.join();
    metagc_thread.join();

    ASSERT_TRUE(fs->path_exists(tablet_mgr->tablet_metadata_location(tablet_id_1, 1)).is_not_found());
    ASSERT_TRUE(fs->path_exists(tablet_mgr->tablet_metadata_location(tablet_id_1, 2)).ok());
    ASSERT_TRUE(fs->path_exists(tablet_mgr->segment_location(tablet_id_1, segments[0])).ok());
    ASSERT_TRUE(fs->path_exists(tablet_mgr->segment_location(tablet_id_1, segments[1])).ok());

    SyncPoint::GetInstance()->DisableProcessing();
}

} // namespace starrocks::lake

// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/gc.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/uid_util.h"

namespace starrocks::lake {

class GCTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        CHECK_OK(fs::create_directories(join_path(kTestDir, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kSegmentDirectoryName)));

        s_location_provider = std::make_unique<FixedLocationProvider>(kTestDir);
        s_tablet_manager = std::make_unique<lake::TabletManager>(s_location_provider.get(), 16384);
    }

    static void TearDownTestCase() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_gc_test";
    inline static std::unique_ptr<lake::LocationProvider> s_location_provider;
    inline static std::unique_ptr<TabletManager> s_tablet_manager;
};

// NOLINTNEXTLINE
TEST_F(GCTest, test_metadata_gc) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto version_count = config::lake_gc_metadata_max_versions + 4;
    int64_t txn_id = 1;
    auto expire_time =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count() +
            100000;
    for (int i = 0; i < version_count; i++) {
        auto metadata = std::make_shared<TabletMetadata>();
        // leave scheme and rowset unsetted.
        metadata->set_id(tablet_id);
        metadata->set_version(i + 1);
        metadata->set_next_rowset_id(i + 1);

        if (i == 0 || i == 1) {
            ASSERT_OK(s_tablet_manager->put_tablet_metadata_lock(tablet_id, i + 1, expire_time));
            ++txn_id;
        }

        ASSERT_OK(s_tablet_manager->put_tablet_metadata(metadata));
        ASSERT_OK(metadata_gc(kTestDir, s_tablet_manager.get(), 0));
    }

    // Ensure all metadata are still exist.
    for (int i = 0; i < version_count; i++) {
        // Version 1 and 2 will be kept because their metadata are locked.
        if (i == 2 || i == 3) {
            auto location = s_tablet_manager->tablet_metadata_location(tablet_id, i + 1);
            auto st = fs->path_exists(location);
            ASSERT_TRUE(st.is_not_found()) << st;
        } else {
            auto location = s_tablet_manager->tablet_metadata_location(tablet_id, i + 1);
            ASSERT_OK(fs->path_exists(location));
        }
    }

    // txn log
    int64_t min_active_txn_log_id = 100;
    {
        for (int i = 0; i < 5; i++) {
            auto txn_log = std::make_shared<lake::TxnLog>();
            txn_log->set_tablet_id(tablet_id);
            txn_log->set_txn_id(i);
            ASSERT_OK(s_tablet_manager->put_txn_log(txn_log));
        }
        ASSERT_OK(metadata_gc(kTestDir, s_tablet_manager.get(), min_active_txn_log_id));

        for (int i = 0; i < 5; i++) {
            auto txn = s_tablet_manager->txn_log_location(tablet_id, i);
            auto st = fs->path_exists(txn);
            ASSERT_TRUE(st.is_not_found()) << st;
        }
    }

    {
        for (int i = 100; i < 105; i++) {
            auto txn_log = std::make_shared<lake::TxnLog>();
            txn_log->set_tablet_id(tablet_id);
            txn_log->set_txn_id(i);
            ASSERT_OK(s_tablet_manager->put_txn_log(txn_log));
        }
        ASSERT_OK(metadata_gc(kTestDir, s_tablet_manager.get(), min_active_txn_log_id));

        for (int i = 100; i < 105; i++) {
            auto location = s_tablet_manager->txn_log_location(tablet_id, i);
            ASSERT_OK(fs->path_exists(location));
        }
    }
}

// NOLINTNEXTLINE
TEST_F(GCTest, segment_metadata_gc) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto segments = std::vector<std::string>();
    for (int i = 0; i < 10; i++) {
        segments.emplace_back(fmt::format("{}.dat", generate_uuid_string()));
        auto location = s_tablet_manager->segment_location(tablet_id, segments.back());
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(location));
        ASSERT_OK(wf->close());
    }
    auto valid_segment_cnt = 0;

    {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        auto rowset = metadata->add_rowsets();
        rowset->add_segments(segments[valid_segment_cnt++]);
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(s_tablet_manager->put_tablet_metadata(metadata));
    }
    {
        auto log_write = std::make_shared<TxnLog>();
        log_write->set_tablet_id(tablet_id);
        log_write->set_txn_id(next_id());

        auto rowset = log_write->mutable_op_write()->mutable_rowset();
        rowset->add_segments(segments[valid_segment_cnt++]);
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(s_tablet_manager->put_txn_log(log_write));
    }
    {
        auto log_compaction = std::make_shared<TxnLog>();
        log_compaction->set_tablet_id(tablet_id);
        log_compaction->set_txn_id(next_id());

        auto rowset = log_compaction->mutable_op_compaction()->mutable_output_rowset();
        rowset->add_segments(segments[valid_segment_cnt++]);
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(s_tablet_manager->put_txn_log(log_compaction));
    }
    {
        auto log_schema_change = std::make_shared<TxnLog>();
        log_schema_change->set_tablet_id(tablet_id);
        log_schema_change->set_txn_id(next_id());

        auto rowset = log_schema_change->mutable_op_schema_change()->add_rowsets();
        rowset->add_segments(segments[valid_segment_cnt++]);
        rowset->add_segments(segments[valid_segment_cnt++]);
        ASSERT_OK(s_tablet_manager->put_txn_log(log_schema_change));
    }
    ASSERT_LT(valid_segment_cnt, segments.size());

    config::lake_gc_segment_expire_seconds = 600;
    ASSERT_OK(segment_gc(kTestDir, s_tablet_manager.get()));

    for (const auto& seg : segments) {
        auto location = s_tablet_manager->segment_location(tablet_id, seg);
        ASSERT_OK(fs->path_exists(location));
    }

    config::lake_gc_segment_expire_seconds = 0;
    ASSERT_OK(segment_gc(kTestDir, s_tablet_manager.get()));

    for (int i = 0, sz = segments.size(); i < sz; i++) {
        auto location = s_tablet_manager->segment_location(tablet_id, segments[i]);
        if (i < valid_segment_cnt) {
            ASSERT_OK(fs->path_exists(location));
        } else {
            auto st = fs->path_exists(location);
            ASSERT_TRUE(st.is_not_found()) << st;
        }
    }
}

} // namespace starrocks::lake

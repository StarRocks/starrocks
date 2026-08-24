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

#include "storage/tablet.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "common/config_storage_fwd.h"
#include "fs/fs_util.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet_manager.h"
#include "storage_primitive/flat_json_config.h"
#include "storage_primitive/tablet_basic_info.h"

namespace starrocks {

using namespace std::chrono_literals;

class TabletTest : public testing::Test {
public:
    TabletTest() {}
};

static void add_rowset(Tablet* tablet, std::shared_ptr<Rowset>* rowset, std::atomic<bool>* stop) {
    while (!stop->load()) {
        tablet->add_committed_rowset(*rowset);
        std::this_thread::sleep_for(2us);
    }
}
static void erase_rowset(Tablet* tablet, std::shared_ptr<Rowset>* rowset, std::atomic<bool>* stop) {
    while (!stop->load()) {
        tablet->erase_committed_rowset(*rowset);
        std::this_thread::sleep_for(2us);
    }
}

TEST_F(TabletTest, test_concurrent_add_remove_committed_rowsets) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_id(1024);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    tablet_meta->set_tablet_schema(schema);
    tablet_meta->set_tablet_id(1024);
    auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
    rs_meta_pb->set_rowset_id("123");
    rs_meta_pb->set_start_version(0);
    rs_meta_pb->set_end_version(1);
    rs_meta_pb->mutable_tablet_schema()->CopyFrom(schema_pb);
    auto rowset_meta = std::make_shared<RowsetMeta>(rs_meta_pb);
    DataDir data_dir("./data_dir");
    auto rowset = std::make_shared<Rowset>(schema, "", rowset_meta, data_dir.get_meta());
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, &data_dir);
    tablet->set_data_dir(&data_dir);
    tablet->add_committed_rowset(rowset);
    tablet->erase_committed_rowset(rowset);
    std::vector<std::thread> all_threads;
    std::atomic<bool> stop{false};
    for (int i = 0; i < 10; ++i) {
        all_threads.emplace_back(&add_rowset, tablet.get(), &rowset, &stop);
    }
    for (int i = 0; i < 10; ++i) {
        all_threads.emplace_back(&erase_rowset, tablet.get(), &rowset, &stop);
    }
    std::this_thread::sleep_for(5s);
    stop = true;
    for (auto& t : all_threads) {
        t.join();
    }
}

TEST_F(TabletTest, test_get_basic_info_uses_tablet_footprint) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->TEST_set_table_id(20001);
    tablet_meta->set_partition_id(20002);
    tablet_meta->set_tablet_id(20003);
    tablet_meta->set_creation_time(123456789);

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_id(20004);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    tablet_meta->set_tablet_schema(schema);

    RowsetMetaPB rowset_meta_pb;
    rowset_meta_pb.set_tablet_id(20003);
    rowset_meta_pb.set_partition_id(20002);
    rowset_meta_pb.set_creation_time(0);
    rowset_meta_pb.set_empty(false);
    rowset_meta_pb.set_num_segments(1);
    rowset_meta_pb.set_num_rows(321);
    rowset_meta_pb.set_start_version(0);
    rowset_meta_pb.set_end_version(0);
    rowset_meta_pb.set_rowset_state(VISIBLE);
    rowset_meta_pb.set_data_disk_size(54321);
    rowset_meta_pb.set_index_disk_size(999);
    RowsetId rowset_id;
    rowset_id.init(2, 2, 0, 0);
    rowset_meta_pb.set_rowset_id(rowset_id.to_string());
    tablet_meta->add_rs_meta(std::make_shared<RowsetMeta>(rowset_meta_pb));

    DataDir data_dir("./data_dir");
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, &data_dir);
    tablet->set_data_dir(&data_dir);

    TabletBasicInfo info;
    tablet->get_basic_info(info);
    ASSERT_EQ(54321 + 999, info.data_size);
    ASSERT_EQ(321, info.num_row);
    ASSERT_EQ(1, info.num_segment);
}

TEST_F(TabletTest, test_update_flat_json_config_version_gate) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->set_tablet_id(1026);
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_id(1027);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    tablet_meta->set_tablet_schema(schema);
    DataDir data_dir("./data_dir");
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, &data_dir);

    // a higher version installs the config
    FlatJsonConfig config_v2(true, 0.25, 0.75, 12);
    config_v2.set_flat_json_config_version(2);
    tablet->update_flat_json_config(config_v2);
    auto installed = tablet->tablet_meta()->get_flat_json_config();
    ASSERT_TRUE(installed != nullptr);
    ASSERT_EQ(2, installed->get_flat_json_config_version());
    ASSERT_DOUBLE_EQ(0.25, installed->get_flat_json_null_factor());

    // a stale (lower or equal) version is skipped, keeping the current config
    FlatJsonConfig config_v1(false, 0.5, 0.5, 8);
    config_v1.set_flat_json_config_version(1);
    tablet->update_flat_json_config(config_v1);
    installed = tablet->tablet_meta()->get_flat_json_config();
    ASSERT_EQ(2, installed->get_flat_json_config_version());
    ASSERT_TRUE(installed->is_flat_json_enabled());
    ASSERT_DOUBLE_EQ(0.25, installed->get_flat_json_null_factor());
}

// ---------------------------------------------------------------------------
// Regression: compaction must not create a rowset that straddles the version an
// online optimize job's pinned overwrite will publish at.
//
// ALTER TABLE ... DISTRIBUTED BY (OnlineOptimizeJobV2) rewrites the partition
// with an INSERT OVERWRITE pinned to the version V its SELECT scanned, while
// concurrent loads keep double-writing into the temporary partition with the
// source partition's version numbers. Compaction there could merge
// double-written versions across V; overwrite_rowset() then picks that rowset
// for deletion (start_version <= V) but only puts back [0, V], silently losing
// every version after V, and the version tracker / tablet meta divergence
// aborts DCHECK builds:
//     Check failed: v == _max_continuous_version_from_beginning_unlocked().second (53 vs. 41)
//
// The fix: while a tablet is receiving double-writes and its pinned overwrite
// has not been applied yet, compaction on it is suspended, so no rowset
// straddling the (not yet known) overwrite version can form. The phase is
// persisted in the tablet meta so a restart cannot forget it in either
// direction. Confirmed end to end against a fault-injection build by parking
// the overwrite publish, forcing compaction across the pinned version, and
// releasing the publish.
// ---------------------------------------------------------------------------

class OverwriteRowsetTest : public testing::Test {
public:
    void SetUp() override {
        _saved_event_based_compaction = config::enable_event_based_compaction_framework;
        // The event based compaction framework needs a live StorageEngine.
        config::enable_event_based_compaction_framework = false;

        _schema_pb.set_keys_type(KeysType::DUP_KEYS);
        _schema_pb.set_id(kSchemaId);
        _schema = std::make_shared<const TabletSchema>(_schema_pb);

        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_id(kTabletId);
        tablet_meta->set_partition_id(kPartitionId);
        tablet_meta->set_tablet_schema(_schema);
        // TabletMeta::_save_meta() CHECKs a valid tablet uid (revise_tablet_meta() saves through
        // it); there is no setter, so inject one through the protobuf roundtrip.
        TabletMetaPB uid_pb;
        tablet_meta->to_meta_pb(&uid_pb);
        *uid_pb.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();
        tablet_meta->init_from_pb(&uid_pb);
        tablet_meta->set_tablet_schema(_schema);

        // revise_tablet_meta() (clone repair) saves the meta through the data dir's kv store,
        // so back the test data dir with a real one.
        _data_dir_path = "./overwrite_rowset_test_" + std::to_string(::getpid());
        ASSERT_OK(fs::create_directories(_data_dir_path + "/meta"));
        _data_dir = std::make_unique<DataDir>(_data_dir_path);
        ASSERT_OK(_data_dir->init());
        _tablet = Tablet::create_tablet_from_meta(tablet_meta, _data_dir.get());
        _tablet->set_data_dir(_data_dir.get());
        ASSERT_OK(_tablet->set_tablet_state(TABLET_RUNNING));
    }

    void TearDown() override {
        config::enable_event_based_compaction_framework = _saved_event_based_compaction;
        (void)fs::remove_all(_data_dir_path);
    }

protected:
    static constexpr int64_t kTabletId = 40001;
    static constexpr int64_t kPartitionId = 40002;
    static constexpr int64_t kSchemaId = 40003;
    // The version the online-optimize INSERT OVERWRITE is pinned to (the version
    // its SELECT scanned), and the highest version already published to the tablet.
    static constexpr int64_t kOverwriteVersion = 41;
    static constexpr int64_t kMaxVersion = 53;

    RowsetSharedPtr create_rowset(int64_t start, int64_t end) {
        auto rs_meta_pb = std::make_unique<RowsetMetaPB>();
        RowsetId rowset_id;
        rowset_id.init(2, ++_next_rowset_seq, 0, 0);
        rs_meta_pb->set_deprecated_rowset_id(0);
        rs_meta_pb->set_rowset_id(rowset_id.to_string());
        rs_meta_pb->set_tablet_id(kTabletId);
        rs_meta_pb->set_partition_id(kPartitionId);
        rs_meta_pb->set_start_version(start);
        rs_meta_pb->set_end_version(end);
        rs_meta_pb->set_rowset_state(VISIBLE);
        rs_meta_pb->set_empty(true);
        rs_meta_pb->set_num_segments(0);
        rs_meta_pb->set_num_rows(0);
        rs_meta_pb->set_creation_time(0);
        rs_meta_pb->mutable_tablet_schema()->CopyFrom(_schema_pb);
        auto rs_meta = std::make_shared<RowsetMeta>(rs_meta_pb);
        return std::make_shared<Rowset>(_schema, "", rs_meta, _data_dir->get_meta());
    }

    // Mirror of Tablet::_max_continuous_version_from_beginning_unlocked(): the
    // max continuous version the tablet meta actually backs.
    int64_t max_continuous_version_from_meta() const {
        std::vector<Version> versions;
        for (const auto& rs_meta : _tablet->tablet_meta()->all_rs_metas()) {
            versions.emplace_back(rs_meta->version());
        }
        std::sort(versions.begin(), versions.end(),
                  [](const Version& lhs, const Version& rhs) { return lhs.first < rhs.first; });
        Version max_continuous_version = {-1, 0};
        for (const auto& version : versions) {
            if (version.first > max_continuous_version.second + 1) {
                break;
            }
            max_continuous_version = version;
        }
        return max_continuous_version.second;
    }

    // Builds the tablet state an online-optimize temporary partition reaches
    // right before its INSERT OVERWRITE publishes:
    //   [0-1]   the initial rowset created together with the partition
    //   [40-53] one cumulative-compaction output covering the double-written
    //           versions, which straddles kOverwriteVersion
    void build_tablet_with_rowset_spanning_overwrite_version() {
        ASSERT_OK(_tablet->add_rowset(create_rowset(0, 1), false));

        // Double writes into the temporary partition carry the source partition's
        // version numbers, so they start below the version the INSERT OVERWRITE
        // scanned and run past it.
        std::vector<RowsetSharedPtr> singletons;
        for (int64_t v = 40; v <= kMaxVersion; ++v) {
            auto rowset = create_rowset(v, v);
            ASSERT_OK(_tablet->add_rowset(rowset, false));
            singletons.emplace_back(std::move(rowset));
        }

        // Cumulative compaction merges them into a single rowset, exactly as
        // Compaction::modify_rowsets() does. The inputs become stale, but their
        // edges stay in the version graph until the stale sweep runs.
        auto merged = create_rowset(40, kMaxVersion);
        {
            std::unique_lock wrlock(_tablet->get_header_lock());
            _tablet->modify_rowsets_without_lock({merged}, singletons, nullptr);
        }

        ASSERT_EQ(kMaxVersion, _tablet->max_version().second);
        ASSERT_EQ(2, _tablet->tablet_meta()->all_rs_metas().size());
    }

    std::string _data_dir_path;
    TabletSchemaPB _schema_pb;
    TabletSchemaCSPtr _schema;
    std::unique_ptr<DataDir> _data_dir;
    TabletSharedPtr _tablet;
    int64_t _next_rowset_seq = 0;
    bool _saved_event_based_compaction = true;
};

TEST_F(OverwriteRowsetTest, test_double_write_suspends_compaction) {
    // Prevention for the same defect: while double-writes are flowing, compaction on the tablet is
    // suspended, so it can never create a rowset straddling the (not yet known) overwrite version
    // in the first place. The suspension must not depend on a version hole existing: a partition
    // that was never loaded before the ALTER double-writes versions contiguous with the initial
    // [0-1] rowset, and merging those could straddle the overwrite version just the same.
    ASSERT_OK(_tablet->add_rowset(create_rowset(0, 1), false));
    for (int64_t v = 40; v <= kMaxVersion; ++v) {
        ASSERT_OK(_tablet->add_rowset(create_rowset(v, v), false));
    }
    _tablet->note_double_write_publish();
    ASSERT_TRUE(_tablet->compaction_suspended_for_double_write());

    std::vector<RowsetSharedPtr> candidates;
    _tablet->pick_all_candicate_rowsets(&candidates);
    EXPECT_TRUE(candidates.empty());
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(&candidates);
    EXPECT_TRUE(candidates.empty());
    _tablet->pick_candicate_rowsets_to_base_compaction(&candidates);
    EXPECT_TRUE(candidates.empty());

    // The pinned version-overwrite ends the double-write phase: everything is compactable again.
    _tablet->overwrite_rowset(create_rowset(0, 0), kOverwriteVersion);
    ASSERT_FALSE(_tablet->compaction_suspended_for_double_write());
    _tablet->pick_all_candicate_rowsets(&candidates);
    EXPECT_EQ(1 + (kMaxVersion - kOverwriteVersion), candidates.size());

    // Double-writes keep flowing between the overwrite publish and the partition swap, and the
    // swap itself is invisible to the backend: if those publishes re-entered the suspension,
    // nothing would ever lift it again and the swapped-in partition would stop compacting for
    // good. Once the overwrite has been applied they must be no-ops.
    _tablet->note_double_write_publish();
    EXPECT_FALSE(_tablet->compaction_suspended_for_double_write());
    candidates.clear();
    _tablet->pick_all_candicate_rowsets(&candidates);
    EXPECT_EQ(1 + (kMaxVersion - kOverwriteVersion), candidates.size());
}

TEST_F(OverwriteRowsetTest, test_double_write_phase_survives_reload) {
    ASSERT_OK(_tablet->add_rowset(create_rowset(0, 1), false));
    for (int64_t v = 40; v <= kMaxVersion; ++v) {
        ASSERT_OK(_tablet->add_rowset(create_rowset(v, v), false));
    }
    _tablet->note_double_write_publish();
    ASSERT_TRUE(_tablet->compaction_suspended_for_double_write());

    // The phase reaches the tablet meta and survives the protobuf roundtrip the meta store uses.
    TabletMetaPB meta_pb;
    _tablet->tablet_meta()->to_meta_pb(&meta_pb);
    EXPECT_EQ(1, meta_pb.double_write_phase());

    // A tablet reloaded from that meta (a restart) must come back suspended: with the phase
    // forgotten, compaction could merge a rowset across the overwrite version before the next
    // double-write publish re-arms the suspension.
    auto reloaded = Tablet::create_tablet_from_meta(_tablet->tablet_meta(), _data_dir.get());
    EXPECT_TRUE(reloaded->compaction_suspended_for_double_write());
    std::vector<RowsetSharedPtr> candidates;
    reloaded->pick_all_candicate_rowsets(&candidates);
    EXPECT_TRUE(candidates.empty());

    // The terminal phase must survive a reload as well: a double-write publish arriving between
    // the overwrite and the partition swap must not re-suspend the reloaded tablet, or nothing
    // would ever lift the suspension after the swap.
    _tablet->overwrite_rowset(create_rowset(0, 0), kOverwriteVersion);
    EXPECT_EQ(2, _tablet->tablet_meta()->double_write_phase());
    auto reloaded_after_overwrite = Tablet::create_tablet_from_meta(_tablet->tablet_meta(), _data_dir.get());
    EXPECT_FALSE(reloaded_after_overwrite->compaction_suspended_for_double_write());
    reloaded_after_overwrite->note_double_write_publish();
    EXPECT_FALSE(reloaded_after_overwrite->compaction_suspended_for_double_write());
}

TEST_F(OverwriteRowsetTest, test_clone_repair_merges_donor_double_write_phase) {
    // Clone repair and replication rebuild the tablet's content from another replica, so
    // revise_tablet_meta() merges the donor's double-write phase by taking the max; the phase
    // ladder only moves forward. Two directions matter:
    //  - a donor that already applied the pinned overwrite lifts a suspension the local replica
    //    could never lift itself (it missed the overwrite publish, which is never republished);
    //  - a mid-job clone must NOT lower the phase: the cloned double-written rowsets would
    //    otherwise be immediately eligible for the compaction revise_tablet_meta() queues,
    //    recreating the straddling rowset this change prevents.
    ASSERT_OK(_tablet->add_rowset(create_rowset(0, 1), false));
    _tablet->note_double_write_publish();
    ASSERT_TRUE(_tablet->compaction_suspended_for_double_write());

    // Mid-job clone from a donor that has not applied the overwrite (or predates the phase
    // field): the suspension must survive.
    ASSERT_OK(_tablet->revise_tablet_meta({}, {}, /*donor_double_write_phase=*/0));
    EXPECT_TRUE(_tablet->compaction_suspended_for_double_write());
    ASSERT_OK(_tablet->revise_tablet_meta({}, {}, /*donor_double_write_phase=*/1));
    EXPECT_TRUE(_tablet->compaction_suspended_for_double_write());

    // Repair from a donor that already applied the overwrite: the dangling suspension ends and
    // the terminal phase sticks.
    ASSERT_OK(_tablet->revise_tablet_meta({}, {}, /*donor_double_write_phase=*/2));
    EXPECT_FALSE(_tablet->compaction_suspended_for_double_write());
    EXPECT_EQ(2, _tablet->tablet_meta()->double_write_phase());
    _tablet->note_double_write_publish();
    EXPECT_FALSE(_tablet->compaction_suspended_for_double_write());
}

TEST_F(OverwriteRowsetTest, test_overwrite_replaces_fully_covered_rowsets) {
    // The same online-optimize state, except compaction has not merged across
    // the overwrite version: [40-41] and the singletons 42..53 after it.
    ASSERT_OK(_tablet->add_rowset(create_rowset(0, 1), false));
    ASSERT_OK(_tablet->add_rowset(create_rowset(40, kOverwriteVersion), false));
    for (int64_t v = kOverwriteVersion + 1; v <= kMaxVersion; ++v) {
        ASSERT_OK(_tablet->add_rowset(create_rowset(v, v), false));
    }

    _tablet->overwrite_rowset(create_rowset(0, 0), kOverwriteVersion);

    // [0-1] and [40-41] are replaced by [0-41]; the versions after the
    // overwrite point survive.
    EXPECT_EQ(kMaxVersion, _tablet->max_version().second);
    EXPECT_EQ(1 + (kMaxVersion - kOverwriteVersion), _tablet->tablet_meta()->all_rs_metas().size());
    EXPECT_EQ(kMaxVersion, max_continuous_version_from_meta());
    EXPECT_EQ(max_continuous_version_from_meta(), _tablet->max_continuous_version());
}

} // namespace starrocks

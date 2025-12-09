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

#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/persistent_index.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "test_util.h"
#include "testutil/assert.h"

namespace starrocks::lake {

class LakePersistentIndexParallelCompactMgrTest : public TestBase {
public:
    LakePersistentIndexParallelCompactMgrTest() : TestBase(kTestDir) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    // Helper function to create a test sstable file
    Status create_test_sstable(const std::string& filename, int start_key, int count,
                               PersistentIndexSstablePB* sst_pb) {
        sstable::Options options;
        std::string filepath = _tablet_mgr->sst_location(_tablet_metadata->id(), filename);
        ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(filepath));
        sstable::TableBuilder builder(options, wf.get());

        std::string first_key;
        std::string last_key;
        for (int i = 0; i < count; i++) {
            std::string key = fmt::format("key_{:016X}", start_key + i);
            if (i == 0) first_key = key;
            if (i == count - 1) last_key = key;
            IndexValue val(start_key + i);
            IndexValuesWithVerPB val_pb;
            auto* v = val_pb.add_values();
            v->set_version(1);
            v->set_rssid(val.get_rssid());
            v->set_rowid(val.get_rowid());
            builder.Add(Slice(key), val_pb.SerializeAsString());
        }
        RETURN_IF_ERROR(builder.Finish());
        uint64_t filesize = builder.FileSize();
        RETURN_IF_ERROR(wf->close());

        // Fill the sstable pb
        sst_pb->set_filename(filename);
        sst_pb->set_filesize(filesize);
        if (count > 0) {
            sst_pb->mutable_range()->set_start_key(builder.KeyRange().first.to_string());
            sst_pb->mutable_range()->set_end_key(builder.KeyRange().second.to_string());
        }
        auto fileset_id = UniqueId::gen_uid();
        sst_pb->mutable_fileset_id()->CopyFrom(fileset_id.to_proto());

        return Status::OK();
    }

    constexpr static const char* const kTestDir = "test_lake_persistent_index_parallel_compact_mgr";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
};

// ============================================================================
// Tests for key_ranges_overlap static method
// ============================================================================

class KeyRangesOverlapTest : public ::testing::Test {};

TEST_F(KeyRangesOverlapTest, test_non_overlapping_left_before_right) {
    // [000, 100] and [200, 300] - no overlap
    std::string start1 = "000";
    std::string end1 = "100";
    std::string start2 = "200";
    std::string end2 = "300";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_FALSE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_non_overlapping_right_before_left) {
    // [200, 300] and [000, 100] - no overlap
    std::string start1 = "200";
    std::string end1 = "300";
    std::string start2 = "000";
    std::string end2 = "100";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_FALSE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_adjacent_ranges_with_overlap) {
    // [000, 100] and [100, 200] - overlap at point 100 (end is inclusive)
    std::string start1 = "000";
    std::string end1 = "100";
    std::string start2 = "100";
    std::string end2 = "200";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_TRUE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_adjacent_ranges_no_overlap) {
    // [000, 099] and [100, 200] - no overlap (truly adjacent)
    std::string start1 = "000";
    std::string end1 = "099";
    std::string start2 = "100";
    std::string end2 = "200";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_FALSE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_partial_overlap_left_extends_into_right) {
    // [000, 150] and [100, 200] - overlaps in [100, 150]
    std::string start1 = "000";
    std::string end1 = "150";
    std::string start2 = "100";
    std::string end2 = "200";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_TRUE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_partial_overlap_right_extends_into_left) {
    // [100, 200] and [000, 150] - overlaps in [100, 150]
    std::string start1 = "100";
    std::string end1 = "200";
    std::string start2 = "000";
    std::string end2 = "150";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_TRUE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_complete_overlap_same_ranges) {
    // [100, 200] and [100, 200] - completely overlap
    std::string start1 = "100";
    std::string end1 = "200";
    std::string start2 = "100";
    std::string end2 = "200";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_TRUE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_containment_left_contains_right) {
    // [000, 300] contains [100, 200]
    std::string start1 = "000";
    std::string end1 = "300";
    std::string start2 = "100";
    std::string end2 = "200";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_TRUE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_containment_right_contains_left) {
    // [100, 200] is contained by [000, 300]
    std::string start1 = "100";
    std::string end1 = "200";
    std::string start2 = "000";
    std::string end2 = "300";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_TRUE(overlap);
}

TEST_F(KeyRangesOverlapTest, test_partial_overlap_ranges) {
    // [000, 150] and [100, 200] - partial overlap
    std::string start1 = "000";
    std::string end1 = "150";
    std::string start2 = "100";
    std::string end2 = "200";

    bool overlap = LakePersistentIndexParallelCompactMgr::key_ranges_overlap(start1, end1, start2, end2);
    ASSERT_TRUE(overlap);
}

// ============================================================================
// Tests for LakePersistentIndexParallelCompactMgr
// ============================================================================

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_init) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_init_and_shutdown) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());
    mgr->shutdown();
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_empty_candidates) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    std::vector<PersistentIndexSstablePB> output_sstables;

    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));
    ASSERT_TRUE(output_sstables.empty());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_single_sstable) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create one sstable
    PersistentIndexSstablePB sst1;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1});

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    // Should have one output (optimization: single sstable is reused)
    ASSERT_EQ(1, output_sstables.size());
    ASSERT_EQ(sst1.filename(), output_sstables[0].filename());
    // fileset id should be updated
    ASSERT_TRUE(UniqueId(sst1.fileset_id()) != UniqueId(output_sstables[0].fileset_id()));
    // check range
    ASSERT_EQ(output_sstables[0].range().start_key(), sst1.range().start_key());
    ASSERT_EQ(output_sstables[0].range().end_key(), sst1.range().end_key());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_multiple_non_overlapping_sstables) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create multiple non-overlapping sstables
    PersistentIndexSstablePB sst1, sst2, sst3;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 1000, 100, &sst2));
    ASSERT_OK(create_test_sstable("test_sst_3.sst", 2000, 100, &sst3));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1, sst2, sst3});

    // Set threshold to force splitting into multiple tasks
    auto old_threshold = config::pk_index_parallel_compaction_task_split_threshold_bytes;
    config::pk_index_parallel_compaction_task_split_threshold_bytes = 1; // Very small threshold

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    ASSERT_TRUE(output_sstables.size() == 3);

    config::pk_index_parallel_compaction_task_split_threshold_bytes = old_threshold;
    // check ranges
    ASSERT_EQ(output_sstables[0].range().start_key(), sst1.range().start_key());
    ASSERT_EQ(output_sstables[0].range().end_key(), sst1.range().end_key());
    ASSERT_EQ(output_sstables[1].range().start_key(), sst2.range().start_key());
    ASSERT_EQ(output_sstables[1].range().end_key(), sst2.range().end_key());
    ASSERT_EQ(output_sstables[2].range().start_key(), sst3.range().start_key());
    ASSERT_EQ(output_sstables[2].range().end_key(), sst3.range().end_key());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_multiple_non_overlapping_sstables2) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create multiple non-overlapping sstables
    PersistentIndexSstablePB sst1, sst2, sst3;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 1000, 100, &sst2));
    ASSERT_OK(create_test_sstable("test_sst_3.sst", 2000, 100, &sst3));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1, sst2, sst3});

    // Set threshold to force splitting into multiple tasks
    auto old_threshold = config::pk_index_parallel_compaction_task_split_threshold_bytes;
    config::pk_index_parallel_compaction_task_split_threshold_bytes = 1; // Very small threshold

    std::vector<std::shared_ptr<LakePersistentIndexParallelCompactTask>> tasks;
    mgr->generate_compaction_tasks(candidates, _tablet_metadata, false, &tasks);

    ASSERT_EQ(tasks.size(), 3);

    config::pk_index_parallel_compaction_task_split_threshold_bytes = old_threshold;
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_overlapping_sstables) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create overlapping sstables (keys 0-99 and 50-149 overlap)
    PersistentIndexSstablePB sst1, sst2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 50, 100, &sst2));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1});
    candidates.push_back({sst2});

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    // Should successfully compact overlapping sstables
    ASSERT_TRUE(output_sstables.size() == 1);
    // check range
    ASSERT_EQ(output_sstables[0].range().start_key(), sst1.range().start_key());
    ASSERT_EQ(output_sstables[0].range().end_key(), sst2.range().end_key());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_multiple_filesets) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create sstables from multiple filesets
    PersistentIndexSstablePB sst1, sst2, sst3, sst4;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 50, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 100, 50, &sst2));
    ASSERT_OK(create_test_sstable("test_sst_3.sst", 25, 50, &sst3));  // Overlaps with sst1
    ASSERT_OK(create_test_sstable("test_sst_4.sst", 125, 50, &sst4)); // Overlaps with sst2

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1, sst2}); // First fileset
    candidates.push_back({sst3, sst4}); // Second fileset

    // Set threshold to force splitting into multiple tasks
    auto old_threshold = config::pk_index_parallel_compaction_task_split_threshold_bytes;
    config::pk_index_parallel_compaction_task_split_threshold_bytes = 1; // Very small threshold

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    config::pk_index_parallel_compaction_task_split_threshold_bytes = old_threshold;

    // Should successfully merge multiple filesets
    ASSERT_TRUE(output_sstables.size() == 4);
    // check range
    ASSERT_EQ(output_sstables[0].range().start_key(), sst1.range().start_key());
    ASSERT_EQ(output_sstables[3].range().end_key(), sst4.range().end_key());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_with_merge_base_level) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create test sstables
    PersistentIndexSstablePB sst1, sst2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 50, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 100, 50, &sst2));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1, sst2});

    std::vector<PersistentIndexSstablePB> output_sstables;
    // Test with merge_base_level = true
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, true, &output_sstables));

    ASSERT_EQ(output_sstables.size(), 2);
    // check ranges
    ASSERT_EQ(output_sstables[0].range().start_key(), sst1.range().start_key());
    ASSERT_EQ(output_sstables[0].range().end_key(), sst1.range().end_key());
    ASSERT_EQ(output_sstables[1].range().start_key(), sst2.range().start_key());
    ASSERT_EQ(output_sstables[1].range().end_key(), sst2.range().end_key());
    // fileset ids should be updated
    ASSERT_TRUE(UniqueId(sst1.fileset_id()) != UniqueId(output_sstables[0].fileset_id()));
    ASSERT_TRUE(UniqueId(sst2.fileset_id()) != UniqueId(output_sstables[1].fileset_id()));
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_parallel_execution) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create many non-overlapping sstables to trigger parallel execution
    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    std::vector<PersistentIndexSstablePB> fileset;

    for (int i = 0; i < 10; i++) {
        PersistentIndexSstablePB sst;
        ASSERT_OK(create_test_sstable(fmt::format("test_sst_{}.sst", i), i * 1000, 100, &sst));
        fileset.push_back(sst);
    }
    candidates.push_back(fileset);

    // Set threshold to force splitting into multiple tasks
    auto old_threshold = config::pk_index_parallel_compaction_task_split_threshold_bytes;
    config::pk_index_parallel_compaction_task_split_threshold_bytes = 1;

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    // Should have outputs
    ASSERT_EQ(output_sstables.size(), 10);

    // Verify outputs are sorted by start_key
    for (size_t i = 1; i < output_sstables.size(); i++) {
        ASSERT_LT(output_sstables[i - 1].range().start_key(), output_sstables[i].range().start_key());
    }

    config::pk_index_parallel_compaction_task_split_threshold_bytes = old_threshold;
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_with_threshold_control) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create sstables
    PersistentIndexSstablePB sst1, sst2, sst3;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 1000, 100, &sst2));
    ASSERT_OK(create_test_sstable("test_sst_3.sst", 2000, 100, &sst3));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1});
    candidates.push_back({sst2});
    candidates.push_back({sst3});

    // Test with very large threshold - should merge into single task
    auto old_threshold = config::pk_index_parallel_compaction_task_split_threshold_bytes;
    config::pk_index_parallel_compaction_task_split_threshold_bytes = 1024 * 1024 * 1024; // 1GB

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    ASSERT_EQ(output_sstables.size(), 1);
    // check range
    ASSERT_EQ(output_sstables[0].range().start_key(), sst1.range().start_key());
    ASSERT_EQ(output_sstables[0].range().end_key(), sst3.range().end_key());

    config::pk_index_parallel_compaction_task_split_threshold_bytes = old_threshold;
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_compact_with_empty_sstable) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create empty sstable (0 entries)
    PersistentIndexSstablePB sst1, sst2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 0, &sst1)); // Empty
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 100, 50, &sst2));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1});
    candidates.push_back({sst2});

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    // Should handle empty sstables gracefully
    ASSERT_EQ(output_sstables.size(), 1);
    // check range
    ASSERT_EQ(output_sstables[0].range().start_key(), sst2.range().start_key());
    ASSERT_EQ(output_sstables[0].range().end_key(), sst2.range().end_key());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_sstable_without_range) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create sstable without range (infinite boundary)
    PersistentIndexSstablePB sst1;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));
    sst1.clear_range(); // Clear the range to simulate infinite boundary

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1});

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    // Should create single task without parallel splitting
    ASSERT_EQ(output_sstables.size(), 1);
    // should reuse the sstable
    ASSERT_EQ(output_sstables[0].filename(), sst1.filename());
}

// ============================================================================
// Tests for LakePersistentIndexParallelCompactTask
// ============================================================================

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_with_empty_input) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());
    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(
            input_sstables, _tablet_mgr.get(), _tablet_metadata, false, fileset_id, seek_range);

    auto cb = std::make_unique<AsyncCompactCB>(
            mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
            [](const std::vector<PersistentIndexSstablePB>& sstables) { return Status::OK(); });
    task->set_cb(cb.get());
    // Empty input should return error
    ASSERT_OK(cb->thread_pool_token()->submit(task));
    auto st = cb->wait_for();
    ASSERT_FALSE(st.ok());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_with_null_tablet_mgr) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    PersistentIndexSstablePB sst1;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));

    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    input_sstables.push_back({sst1});
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(input_sstables, nullptr, _tablet_metadata,
                                                                         false, fileset_id, seek_range);

    auto cb = std::make_unique<AsyncCompactCB>(
            mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
            [](const std::vector<PersistentIndexSstablePB>& sstables) { return Status::OK(); });
    task->set_cb(cb.get());
    // Null tablet_mgr should return error
    ASSERT_OK(cb->thread_pool_token()->submit(task));
    auto st = cb->wait_for();
    ASSERT_FALSE(st.ok());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_single_sstable_optimization) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    PersistentIndexSstablePB sst1;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));

    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    input_sstables.push_back({sst1});
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(
            input_sstables, _tablet_mgr.get(), _tablet_metadata, false, fileset_id, seek_range);

    std::vector<PersistentIndexSstablePB> output;
    auto cb = std::make_unique<AsyncCompactCB>(mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
                                               [&](const std::vector<PersistentIndexSstablePB>& sstables) {
                                                   output = sstables;
                                                   return Status::OK();
                                               });
    task->set_cb(cb.get());
    ASSERT_OK(cb->thread_pool_token()->submit(task));

    // Single sstable should be reused without merge
    ASSERT_OK(cb->wait_for());
    ASSERT_EQ(1, output.size());
    ASSERT_EQ(sst1.filename(), output[0].filename());
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_multiple_sstables) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    PersistentIndexSstablePB sst1, sst2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 50, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 100, 50, &sst2));

    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    input_sstables.push_back({sst1, sst2});
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    std::vector<PersistentIndexSstablePB> output;
    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(
            input_sstables, _tablet_mgr.get(), _tablet_metadata, false, fileset_id, seek_range);
    auto cb = std::make_unique<AsyncCompactCB>(mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
                                               [&](const std::vector<PersistentIndexSstablePB>& sstables) {
                                                   output = sstables;
                                                   return Status::OK();
                                               });
    task->set_cb(cb.get());
    ASSERT_OK(cb->thread_pool_token()->submit(task));

    ASSERT_OK(cb->wait_for());
    // Should successfully merge multiple sstables
    ASSERT_GE(output.size(), 1);

    // Verify output fileset_id matches
    for (const auto& sst : output) {
        ASSERT_TRUE(sst.has_fileset_id());
        ASSERT_EQ(fileset_id.to_string(), UniqueId(sst.fileset_id()).to_string());
    }
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_with_overlapping_data) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());
    PersistentIndexSstablePB sst1, sst2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 50, 100, &sst2)); // Overlaps with sst1

    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    input_sstables.push_back({sst1, sst2});
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    std::vector<PersistentIndexSstablePB> output;
    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(
            input_sstables, _tablet_mgr.get(), _tablet_metadata, false, fileset_id, seek_range);
    auto cb = std::make_unique<AsyncCompactCB>(mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
                                               [&](const std::vector<PersistentIndexSstablePB>& sstables) {
                                                   output = sstables;
                                                   return Status::OK();
                                               });
    task->set_cb(cb.get());
    ASSERT_OK(cb->thread_pool_token()->submit(task));
    ASSERT_OK(cb->wait_for());

    ASSERT_GE(output.size(), 1);
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_with_merge_base_level) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());
    PersistentIndexSstablePB sst1, sst2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 50, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 100, 50, &sst2));

    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    input_sstables.push_back({sst1, sst2});
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    // Test with merge_base_level = true
    std::vector<PersistentIndexSstablePB> output;
    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(
            input_sstables, _tablet_mgr.get(), _tablet_metadata, true, fileset_id, seek_range);
    auto cb = std::make_unique<AsyncCompactCB>(mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
                                               [&](const std::vector<PersistentIndexSstablePB>& sstables) {
                                                   output = sstables;
                                                   return Status::OK();
                                               });
    task->set_cb(cb.get());
    ASSERT_OK(cb->thread_pool_token()->submit(task));
    ASSERT_OK(cb->wait_for());

    ASSERT_GE(output.size(), 1);
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_multiple_filesets) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());
    PersistentIndexSstablePB sst1, sst2, sst3, sst4;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 25, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 50, 25, &sst2));
    ASSERT_OK(create_test_sstable("test_sst_3.sst", 0, 30, &sst3));
    ASSERT_OK(create_test_sstable("test_sst_4.sst", 50, 30, &sst4));

    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    input_sstables.push_back({sst1, sst2}); // Fileset 1
    input_sstables.push_back({sst3, sst4}); // Fileset 2
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    std::vector<PersistentIndexSstablePB> output;
    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(
            input_sstables, _tablet_mgr.get(), _tablet_metadata, false, fileset_id, seek_range);
    auto cb = std::make_unique<AsyncCompactCB>(mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
                                               [&](const std::vector<PersistentIndexSstablePB>& sstables) {
                                                   output = sstables;
                                                   return Status::OK();
                                               });
    task->set_cb(cb.get());
    ASSERT_OK(cb->thread_pool_token()->submit(task));
    ASSERT_OK(cb->wait_for());

    ASSERT_EQ(output.size(), 1);
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_task_run_with_zero_size_sstable) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());
    PersistentIndexSstablePB sst1, sst2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 0, &sst1)); // Empty sstable
    sst1.set_filesize(0);                                          // Explicitly set to 0
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 100, 50, &sst2));

    std::vector<std::vector<PersistentIndexSstablePB>> input_sstables;
    input_sstables.push_back({sst1, sst2});
    auto fileset_id = UniqueId::gen_uid();
    SeekRange seek_range{"", ""}; // Empty range means full range

    std::vector<PersistentIndexSstablePB> output;
    auto task = std::make_shared<LakePersistentIndexParallelCompactTask>(
            input_sstables, _tablet_mgr.get(), _tablet_metadata, false, fileset_id, seek_range);
    auto cb = std::make_unique<AsyncCompactCB>(mgr->thread_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT),
                                               [&](const std::vector<PersistentIndexSstablePB>& sstables) {
                                                   output = sstables;
                                                   return Status::OK();
                                               });
    task->set_cb(cb.get());
    ASSERT_OK(cb->thread_pool_token()->submit(task));
    ASSERT_OK(cb->wait_for());

    // Should skip zero-size sstables
    ASSERT_TRUE(output.size() > 0);
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_output_sstables_have_valid_ranges) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    PersistentIndexSstablePB sst1, sst2, sst3;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 50, &sst1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 100, 50, &sst2));
    ASSERT_OK(create_test_sstable("test_sst_3.sst", 200, 50, &sst3));

    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    candidates.push_back({sst1, sst2, sst3});

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    // Verify all outputs have valid ranges
    for (const auto& sst : output_sstables) {
        ASSERT_TRUE(sst.has_range());
        ASSERT_FALSE(sst.range().start_key().empty());
        ASSERT_FALSE(sst.range().end_key().empty());
        // start_key should be <= end_key
        ASSERT_LE(sst.range().start_key(), sst.range().end_key());
    }
}

TEST_F(LakePersistentIndexParallelCompactMgrTest, test_concurrent_compaction_tasks) {
    auto mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
    ASSERT_OK(mgr->init());

    // Create many sstables to trigger parallel tasks
    std::vector<std::vector<PersistentIndexSstablePB>> candidates;
    std::vector<PersistentIndexSstablePB> fileset;

    for (int i = 0; i < 20; i++) {
        PersistentIndexSstablePB sst;
        // Create non-overlapping ranges
        ASSERT_OK(create_test_sstable(fmt::format("test_sst_{}.sst", i), i * 500, 50, &sst));
        fileset.push_back(sst);
    }
    candidates.push_back(fileset);

    // Force small threshold to create multiple tasks
    auto old_threshold = config::pk_index_parallel_compaction_task_split_threshold_bytes;
    config::pk_index_parallel_compaction_task_split_threshold_bytes = 1;

    std::vector<PersistentIndexSstablePB> output_sstables;
    ASSERT_OK(mgr->compact(candidates, _tablet_metadata, false, &output_sstables));

    // Should have multiple outputs
    ASSERT_EQ(output_sstables.size(), 20);

    // Verify outputs are sorted and non-overlapping
    for (size_t i = 1; i < output_sstables.size(); i++) {
        ASSERT_LE(output_sstables[i - 1].range().end_key(), output_sstables[i].range().start_key());
    }

    config::pk_index_parallel_compaction_task_split_threshold_bytes = old_threshold;
}

} // namespace starrocks::lake

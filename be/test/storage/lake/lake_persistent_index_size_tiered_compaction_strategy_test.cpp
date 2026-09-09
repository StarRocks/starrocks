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

#include "storage/lake/lake_persistent_index_size_tiered_compaction_strategy.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "testutil/assert.h"

namespace starrocks::lake {

class LakePersistentIndexSizeTieredCompactionStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original config values
        _original_min_level_size = config::pk_index_size_tiered_min_level_size;
        _original_level_multiple = config::pk_index_size_tiered_level_multiplier;
        _original_level_num = config::pk_index_size_tiered_max_level;

        // Set default config values for tests
        config::pk_index_size_tiered_min_level_size = 131072; // 128KB
        config::pk_index_size_tiered_level_multiplier = 5;
        config::pk_index_size_tiered_max_level = 7;
    }

    void TearDown() override {
        // Restore original config values
        config::pk_index_size_tiered_min_level_size = _original_min_level_size;
        config::pk_index_size_tiered_level_multiplier = _original_level_multiple;
        config::pk_index_size_tiered_max_level = _original_level_num;
    }

    // Helper function to create a TabletMetadata with sstables
    TabletMetadataPtr create_tablet_metadata_with_sstables(
            const std::vector<std::tuple<int64_t, int64_t, uint64_t>>& sstables_info) {
        // tuple: (fileset_id_hi, filesize, max_rss_rowid)
        // For simplicity, we use hi as fileset_id and set lo to 0
        auto metadata = std::make_shared<TabletMetadata>();
        auto* sstable_meta = metadata->mutable_sstable_meta();

        for (const auto& [fileset_id_hi, filesize, max_rss_rowid] : sstables_info) {
            auto* sstable = sstable_meta->add_sstables();
            auto* fileset_id = sstable->mutable_fileset_id();
            fileset_id->set_hi(fileset_id_hi);
            fileset_id->set_lo(0);
            sstable->set_filesize(filesize);
            sstable->set_max_rss_rowid(max_rss_rowid);
        }

        return metadata;
    }

private:
    int64_t _original_min_level_size;
    int32_t _original_level_multiple;
    int32_t _original_level_num;
};

// Test 1: Empty sstables list - no compaction needed
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_empty_sstables) {
    auto metadata = std::make_shared<TabletMetadata>();

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(0, result.candidate_filesets.size());
    EXPECT_FALSE(result.merge_base_level);
}

// Test 2: Single fileset - no compaction needed
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_single_fileset) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 100000, 100}, // fileset_id_hi=1, size=100KB, max_rss_rowid=100
            {1, 50000, 50},   // same fileset
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(0, result.candidate_filesets.size());
}

// Test 3: Two filesets with same size - should be selected for compaction
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_two_filesets_same_size) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 100000, 100}, // fileset 1: 100KB
            {2, 100000, 200}, // fileset 2: 100KB
            {3, 500000, 300}, // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(2, result.candidate_filesets.size());
    EXPECT_TRUE(result.merge_base_level);

    // Check fileset 1
    EXPECT_EQ(1, result.candidate_filesets[0].size());
    EXPECT_EQ(1, result.candidate_filesets[0][0].fileset_id().hi());
    EXPECT_EQ(0, result.candidate_filesets[0][0].fileset_id().lo());
    EXPECT_EQ(100000, result.candidate_filesets[0][0].filesize());

    // Check fileset 2
    EXPECT_EQ(1, result.candidate_filesets[1].size());
    EXPECT_EQ(2, result.candidate_filesets[1][0].fileset_id().hi());
    EXPECT_EQ(0, result.candidate_filesets[1][0].fileset_id().lo());
    EXPECT_EQ(100000, result.candidate_filesets[1][0].filesize());
}

// Test 4: Multiple sstables in same fileset
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_multiple_sstables_per_fileset) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 50000, 100},  // fileset 1
            {1, 30000, 150},  // fileset 1
            {1, 20000, 200},  // fileset 1, total: 100KB
            {2, 100000, 300}, // fileset 2: 100KB
            {3, 100000, 300}, // fileset 3: active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(2, result.candidate_filesets.size());
    EXPECT_TRUE(result.merge_base_level);

    // Check fileset 1 has 3 sstables
    EXPECT_EQ(3, result.candidate_filesets[0].size());
    EXPECT_EQ(1, result.candidate_filesets[0][0].fileset_id().hi());
    EXPECT_EQ(1, result.candidate_filesets[0][1].fileset_id().hi());
    EXPECT_EQ(1, result.candidate_filesets[0][2].fileset_id().hi());

    // Check fileset 2 has 1 sstable
    EXPECT_EQ(1, result.candidate_filesets[1].size());
    EXPECT_EQ(2, result.candidate_filesets[1][0].fileset_id().hi());
}

// Test 5: Three filesets with similar sizes - all should be selected
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_three_filesets_similar_size) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 100000, 100}, {2, 110000, 200}, {3, 95000, 300}, {4, 500000, 400}, // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(3, result.candidate_filesets.size());
    EXPECT_TRUE(result.merge_base_level);
}

// Test 6: Different size levels - should select one level
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_different_size_levels) {
    // Create filesets with different sizes
    // Level 1: 100KB, 110KB (similar size)
    // Level 2: 500KB (much larger)
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {3, 500000, 300}, {1, 100000, 100}, {2, 110000, 200}, {4, 110000, 200} // Much larger, different level
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should select the smaller filesets (higher score due to more filesets and smaller level)
    EXPECT_EQ(2, result.candidate_filesets.size());
    EXPECT_FALSE(result.merge_base_level);
}

// Test 7: Size-tiered with multiple levels - select highest score
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_multiple_levels_highest_score) {
    config::pk_index_size_tiered_min_level_size = 100000; // 100KB
    config::pk_index_size_tiered_level_multiplier = 5;

    // Level 1: 3 filesets of ~100KB each (high score due to many filesets)
    // Level 2: 2 filesets of ~500KB each
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {4, 500000, 400}, {5, 510000, 500}, {1, 100000, 100}, {2, 105000, 200}, {3, 98000, 300}, {6, 2600000, 600}};

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should select level with highest score (3 filesets)
    EXPECT_EQ(3, result.candidate_filesets.size());
    EXPECT_FALSE(result.merge_base_level);
}

// Test 8: Base level not included in compaction
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_base_level_not_included) {
    // First fileset (base level) is much larger and won't be selected
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 5000000, 100}, // Base level: 5MB, very large
            {2, 100000, 200},  // Level 2: 100KB
            {3, 110000, 300},  // Level 2: 110KB
            {4, 105000, 400},  // Level 2: 105KB
            {5, 105000, 400},  // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should select the 3 smaller filesets, not including base level
    EXPECT_EQ(3, result.candidate_filesets.size());
    EXPECT_FALSE(result.merge_base_level); // Base level not included

    // Verify base level is not in candidates
    for (const auto& fileset : result.candidate_filesets) {
        EXPECT_NE(1, fileset[0].fileset_id().hi());
    }
}

// Test 9: Filesets without explicit fileset_id (generates unique IDs)
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_sstables_without_fileset_id) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto* sstable_meta = metadata->mutable_sstable_meta();

    // Add sstables without setting fileset_id
    for (int i = 0; i < 3; ++i) {
        auto* sstable = sstable_meta->add_sstables();
        sstable->set_filesize(100000);
        sstable->set_max_rss_rowid((i + 1) * 100);
        // Don't set fileset_id - should generate unique IDs for each sstable
    }

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // All 3 sstables are in different filesets (each gets a unique generated ID), should be selected
    EXPECT_EQ(3, result.candidate_filesets.size());
    EXPECT_TRUE(result.merge_base_level);
}

// Test 10: Only one fileset meets minimum compaction requirement
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_minimum_compaction_filesets) {
    // Only 1 fileset - shouldn't compact (need at least 2)
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 100000, 100},
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(0, result.candidate_filesets.size());
}

// Test 11: Large number of filesets
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_large_number_of_filesets) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info;

    // Create 10 filesets with similar sizes
    for (int i = 1; i <= 10; ++i) {
        sstables_info.emplace_back(i, 100000 + i * 1000, i * 100);
    }

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should select multiple filesets for compaction
    EXPECT_GT(result.candidate_filesets.size(), 0);
    EXPECT_LE(result.candidate_filesets.size(), 10);
}

// Test 12: Mix of very small and very large filesets
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_mixed_sizes) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {4, 10000000, 400}, // 10MB - much larger
            {1, 10000, 100},    // 10KB
            {2, 15000, 200},    // 15KB
            {3, 12000, 300},    // 12KB
            {5, 100000, 500},   // 100KB - active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should select the 3 small filesets
    EXPECT_EQ(3, result.candidate_filesets.size());
    EXPECT_FALSE(result.merge_base_level);
}

// Test 13: Sequential fileset IDs vs non-sequential
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_non_sequential_fileset_ids) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {100, 100000, 100}, {200, 100000, 200}, {300, 100000, 300}, {400, 500000, 400}, // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(3, result.candidate_filesets.size());
    EXPECT_TRUE(result.merge_base_level);

    // Check fileset IDs are preserved
    EXPECT_EQ(100, result.candidate_filesets[0][0].fileset_id().hi());
    EXPECT_EQ(200, result.candidate_filesets[1][0].fileset_id().hi());
    EXPECT_EQ(300, result.candidate_filesets[2][0].fileset_id().hi());
}

// Test 14: Zero-size sstables
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_zero_size_sstables) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 0, 100},      // Zero size
            {2, 0, 200},      // Zero size
            {3, 100000, 300}, // Normal size
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should handle zero-size sstables gracefully
    EXPECT_GT(result.candidate_filesets.size(), 0);
}

// Test 15: Verify max_rss_rowid calculation with multiple sstables
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_max_rss_rowid_calculation) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 100000, 50},  {1, 100000, 999},  // Max in fileset 1
            {1, 100000, 100}, {2, 100000, 1500}, // Overall max
            {2, 100000, 200}, {3, 100000, 300},  // Max in fileset 3
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(2, result.candidate_filesets.size());
}

// Test 16: Consecutive levels with increasing sizes
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_consecutive_levels) {
    config::pk_index_size_tiered_min_level_size = 100000;
    config::pk_index_size_tiered_level_multiplier = 5;

    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            // Level 1: ~100KB
            {1, 100000, 100},
            {2, 105000, 200},
            // Level 2: ~500KB (100KB * 5)
            {3, 500000, 300},
            {4, 520000, 400},
            // Level 3: ~2.5MB (500KB * 5)
            {5, 2500000, 500},
            {6, 2600000, 600}, // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should select one of the levels with 5 filesets
    EXPECT_EQ(5, result.candidate_filesets.size());
}

// Test 17: Edge case - all filesets same size
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_all_same_size) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info;

    // Create 5 filesets with exactly the same size
    for (int i = 1; i <= 6; ++i) {
        sstables_info.emplace_back(i, 100000, i * 100);
    }

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // All should be in the same level and selected
    EXPECT_EQ(5, result.candidate_filesets.size());
    EXPECT_TRUE(result.merge_base_level);
}

// Test 18: Verify result is cleared on subsequent calls
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_result_cleared) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 100000, 100}, {2, 100000, 200}, {3, 100000, 300}, // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    // First call
    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));
    EXPECT_EQ(2, result.candidate_filesets.size());

    // Second call with empty metadata
    auto empty_metadata = std::make_shared<TabletMetadata>();
    ASSIGN_OR_ABORT(result, LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(
                                    empty_metadata->sstable_meta()));

    // Result should be cleared
    EXPECT_EQ(0, result.candidate_filesets.size());
}

// Test 19: Stress test with many sstables per fileset
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_many_sstables_per_fileset) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto* sstable_meta = metadata->mutable_sstable_meta();

    // Create 3 filesets, each with 100 sstables
    for (int fileset = 1; fileset <= 3; ++fileset) {
        for (int i = 0; i < 100; ++i) {
            auto* sstable = sstable_meta->add_sstables();
            auto* fileset_id = sstable->mutable_fileset_id();
            fileset_id->set_hi(fileset);
            fileset_id->set_lo(0);
            sstable->set_filesize(1000); // 1KB each
            sstable->set_max_rss_rowid(fileset * 100 + i);
        }
    }

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(2, result.candidate_filesets.size());
    EXPECT_EQ(100, result.candidate_filesets[0].size()); // 100 sstables in fileset 1
    EXPECT_EQ(100, result.candidate_filesets[1].size()); // 100 sstables in fileset 2
}

// Test 20: Boundary test with exact level_multiple threshold
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_level_multiple_threshold) {
    config::pk_index_size_tiered_min_level_size = 100000;
    config::pk_index_size_tiered_level_multiplier = 5;

    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {2, 500000, 200}, // 500KB = 100KB * 5 (exact threshold)
            {3, 500000, 300}, // 500KB
            {1, 100000, 100}, // 100KB
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // Should separate into different levels based on size
    EXPECT_EQ(result.candidate_filesets.size(), 2);
}

// Test 21: The highest-scoring level holds a single fileset, a lower-scoring level holds two.
//
// Regression test for the index compactor declining to compact anything at all in this layout.
// The level bonus rewards small levels by up to pk_index_size_tiered_max_level, so a single small
// fileset can outscore an older level of two large filesets and, because only the top level was
// considered, that round produced no index compaction at all while the flush side kept adding
// sstables.
//
// NOTE: the scores below are computed with THIS FIXTURE's config (SetUp overrides
// pk_index_size_tiered_level_multiplier to 5 and pk_index_size_tiered_max_level to 7), not with the
// shipped defaults of 10 and 5. At the shipped defaults the 200KB level scores 6, below the
// two-fileset level's 7, so this exact layout is not starved on a real cluster.
// test_falls_back_at_product_default_config below covers a layout that is.
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_falls_back_when_top_level_has_one_fileset) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 104857600, 100}, // fileset 1: 100MB  -- base, level {1,2}, score 3*2-2+3 = 7
            {2, 104857600, 200}, // fileset 2: 100MB
            {3, 204800, 300},    // fileset 3: 200KB  -- own level, alone, score 3*1-2+7 = 8
            {4, 500000, 400},    // active, never a candidate
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    // The single-fileset level cannot be compacted, so the two-fileset level must be taken instead.
    ASSERT_EQ(2, result.candidate_filesets.size());
    EXPECT_EQ(1, result.candidate_filesets[0][0].fileset_id().hi());
    EXPECT_EQ(2, result.candidate_filesets[1][0].fileset_id().hi());
    EXPECT_TRUE(result.merge_base_level);
    EXPECT_EQ(200, result.max_max_rss_rowid);
}

// Test 22: Every level holds exactly one fileset -- the fallback must still decline.
// Guards the other direction of test 21: falling back may not lower the two-fileset minimum.
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_no_fallback_when_every_level_has_one_fileset) {
    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 104857600, 100}, // 100MB
            {2, 4194304, 200},   // 4MB   -- 25x smaller, own level
            {3, 204800, 300},    // 200KB -- 20x smaller, own level
            {4, 500000, 400},    // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    EXPECT_EQ(0, result.candidate_filesets.size());
    EXPECT_FALSE(result.merge_base_level);
}

// Test 23: the same starvation, reached at the SHIPPED config rather than this fixture's.
//
// The defaults are pk_index_size_tiered_min_level_size = 131072, _level_multiplier = 10 and
// _max_level = 5, so max_level_size = 131072 * 10^5. There the level bonus tops out at 6 and a
// single small fileset can only *tie* a two-fileset level, never outscore it:
//
//   two 100MB filesets   -> level_bonus 3, score 3*2-2+3 = 7
//   one 200KB fileset    -> level_bonus 5, score 3*1-2+5 = 6   (not starved)
//   one fileset < 131072 -> level_bonus 6, score 3*1-2+6 = 7   (ties)
//
// A tie is enough, because LevelComparator breaks it on `fileset_indexes[0] >` and filesets are
// ordered oldest-first, so the newest single-fileset level sorts ahead of the compactable one and
// *priority_levels.begin() lands on the level that cannot be compacted. The boundary is exact:
// 131071 bytes reproduces it, 131072 does not.
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_falls_back_at_product_default_config) {
    config::pk_index_size_tiered_level_multiplier = 10;
    config::pk_index_size_tiered_max_level = 5;
    // min_level_size is already the shipped 131072; TearDown restores all three.

    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 104857600, 100}, // fileset 1: 100MB   -- base, level {1,2}, score 7
            {2, 104857600, 200}, // fileset 2: 100MB
            {3, 131071, 300},    // fileset 3: one byte under min_level_size -- own level, score 7
            {4, 500000, 400},    // active, never a candidate
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    ASSERT_EQ(2, result.candidate_filesets.size());
    EXPECT_EQ(1, result.candidate_filesets[0][0].fileset_id().hi());
    EXPECT_EQ(2, result.candidate_filesets[1][0].fileset_id().hi());
    EXPECT_TRUE(result.merge_base_level);
    EXPECT_EQ(200, result.max_max_rss_rowid);
}

// Test 24: the other side of test 23's boundary. At the shipped config a fileset of exactly
// min_level_size scores 6, strictly below the two-fileset level's 7, so the pre-existing code
// already selected the compactable level and the fallback changes nothing here.
TEST_F(LakePersistentIndexSizeTieredCompactionStrategyTest, test_no_starvation_at_min_level_size_boundary) {
    config::pk_index_size_tiered_level_multiplier = 10;
    config::pk_index_size_tiered_max_level = 5;

    std::vector<std::tuple<int64_t, int64_t, uint64_t>> sstables_info = {
            {1, 104857600, 100}, // 100MB -- level {1,2}, score 7
            {2, 104857600, 200}, // 100MB
            {3, 131072, 300},    // exactly min_level_size -- own level, score 6
            {4, 500000, 400},    // active
    };

    auto metadata = create_tablet_metadata_with_sstables(sstables_info);

    ASSIGN_OR_ABORT(
            CompactionCandidateResult result,
            LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(metadata->sstable_meta()));

    ASSERT_EQ(2, result.candidate_filesets.size());
    EXPECT_EQ(1, result.candidate_filesets[0][0].fileset_id().hi());
    EXPECT_EQ(2, result.candidate_filesets[1][0].fileset_id().hi());
}

} // namespace starrocks::lake

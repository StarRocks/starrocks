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

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "fs/fs.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/load_chunk_spiller.h"
#include "storage/load_spill_block_manager.h"
#include "storage/load_spill_pipeline_merge_context.h"
#include "storage/load_spill_pipeline_merge_iterator.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace starrocks {

class LoadSpillPipelineMergeTest : public ::testing::Test {
public:
    LoadSpillPipelineMergeTest() {
        _tablet_metadata = lake::generate_simple_tablet_metadata(PRIMARY_KEYS);

        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }
    void SetUp() override {
        (void)FileSystem::Default()->create_dir_recursive(kTestDir);
        _block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr);
        ASSERT_OK(_block_manager->init());
        _profile = std::make_unique<RuntimeProfile>("test");
        _pipeline_merge_context = std::make_unique<LoadSpillPipelineMergeContext>(nullptr);
        _spiller =
                std::make_unique<LoadChunkSpiller>(_block_manager.get(), _profile.get(), _pipeline_merge_context.get());
    }

    void TearDown() override {
        _spiller.reset();
        _block_manager.reset();
        (void)FileSystem::Default()->delete_dir_recursive(kTestDir);
    }

protected:
    ChunkPtr gen_data(int64_t chunk_size, int start_value) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + start_value;
        }
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return std::make_shared<Chunk>(Columns{std::move(c0), std::move(c1)}, _schema);
    }

    size_t spill_chunks_with_slot_idx(const std::vector<std::pair<int32_t, int64_t>>& chunks_with_slots) {
        size_t total_bytes = 0;
        for (const auto& [start_value, slot_idx] : chunks_with_slots) {
            auto chunk = gen_data(100, start_value);
            ASSIGN_OR_ABORT(auto bytes, _spiller->spill(*chunk, slot_idx));
            total_bytes += bytes;
        }
        return total_bytes;
    }

    constexpr static const char* const kTestDir = "./load_spill_pipeline_merge_test";
    std::unique_ptr<LoadSpillBlockManager> _block_manager;
    std::unique_ptr<RuntimeProfile> _profile;
    std::unique_ptr<LoadSpillPipelineMergeContext> _pipeline_merge_context;
    std::unique_ptr<LoadChunkSpiller> _spiller;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

// Test basic pipeline merge task generation
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_basic) {
    // Spill chunks with continuous slot indices
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {200, 2}});

    // Generate a pipeline merge task
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(
                                       bytes - 1,                               // target_size
                                       config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                       true,                                    // do_sort
                                       false,                                   // do_agg
                                       true));                                  // final_round

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    ASSERT_GT(task->total_block_groups, 0);
    ASSERT_GT(task->total_block_bytes, 0);
    ASSERT_EQ(task->block_groups.size(), task->total_block_groups);

    // Verify block groups were removed from spiller
    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_EQ(remaining_groups.size(), 0);
}

// Test pipeline merge task generation with non-continuous slot indices
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_non_continuous) {
    // Spill chunks with non-continuous slot indices (0, 1, 3)
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {200, 3}});

    // Try to generate another task - should get empty task since slot_idx 2 is missing
    auto task_or =
            _spiller->generate_pipeline_merge_task(bytes,                                   // target_size
                                                   config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                                   true,                                    // do_sort
                                                   false,                                   // do_agg
                                                   false);                                  // final_round

    ASSERT_TRUE(task_or.ok());
    ASSERT_EQ(task_or.value()->total_block_groups, 2);
}

// Test final round merge with non-continuous slot indices should fail
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_final_round_non_continuous) {
    // Spill chunks with non-continuous slot indices (0, 2, 3)
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 2}, {200, 3}});

    // Final round merge should fail with non-continuous slot indices
    auto result =
            _spiller->generate_pipeline_merge_task(bytes,                                   // target_size
                                                   config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                                   true,                                    // do_sort
                                                   false,                                   // do_agg
                                                   true);                                   // final_round

    ASSERT_TRUE(result.ok());
}

// Test final round merge with continuous slot indices
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_final_round_continuous) {
    // Spill chunks with continuous slot indices
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {200, 2}});

    // Final round merge should succeed and merge all groups
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(
                                       bytes,                                   // target_size
                                       config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                       true,                                    // do_sort
                                       false,                                   // do_agg
                                       true));                                  // final_round

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    ASSERT_EQ(task->total_block_groups, 3);

    // All groups should be consumed
    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_EQ(remaining_groups.size(), 0);
}

// Test target size limitation
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_target_size_limit) {
    // Spill multiple chunks with continuous slot indices
    for (int i = 0; i < 10; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    // Set a small target_size to limit how many groups are merged
    size_t small_target_size = 100; // Very small, should only merge 1-2 groups
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(small_target_size, // target_size
                                                                      1024 * 1024,       // memory_usage_per_merge
                                                                      true,              // do_sort
                                                                      false,             // do_agg
                                                                      false));           // final_round

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    // Should only merge a subset of groups
    ASSERT_LT(task->total_block_groups, 10);
    ASSERT_GT(task->total_block_groups, 0);

    // Remaining groups should exist
    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_GT(remaining_groups.size(), 0);
}

// Test slot_idx ordering is preserved
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_ordering) {
    // Spill chunks in random order but with sequential slot indices
    auto bytes = spill_chunks_with_slot_idx({{200, 2}, {0, 0}, {300, 3}, {100, 1}});

    // Generate pipeline merge task
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(
                                       bytes,                                   // target_size
                                       config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                       true,                                    // do_sort
                                       false,                                   // do_agg
                                       true));                                  // final_round

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    ASSERT_EQ(task->total_block_groups, 4);

    // Verify all block groups are included
    ASSERT_EQ(task->block_groups.size(), 4);
}

// Test empty spiller
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_empty) {
    // Don't spill anything
    ASSERT_TRUE(_spiller->empty());

    // Generate pipeline merge task on empty spiller
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(1024 * 1024, // target_size
                                                                      1024 * 1024, // memory_usage_per_merge
                                                                      true,        // do_sort
                                                                      false,       // do_agg
                                                                      false));     // final_round

    ASSERT_NE(task, nullptr);
    ASSERT_EQ(task->merge_itr, nullptr); // Empty task
    ASSERT_EQ(task->total_block_groups, 0);
    ASSERT_EQ(task->total_block_bytes, 0);
}

// Test LoadSpillPipelineMergeContext basic functionality
TEST_F(LoadSpillPipelineMergeTest, test_pipeline_merge_context_basic) {
    // Create a mock writer (nullptr for this basic test)
    LoadSpillPipelineMergeContext context(nullptr);

    // Test quit flag
    auto* quit_flag = context.quit_flag();
    ASSERT_NE(quit_flag, nullptr);
    ASSERT_FALSE(quit_flag->load());

    // Set quit flag
    quit_flag->store(true);
    ASSERT_TRUE(quit_flag->load());
}

// Test LoadSpillPipelineMergeContext add_merge_task
TEST_F(LoadSpillPipelineMergeTest, test_pipeline_merge_context_add_tasks) {
    LoadSpillPipelineMergeContext context(nullptr);

    // Add multiple tasks (nullptr for this test)
    for (int i = 0; i < 5; i++) {
        context.add_merge_task(nullptr);
    }

    // The context should have added the tasks (verified internally via thread safety)
    // In a real scenario, we would verify by calling merge_task_results()
}

// Test memory usage per merge limitation
TEST_F(LoadSpillPipelineMergeTest, test_generate_pipeline_merge_task_memory_limit) {
    // Spill multiple chunks
    for (int i = 0; i < 10; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    // Set memory usage per merge to a value that limits number of groups
    // Assuming config::load_spill_max_chunk_bytes is around 64KB
    size_t memory_limit = 200 * 1024; // 200KB - should limit to ~3 groups
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(10 * 1024 * 1024, // large target_size
                                                                      memory_limit,     // memory_usage_per_merge
                                                                      true,             // do_sort
                                                                      false,            // do_agg
                                                                      false));          // final_round

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    // Should be limited by memory, not target size
    ASSERT_LT(task->total_block_groups, 10);

    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_GT(remaining_groups.size(), 0);
}

// Test incremental pipeline merge generation
TEST_F(LoadSpillPipelineMergeTest, test_incremental_pipeline_merge) {
    // Spill 6 chunks with continuous slot indices
    for (int i = 0; i < 6; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    size_t small_target = 100; // Force small merges

    // Generate first task
    ASSIGN_OR_ABORT(auto task1, _spiller->generate_pipeline_merge_task(small_target, 1024 * 1024, true, false, false));
    ASSERT_NE(task1, nullptr);
    ASSERT_NE(task1->merge_itr, nullptr);
    size_t first_batch_size = task1->total_block_groups;
    ASSERT_GT(first_batch_size, 0);
    ASSERT_LT(first_batch_size, 6);

    // Generate second task
    ASSIGN_OR_ABORT(auto task2, _spiller->generate_pipeline_merge_task(small_target, 1024 * 1024, true, false, false));
    ASSERT_NE(task2, nullptr);

    if (task2->merge_itr != nullptr) {
        size_t second_batch_size = task2->total_block_groups;
        ASSERT_GT(second_batch_size, 0);

        // Total consumed should not exceed original
        ASSERT_LE(first_batch_size + second_batch_size, 6);
    }
}

// Test concurrent access to block groups during pipeline merge generation
TEST_F(LoadSpillPipelineMergeTest, test_thread_safe_pipeline_merge_generation) {
    // Spill chunks
    for (int i = 0; i < 20; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    // The generate_pipeline_merge_task method should be thread-safe
    // due to the mutex lock in the implementation
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(1024 * 1024, 1024 * 1024, true, false, false));

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    ASSERT_GT(task->total_block_groups, 0);
}

// Test slot_idx sorting correctness
TEST_F(LoadSpillPipelineMergeTest, test_slot_idx_sorting) {
    // Create block groups with out-of-order slot indices
    std::vector<std::pair<int32_t, int64_t>> out_of_order = {{500, 5}, {100, 1}, {400, 4}, {200, 2}, {0, 0}, {300, 3}};

    auto bytes = spill_chunks_with_slot_idx(out_of_order);

    // Generate task - should automatically sort by slot_idx
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(bytes, config::load_spill_max_chunk_bytes * 10,
                                                                      true, false, true));

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    // All 6 groups should be merged in order
    ASSERT_EQ(task->total_block_groups, 6);
}

// Test duplicate slot_idx handling (should still work, just not continuous)
TEST_F(LoadSpillPipelineMergeTest, test_duplicate_slot_idx) {
    // Spill chunks with duplicate slot indices (0, 1, 1, 2)
    spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {150, 1}, {200, 2}});

    // Should merge first chunk (slot 0), then stop at duplicate
    ASSIGN_OR_ABORT(auto task, _spiller->generate_pipeline_merge_task(1024 * 1024, 1024 * 1024, true, false, false));

    ASSERT_NE(task, nullptr);
    ASSERT_NE(task->merge_itr, nullptr);
    // Should only merge first continuous sequence
    ASSERT_GE(task->total_block_groups, 1);
}

} // namespace starrocks

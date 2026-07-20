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

#include "compute_env/load_spill/load_chunk_spiller.h"

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>
#include <unordered_set>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/runtime_profile.h"
#include "compute_env/load_spill/load_spill_block_manager.h"
#include "compute_env/load_spill/load_spill_slot_tracker.h"
#include "compute_env/spill/dir_manager.h"
#include "compute_env/spill/spiller.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"

namespace starrocks {

class TestLoadSpillSlotTracker : public LoadSpillSlotTracker {
public:
    void mark_slot_ready(int64_t slot_idx) override { _ready_slots.insert(slot_idx); }

    bool is_slot_ready(int64_t from_slot_idx, int64_t to_slot_idx) override {
        for (int64_t i = from_slot_idx; i <= to_slot_idx; ++i) {
            if (i >= 0 && _ready_slots.find(i) == _ready_slots.end()) {
                return false;
            }
        }
        return true;
    }

private:
    std::unordered_set<int64_t> _ready_slots;
};

class LoadChunkSpillerTest : public ::testing::Test {
public:
    LoadChunkSpillerTest() {
        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "c0", TYPE_INT, false));
        fields.emplace_back(std::make_shared<Field>(1, "c1", TYPE_INT, false));
        _schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, std::vector<ColumnId>{});
    }

    void SetUp() override {
        ASSERT_OK(FileSystem::Default()->create_dir_recursive(kTestDir));
        ASSERT_OK(FileSystem::Default()->create_dir_recursive(local_spill_dir()));
        ASSIGN_OR_ABORT(auto local_fs, FileSystemFactory::CreateSharedFromString(local_spill_dir()));
        _local_spill_dir_mgr = std::make_unique<spill::DirManager>(std::vector<std::shared_ptr<spill::Dir>>{
                std::make_shared<spill::Dir>(local_spill_dir(), local_fs, std::numeric_limits<int64_t>::max())});
        _block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr,
                                                                 _local_spill_dir_mgr.get());
        ASSERT_OK(_block_manager->init());
        _profile = std::make_unique<RuntimeProfile>("test");
        _slot_tracker = std::make_unique<TestLoadSpillSlotTracker>();
        _spiller = std::make_unique<LoadChunkSpiller>(_block_manager.get(), _profile.get(), _slot_tracker.get());
    }

    void TearDown() override {
        _spiller.reset();
        _block_manager.reset();
        _slot_tracker.reset();
        _local_spill_dir_mgr.reset();
        (void)FileSystem::Default()->delete_dir_recursive(kTestDir);
    }

protected:
    std::string local_spill_dir() const { return std::string(kTestDir) + "/local_spill"; }

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

    constexpr static const char* const kTestDir = "./load_chunk_spiller_test";
    std::unique_ptr<spill::DirManager> _local_spill_dir_mgr;
    std::unique_ptr<LoadSpillBlockManager> _block_manager;
    std::unique_ptr<RuntimeProfile> _profile;
    std::unique_ptr<TestLoadSpillSlotTracker> _slot_tracker;
    std::unique_ptr<LoadChunkSpiller> _spiller;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LoadChunkSpillerTest, test_spill_without_query_context_uses_local_spill_counter) {
    auto chunk = gen_data(100, 0);

    auto result = _spiller->spill(*chunk, 0);
    ASSERT_OK(result.status());
    ASSERT_GT(result.value(), 0);
    ASSERT_NE(nullptr, _spiller->spiller());
    ASSERT_NE(nullptr, _spiller->spiller()->metrics().total_spill_bytes);
}

// Test basic pipeline merge task generation
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_basic) {
    // Spill chunks with continuous slot indices
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {200, 2}});

    // Generate a pipeline merge task
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(
                                       bytes - 1,                               // target_size
                                       config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                       true,                                    // do_sort
                                       false,                                   // do_agg
                                       true));                                  // final_round

    ASSERT_NE(task.merge_itr, nullptr);
    ASSERT_GT(task.total_block_groups, 0);
    ASSERT_GT(task.total_block_bytes, 0);
    ASSERT_EQ(task.block_groups.size(), task.total_block_groups);

    // Verify block groups were removed from spiller
    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_EQ(remaining_groups.size(), 0);
}

// Test pipeline merge task generation with non-continuous slot indices
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_non_continuous) {
    // Spill chunks with non-continuous slot indices (0, 1, 3)
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {200, 3}});

    // Try to generate another task - should get empty task since slot_idx 2 is missing
    auto task_or =
            _spiller->generate_merge_input_batch(bytes,                                   // target_size
                                                 config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                                 true,                                    // do_sort
                                                 false,                                   // do_agg
                                                 false);                                  // final_round

    ASSERT_TRUE(task_or.ok());
    ASSERT_EQ(task_or.value().total_block_groups, 2);
}

// Test final round merge with non-continuous slot indices should fail
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_final_round_non_continuous) {
    // Spill chunks with non-continuous slot indices (0, 2, 3)
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 2}, {200, 3}});

    // Final round merge should fail with non-continuous slot indices
    auto result =
            _spiller->generate_merge_input_batch(bytes,                                   // target_size
                                                 config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                                 true,                                    // do_sort
                                                 false,                                   // do_agg
                                                 true);                                   // final_round

    ASSERT_TRUE(result.ok());
}

// Test final round merge with continuous slot indices
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_final_round_continuous) {
    // Spill chunks with continuous slot indices
    auto bytes = spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {200, 2}});

    // Final round merge should succeed and merge all groups
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(
                                       bytes,                                   // target_size
                                       config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                       true,                                    // do_sort
                                       false,                                   // do_agg
                                       true));                                  // final_round

    ASSERT_NE(task.merge_itr, nullptr);
    ASSERT_EQ(task.total_block_groups, 3);

    // All groups should be consumed
    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_EQ(remaining_groups.size(), 0);
}

// Test target size limitation
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_target_size_limit) {
    // Spill multiple chunks with continuous slot indices
    for (int i = 0; i < 10; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    // Set a small target_size to limit how many groups are merged
    size_t small_target_size = 100; // Very small, should only merge 1-2 groups
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(small_target_size, // target_size
                                                                    1024 * 1024,       // memory_usage_per_merge
                                                                    true,              // do_sort
                                                                    false,             // do_agg
                                                                    false));           // final_round

    ASSERT_NE(task.merge_itr, nullptr);
    // Should only merge a subset of groups
    ASSERT_LT(task.total_block_groups, 10);
    ASSERT_GT(task.total_block_groups, 0);

    // Remaining groups should exist
    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_GT(remaining_groups.size(), 0);
}

// Test slot_idx ordering is preserved
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_ordering) {
    // Spill chunks in random order but with sequential slot indices
    auto bytes = spill_chunks_with_slot_idx({{200, 2}, {0, 0}, {300, 3}, {100, 1}});

    // Generate pipeline merge task
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(
                                       bytes,                                   // target_size
                                       config::load_spill_max_chunk_bytes * 10, // memory_usage_per_merge
                                       true,                                    // do_sort
                                       false,                                   // do_agg
                                       true));                                  // final_round

    ASSERT_NE(task.merge_itr, nullptr);
    ASSERT_EQ(task.total_block_groups, 4);

    // Verify all block groups are included
    ASSERT_EQ(task.block_groups.size(), 4);
}

// Test empty spiller
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_empty) {
    // Don't spill anything
    ASSERT_TRUE(_spiller->empty());

    // Generate pipeline merge task on empty spiller
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(1024 * 1024, // target_size
                                                                    1024 * 1024, // memory_usage_per_merge
                                                                    true,        // do_sort
                                                                    false,       // do_agg
                                                                    false));     // final_round

    ASSERT_EQ(task.merge_itr, nullptr); // Empty task
    ASSERT_EQ(task.total_block_groups, 0);
    ASSERT_EQ(task.total_block_bytes, 0);
}

// Test memory usage per merge limitation
TEST_F(LoadChunkSpillerTest, test_generate_merge_input_batch_memory_limit) {
    // Spill multiple chunks
    for (int i = 0; i < 10; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    // Set memory usage per merge to a value that limits number of groups
    // Assuming config::load_spill_max_chunk_bytes is around 64KB
    size_t memory_limit = 200 * 1024; // 200KB - should limit to ~3 groups
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(10 * 1024 * 1024, // large target_size
                                                                    memory_limit,     // memory_usage_per_merge
                                                                    true,             // do_sort
                                                                    false,            // do_agg
                                                                    false));          // final_round

    ASSERT_NE(task.merge_itr, nullptr);
    // Should be limited by memory, not target size
    ASSERT_LT(task.total_block_groups, 10);

    auto& remaining_groups = _block_manager->block_container()->block_groups();
    ASSERT_GT(remaining_groups.size(), 0);
}

// Test incremental pipeline merge generation
TEST_F(LoadChunkSpillerTest, test_incremental_pipeline_merge) {
    // Spill 6 chunks with continuous slot indices
    for (int i = 0; i < 6; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    size_t small_target = 100; // Force small merges

    // Generate first task
    ASSIGN_OR_ABORT(auto task1, _spiller->generate_merge_input_batch(small_target, 1024 * 1024, true, false, false));
    ASSERT_NE(task1.merge_itr, nullptr);
    size_t first_batch_size = task1.total_block_groups;
    ASSERT_GT(first_batch_size, 0);
    ASSERT_LT(first_batch_size, 6);

    // Generate second task
    ASSIGN_OR_ABORT(auto task2, _spiller->generate_merge_input_batch(small_target, 1024 * 1024, true, false, false));

    if (task2.merge_itr != nullptr) {
        size_t second_batch_size = task2.total_block_groups;
        ASSERT_GT(second_batch_size, 0);

        // Total consumed should not exceed original
        ASSERT_LE(first_batch_size + second_batch_size, 6);
    }
}

// Test concurrent access to block groups during pipeline merge generation
TEST_F(LoadChunkSpillerTest, test_thread_safe_pipeline_merge_generation) {
    // Spill chunks
    for (int i = 0; i < 20; i++) {
        spill_chunks_with_slot_idx({{i * 100, i}});
    }

    // The generate_merge_input_batch method should be thread-safe
    // due to the mutex lock in the implementation
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(1024 * 1024, 1024 * 1024, true, false, false));

    ASSERT_NE(task.merge_itr, nullptr);
    ASSERT_GT(task.total_block_groups, 0);
}

// Test slot_idx sorting correctness
TEST_F(LoadChunkSpillerTest, test_slot_idx_sorting) {
    // Create block groups with out-of-order slot indices
    std::vector<std::pair<int32_t, int64_t>> out_of_order = {{500, 5}, {100, 1}, {400, 4}, {200, 2}, {0, 0}, {300, 3}};

    auto bytes = spill_chunks_with_slot_idx(out_of_order);

    // Generate task - should automatically sort by slot_idx
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(bytes, config::load_spill_max_chunk_bytes * 10,
                                                                    true, false, true));

    ASSERT_NE(task.merge_itr, nullptr);
    // All 6 groups should be merged in order
    ASSERT_EQ(task.total_block_groups, 6);
}

// Test duplicate slot_idx handling (should still work, just not continuous)
TEST_F(LoadChunkSpillerTest, test_duplicate_slot_idx) {
    // Spill chunks with duplicate slot indices (0, 1, 1, 2)
    spill_chunks_with_slot_idx({{0, 0}, {100, 1}, {150, 1}, {200, 2}});

    // Should merge first chunk (slot 0), then stop at duplicate
    ASSIGN_OR_ABORT(auto task, _spiller->generate_merge_input_batch(1024 * 1024, 1024 * 1024, true, false, false));

    ASSERT_NE(task.merge_itr, nullptr);
    // Should only merge first continuous sequence
    ASSERT_GE(task.total_block_groups, 1);
}

// Regression test for the LoadChunkSpiller::_prepare() initialization race.
//
// Multiple memtable-flush threads share a single LoadChunkSpiller and call spill()
// concurrently on the first spill (see the parallel-flush comment on
// LoadChunkSpiller::spill). Before the fix, _prepare() used `_spiller == nullptr` as its
// readiness flag with no lock, so one thread could publish `_spiller` before creating the
// serde's encode context, while a racing thread observed the non-null spiller, skipped
// initialization, and called ColumnarSerde::serialize() with a null encode context --
// dereferencing it in _get_encode_levels() and crashing with SIGSEGV.
//
// Each round drives many threads through the first spill of a fresh LoadChunkSpiller so the
// narrow initialization window is exercised repeatedly; every concurrent spill must succeed.
TEST_F(LoadChunkSpillerTest, test_concurrent_first_spill_is_thread_safe) {
    constexpr int kRounds = 200;
    constexpr int kThreads = 8;
    for (int round = 0; round < kRounds; round++) {
        auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr,
                                                                     _local_spill_dir_mgr.get());
        ASSERT_OK(block_manager->init());
        RuntimeProfile profile("test");
        // No slot tracker: spill() then skips mark_slot_ready(), keeping the test focused on
        // the _prepare() initialization race rather than slot bookkeeping.
        LoadChunkSpiller spiller(block_manager.get(), &profile, nullptr);

        std::atomic<int> ready{0};
        std::atomic<bool> go{false};
        std::vector<Status> results(kThreads);
        std::vector<std::thread> threads;
        threads.reserve(kThreads);
        for (int t = 0; t < kThreads; t++) {
            threads.emplace_back([&, t]() {
                auto chunk = gen_data(100, t * 100);
                ready.fetch_add(1, std::memory_order_release);
                // Spin so all threads reach the first spill as simultaneously as possible,
                // maximizing the chance of hitting the initialization window.
                while (!go.load(std::memory_order_acquire)) {
                }
                results[t] = spiller.spill(*chunk, t).status();
            });
        }
        while (ready.load(std::memory_order_acquire) < kThreads) {
        }
        go.store(true, std::memory_order_release);
        for (auto& th : threads) {
            th.join();
        }
        for (int t = 0; t < kThreads; t++) {
            ASSERT_TRUE(results[t].ok()) << "round " << round << " thread " << t << ": " << results[t].to_string();
        }
        ASSERT_NE(nullptr, spiller.spiller());
    }
}

} // namespace starrocks

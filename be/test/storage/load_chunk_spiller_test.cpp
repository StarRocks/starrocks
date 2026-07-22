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

#include "storage/load_chunk_spiller.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "column/chunk.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "exec/spill/spiller.h"
#include "fs/fs.h"
#include "storage/load_spill_block_manager.h"
#include "testutil/assert.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace starrocks {

class LoadChunkSpillerTest : public ::testing::Test {
public:
    LoadChunkSpillerTest() {
        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "c0", TYPE_INT, false));
        fields.emplace_back(std::make_shared<Field>(1, "c1", TYPE_INT, false));
        _schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, std::vector<ColumnId>{});
    }

    void SetUp() override { ASSERT_OK(FileSystem::Default()->create_dir_recursive(kTestDir)); }

    void TearDown() override { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

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

    constexpr static const char* const kTestDir = "./load_chunk_spiller_test";
    std::shared_ptr<Schema> _schema;
};

// Regression test for the LoadChunkSpiller::_prepare() initialization race.
//
// Multiple memtable-flush threads share a single LoadChunkSpiller and call spill()
// concurrently on the first spill. Before the fix, _prepare() used `_spiller == nullptr` as
// its readiness flag with no lock, so one thread could publish `_spiller` before creating the
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
        auto block_manager = std::make_unique<LoadSpillBlockManager>(TUniqueId(), TUniqueId(), kTestDir, nullptr);
        ASSERT_OK(block_manager->init());
        RuntimeProfile profile("test");
        LoadChunkSpiller spiller(block_manager.get(), &profile);

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
                results[t] = spiller.spill(*chunk).status();
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

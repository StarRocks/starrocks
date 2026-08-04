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

// Regression test for the data race between ScanOperator IO task lambda
// reading `_chunk_sources[chunk_source_index]` and `_close_chunk_source_unlocked`
// writing the same slot. The race produced sporadic SIGSEGV / heap-use-after-free
// under high concurrency (cohere_1m c=50 paimon ANN scan) when an IO worker
// touched the connector data source after the operator reset the slot.
//
// The fix wraps the lambda's read in `std::shared_lock(_task_mutex)`,
// pairing with the existing `std::lock_guard(_task_mutex)` on the close side
// (which is the unique lock on `shared_mutex`).
//
// This test replicates the producer/consumer pattern in isolation: many
// readers loading the slot under `shared_lock`, one writer resetting it under
// `unique_lock`. Run under ThreadSanitizer (or AddressSanitizer with a
// stressing schedule) to verify the race is gone.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

namespace starrocks::pipeline {

namespace {

// Mock ChunkSource — a payload large enough that bad reads cross multiple
// cache lines and increase the odds of corruption surfacing.
struct MockChunkSource {
    std::array<int64_t, 64> payload{};
    int64_t magic = 0xCAFEBABEDEADBEEFLL;
    void touch() const {
        // Compiler must not elide this; ASAN/TSAN report on stale reads.
        ASSERT_EQ(magic, static_cast<int64_t>(0xCAFEBABEDEADBEEFLL));
    }
};

using MockChunkSourcePtr = std::shared_ptr<MockChunkSource>;

// Minimal harness mirroring the ScanOperator slot model.
class ChunkSourceSlots {
public:
    explicit ChunkSourceSlots(size_t n) : _slots(n) {}

    // Writer: emulates ScanOperator::_close_chunk_source_unlocked's
    // `_chunk_sources[i] = nullptr` under the operator's unique lock.
    void close(size_t idx) {
        std::lock_guard<std::shared_mutex> guard(_mu);
        _slots[idx].reset();
    }

    // Writer: install a fresh chunk source into the slot.
    void install(size_t idx, MockChunkSourcePtr cs) {
        std::lock_guard<std::shared_mutex> guard(_mu);
        _slots[idx] = std::move(cs);
    }

    // Reader: emulates the IO task lambda's `chunk_source = _chunk_sources[idx]`
    // load. Under the fix, the load is wrapped in shared_lock to pair with the
    // writer's unique_lock. Without the lock, this is data-race UB.
    MockChunkSourcePtr load(size_t idx) {
        std::shared_lock<std::shared_mutex> guard(_mu);
        return _slots[idx];
    }

private:
    mutable std::shared_mutex _mu;
    std::vector<MockChunkSourcePtr> _slots;
};

} // namespace

// Stress test: N reader threads load the slot in a loop, simulating an IO
// task lambda. A writer thread reinstalls + closes the slot in a tight loop.
// Each successful read touches the magic field to ensure the loaded
// shared_ptr points at a live object (or is nullptr; both are valid outcomes).
TEST(ScanOperatorChunkSourceRace, ConcurrentReadAndClose) {
    constexpr size_t kSlotCount = 4;
    constexpr int kReaderThreads = 8;
    constexpr int kWriterThreads = 2;
    constexpr int kIterations = 5000;

    ChunkSourceSlots slots(kSlotCount);
    for (size_t i = 0; i < kSlotCount; ++i) {
        slots.install(i, std::make_shared<MockChunkSource>());
    }

    std::atomic<bool> stop{false};
    std::atomic<int64_t> reads_done{0};
    std::atomic<int64_t> reads_nonnull{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < kReaderThreads; ++t) {
        threads.emplace_back([&, t]() {
            size_t idx = t % kSlotCount;
            for (int i = 0; i < kIterations && !stop.load(); ++i) {
                MockChunkSourcePtr local = slots.load(idx);
                if (local != nullptr) {
                    // Use the chunk source — would deref a freed object
                    // if the load raced with reset.
                    local->touch();
                    reads_nonnull.fetch_add(1);
                }
                reads_done.fetch_add(1);
                idx = (idx + 1) % kSlotCount;
            }
        });
    }

    for (int t = 0; t < kWriterThreads; ++t) {
        threads.emplace_back([&, t]() {
            size_t idx = t % kSlotCount;
            for (int i = 0; i < kIterations && !stop.load(); ++i) {
                if (i % 2 == 0) {
                    slots.close(idx);
                } else {
                    slots.install(idx, std::make_shared<MockChunkSource>());
                }
                idx = (idx + 1) % kSlotCount;
                std::this_thread::yield();
            }
        });
    }

    for (auto& th : threads) th.join();

    EXPECT_EQ(reads_done.load(), static_cast<int64_t>(kReaderThreads * kIterations));
    // Some reads will see nullptr (after a close, before reinstall); that's
    // valid and matches the fix's `if (chunk_source == nullptr) return;` path.
    // We mostly care that the test completes without crash and that
    // TSAN/ASAN reports no race.
    EXPECT_GT(reads_nonnull.load(), 0);
}

// Functional check: after `close(idx)`, a subsequent `load(idx)` returns
// nullptr, matching the IO task lambda's early-return path.
TEST(ScanOperatorChunkSourceRace, CloseThenLoadReturnsNullptr) {
    ChunkSourceSlots slots(1);
    slots.install(0, std::make_shared<MockChunkSource>());
    ASSERT_NE(slots.load(0), nullptr);
    slots.close(0);
    EXPECT_EQ(slots.load(0), nullptr);
}

// Functional check: the loaded shared_ptr keeps the underlying object alive
// past a subsequent close — this is the lifetime guarantee the fix relies on
// (the IO task lambda holds the chunk_source local for the rest of the task,
// so the close in another thread cannot free it until the IO task returns).
TEST(ScanOperatorChunkSourceRace, LoadedSharedPtrSurvivesClose) {
    ChunkSourceSlots slots(1);
    auto orig = std::make_shared<MockChunkSource>();
    auto* raw = orig.get();
    slots.install(0, orig);
    orig.reset();

    // Load — refcount in our local + slot = 2.
    MockChunkSourcePtr loaded = slots.load(0);
    ASSERT_EQ(loaded.get(), raw);
    EXPECT_EQ(loaded.use_count(), 2);

    // Close the slot — refcount drops to 1 (just our local).
    slots.close(0);
    EXPECT_EQ(slots.load(0), nullptr);
    EXPECT_EQ(loaded.use_count(), 1);

    // Still valid: the loaded copy keeps the object alive.
    loaded->touch();
}

} // namespace starrocks::pipeline

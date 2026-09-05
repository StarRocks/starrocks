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

#include "storage/index/inverted/tantivy/tantivy_cache.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "runtime/mem_tracker.h"
#include "storage/index/inverted/tantivy/random_access_bridge.h"
#include "testutil/assert.h"

namespace starrocks {

TEST(TantivyReadBufferPoolTest, ReusesReleasedSizeClassBuffer) {
    TantivyReadBufferPool pool(64 * 1024, 1024 * 1024, nullptr);
    size_t first_capacity = 0;
    auto* first = pool.acquire(1500, &first_capacity);
    ASSERT_NE(nullptr, first);
    EXPECT_EQ(2048, first_capacity);
    pool.release(first, first_capacity);

    size_t second_capacity = 0;
    auto* second = pool.acquire(1800, &second_capacity);
    ASSERT_EQ(first, second);
    EXPECT_EQ(first_capacity, second_capacity);
    auto stats = pool.stats();
    EXPECT_EQ(2, stats.acquire);
    EXPECT_EQ(1, stats.hit);
    EXPECT_EQ(1, stats.miss);
    EXPECT_EQ(0, stats.cached_bytes);
    EXPECT_EQ(second_capacity, stats.in_use_bytes);
    pool.release(second, second_capacity);
}

TEST(TantivyReadBufferPoolTest, RespectsRetainedCapacity) {
    TantivyReadBufferPool pool(1024, 1024 * 1024, nullptr);
    size_t capacity = 0;
    auto* buffer = pool.acquire(1500, &capacity);
    ASSERT_NE(nullptr, buffer);
    EXPECT_EQ(2048, capacity);
    pool.release(buffer, capacity);
    EXPECT_EQ(0, pool.stats().cached_bytes);
}

TEST(TantivyReadBufferPoolTest, ConcurrentAcquireReleaseAndPrune) {
    TantivyReadBufferPool pool(256 * 1024, 1024 * 1024, nullptr);
    std::atomic<bool> stop_pruning{false};
    std::thread pruner([&]() {
        while (!stop_pruning.load(std::memory_order_relaxed)) {
            pool.prune();
        }
    });
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker]() {
            for (size_t iteration = 0; iteration < 1000; ++iteration) {
                size_t capacity = 0;
                auto* buffer = pool.acquire(1024 + worker * 257, &capacity);
                ASSERT_NE(nullptr, buffer);
                buffer[0] = static_cast<uint8_t>(iteration);
                pool.release(buffer, capacity);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    stop_pruning.store(true, std::memory_order_relaxed);
    pruner.join();
    pool.prune();
    auto stats = pool.stats();
    EXPECT_EQ(0, stats.in_use_bytes);
    EXPECT_EQ(0, stats.cached_bytes);
    EXPECT_EQ(4000, stats.acquire);
    EXPECT_EQ(4000, stats.release);
}

namespace {

TantivyIndexIdentity make_identity(std::string path) {
    TantivyIndexIdentity identity;
    identity.storage_mode = TantivyStorageMode::COMPOUND;
    identity.canonical_path = std::move(path);
    identity.file_size = 1024;
    identity.compound_format_version = 1;
    identity.index_id = 7;
    identity.index_suffix = "9";
    identity.field_name = "title";
    identity.tokenizer_name = "english";
    return identity;
}

std::shared_ptr<TantivyReaderResource> make_resource(const TantivyIndexIdentity& identity) {
    auto resource = std::make_shared<TantivyReaderResource>();
    resource->identity = identity;
    resource->estimated_bytes = 128;
    resource->fd_charge = 1;
    return resource;
}

TEST(TantivyCacheKeyTest, IdentityFieldsAreUnambiguous) {
    auto base = make_identity("/data/a.idx");
    auto changed = base;
    changed.tokenizer_name = "raw";
    EXPECT_NE(base.encode(), changed.encode());

    changed = base;
    changed.field_name = "body";
    EXPECT_NE(base.encode(), changed.encode());

    changed = base;
    changed.object_version = "etag-2";
    EXPECT_NE(base.encode(), changed.encode());

    changed = base;
    changed.analyzer_digest = "sha256-v2";
    EXPECT_NE(base.encode(), changed.encode());

    changed = base;
    changed.encryption_meta_hash = 42;
    EXPECT_NE(base.encode(), changed.encode());

    auto with_separator = base;
    with_separator.canonical_path = "/data/a@b.idx";
    auto split_like = base;
    split_like.canonical_path = "/data/a";
    split_like.object_version = "b.idx";
    EXPECT_NE(with_separator.encode(), split_like.encode());
}

TEST(TantivyCacheKeyTest, QueryFieldsAreUnambiguous) {
    const auto identity = make_identity("/data/query.idx");
    TantivyCanonicalQuery any;
    any.type = TantivyCanonicalQueryType::MATCH_ANY;
    any.terms = {"brown", "fox"};

    auto all = any;
    all.type = TantivyCanonicalQueryType::MATCH_ALL;
    EXPECT_NE(any.encode_with(identity), all.encode_with(identity));

    auto phrase = any;
    phrase.type = TantivyCanonicalQueryType::MATCH_PHRASE;
    phrase.slop = 2;
    EXPECT_NE(any.encode_with(identity), phrase.encode_with(identity));

    auto reversed = any;
    reversed.terms = {"fox", "brown"};
    EXPECT_NE(any.encode_with(identity), reversed.encode_with(identity));
}

TEST(TantivyReaderCacheTest, ReusesLoadedResource) {
    TantivyReaderCache cache(1024 * 1024, 128, 64 * 1024, nullptr);
    auto identity = make_identity("/data/reuse.idx");
    std::atomic<int> builds{0};
    auto loader = [&]() -> StatusOr<std::shared_ptr<TantivyReaderResource>> {
        builds.fetch_add(1);
        return make_resource(identity);
    };

    ASSIGN_OR_ABORT(auto first, cache.get_or_load(identity, loader));
    ASSIGN_OR_ABORT(auto second, cache.get_or_load(identity, loader));
    EXPECT_EQ(first.get(), second.get());
    EXPECT_EQ(1, builds.load());
    EXPECT_EQ(1, cache.stats().hit);
}

TEST(TantivyReaderCacheTest, ConcurrentMissUsesSingleflight) {
    TantivyReaderCache cache(1024 * 1024, 128, 64 * 1024, nullptr);
    auto identity = make_identity("/data/concurrent.idx");
    std::atomic<int> builds{0};
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    constexpr int kThreads = 32;
    std::vector<std::shared_ptr<TantivyReaderResource>> resources(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            auto result = cache.get_or_load(identity, [&]() -> StatusOr<std::shared_ptr<TantivyReaderResource>> {
                builds.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return make_resource(identity);
            });
            ASSERT_TRUE(result.ok()) << result.status();
            resources[i] = std::move(result).value();
        });
    }
    while (ready.load() != kThreads) {
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(1, builds.load());
    for (const auto& resource : resources) {
        EXPECT_EQ(resources[0].get(), resource.get());
    }
    EXPECT_GE(cache.stats().duplicate_build_prevented, kThreads - 1);
}

TEST(TantivyReaderCacheTest, EraseDoesNotInvalidatePinnedResource) {
    TantivyReaderCache cache(1024 * 1024, 128, 64 * 1024, nullptr);
    auto identity = make_identity("/data/pinned.idx");
    ASSIGN_OR_ABORT(auto pinned, cache.get_or_load(identity, [&] { return make_resource(identity); }));
    cache.erase(identity);
    EXPECT_EQ(nullptr, cache.lookup(identity));
    EXPECT_EQ(identity.canonical_path, pinned->identity.canonical_path);
}

TEST(TantivyReaderCacheTest, OversizedResourceIsSharedButNotCached) {
    TantivyReaderCache cache(1024 * 1024, 128, 64, nullptr);
    auto identity = make_identity("/data/large.idx");
    auto loader = [&]() -> StatusOr<std::shared_ptr<TantivyReaderResource>> {
        auto resource = make_resource(identity);
        resource->estimated_bytes = 1024;
        return resource;
    };
    ASSERT_OK(cache.get_or_load(identity, loader));
    EXPECT_EQ(nullptr, cache.lookup(identity));
    EXPECT_EQ(1, cache.stats().oversize_reject);
}

TEST(TantivyReaderCacheTest, ResidentAdmissionHonorsEntryAndCapacityLimits) {
    TantivyReaderCache cache(32 * 1024 * 1024, 128, 2 * 1024 * 1024, nullptr);
    auto identity = make_identity("/data/resident.idx");
    EXPECT_TRUE(cache.would_admit(identity, 512 * 1024));
    EXPECT_FALSE(cache.would_admit(identity, 2 * 1024 * 1024));

    TantivyReaderCache small_cache(1024 * 1024, 128, 2 * 1024 * 1024, nullptr);
    EXPECT_TRUE(small_cache.would_admit(identity, 64 * 1024));
    EXPECT_FALSE(small_cache.would_admit(identity, 1024 * 1024));
}

TEST(TantivyReaderCacheTest, TracksResidentDirectoryStats) {
    TantivyReaderCache cache(32 * 1024 * 1024, 128, 2 * 1024 * 1024, nullptr);
    auto identity = make_identity("/data/resident-stats.idx");
    auto loaded = cache.get_or_load(identity, [&]() -> StatusOr<std::shared_ptr<TantivyReaderResource>> {
        auto resource = make_resource(identity);
        resource->resident_directory = true;
        resource->resident_bytes = 4096;
        resource->materialized_bytes = 4096;
        resource->estimated_bytes = 8192;
        return resource;
    });
    ASSERT_OK(loaded.status());
    EXPECT_EQ(1, cache.stats().resident_directory_entries);
    EXPECT_EQ(4096, cache.stats().resident_directory_bytes);
    loaded.value().reset();
    cache.prune();
    EXPECT_EQ(0, cache.stats().resident_directory_entries);
    EXPECT_EQ(0, cache.stats().resident_directory_bytes);
}

TEST(TantivyReaderCacheTest, TracksPinnedReaderUntilLastReference) {
    constexpr int64_t kAllocatorHookBytes = 4096;
    MemTracker root_tracker(-1, "tantivy-reader-cache-root-test");
    MemTracker tracker(-1, "tantivy-reader-cache-test", &root_tracker);
    // BE_TEST does not wire heap allocations into the tracker hierarchy, so
    // model the one real allocator-hook charge explicitly.
    root_tracker.consume(kAllocatorHookBytes);
    TantivyReaderCache cache(1024 * 1024, 128, 64 * 1024, &tracker);
    auto identity = make_identity("/data/tracked.idx");
    auto loaded = cache.get_or_load(identity, [&] { return make_resource(identity); });
    ASSERT_OK(loaded.status());
    auto pinned = std::move(loaded).value();
    EXPECT_GT(tracker.consumption(), 0);
    EXPECT_EQ(kAllocatorHookBytes, root_tracker.consumption());

    cache.erase(identity);
    EXPECT_GT(tracker.consumption(), 0);
    pinned.reset();
    EXPECT_EQ(0, tracker.consumption());
    EXPECT_EQ(kAllocatorHookBytes, root_tracker.consumption());
    root_tracker.release(kAllocatorHookBytes);
}

TEST(TantivyQueryCacheTest, PublishesImmutableBitmap) {
    TantivyQueryCache cache(1024 * 1024, 64 * 1024, 1024, 0.7, 128, nullptr);
    roaring::Roaring source;
    source.add(1);
    source.add(7);
    cache.maybe_insert("term-key", source);
    source.add(99);

    auto cached = cache.lookup("term-key");
    ASSERT_NE(nullptr, cached);
    EXPECT_TRUE(cached->contains(1));
    EXPECT_FALSE(cached->contains(99));
    EXPECT_EQ(1, cache.stats().hit);
}

TEST(TantivyQueryCacheTest, TwoHitAdmissionAtHighWatermark) {
    TantivyQueryCache cache(1024 * 1024, 64 * 1024, 1024, 0.0, 128, nullptr);
    roaring::Roaring bitmap;
    bitmap.add(42);

    cache.maybe_insert("two-hit", bitmap);
    EXPECT_EQ(nullptr, cache.lookup("two-hit"));
    EXPECT_EQ(1, cache.stats().ghost_record);

    cache.maybe_insert("two-hit", bitmap);
    ASSERT_NE(nullptr, cache.lookup("two-hit"));
    EXPECT_EQ(1, cache.stats().ghost_admit);
}

TEST(TantivyQueryCacheTest, RejectsOversizedKeyAndValue) {
    TantivyQueryCache cache(1024 * 1024, 64, 8, 0.7, 128, nullptr);
    roaring::Roaring bitmap;
    for (uint32_t value = 0; value < 10000; value += 2) {
        bitmap.add(value);
    }
    cache.maybe_insert("key-too-long", bitmap);
    EXPECT_EQ(nullptr, cache.lookup("key-too-long"));
    EXPECT_GE(cache.stats().key_too_large, 1);

    cache.maybe_insert("large", bitmap);
    EXPECT_EQ(nullptr, cache.lookup("large"));
    EXPECT_EQ(1, cache.stats().oversize_reject);
}

TEST(TantivyQueryCacheTest, TracksPinnedBitmapUntilLastReference) {
    constexpr int64_t kAllocatorHookBytes = 4096;
    MemTracker root_tracker(-1, "tantivy-query-cache-root-test");
    MemTracker tracker(-1, "tantivy-query-cache-test", &root_tracker);
    root_tracker.consume(kAllocatorHookBytes);
    TantivyQueryCache cache(1024 * 1024, 64 * 1024, 1024, 0.7, 128, &tracker);
    roaring::Roaring bitmap;
    bitmap.add(42);
    cache.maybe_insert("tracked", bitmap);
    EXPECT_GT(tracker.consumption(), 0);
    EXPECT_EQ(kAllocatorHookBytes, root_tracker.consumption());

    auto pinned = cache.lookup("tracked");
    ASSERT_NE(nullptr, pinned);
    cache.prune();
    EXPECT_GT(tracker.consumption(), 0);
    pinned.reset();
    EXPECT_EQ(0, tracker.consumption());
    EXPECT_EQ(kAllocatorHookBytes, root_tracker.consumption());
    root_tracker.release(kAllocatorHookBytes);
}

TEST(TantivyCacheStatsTest, RecordsBypassSeparatelyFromMiss) {
    TantivyReaderCache reader_cache(1024, 8, 1024, nullptr);
    TantivyQueryCache query_cache(1024, 1024, 1024, 0.7, 8, nullptr);
    reader_cache.record_bypass();
    query_cache.record_bypass();
    EXPECT_EQ(1, reader_cache.stats().bypass);
    EXPECT_EQ(0, reader_cache.stats().miss);
    EXPECT_EQ(1, query_cache.stats().bypass);
    EXPECT_EQ(0, query_cache.stats().miss);
}

} // namespace
} // namespace starrocks

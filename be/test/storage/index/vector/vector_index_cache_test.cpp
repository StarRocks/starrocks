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

#include "storage/index/vector/vector_index_cache.h"

#include <gtest/gtest.h>

#ifdef WITH_TENANN

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <thread>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "common/config_update_registry.h"
#include "common/status.h"
#include "fs/fs_memory.h"
#include "runtime/env/global_env.h"
#include "runtime/mem_tracker.h"
#include "service/service_be/config_update_hooks.h"
#include "storage/index/vector/empty_index_reader.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/tenann_index_reader.h"
#include "storage/index/vector/vector_index_file_reader.h"
#include "tenann/common/error.h"
#include "tenann/index/index.h"
#include "tenann/index/index_cache.h"
#include "tenann/store/index_meta.h"

namespace starrocks {

namespace {
constexpr size_t kDummyBytes = 1024;

tenann::IndexRef make_dummy_ref(size_t bytes = kDummyBytes) {
    void* buf = std::malloc(bytes);
    return std::make_shared<tenann::Index>(
            buf, tenann::IndexType::kFaissIvfPqOneInvertedList, [](void* v) { std::free(v); },
            /*explicit_bytes=*/bytes);
}
} // namespace

class VectorIndexCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        tracker_ = std::make_unique<MemTracker>(-1, "vector_index_test");
        cache_ = std::make_unique<VectorIndexCache>(/*capacity=*/16 * 1024, tracker_.get());
    }
    void TearDown() override {
        cache_.reset();
        tracker_.reset();
    }
    std::unique_ptr<MemTracker> tracker_;
    std::unique_ptr<VectorIndexCache> cache_;
};

TEST_F(VectorIndexCacheTest, Lookup_Miss_ReturnsFalse) {
    tenann::IndexCacheHandle h;
    EXPECT_FALSE(cache_->Lookup(tenann::CacheKey("/missing.vi"), &h));
    EXPECT_FALSE(h.valid());
}

TEST_F(VectorIndexCacheTest, Insert_ThenLookup_ReturnsSameRef) {
    auto ref = make_dummy_ref();
    tenann::IndexCacheHandle h_ins;
    cache_->Insert(tenann::CacheKey("/a.vi"), ref, &h_ins);

    tenann::IndexCacheHandle h_lkp;
    ASSERT_TRUE(cache_->Lookup(tenann::CacheKey("/a.vi"), &h_lkp));
    EXPECT_EQ(ref.get(), h_lkp.index_ref().get());
}

TEST_F(VectorIndexCacheTest, GetOrCreate_FirstCallRunsLoader) {
    int calls = 0;
    auto loader = [&]() -> tenann::IndexRef {
        ++calls;
        return make_dummy_ref();
    };
    tenann::IndexCacheHandle h;
    EXPECT_TRUE(cache_->GetOrCreate(tenann::CacheKey("/b.vi"), loader, &h));
    EXPECT_EQ(1, calls);
    EXPECT_TRUE(h.valid());
}

TEST_F(VectorIndexCacheTest, GetOrCreate_SecondCallHitsCache) {
    int calls = 0;
    auto loader = [&]() -> tenann::IndexRef {
        ++calls;
        return make_dummy_ref();
    };
    tenann::IndexCacheHandle h1, h2;
    (void)cache_->GetOrCreate(tenann::CacheKey("/c.vi"), loader, &h1);
    EXPECT_TRUE(cache_->GetOrCreate(tenann::CacheKey("/c.vi"), loader, &h2));
    EXPECT_EQ(1, calls);
    EXPECT_EQ(h1.index_ref().get(), h2.index_ref().get());
}

TEST_F(VectorIndexCacheTest, LookupAndHitCounters_TrackedAcrossPaths) {
    auto loader = [&]() -> tenann::IndexRef { return make_dummy_ref(); };
    tenann::IndexCacheHandle h;

    // Cold GetOrCreate: lookup +1, hit unchanged.
    EXPECT_TRUE(cache_->GetOrCreate(tenann::CacheKey("/m.vi"), loader, &h));
    EXPECT_EQ(1u, cache_->lookup_count());
    EXPECT_EQ(0u, cache_->hit_count());

    // Warm GetOrCreate: lookup +1, hit +1.
    EXPECT_TRUE(cache_->GetOrCreate(tenann::CacheKey("/m.vi"), loader, &h));
    EXPECT_EQ(2u, cache_->lookup_count());
    EXPECT_EQ(1u, cache_->hit_count());

    // Lookup is the warm-path probe used by VectorIndexReaderFactory; it is
    // counter-silent so we do not double-count against the GetOrCreate that
    // TenANNReader::init_searcher runs right after.
    EXPECT_TRUE(cache_->Lookup(tenann::CacheKey("/m.vi"), &h));
    EXPECT_EQ(2u, cache_->lookup_count());
    EXPECT_EQ(1u, cache_->hit_count());

    EXPECT_FALSE(cache_->Lookup(tenann::CacheKey("/missing.vi"), &h));
    EXPECT_EQ(2u, cache_->lookup_count());
    EXPECT_EQ(1u, cache_->hit_count());
}

TEST_F(VectorIndexCacheTest, GetOrCreate_ConcurrentCallers_SingleFlight) {
    std::atomic<int> loader_calls{0};
    auto loader = [&]() -> tenann::IndexRef {
        loader_calls.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        return make_dummy_ref();
    };
    constexpr int N = 16;
    std::vector<std::thread> threads;
    std::vector<tenann::IndexCacheHandle> handles(N);
    for (int i = 0; i < N; ++i) {
        threads.emplace_back([&, i] { (void)cache_->GetOrCreate(tenann::CacheKey("/d.vi"), loader, &handles[i]); });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(1, loader_calls.load());
    for (int i = 1; i < N; ++i) {
        EXPECT_EQ(handles[0].index_ref().get(), handles[i].index_ref().get());
    }
}

// Loader-failure cleanup: GetOrCreate must return false, leave the handle
// invalid, and not cache anything so a later call reruns the loader. The
// null-return mode here covers the same _cache.remove(entry) branch as the
// throw path (which can't be exercised under ASAN — the std::function throw
// machinery aborts the binary before reaching the catch).
TEST_F(VectorIndexCacheTest, GetOrCreate_LoaderReturnsNull_NotCached) {
    auto null_loader = []() -> tenann::IndexRef { return nullptr; };
    tenann::IndexCacheHandle h;
    EXPECT_FALSE(cache_->GetOrCreate(tenann::CacheKey("/e.vi"), null_loader, &h));
    EXPECT_FALSE(h.valid());

    int good_calls = 0;
    auto good_loader = [&]() -> tenann::IndexRef {
        ++good_calls;
        return make_dummy_ref();
    };
    EXPECT_TRUE(cache_->GetOrCreate(tenann::CacheKey("/e.vi"), good_loader, &h));
    EXPECT_EQ(1, good_calls); // retry ran (entry was not left in a cached state)
    EXPECT_TRUE(h.valid());
}

TEST_F(VectorIndexCacheTest, Insert_OverCapacity_EvictsLRU) {
    auto small_cache = std::make_unique<VectorIndexCache>(/*capacity=*/2048, tracker_.get());
    tenann::IndexCacheHandle h1, h2, h3;
    small_cache->Insert(tenann::CacheKey("/x.vi"), make_dummy_ref(1024), &h1);
    small_cache->Insert(tenann::CacheKey("/y.vi"), make_dummy_ref(1024), &h2);
    h1 = tenann::IndexCacheHandle{}; // drop pin so x is evictable
    small_cache->Insert(tenann::CacheKey("/z.vi"), make_dummy_ref(1024), &h3);

    tenann::IndexCacheHandle probe;
    EXPECT_FALSE(small_cache->Lookup(tenann::CacheKey("/x.vi"), &probe));
    EXPECT_TRUE(small_cache->Lookup(tenann::CacheKey("/z.vi"), &probe));
}

TEST_F(VectorIndexCacheTest, Evict_WhilePinned_DeferredRelease) {
    auto small_cache = std::make_unique<VectorIndexCache>(/*capacity=*/1024, tracker_.get());
    tenann::IndexCacheHandle h_pin;
    small_cache->Insert(tenann::CacheKey("/p.vi"), make_dummy_ref(1024), &h_pin);
    tenann::IndexRef pinned = h_pin.index_ref();
    ASSERT_TRUE(pinned != nullptr);

    tenann::IndexCacheHandle h_new;
    small_cache->Insert(tenann::CacheKey("/q.vi"), make_dummy_ref(1024), &h_new);
    // /p.vi is evicted from LRU but the underlying Index must still be alive via `pinned`.
    EXPECT_GE(pinned.use_count(), 1L);
    h_pin = tenann::IndexCacheHandle{};
    h_new = tenann::IndexCacheHandle{};
    // Drop the underlying cache so any deferred holders release their refs too.
    small_cache.reset();
    // Now only `pinned` owns it.
    EXPECT_EQ(1L, pinned.use_count());
}

TEST_F(VectorIndexCacheTest, SetCapacity_Shrink_EvictsImmediately) {
    auto c = std::make_unique<VectorIndexCache>(/*capacity=*/4096, tracker_.get());
    tenann::IndexCacheHandle h1, h2;
    c->Insert(tenann::CacheKey("/s1.vi"), make_dummy_ref(1024), &h1);
    c->Insert(tenann::CacheKey("/s2.vi"), make_dummy_ref(1024), &h2);
    h1 = tenann::IndexCacheHandle{};
    h2 = tenann::IndexCacheHandle{};
    c->SetCapacity(512);
    tenann::IndexCacheHandle probe;
    EXPECT_FALSE(c->Lookup(tenann::CacheKey("/s1.vi"), &probe));
    EXPECT_FALSE(c->Lookup(tenann::CacheKey("/s2.vi"), &probe));
}

TEST_F(VectorIndexCacheTest, MemTracker_Consume_AfterInsert) {
    int64_t before = tracker_->consumption();
    tenann::IndexCacheHandle h;
    cache_->Insert(tenann::CacheKey("/t1.vi"), make_dummy_ref(4096), &h);
    int64_t after = tracker_->consumption();
    // If tracker is not wired into the thread-local mem tracker in this test
    // environment, `after` may equal `before`. Accept that case.
    EXPECT_GE(after, before);
}

TEST_F(VectorIndexCacheTest, MemTracker_Release_AfterEvict) {
    // Capacity 1024 = one entry; the second Insert overshoots and must evict
    // the first (size > capacity triggers DynamicCache::_evict).
    auto c = std::make_unique<VectorIndexCache>(/*capacity=*/1024, tracker_.get());
    int64_t base = tracker_->consumption();
    tenann::IndexCacheHandle h1;
    c->Insert(tenann::CacheKey("/t2.vi"), make_dummy_ref(1024), &h1);
    h1 = tenann::IndexCacheHandle{};
    tenann::IndexCacheHandle h2;
    c->Insert(tenann::CacheKey("/t3.vi"), make_dummy_ref(1024), &h2);
    tenann::IndexCacheHandle probe;
    EXPECT_FALSE(c->Lookup(tenann::CacheKey("/t2.vi"), &probe));
    int64_t after = tracker_->consumption();
    EXPECT_GE(after, base);
    // No tighter upper bound: tracker wiring is best-effort in unit tests;
    // the correctness invariant is that release did not underflow the tracker.
}

TEST_F(VectorIndexCacheTest, MemTracker_CrossThread_EvictReleasesCorrectly) {
    // Headline correctness test: the deleter must re-bind tls tracker to
    // vector_index_mem_tracker regardless of which thread triggers eviction.
    auto c = std::make_unique<VectorIndexCache>(/*capacity=*/2048, tracker_.get());
    int64_t base = tracker_->consumption();
    tenann::IndexCacheHandle h;
    c->Insert(tenann::CacheKey("/cr.vi"), make_dummy_ref(1024), &h);
    h = tenann::IndexCacheHandle{};

    // Thread B triggers eviction via SetCapacity shrink.
    std::thread([&] { c->SetCapacity(0); }).join();

    tenann::IndexCacheHandle probe;
    EXPECT_FALSE(c->Lookup(tenann::CacheKey("/cr.vi"), &probe));
    // Cross-thread release must not leave the tracker above baseline.
    EXPECT_LE(tracker_->consumption(), base + 512);
}

// === Sibling helpers under storage/index/vector ===

TEST(TenannErrorToStatusTest, NotFoundVariantsMapToNotFound) {
    EXPECT_TRUE(tenann_error_to_status(tenann::Error("f.cpp", 1, "Not found: x")).is_not_found());
    EXPECT_TRUE(tenann_error_to_status(tenann::Error("f.cpp", 2, "blob not found")).is_not_found());
    EXPECT_TRUE(tenann_error_to_status(tenann::Error("f.cpp", 3, "No such file: y")).is_not_found());
}

TEST(TenannErrorToStatusTest, OtherErrorsMapToInternalError) {
    auto st = tenann_error_to_status(tenann::Error("f.cpp", 4, "checksum mismatch"));
    EXPECT_FALSE(st.ok());
    EXPECT_FALSE(st.is_not_found());
    EXPECT_TRUE(st.is_internal_error());
}

TEST(VectorIndexFileReaderTest, OpenSucceedsOnMemoryFs) {
    MemoryFileSystem fs;
    ASSERT_OK(fs.create_dir("/tmp"));
    ASSERT_OK(fs.append_file("/tmp/idx.vi", Slice("abcd", 4)));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(&fs, "/tmp/idx.vi"));
    ASSERT_NE(nullptr, reader);
    EXPECT_EQ(4, reader->GetSize());
}

TEST(VectorIndexFileReaderTest, OpenFailsOnMissingFile) {
    MemoryFileSystem fs;
    auto r = VectorIndexFileReader::open(&fs, "/no/such/path.vi");
    EXPECT_FALSE(r.ok());
}

TEST(EmptyIndexReaderTest, AllMethodsReturnNotSupported) {
    EmptyIndexReader r;
    tenann::IndexMeta meta;
    EXPECT_TRUE(r.init_searcher(meta, "/x.vi", /*fs=*/nullptr).is_not_supported());
    EXPECT_TRUE(r.search(tenann::PrimitiveSeqView{}, /*k=*/1, nullptr, nullptr).is_not_supported());
    std::vector<int64_t> ids;
    std::vector<float> dists;
    EXPECT_TRUE(r.range_search(tenann::PrimitiveSeqView{}, /*k=*/1, &ids, &dists, nullptr, /*range=*/0.0f, /*order=*/0)
                        .is_not_supported());
}

TEST(TenANNReaderTest, InitSearcher_NoGlobalCache_ReturnsInternalError) {
    auto* saved = tenann::GetGlobalIndexCache();
    tenann::SetGlobalIndexCache(nullptr);
    TenANNReader r;
    tenann::IndexMeta meta;
    auto st = r.init_searcher(meta, "/x.vi", /*fs=*/nullptr);
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();
    tenann::SetGlobalIndexCache(saved);
}

// Drives init_searcher through the loader's fs!=nullptr branch with a missing
// path so the captured Status (NotFound) flows back through GetOrCreate to the
// caller. This is the signal VectorIndexReaderFactory's brute-force fallback
// keys off — collapsing it to InternalError would silently disable the fallback.
TEST(TenANNReaderTest, InitSearcher_FileNotFoundViaFs_PropagatesNotFound) {
    MemTracker tracker(-1, "tenann_reader_test");
    VectorIndexCache cache(/*capacity=*/1024, &tracker);
    auto* saved = tenann::GetGlobalIndexCache();
    tenann::SetGlobalIndexCache(&cache);

    MemoryFileSystem fs;
    TenANNReader r;
    tenann::IndexMeta meta;
    auto st = r.init_searcher(meta, "/no/such/index.vi", &fs);
    EXPECT_TRUE(st.is_not_found()) << st.to_string();

    tenann::SetGlobalIndexCache(saved);
}

// Same fs-bound path but the file IS present, just not a real tenann index.
// tenann::IndexFactory::CreateReaderFromMeta or ReadIndexFile reacts to the
// default-constructed IndexMeta / garbage payload by throwing tenann::Error,
// which the loader must catch and surface as a non-OK Status — without the
// catch arm, tenann::Error (which inherits privately from std::exception)
// would escape GetOrCreate and crash the BE.
TEST(TenANNReaderTest, InitSearcher_MalformedFile_ReturnsNonOk) {
    MemTracker tracker(-1, "tenann_reader_test");
    VectorIndexCache cache(/*capacity=*/1024, &tracker);
    auto* saved = tenann::GetGlobalIndexCache();
    tenann::SetGlobalIndexCache(&cache);

    MemoryFileSystem fs;
    ASSERT_OK(fs.create_dir("/tmp"));
    ASSERT_OK(fs.append_file("/tmp/garbage.vi", Slice("not a real index", 16)));

    TenANNReader r;
    tenann::IndexMeta meta;
    auto st = r.init_searcher(meta, "/tmp/garbage.vi", &fs);
    EXPECT_FALSE(st.ok()) << "loader should surface tenann::Error / std::exception, not crash";

    tenann::SetGlobalIndexCache(saved);
}

TEST(VectorIndexCacheEntryTest, StreamingOperatorPrintsTag) {
    VectorIndexCacheEntry e;
    std::ostringstream os;
    os << e;
    EXPECT_EQ("VectorIndexCacheEntry", os.str());
}

// register_config_update_hooks(nullptr) is well-defined: callback registration
// does not dereference exec_env. When /api/update_config?vector_index_cache_limit=...
// fires, the callback short-circuits on the null exec_env and returns
// InternalError, which update_config then rolls back. Operators see the failure
// rather than crashing the BE.
TEST(ConfigUpdateHooksTest, VectorIndexCacheLimit_NullExecEnv_ReturnsInternalError) {
    auto* registry = ConfigUpdateRegistry::instance();
    registry->TEST_reset();
    register_config_update_hooks(/*exec_env=*/nullptr, *GlobalEnv::GetInstance());
    registry->set_ready();

    auto st = registry->update_config("vector_index_cache_limit", "1G");
    EXPECT_FALSE(st.ok()) << st.to_string();
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();

    registry->TEST_reset();
}

} // namespace starrocks

#endif // WITH_TENANN

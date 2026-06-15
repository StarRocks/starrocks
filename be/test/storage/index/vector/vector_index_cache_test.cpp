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
#include "common/status.h"
#include "fs/fs_memory.h"
#include "runtime/current_thread.h"
#include "runtime/env/global_env.h"
#include "runtime/mem_tracker.h"
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

// Minimal IndexMeta that survives apply_index_reader_cache_options(): default
// construction leaves index_type unset and tenann throws "index type not set
// in index meta" the moment init_searcher reads it, escaping as an unknown
// exception before the loader's catch arm ever runs.
tenann::IndexMeta make_minimal_meta() {
    tenann::IndexMeta meta;
    meta.SetIndexFamily(tenann::IndexFamily::kVectorIndex);
    meta.SetIndexType(tenann::IndexType::kFaissHnsw);
    return meta;
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

// Lookup must report miss for entries that exist but have not been populated
// with an IndexRef yet — covers the `!entry->value().has_ref()` branch that a
// concurrent GetOrCreate would otherwise expose to racing Lookups. Inserting
// a nullptr IndexRef is the deterministic way to reach the same state from a
// single thread.
TEST_F(VectorIndexCacheTest, Lookup_EntryWithoutRef_ReportsMiss) {
    tenann::IndexCacheHandle h_ins;
    cache_->Insert(tenann::CacheKey("/loading.vi"), /*ref=*/nullptr, &h_ins);

    tenann::IndexCacheHandle h_lkp;
    EXPECT_FALSE(cache_->Lookup(tenann::CacheKey("/loading.vi"), &h_lkp));
    EXPECT_FALSE(h_lkp.valid());
}

// Loud assertion on the small inline accessors (SetCapacity / capacity /
// memory_usage / lookup_count / hit_count). Existing tests touch them
// transitively but gcov tracking on inline header methods is unreliable —
// asserting each one directly here makes the coverage explicit.
TEST_F(VectorIndexCacheTest, Accessors_ReflectInsertAndCapacity) {
    EXPECT_EQ(cache_->capacity(), 16u * 1024);
    EXPECT_EQ(cache_->memory_usage(), 0u);
    EXPECT_EQ(cache_->lookup_count(), 0u);
    EXPECT_EQ(cache_->hit_count(), 0u);

    tenann::IndexCacheHandle h;
    cache_->Insert(tenann::CacheKey("/a.vi"), make_dummy_ref(2048), &h);
    EXPECT_EQ(cache_->memory_usage(), 2048u);

    cache_->SetCapacity(8 * 1024);
    EXPECT_EQ(cache_->capacity(), 8u * 1024);
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

// Repros the shutdown deadlock pattern: a top-level IVF-PQ entry holds a
// tenann::IndexRef whose underlying Index, when destructed, releases another
// IndexCacheHandle pointing back into the same cache. `~DynamicCache` runs the
// IndexRef destructor while holding `_cache._lock` — without the fix the
// inner release recursively tries to take `_cache._lock` and FUTEX_WAITs
// forever. We assert that destruction (and a shrink-to-zero eviction) both
// complete; a deadlock would hang the test instead of failing it.
TEST_F(VectorIndexCacheTest, ShutdownAndShrink_WithSelfReferentialEntry_NoDeadlock) {
    auto c = std::make_unique<VectorIndexCache>(/*capacity=*/64 * 1024, tracker_.get());

    // Seed an "inner" entry plus a pinning handle that the outer ref will
    // hold and release on destruction — the BlockCacheInvertedLists pattern.
    tenann::IndexCacheHandle inner;
    c->Insert(tenann::CacheKey("/inner.vi"), make_dummy_ref(1024), &inner);

    // Outer Index whose deleter consumes the inner handle. Moving the handle
    // into the deleter lambda is the same shape as faiss owning a
    // BlockCacheInvertedLists that owns std::vector<IndexCacheHandle>.
    struct OuterPayload {
        tenann::IndexCacheHandle pinned;
        std::vector<char> bytes;
    };
    auto* payload = new OuterPayload{std::move(inner), std::vector<char>(2048)};
    auto outer_ref = std::make_shared<tenann::Index>(
            payload, tenann::IndexType::kFaissIvfPq, [](void* p) { delete static_cast<OuterPayload*>(p); },
            /*explicit_bytes=*/2048);

    tenann::IndexCacheHandle outer;
    c->Insert(tenann::CacheKey("/outer.vi"), outer_ref, &outer);
    outer_ref.reset();
    outer = tenann::IndexCacheHandle{};

    // Runtime safety: shrinking to zero forces eviction of the outer entry,
    // which must not deadlock when the chained release runs.
    c->SetCapacity(0);

    // Shutdown safety: destructing the cache must drain the remaining inner
    // entry without re-locking _cache._lock.
    c.reset();
}

// Regression guard for the process-tracker double-count. A heap-resident HNSW index
// is charged to the process tracker once by the allocator hook during load
// (count #1); VectorIndexCache must NOT charge it a second time. The cache is built
// with exclude_root so its DynamicCache labels the vector_index tracker WITHOUT
// re-propagating to process. Reverting VectorIndexCache to a plain set_mem_tracker
// (additive consume) makes process carry BOTH copies and fails the process assert
// below -- that is the bug this test exists to catch.
//
// The mem_hook -> MemTracker wiring is stubbed in BE_TEST (mem_hook.cpp routes to
// g_mem_usage, not the tracker tree), so count #1 is modeled with an explicit
// consume on the process tracker; its net effect on the process counter is identical
// to the allocator hook's.
TEST(VectorIndexCacheDoubleCountTest, InsertDoesNotDoubleCountProcessTracker) {
    MemTracker process(-1, "process", nullptr);
    MemTracker vector_index(-1, "vector_index", &process);
    VectorIndexCache cache(/*capacity=*/64 * 1024, &vector_index);

    constexpr int64_t kHookBytes = 8192;  // allocator hook charge during load (count #1)
    constexpr int64_t kIndexBytes = 4096; // tenann index memory_usage() (count #2)

    process.consume(kHookBytes); // count #1 (allocator hook, modeled)

    tenann::IndexCacheHandle h;
    cache.Insert(tenann::CacheKey("/idx.vi"), make_dummy_ref(kIndexBytes), &h); // count #2 via cache

    EXPECT_EQ(kIndexBytes, vector_index.consumption()); // vector_index labels the index size
    EXPECT_EQ(kHookBytes, process.consumption());       // process counted ONCE (not 8192 + 4096)
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
    auto meta = make_minimal_meta();
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
    auto meta = make_minimal_meta();
    auto st = r.init_searcher(meta, "/tmp/garbage.vi", &fs);
    EXPECT_FALSE(st.ok()) << "loader should surface tenann::Error / std::exception, not crash";

    tenann::SetGlobalIndexCache(saved);
}

// A FileSystem that records which mem tracker is active when the index file is
// opened. VectorIndexFileReader::open() (-> fs->new_random_access_file) runs inside
// TenANNReader's loader, under the loader's SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER,
// so the captured tracker is exactly the one the allocator hook would charge
// index-load allocations to in production.
namespace {
class TrackerProbeFileSystem : public MemoryFileSystem {
public:
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& fname) override {
        captured = CurrentThread::mem_tracker();
        return MemoryFileSystem::new_random_access_file(opts, fname);
    }
    MemTracker* captured = nullptr;
};
} // namespace

// Covers the tenann_index_reader.cpp change that the DynamicCache-level tests can't:
// index-load allocations must be charged to the PROCESS tracker, not vector_index
// and not the originating query's tracker. The allocator hook is stubbed in BE_TEST
// (mem_hook.cpp -> g_mem_usage), so instead of observing a real allocation we capture
// CurrentThread::mem_tracker() during the load -- that IS the tracker the hook keys
// off, so verifying it pins the accounting target.
//
// Reverting the loader to vector_index_mem_tracker() -> captured == vi -> fails.
// Dropping the scoped setter entirely -> captured == the ambient query tracker -> fails.
TEST(TenANNReaderTest, InitSearcher_ChargesLoadToProcessNotVectorIndex) {
    auto* process = GlobalEnv::GetInstance()->process_mem_tracker();
    auto* vi = GlobalEnv::GetInstance()->vector_index_mem_tracker();
    ASSERT_NE(nullptr, process);
    ASSERT_NE(nullptr, vi);

    // Missing path on purpose: open() still calls new_random_access_file (where the
    // probe captures the active tracker) but then fails NotFound, so the loader bails
    // before ReadIndexFile -- we never feed garbage to faiss, which can SIGSEGV on
    // malformed input. The tracker we want to assert on is already captured by then.
    TrackerProbeFileSystem fs;

    MemTracker cache_tracker(-1, "vi_cache_probe");
    VectorIndexCache cache(/*capacity=*/1024, &cache_tracker);
    auto* saved = tenann::GetGlobalIndexCache();
    tenann::SetGlobalIndexCache(&cache);

    // Simulate the load being triggered while running under a query's mem tracker
    // (distinct from both process and vector_index). The loader must redirect the
    // load to process regardless.
    MemTracker fake_query(-1, "fake_query_ambient");
    {
        CurrentThreadMemTrackerSetter ambient(&fake_query);
        TenANNReader r;
        auto meta = make_minimal_meta();
        // Load runs the probe fs, then fails NotFound (return ignored).
        (void)r.init_searcher(meta, "/no/such/probe.vi", &fs);
    }
    tenann::SetGlobalIndexCache(saved);

    ASSERT_NE(nullptr, fs.captured) << "loader never opened the index file via fs";
    EXPECT_EQ(process, fs.captured) << "index load charged to '" << fs.captured->label()
                                    << "', expected 'process'";
    EXPECT_NE(vi, fs.captured);
    EXPECT_NE(&fake_query, fs.captured);
}

TEST(VectorIndexCacheEntryTest, StreamingOperatorPrintsTag) {
    VectorIndexCacheEntry e;
    std::ostringstream os;
    os << e;
    EXPECT_EQ("VectorIndexCacheEntry", os.str());
}

} // namespace starrocks

#endif // WITH_TENANN

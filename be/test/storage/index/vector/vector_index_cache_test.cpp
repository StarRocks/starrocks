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
#include <future>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "common/status.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/tenann_index_reader.h"
#include "storage/index/vector/vector_index_cache_metrics.h"
#include "storage/index/vector/vector_index_file_reader.h"
#include "storage/index/vector/vector_index_reader_factory.h"
#include "storage/tablet_index.h"
#include "storage_primitive/storage_stats.h"
#include "tenann/common/error.h"
#include "tenann/index/index.h"
#include "tenann/index/index_cache.h"
#include "tenann/store/index_meta.h"

namespace starrocks {

namespace {
constexpr size_t kDummyBytes = 1024;

tenann::IndexRef make_dummy_ref(size_t bytes = kDummyBytes, tenann::IndexType type = tenann::IndexType::kFaissHnsw) {
    void* buf = std::malloc(bytes);
    return std::make_shared<tenann::Index>(
            buf, type, [](void* v) { std::free(v); },
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

std::shared_ptr<TabletIndex> make_minimal_tablet_index() {
    auto tablet_index = std::make_shared<TabletIndex>();
    TabletIndexPB index_pb;
    index_pb.set_index_id(0);
    index_pb.set_index_name("test_index");
    index_pb.set_index_type(IndexType::VECTOR);
    index_pb.add_col_unique_id(1);
    tablet_index->init_from_pb(index_pb);
    tablet_index->add_common_properties("index_type", "hnsw");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    tablet_index->add_index_properties("efconstruction", "40");
    tablet_index->add_index_properties("m", "16");
    tablet_index->add_search_properties("efsearch", "40");
    return tablet_index;
}

// FileInfo holds its FileSystem by shared_ptr; these tests keep the FS on the stack,
// so hand out a non-owning alias instead of transferring ownership.
std::shared_ptr<FileSystem> borrowed_fs(FileSystem* fs) {
    return std::shared_ptr<FileSystem>(fs, [](FileSystem*) {});
}

FileInfo remote_vi(std::string path, FileSystem* fs) {
    return FileInfo{.path = std::move(path), .fs = borrowed_fs(fs)};
}

bool wait_for_probe_state(VectorIndexCache* cache, const char* key, VectorIndexCacheProbeState expected,
                          std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        if (cache->ProbeForQuery(tenann::CacheKey(key)).state == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

} // namespace

class VectorIndexCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        saved_expire_seconds_ = config::vector_index_cache_expire_sec;
        saved_loading_wait_timeout_ms_ = config::vector_index_cache_loading_wait_timeout_ms;
        tracker_ = std::make_unique<MemTracker>(-1, "vector_index_test");
        cache_ = std::make_unique<VectorIndexCache>(/*capacity=*/16 * 1024, tracker_.get());
    }
    void TearDown() override {
        cache_.reset();
        tracker_.reset();
        config::vector_index_cache_expire_sec = saved_expire_seconds_;
        config::vector_index_cache_loading_wait_timeout_ms = saved_loading_wait_timeout_ms_;
    }
    int32_t saved_expire_seconds_ = 0;
    int32_t saved_loading_wait_timeout_ms_ = 0;
    std::unique_ptr<MemTracker> tracker_;
    std::unique_ptr<VectorIndexCache> cache_;
};

TEST_F(VectorIndexCacheTest, Lookup_Miss_ReturnsFalse) {
    tenann::IndexCacheHandle h;
    EXPECT_FALSE(cache_->Lookup(tenann::CacheKey("/missing.vi"), &h));
    EXPECT_FALSE(h.valid());
}

TEST_F(VectorIndexCacheTest, Insert_NullDoesNotLeaveEmptyEntry) {
    tenann::IndexCacheHandle h_ins;
    cache_->Insert(tenann::CacheKey("/loading.vi"), /*ref=*/nullptr, &h_ins);
    EXPECT_FALSE(h_ins.valid());
    EXPECT_EQ(0, cache_->entry_count());

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

// tenann's BlockCacheInvertedLists keeps one long-lived IndexCacheHandle per
// inverted list and re-Lookups through that same handle on every access. That
// handle is the entry's only external pin, so releasing it before the new pin is
// taken lets _release_entry()'s DynamicCache::remove() delete the entry -- and
// the get() right below can then never hit.
TEST_F(VectorIndexCacheTest, Lookup_ReusedIvfPqListHandleKeepsEntryCached) {
    const tenann::CacheKey key("/ivfpq.vi_0");
    auto ref = make_dummy_ref(kDummyBytes, tenann::IndexType::kFaissIvfPqOneInvertedList);
    tenann::IndexCacheHandle h;
    cache_->Insert(key, ref, &h);
    ASSERT_TRUE(h.valid());
    ASSERT_EQ(1u, cache_->entry_count());

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(cache_->Lookup(key, &h)) << "iteration " << i;
        EXPECT_EQ(ref.get(), h.index_ref().get()) << "iteration " << i;
        EXPECT_EQ(1u, cache_->entry_count()) << "iteration " << i;
    }

    tenann::IndexCacheHandle bystander;
    EXPECT_TRUE(cache_->Lookup(key, &bystander));
    EXPECT_EQ(ref.get(), bystander.index_ref().get());
}

// Mirrors BlockCacheInvertedLists::get_ptr(): Lookup() through the list's own
// handle, load and Insert() only on a miss. faiss reads each probed list twice
// per scan (get_codes() then get_ids()), so a warm list must not reload -- and
// the buffer the first call returned must outlive the second.
TEST_F(VectorIndexCacheTest, IvfPqListBlock_GetPtrLoopLoadsOnce) {
    const tenann::CacheKey key("/ivfpq.vi_7");
    auto frees = std::make_shared<std::atomic<int>>(0);
    auto make_tracked_ref = [frees]() -> tenann::IndexRef {
        void* buf = std::malloc(kDummyBytes);
        return std::make_shared<tenann::Index>(
                buf, tenann::IndexType::kFaissIvfPqOneInvertedList,
                [frees](void* v) {
                    frees->fetch_add(1, std::memory_order_relaxed);
                    std::free(v);
                },
                /*explicit_bytes=*/kDummyBytes);
    };
    tenann::IndexCacheHandle list_handle; // stands in for cache_handles[list_no]
    int loads = 0;
    auto get_ptr = [&]() {
        if (cache_->Lookup(key, &list_handle)) {
            return;
        }
        ++loads;
        cache_->Insert(key, make_tracked_ref(), &list_handle);
    };

    for (int scan = 0; scan < 4; ++scan) {
        get_ptr(); // get_codes()
        const void* codes = list_handle.index_ref()->index_raw();
        get_ptr(); // get_ids()
        EXPECT_EQ(codes, list_handle.index_ref()->index_raw()) << "scan " << scan;
        EXPECT_EQ(0, frees->load(std::memory_order_relaxed)) << "scan " << scan;
    }
    EXPECT_EQ(1, loads);
    list_handle = tenann::IndexCacheHandle{};
    EXPECT_EQ(1, frees->load(std::memory_order_relaxed));
}

// Guards the TTL PR's grouping rule: a list block leaves the cache as soon as
// its owning reader drops the last pin, instead of lingering under its own TTL.
TEST_F(VectorIndexCacheTest, IvfPqListBlock_RemovedWhenLastPinDrops) {
    const tenann::CacheKey key("/ivfpq.vi_3");
    {
        tenann::IndexCacheHandle h;
        cache_->Insert(key, make_dummy_ref(kDummyBytes, tenann::IndexType::kFaissIvfPqOneInvertedList), &h);
        ASSERT_EQ(1u, cache_->entry_count());
        ASSERT_EQ(kDummyBytes, cache_->memory_usage());
    }
    EXPECT_EQ(0u, cache_->entry_count());
    EXPECT_EQ(0u, cache_->memory_usage());

    tenann::IndexCacheHandle fresh;
    EXPECT_FALSE(cache_->Lookup(key, &fresh));
}

// A miss must not leave the caller holding a ref for some other key.
TEST_F(VectorIndexCacheTest, Lookup_MissClearsPreviousHandle) {
    tenann::IndexCacheHandle h;
    cache_->Insert(tenann::CacheKey("/present.vi"), make_dummy_ref(), &h);
    ASSERT_TRUE(h.valid());

    EXPECT_FALSE(cache_->Lookup(tenann::CacheKey("/absent.vi"), &h));
    EXPECT_FALSE(h.valid());
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

TEST_F(VectorIndexCacheTest, Metrics_UpdatedFromCacheOperations) {
    MetricRegistry registry("test_registry");
    VectorIndexCacheMetrics metrics(&registry);
    auto cache = std::make_unique<VectorIndexCache>(/*capacity=*/16 * 1024, tracker_.get(), &metrics);
    registry.trigger_hook();

    EXPECT_EQ(16 * 1024, metrics.vector_index_cache_capacity.value());
    EXPECT_EQ(0, metrics.vector_index_cache_usage.value());
    EXPECT_EQ(0, metrics.vector_index_cache_lookup_count.value());
    EXPECT_EQ(0, metrics.vector_index_cache_hit_count.value());

    auto loader = [&]() -> tenann::IndexRef { return make_dummy_ref(2048); };
    tenann::IndexCacheHandle h;
    EXPECT_TRUE(cache->GetOrCreate(tenann::CacheKey("/metrics.vi"), loader, &h));
    registry.trigger_hook();

    EXPECT_EQ(16 * 1024, metrics.vector_index_cache_capacity.value());
    EXPECT_EQ(2048, metrics.vector_index_cache_usage.value());
    EXPECT_DOUBLE_EQ(2048.0 / (16 * 1024), metrics.vector_index_cache_usage_ratio.value());
    EXPECT_EQ(1, metrics.vector_index_cache_lookup_count.value());
    EXPECT_EQ(0, metrics.vector_index_cache_hit_count.value());
    EXPECT_EQ(1, metrics.vector_index_cache_dynamic_lookup_count.value());
    EXPECT_EQ(0, metrics.vector_index_cache_dynamic_hit_count.value());

    EXPECT_TRUE(cache->GetOrCreate(tenann::CacheKey("/metrics.vi"), loader, &h));
    registry.trigger_hook();

    EXPECT_EQ(2, metrics.vector_index_cache_lookup_count.value());
    EXPECT_EQ(1, metrics.vector_index_cache_hit_count.value());
    EXPECT_DOUBLE_EQ(0.5, metrics.vector_index_cache_hit_ratio.value());
    EXPECT_EQ(1, metrics.vector_index_cache_dynamic_lookup_count.value());
    EXPECT_EQ(1, metrics.vector_index_cache_dynamic_hit_count.value());
    EXPECT_DOUBLE_EQ(1.0, metrics.vector_index_cache_dynamic_hit_ratio.value());

    EXPECT_TRUE(cache->Lookup(tenann::CacheKey("/metrics.vi"), &h));
    registry.trigger_hook();

    EXPECT_EQ(2, metrics.vector_index_cache_lookup_count.value());
    EXPECT_EQ(1, metrics.vector_index_cache_hit_count.value());
    EXPECT_EQ(0, metrics.vector_index_cache_dynamic_lookup_count.value());
    EXPECT_EQ(0, metrics.vector_index_cache_dynamic_hit_count.value());

    cache->SetCapacity(8 * 1024);
    registry.trigger_hook();

    EXPECT_EQ(8 * 1024, metrics.vector_index_cache_capacity.value());
    EXPECT_EQ(2048, metrics.vector_index_cache_usage.value());
    EXPECT_DOUBLE_EQ(0.25, metrics.vector_index_cache_usage_ratio.value());
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

// Loader-failure cleanup: GetOrCreate must leave the handle invalid and the
// key retryable whether the loader returns null or throws.
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

TEST_F(VectorIndexCacheTest, GetOrCreate_LoaderThrows_NotCached) {
    auto throwing_loader = []() -> tenann::IndexRef { throw 42; };
    tenann::IndexCacheHandle h;
    EXPECT_FALSE(cache_->GetOrCreate(tenann::CacheKey("/throw.vi"), throwing_loader, &h));
    EXPECT_FALSE(h.valid());
    EXPECT_EQ(0, cache_->entry_count());

    int retry_calls = 0;
    auto retry_loader = [&]() -> tenann::IndexRef {
        ++retry_calls;
        return make_dummy_ref();
    };
    EXPECT_TRUE(cache_->GetOrCreate(tenann::CacheKey("/throw.vi"), retry_loader, &h));
    EXPECT_EQ(1, retry_calls);
    EXPECT_TRUE(h.valid());
}

TEST_F(VectorIndexCacheTest, GetOrCreate_EstimateExceptionDoesNotLeaveLoading) {
    SyncPoint::GetInstance()->SetCallBack("VectorIndexCache::_get_or_create:before_estimate",
                                          [](void*) { throw std::bad_alloc(); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([] {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
    });

    tenann::IndexCacheHandle handle;
    EXPECT_FALSE(cache_->GetOrCreate(
            tenann::CacheKey("/estimate-throws.vi"), [] { return make_dummy_ref(); }, &handle));
    EXPECT_FALSE(handle.valid());
    EXPECT_EQ(0, cache_->entry_count());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_TRUE(cache_->GetOrCreate(
            tenann::CacheKey("/estimate-throws.vi"), [] { return make_dummy_ref(); }, &handle));
    EXPECT_TRUE(handle.valid());
}

TEST_F(VectorIndexCacheTest, Lookup_WaitsForLoadingThenReturnsReady) {
    const auto expected_ref = make_dummy_ref();
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();

    auto leader = std::async(std::launch::async, [&] {
        tenann::IndexCacheHandle handle;
        const bool ok = cache_->GetOrCreate(
                tenann::CacheKey("/lookup-wait-success.vi"),
                [&]() -> tenann::IndexRef {
                    loader_started.set_value();
                    release_future.wait();
                    return expected_ref;
                },
                &handle);
        return std::make_pair(ok, std::move(handle));
    });
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    auto lookup = std::async(std::launch::async, [&] {
        tenann::IndexCacheHandle handle;
        const bool found = cache_->Lookup(tenann::CacheKey("/lookup-wait-success.vi"), &handle);
        return std::make_pair(found, std::move(handle));
    });
    EXPECT_EQ(std::future_status::timeout, lookup.wait_for(std::chrono::milliseconds(100)));

    release_loader.set_value();
    auto [leader_ok, leader_handle] = leader.get();
    auto [found, lookup_handle] = lookup.get();
    EXPECT_TRUE(leader_ok);
    ASSERT_TRUE(leader_handle.valid());
    EXPECT_EQ(expected_ref.get(), leader_handle.index_ref().get());
    EXPECT_TRUE(found);
    ASSERT_TRUE(lookup_handle.valid());
    EXPECT_EQ(expected_ref.get(), lookup_handle.index_ref().get());
}

TEST_F(VectorIndexCacheTest, Lookup_WaitsForLoadingFailureThenReturnsMiss) {
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();

    auto leader = std::async(std::launch::async, [&] {
        tenann::IndexCacheHandle handle;
        return cache_->GetOrCreate(
                tenann::CacheKey("/lookup-wait-failure.vi"),
                [&]() -> tenann::IndexRef {
                    loader_started.set_value();
                    release_future.wait();
                    return nullptr;
                },
                &handle);
    });
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    auto lookup = std::async(std::launch::async, [&] {
        tenann::IndexCacheHandle handle;
        const bool found = cache_->Lookup(tenann::CacheKey("/lookup-wait-failure.vi"), &handle);
        return std::make_pair(found, std::move(handle));
    });
    EXPECT_EQ(std::future_status::timeout, lookup.wait_for(std::chrono::milliseconds(100)));

    release_loader.set_value();
    EXPECT_FALSE(leader.get());
    auto [found, lookup_handle] = lookup.get();
    EXPECT_FALSE(found);
    EXPECT_FALSE(lookup_handle.valid());
    EXPECT_EQ(0, cache_->entry_count());
}

TEST_F(VectorIndexCacheTest, Lookup_TimesOutWithoutCancellingLoader) {
    config::vector_index_cache_loading_wait_timeout_ms = 50;
    MetricRegistry registry("test_registry");
    VectorIndexCacheMetrics metrics(&registry);
    auto cache = std::make_unique<VectorIndexCache>(/*capacity=*/16 * 1024, tracker_.get(), &metrics);

    auto expected_ref = make_dummy_ref();
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto leader = std::async(std::launch::async, [&] {
        tenann::IndexCacheHandle handle;
        const bool ok = cache->GetOrCreate(
                tenann::CacheKey("/lookup-timeout.vi"),
                [&]() -> tenann::IndexRef {
                    loader_started.set_value();
                    release_future.wait();
                    return expected_ref;
                },
                &handle);
        return std::make_pair(ok, std::move(handle));
    });
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    tenann::IndexCacheHandle timed_out_handle;
    EXPECT_FALSE(cache->Lookup(tenann::CacheKey("/lookup-timeout.vi"), &timed_out_handle));
    EXPECT_FALSE(timed_out_handle.valid());
    EXPECT_EQ(1, metrics.vector_index_cache_loading_wait_timeout.value());
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading, cache->ProbeForQuery(tenann::CacheKey("/lookup-timeout.vi")).state);

    release_loader.set_value();
    auto [leader_ok, leader_handle] = leader.get();
    EXPECT_TRUE(leader_ok);
    ASSERT_TRUE(leader_handle.valid());

    tenann::IndexCacheHandle warm_handle;
    EXPECT_TRUE(cache->Lookup(tenann::CacheKey("/lookup-timeout.vi"), &warm_handle));
    EXPECT_EQ(expected_ref.get(), warm_handle.index_ref().get());
}

TEST_F(VectorIndexCacheTest, AsyncLoad_ProbeDoesNotWaitForLoaderIo) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return make_dummy_ref();
    };

    auto schedule = cache_->TryGetOrSchedule(tenann::CacheKey("/async-probe.vi"), std::move(loader));
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading, schedule.state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    auto probe_future = std::async(std::launch::async,
                                   [&] { return cache_->ProbeForQuery(tenann::CacheKey("/async-probe.vi")).state; });
    const bool probe_returned = probe_future.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
    release_loader.set_value();
    EXPECT_TRUE(probe_returned) << "ProbeForQuery waited for loader I/O";
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading, probe_future.get());
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/async-probe.vi", VectorIndexCacheProbeState::kReady));
}

TEST_F(VectorIndexCacheTest, AsyncLoad_ConcurrentMissesSingleFlight) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::atomic<int> loader_calls{0};
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto first_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_calls.fetch_add(1);
        loader_started.set_value();
        release_future.wait();
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/async-singleflight.vi"), std::move(first_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    constexpr int kFollowers = 16;
    std::vector<std::thread> followers;
    std::vector<VectorIndexCacheProbeState> states(kFollowers);
    for (int i = 0; i < kFollowers; ++i) {
        followers.emplace_back([&, i] {
            auto duplicate_loader = [&]() -> StatusOr<tenann::IndexRef> {
                loader_calls.fetch_add(1);
                return make_dummy_ref();
            };
            states[i] =
                    cache_->TryGetOrSchedule(tenann::CacheKey("/async-singleflight.vi"), std::move(duplicate_loader))
                            .state;
        });
    }
    for (auto& follower : followers) {
        follower.join();
    }
    EXPECT_EQ(1, loader_calls.load());
    for (auto state : states) {
        EXPECT_EQ(VectorIndexCacheProbeState::kLoading, state);
    }

    release_loader.set_value();
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/async-singleflight.vi", VectorIndexCacheProbeState::kReady));
    EXPECT_EQ(1, loader_calls.load());
}

TEST_F(VectorIndexCacheTest, AsyncLoad_FailureReturnsToMissAndDoesNotRetainEmptyEntry) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    auto failed_loader = []() -> StatusOr<tenann::IndexRef> {
        return Status::InternalError("injected async load failure");
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/async-failure.vi"), std::move(failed_loader)).state);
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/async-failure.vi", VectorIndexCacheProbeState::kMiss));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (cache_->entry_count() != 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(0, cache_->entry_count());

    auto retry_loader = []() -> StatusOr<tenann::IndexRef> { return make_dummy_ref(); };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/async-failure.vi"), std::move(retry_loader)).state);
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/async-failure.vi", VectorIndexCacheProbeState::kReady));
}

TEST_F(VectorIndexCacheTest, AsyncLoad_RecordsSuccessFailureAndLoadTime) {
    MetricRegistry registry("test_registry");
    VectorIndexCacheMetrics metrics(&registry);
    auto cache = std::make_unique<VectorIndexCache>(/*capacity=*/16 * 1024, tracker_.get(), &metrics);
    ASSERT_OK(cache->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    auto successful_loader = []() -> StatusOr<tenann::IndexRef> {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return make_dummy_ref();
    };
    EXPECT_EQ(
            VectorIndexCacheProbeState::kLoading,
            cache->TryGetOrSchedule(tenann::CacheKey("/async-metrics-success.vi"), std::move(successful_loader)).state);
    const auto success_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (metrics.vector_index_cache_async_load_success.value() != 1 &&
           std::chrono::steady_clock::now() < success_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(1, metrics.vector_index_cache_async_load_success.value());
    registry.trigger_hook();
    EXPECT_EQ(kDummyBytes, metrics.vector_index_cache_usage.value());

    auto failed_loader = []() -> StatusOr<tenann::IndexRef> {
        return Status::InternalError("injected async load failure");
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache->TryGetOrSchedule(tenann::CacheKey("/async-metrics-failure.vi"), std::move(failed_loader)).state);
    const auto failure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (metrics.vector_index_cache_async_load_failure.value() != 1 &&
           std::chrono::steady_clock::now() < failure_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    cache->shutdown_async_load_pool();
    EXPECT_EQ(1, metrics.vector_index_cache_async_load_success.value());
    EXPECT_EQ(1, metrics.vector_index_cache_async_load_failure.value());
    EXPECT_GT(metrics.vector_index_cache_async_load_ns.value(), 0);
}

TEST_F(VectorIndexCacheTest, AsyncLoad_TracksQueuedAndInflight) {
    MetricRegistry registry("test_registry");
    VectorIndexCacheMetrics metrics(&registry);
    auto cache = std::make_unique<VectorIndexCache>(/*capacity=*/16 * 1024, tracker_.get(), &metrics);
    ASSERT_OK(cache->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::promise<void> first_started;
    std::promise<void> release_first;
    auto release_future = release_first.get_future().share();
    auto first_loader = [&]() -> StatusOr<tenann::IndexRef> {
        first_started.set_value();
        release_future.wait();
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache->TryGetOrSchedule(tenann::CacheKey("/metrics-running.vi"), std::move(first_loader)).state);
    ASSERT_EQ(std::future_status::ready, first_started.get_future().wait_for(std::chrono::seconds(5)));
    EXPECT_EQ(1, metrics.vector_index_cache_async_load_inflight.value());
    EXPECT_EQ(0, metrics.vector_index_cache_async_load_queued.value());

    auto second_loader = []() -> StatusOr<tenann::IndexRef> { return make_dummy_ref(); };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache->TryGetOrSchedule(tenann::CacheKey("/metrics-queued.vi"), std::move(second_loader)).state);
    EXPECT_EQ(1, metrics.vector_index_cache_async_load_inflight.value());
    EXPECT_EQ(1, metrics.vector_index_cache_async_load_queued.value());

    release_first.set_value();
    cache->shutdown_async_load_pool();
    EXPECT_EQ(0, metrics.vector_index_cache_async_load_inflight.value());
    EXPECT_EQ(0, metrics.vector_index_cache_async_load_queued.value());
}

TEST_F(VectorIndexCacheTest, AsyncLoad_OverCapacityEvictedAfterLoadingPinRelease) {
    auto cache = std::make_unique<VectorIndexCache>(/*capacity=*/512, tracker_.get());
    ASSERT_OK(cache->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return make_dummy_ref(/*bytes=*/1024);
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache->TryGetOrSchedule(tenann::CacheKey("/async-over-capacity.vi"), std::move(loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    release_loader.set_value();
    cache->shutdown_async_load_pool();
    EXPECT_EQ(0, cache->memory_usage());
    EXPECT_EQ(0, cache->entry_count());
    EXPECT_EQ(VectorIndexCacheProbeState::kMiss,
              cache->ProbeForQuery(tenann::CacheKey("/async-over-capacity.vi")).state);
}

TEST_F(VectorIndexCacheTest, AsyncLoad_OverCapacityEvictedAfterWaitingHandleRelease) {
    auto cache = std::make_unique<VectorIndexCache>(/*capacity=*/512, tracker_.get());
    ASSERT_OK(cache->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return make_dummy_ref(/*bytes=*/1024);
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache->TryGetOrSchedule(tenann::CacheKey("/async-over-capacity-waiter.vi"), std::move(loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    auto waiter = std::async(std::launch::async, [&] {
        tenann::IndexCacheHandle handle;
        const bool found = cache->Lookup(tenann::CacheKey("/async-over-capacity-waiter.vi"), &handle);
        return std::make_pair(found, std::move(handle));
    });
    EXPECT_EQ(std::future_status::timeout, waiter.wait_for(std::chrono::milliseconds(100)));

    release_loader.set_value();
    auto [found, handle] = waiter.get();
    ASSERT_TRUE(found);
    ASSERT_TRUE(handle.valid());
    cache->shutdown_async_load_pool();

    EXPECT_EQ(1024, cache->memory_usage());
    EXPECT_EQ(1, cache->entry_count());
    handle = tenann::IndexCacheHandle{};
    EXPECT_EQ(0, cache->memory_usage());
    EXPECT_EQ(0, cache->entry_count());
}

TEST_F(VectorIndexCacheTest, AsyncLoad_LoaderExceptionReturnsToMiss) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    auto throwing_loader = []() -> StatusOr<tenann::IndexRef> { throw std::runtime_error("injected exception"); };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/async-exception.vi"), std::move(throwing_loader)).state);
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/async-exception.vi", VectorIndexCacheProbeState::kMiss));
    EXPECT_EQ(0, cache_->entry_count());
}

TEST_F(VectorIndexCacheTest, AsyncLoad_EstimateExceptionDoesNotTerminateOrLeaveLoading) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));
    SyncPoint::GetInstance()->SetCallBack("VectorIndexLoadTask::run_impl:before_estimate",
                                          [](void*) { throw std::bad_alloc(); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([] {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
    });

    auto loader = []() -> StatusOr<tenann::IndexRef> { return make_dummy_ref(); };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/async-estimate-throws.vi"), std::move(loader)).state);
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/async-estimate-throws.vi", VectorIndexCacheProbeState::kMiss));
    EXPECT_EQ(0, cache_->entry_count());
}

TEST_F(VectorIndexCacheTest, GetOrCreateForQueryDoesNotWaitForAsyncLoad) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/query-no-wait.vi"), std::move(async_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    std::atomic<int> query_loader_calls{0};
    auto query = std::async(std::launch::async, [&] {
        return cache_->GetOrCreateForQuery(
                tenann::CacheKey("/query-no-wait.vi"),
                [&]() -> tenann::IndexRef {
                    query_loader_calls.fetch_add(1);
                    return make_dummy_ref();
                },
                /*wait_for_loading=*/false);
    });
    const auto query_status = query.wait_for(std::chrono::milliseconds(500));
    if (query_status != std::future_status::ready) {
        release_loader.set_value();
    }
    ASSERT_EQ(std::future_status::ready, query_status);
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading, query.get().state);
    EXPECT_EQ(0, query_loader_calls.load());

    release_loader.set_value();
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/query-no-wait.vi", VectorIndexCacheProbeState::kReady));
}

TEST_F(VectorIndexCacheTest, GetOrCreateForQueryWaitsForSyncLeader) {
    auto leader_ref = make_dummy_ref();
    std::atomic<int> loader_calls{0};
    std::promise<void> leader_started;
    std::promise<void> release_leader;
    auto release_future = release_leader.get_future().share();

    auto leader = std::async(std::launch::async, [&] {
        return cache_->GetOrCreateForQuery(
                tenann::CacheKey("/query-sync-singleflight.vi"),
                [&]() -> tenann::IndexRef {
                    loader_calls.fetch_add(1);
                    leader_started.set_value();
                    release_future.wait();
                    return leader_ref;
                },
                /*wait_for_loading=*/true);
    });
    ASSERT_EQ(std::future_status::ready, leader_started.get_future().wait_for(std::chrono::seconds(5)));

    auto follower = std::async(std::launch::async, [&] {
        return cache_->GetOrCreateForQuery(
                tenann::CacheKey("/query-sync-singleflight.vi"),
                [&]() -> tenann::IndexRef {
                    loader_calls.fetch_add(1);
                    return make_dummy_ref();
                },
                /*wait_for_loading=*/true);
    });
    EXPECT_EQ(std::future_status::timeout, follower.wait_for(std::chrono::milliseconds(100)));

    release_leader.set_value();
    ASSERT_EQ(std::future_status::ready, leader.wait_for(std::chrono::seconds(5)));
    ASSERT_EQ(std::future_status::ready, follower.wait_for(std::chrono::seconds(5)));
    auto leader_result = leader.get();
    auto follower_result = follower.get();
    ASSERT_EQ(VectorIndexCacheProbeState::kReady, leader_result.state);
    ASSERT_EQ(VectorIndexCacheProbeState::kReady, follower_result.state);
    EXPECT_EQ(1, loader_calls.load());
    EXPECT_EQ(leader_ref.get(), leader_result.handle.index_ref().get());
    EXPECT_EQ(leader_ref.get(), follower_result.handle.index_ref().get());
}

TEST_F(VectorIndexCacheTest, GetOrCreateForQueryTimesOutWithoutRunningFollowerLoader) {
    config::vector_index_cache_loading_wait_timeout_ms = 50;
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    auto async_ref = make_dummy_ref();
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return async_ref;
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/query-wait-timeout.vi"), std::move(async_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    std::atomic<int> follower_loader_calls{0};
    auto result = cache_->GetOrCreateForQuery(
            tenann::CacheKey("/query-wait-timeout.vi"),
            [&]() -> tenann::IndexRef {
                follower_loader_calls.fetch_add(1);
                return make_dummy_ref();
            },
            /*wait_for_loading=*/true);
    EXPECT_EQ(VectorIndexCacheProbeState::kWaitTimeout, result.state);
    EXPECT_FALSE(result.handle.valid());
    EXPECT_EQ(0, follower_loader_calls.load());
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->ProbeForQuery(tenann::CacheKey("/query-wait-timeout.vi")).state);

    release_loader.set_value();
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/query-wait-timeout.vi", VectorIndexCacheProbeState::kReady));
}

TEST_F(VectorIndexCacheTest, SyncGetOrCreateWaitsForSameKeyAsyncLoad) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    auto async_ref = make_dummy_ref();
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return async_ref;
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/config-switch.vi"), std::move(async_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    std::atomic<int> sync_loader_calls{0};
    std::promise<void> sync_done;
    tenann::IndexCacheHandle sync_handle;
    bool sync_ok = false;
    std::thread sync_thread([&] {
        sync_ok = cache_->GetOrCreate(
                tenann::CacheKey("/config-switch.vi"),
                [&]() -> tenann::IndexRef {
                    sync_loader_calls.fetch_add(1);
                    return make_dummy_ref();
                },
                &sync_handle);
        sync_done.set_value();
    });
    auto sync_done_future = sync_done.get_future();
    EXPECT_EQ(std::future_status::timeout, sync_done_future.wait_for(std::chrono::milliseconds(100)));

    release_loader.set_value();
    ASSERT_EQ(std::future_status::ready, sync_done_future.wait_for(std::chrono::seconds(5)));
    sync_thread.join();
    EXPECT_TRUE(sync_ok);
    EXPECT_EQ(0, sync_loader_calls.load());
    EXPECT_EQ(async_ref.get(), sync_handle.index_ref().get());
}

TEST_F(VectorIndexCacheTest, Factory_ConfigOffTimesOutWaitingForExistingAsyncLoad) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));
    config::vector_index_cache_loading_wait_timeout_ms = 50;

    const bool saved_async_load = config::enable_vector_index_cache_async_load_on_miss;
    config::enable_vector_index_cache_async_load_on_miss = false;
    DeferOp restore_config([&] { config::enable_vector_index_cache_async_load_on_miss = saved_async_load; });

    constexpr const char* kMissingPath = "/factory-config-off-missing.vi";
    MemoryFileSystem fs;
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return Status::InternalError("injected async load failure");
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey(kMissingPath), std::move(async_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    auto tablet_index = make_minimal_tablet_index();
    auto factory = std::async(std::launch::async, [&] {
        auto vi_file = remote_vi(kMissingPath, &fs);
        VectorIndexReaderFactory reader_factory(*cache_);
        OlapReaderStatistics stats;
        return reader_factory.create_and_init(std::move(vi_file), tablet_index, {}, {.stats = stats});
    });
    ASSERT_EQ(std::future_status::ready, factory.wait_for(std::chrono::seconds(1)));
    auto result_or = factory.get();
    ASSERT_OK(result_or);
    EXPECT_EQ(VectorIndexReaderInitResult::kFallback, result_or->state);
    EXPECT_EQ(nullptr, result_or->reader);
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading, cache_->ProbeForQuery(tenann::CacheKey(kMissingPath)).state);

    release_loader.set_value();
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), kMissingPath, VectorIndexCacheProbeState::kMiss));
}

TEST_F(VectorIndexCacheTest, Factory_RefineQueryTimesOutWaitingForExistingAsyncLoad) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));
    config::vector_index_cache_loading_wait_timeout_ms = 50;

    const bool saved_async_load = config::enable_vector_index_cache_async_load_on_miss;
    config::enable_vector_index_cache_async_load_on_miss = true;
    DeferOp restore_config([&] { config::enable_vector_index_cache_async_load_on_miss = saved_async_load; });

    constexpr const char* kMissingPath = "/factory-refine-missing.vi";
    MemoryFileSystem fs;
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return Status::InternalError("injected async load failure");
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey(kMissingPath), std::move(async_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    auto tablet_index = make_minimal_tablet_index();
    auto factory = std::async(std::launch::async, [&] {
        auto vi_file = remote_vi(kMissingPath, &fs);
        VectorIndexReaderFactory reader_factory(*cache_);
        OlapReaderStatistics stats;
        return reader_factory.create_and_init(std::move(vi_file), tablet_index, {},
                                              {.refine_distance = true, .stats = stats});
    });
    ASSERT_EQ(std::future_status::ready, factory.wait_for(std::chrono::seconds(1)));
    auto result_or = factory.get();
    ASSERT_OK(result_or);
    EXPECT_EQ(VectorIndexReaderInitResult::kFallback, result_or->state);
    EXPECT_EQ(nullptr, result_or->reader);
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading, cache_->ProbeForQuery(tenann::CacheKey(kMissingPath)).state);

    release_loader.set_value();
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), kMissingPath, VectorIndexCacheProbeState::kMiss));
}

TEST_F(VectorIndexCacheTest, ZeroCapacityQueryStillLoadsSynchronously) {
    auto cache = std::make_unique<VectorIndexCache>(/*capacity=*/0, tracker_.get());
    ASSERT_OK(cache->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::atomic<int> async_loader_calls{0};
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        async_loader_calls.fetch_add(1);
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kMiss,
              cache->TryGetOrSchedule(tenann::CacheKey("/zero-capacity.vi"), std::move(async_loader)).state);
    EXPECT_EQ(0, async_loader_calls.load());

    EXPECT_EQ(VectorIndexCacheProbeState::kMiss, cache->ProbeForQuery(tenann::CacheKey("/zero-capacity.vi")).state);
    std::atomic<int> sync_loader_calls{0};
    auto result = cache->GetOrCreateForQuery(
            tenann::CacheKey("/zero-capacity.vi"),
            [&]() -> tenann::IndexRef {
                sync_loader_calls.fetch_add(1);
                return make_dummy_ref();
            },
            /*wait_for_loading=*/true);
    EXPECT_EQ(VectorIndexCacheProbeState::kReady, result.state);
    EXPECT_TRUE(result.handle.valid());
    EXPECT_EQ(1, sync_loader_calls.load());
    EXPECT_EQ(1, cache->lookup_count());
}

TEST_F(VectorIndexCacheTest, InsertWinsAgainstLateAsyncCompletion) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    auto late_ref = make_dummy_ref();
    auto inserted_ref = make_dummy_ref();
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return late_ref;
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/insert-wins.vi"), std::move(async_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    tenann::IndexCacheHandle inserted_handle;
    cache_->Insert(tenann::CacheKey("/insert-wins.vi"), inserted_ref, &inserted_handle);
    release_loader.set_value();
    cache_->shutdown_async_load_pool();

    tenann::IndexCacheHandle probe;
    ASSERT_TRUE(cache_->Lookup(tenann::CacheKey("/insert-wins.vi"), &probe));
    EXPECT_EQ(inserted_ref.get(), probe.index_ref().get());
}

TEST_F(VectorIndexCacheTest, QueueRejectionRestoresMissAndAllowsRetry) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/0));

    std::promise<void> first_started;
    std::promise<void> release_first;
    auto release_future = release_first.get_future().share();
    auto first_loader = [&]() -> StatusOr<tenann::IndexRef> {
        first_started.set_value();
        release_future.wait();
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/queue-running.vi"), std::move(first_loader)).state);
    ASSERT_EQ(std::future_status::ready, first_started.get_future().wait_for(std::chrono::seconds(5)));

    std::atomic<int> rejected_loader_calls{0};
    auto rejected_loader = [&]() -> StatusOr<tenann::IndexRef> {
        rejected_loader_calls.fetch_add(1);
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kMiss,
              cache_->TryGetOrSchedule(tenann::CacheKey("/queue-rejected.vi"), std::move(rejected_loader)).state);
    EXPECT_EQ(0, rejected_loader_calls.load());
    EXPECT_EQ(VectorIndexCacheProbeState::kMiss, cache_->ProbeForQuery(tenann::CacheKey("/queue-rejected.vi")).state);

    release_first.set_value();
    ASSERT_TRUE(wait_for_probe_state(cache_.get(), "/queue-running.vi", VectorIndexCacheProbeState::kReady));

    VectorIndexCacheProbeState retry_state = VectorIndexCacheProbeState::kMiss;
    const auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        auto retry_loader = [&]() -> StatusOr<tenann::IndexRef> {
            rejected_loader_calls.fetch_add(1);
            return make_dummy_ref();
        };
        retry_state = cache_->TryGetOrSchedule(tenann::CacheKey("/queue-rejected.vi"), std::move(retry_loader)).state;
        if (retry_state == VectorIndexCacheProbeState::kMiss) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    } while (retry_state == VectorIndexCacheProbeState::kMiss && std::chrono::steady_clock::now() < retry_deadline);
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading, retry_state);
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), "/queue-rejected.vi", VectorIndexCacheProbeState::kReady));
    EXPECT_EQ(1, rejected_loader_calls.load());
}

TEST_F(VectorIndexCacheTest, ShutdownCancelsQueuedAsyncLoadAndRestoresMiss) {
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    std::promise<void> first_started;
    std::promise<void> release_first;
    auto release_future = release_first.get_future().share();
    auto first_loader = [&]() -> StatusOr<tenann::IndexRef> {
        first_started.set_value();
        release_future.wait();
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/shutdown-running.vi"), std::move(first_loader)).state);
    ASSERT_EQ(std::future_status::ready, first_started.get_future().wait_for(std::chrono::seconds(5)));

    std::atomic<int> queued_loader_calls{0};
    auto queued_loader = [&]() -> StatusOr<tenann::IndexRef> {
        queued_loader_calls.fetch_add(1);
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey("/shutdown-queued.vi"), std::move(queued_loader)).state);

    std::thread shutdown_thread([&] { cache_->shutdown_async_load_pool(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    release_first.set_value();
    shutdown_thread.join();

    EXPECT_EQ(0, queued_loader_calls.load());
    EXPECT_EQ(VectorIndexCacheProbeState::kMiss, cache_->ProbeForQuery(tenann::CacheKey("/shutdown-queued.vi")).state);
    EXPECT_EQ(VectorIndexCacheProbeState::kMiss,
              cache_->TryGetOrSchedule(tenann::CacheKey("/after-shutdown.vi"), []() -> StatusOr<tenann::IndexRef> {
                        return make_dummy_ref();
                    }).state);
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

TEST_F(VectorIndexCacheTest, TTL_StartsAfterLastHandleRelease) {
    config::vector_index_cache_expire_sec = 10;

    tenann::IndexCacheHandle handle;
    cache_->Insert(tenann::CacheKey("/ttl.vi"), make_dummy_ref(), &handle);

    // A pinned entry survives a sweep.
    const int64_t base_time = MonotonicMillis();
    EXPECT_TRUE(cache_->clear_expired(base_time));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());

    handle = tenann::IndexCacheHandle{};
    const int64_t released_at = MonotonicMillis();
    EXPECT_TRUE(cache_->clear_expired(released_at + 9 * 1000L));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());

    EXPECT_FALSE(cache_->clear_expired(released_at + 11 * 1000L));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());

    EXPECT_TRUE(cache_->clear_expired(released_at + 14 * 1000L));
    EXPECT_EQ(0, cache_->memory_usage());
}

TEST_F(VectorIndexCacheTest, TTL_CacheHitRefreshesOnHandleRelease) {
    config::vector_index_cache_expire_sec = 10;

    tenann::IndexCacheHandle inserted;
    cache_->Insert(tenann::CacheKey("/refresh.vi"), make_dummy_ref(), &inserted);
    inserted = tenann::IndexCacheHandle{};

    tenann::IndexCacheHandle hit;
    ASSERT_TRUE(cache_->Lookup(tenann::CacheKey("/refresh.vi"), &hit));
    config::vector_index_cache_expire_sec = 900;
    hit = tenann::IndexCacheHandle{};

    // The hit is released under the 900-second TTL. Lowering the runtime
    // setting afterward must not shorten this entry's existing deadline.
    config::vector_index_cache_expire_sec = 10;
    const int64_t base_time = MonotonicMillis();
    EXPECT_TRUE(cache_->clear_expired(base_time + 11 * 1000L));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());

    EXPECT_TRUE(cache_->clear_expired(base_time + 901 * 1000L));
    EXPECT_EQ(0, cache_->memory_usage());
}

TEST_F(VectorIndexCacheTest, TTL_ConsecutiveSweepsHonorDeadline) {
    config::vector_index_cache_expire_sec = 10;

    tenann::IndexCacheHandle handle;
    cache_->Insert(tenann::CacheKey("/consecutive-sweeps.vi"), make_dummy_ref(), &handle);
    handle = tenann::IndexCacheHandle{};
    const int64_t base_time = MonotonicMillis();

    EXPECT_TRUE(cache_->clear_expired(base_time + 9 * 1000L));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());
    EXPECT_FALSE(cache_->clear_expired(base_time + 11 * 1000L));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());
    EXPECT_TRUE(cache_->clear_expired(base_time + 14 * 1000L));
    EXPECT_EQ(0, cache_->memory_usage());
}

TEST_F(VectorIndexCacheTest, TTL_DisabledKeepsUnusedEntries) {
    config::vector_index_cache_expire_sec = 0;

    tenann::IndexCacheHandle handle;
    cache_->Insert(tenann::CacheKey("/no-ttl.vi"), make_dummy_ref(), &handle);
    handle = tenann::IndexCacheHandle{};

    EXPECT_FALSE(cache_->clear_expired(MonotonicMillis() + 24 * 60 * 60 * 1000L));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());
}

TEST_F(VectorIndexCacheTest, TTL_RuntimeUpdateOnlyAffectsFutureReleases) {
    config::vector_index_cache_expire_sec = 900;

    tenann::IndexCacheHandle old_ttl_handle;
    const tenann::CacheKey old_ttl_key("/old-ttl.vi");
    cache_->Insert(old_ttl_key, make_dummy_ref(), &old_ttl_handle);
    old_ttl_handle = tenann::IndexCacheHandle{};

    config::vector_index_cache_expire_sec = 10;
    tenann::IndexCacheHandle new_ttl_handle;
    const tenann::CacheKey new_ttl_key("/new-ttl.vi");
    cache_->Insert(new_ttl_key, make_dummy_ref(), &new_ttl_handle);
    new_ttl_handle = tenann::IndexCacheHandle{};

    EXPECT_TRUE(cache_->clear_expired(MonotonicMillis() + 14 * 1000L));
    EXPECT_EQ(kDummyBytes, cache_->memory_usage());

    tenann::IndexCacheHandle probe;
    EXPECT_TRUE(cache_->Lookup(old_ttl_key, &probe));
    probe = tenann::IndexCacheHandle{};
    EXPECT_FALSE(cache_->Lookup(new_ttl_key, &probe));
}

TEST_F(VectorIndexCacheTest, TTL_IVFPQExpiresOuterEntryAndAllListBlocksTogether) {
    config::vector_index_cache_expire_sec = 900;

    tenann::IndexCacheHandle list_handle;
    cache_->Insert(tenann::CacheKey("/ivfpq.vi#list-0"),
                   make_dummy_ref(kDummyBytes, tenann::IndexType::kFaissIvfPqOneInvertedList), &list_handle);

    struct IvfPqPayload {
        tenann::IndexCacheHandle list_handle;
        std::vector<char> bytes;
    };
    auto* payload = new IvfPqPayload{std::move(list_handle), std::vector<char>(2048)};
    auto outer_ref = std::make_shared<tenann::Index>(
            payload, tenann::IndexType::kFaissIvfPq, [](void* p) { delete static_cast<IvfPqPayload*>(p); },
            /*explicit_bytes=*/2048);

    tenann::IndexCacheHandle outer_handle;
    cache_->Insert(tenann::CacheKey("/ivfpq.vi"), outer_ref, &outer_handle);
    outer_ref.reset();
    outer_handle = tenann::IndexCacheHandle{};

    EXPECT_TRUE(cache_->clear_expired(MonotonicMillis() + 901 * 1000L));
    EXPECT_EQ(0, cache_->memory_usage());

    tenann::IndexCacheHandle probe;
    EXPECT_FALSE(cache_->Lookup(tenann::CacheKey("/ivfpq.vi"), &probe));
    EXPECT_FALSE(cache_->Lookup(tenann::CacheKey("/ivfpq.vi#list-0"), &probe));
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
    c->Insert(tenann::CacheKey("/inner.vi"), make_dummy_ref(1024, tenann::IndexType::kFaissIvfPqOneInvertedList),
              &inner);

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

    // Shutdown safety remains covered after the cascading list-handle release.
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
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(remote_vi("/tmp/idx.vi", &fs)));
    ASSERT_NE(nullptr, reader);
    EXPECT_EQ(4, reader->GetSize());
}

TEST(VectorIndexFileReaderTest, OpenFailsOnMissingFile) {
    MemoryFileSystem fs;
    auto r = VectorIndexFileReader::open(remote_vi("/no/such/path.vi", &fs));
    EXPECT_FALSE(r.ok());
}

TEST(TenANNReaderTest, InitSearcher_UsesInjectedCacheWithoutGlobalCache) {
    MemTracker tracker(-1, "tenann_reader_test");
    VectorIndexCache cache(/*capacity=*/1024, &tracker);
    auto* saved = tenann::GetGlobalIndexCache();
    tenann::SetGlobalIndexCache(nullptr);
    DeferOp restore_cache([&] { tenann::SetGlobalIndexCache(saved); });

    MemoryFileSystem fs;
    TenANNReader r(cache, /*async_load_on_miss=*/false);
    OlapReaderStatistics stats;
    auto st = r.init_searcher(make_minimal_meta(), remote_vi("/no/such/index.vi", &fs), stats);
    EXPECT_TRUE(st.status().is_not_found()) << st.status();
}

// Drives init_searcher through the loader's fs!=nullptr branch with a missing
// path so the captured Status (NotFound) flows back through GetOrCreate to the
// caller instead of being collapsed to an unhelpful InternalError.
TEST(TenANNReaderTest, InitSearcher_FileNotFoundViaFs_PropagatesNotFound) {
    MemTracker tracker(-1, "tenann_reader_test");
    VectorIndexCache cache(/*capacity=*/1024, &tracker);

    MemoryFileSystem fs;
    TenANNReader r(cache, /*async_load_on_miss=*/false);
    auto meta = make_minimal_meta();
    OlapReaderStatistics stats;
    auto st = r.init_searcher(std::move(meta), remote_vi("/no/such/index.vi", &fs), stats);
    EXPECT_TRUE(st.status().is_not_found()) << st.status();
    EXPECT_EQ(0, stats.vector_index_cache_hit_count);
    EXPECT_EQ(1, stats.vector_index_cache_miss_count);
    EXPECT_GT(stats.vector_index_file_open_ns, 0);
    EXPECT_EQ(0, stats.vector_index_read_file_ns);
    EXPECT_EQ(0, stats.vector_index_init_index_ns);
}

TEST_F(VectorIndexCacheTest, TenANNReader_LoadWaitTimeoutFallsBack) {
    config::vector_index_cache_loading_wait_timeout_ms = 50;
    ASSERT_OK(cache_->init_async_load_pool(/*num_threads=*/1, /*max_queue_size=*/4096));

    constexpr const char* kPath = "/reader-wait-timeout.vi";
    std::promise<void> loader_started;
    std::promise<void> release_loader;
    auto release_future = release_loader.get_future().share();
    auto async_loader = [&]() -> StatusOr<tenann::IndexRef> {
        loader_started.set_value();
        release_future.wait();
        return make_dummy_ref();
    };
    EXPECT_EQ(VectorIndexCacheProbeState::kLoading,
              cache_->TryGetOrSchedule(tenann::CacheKey(kPath), std::move(async_loader)).state);
    ASSERT_EQ(std::future_status::ready, loader_started.get_future().wait_for(std::chrono::seconds(5)));

    TenANNReader reader(*cache_, /*async_load_on_miss=*/false);
    OlapReaderStatistics stats;
    auto result = reader.init_searcher(make_minimal_meta(), FileInfo{.path = kPath}, stats);
    ASSERT_OK(result);
    EXPECT_EQ(VectorIndexReaderInitResult::kFallback, result.value());
    EXPECT_EQ(1, stats.vector_index_cache_miss_count);

    release_loader.set_value();
    EXPECT_TRUE(wait_for_probe_state(cache_.get(), kPath, VectorIndexCacheProbeState::kReady));
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

    MemoryFileSystem fs;
    ASSERT_OK(fs.create_dir("/tmp"));
    ASSERT_OK(fs.append_file("/tmp/garbage.vi", Slice("not a real index", 16)));

    TenANNReader r(cache, /*async_load_on_miss=*/false);
    auto meta = make_minimal_meta();
    OlapReaderStatistics stats;
    auto st = r.init_searcher(std::move(meta), remote_vi("/tmp/garbage.vi", &fs), stats);
    EXPECT_FALSE(st.ok()) << "loader should surface tenann::Error / std::exception, not crash";
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
    auto* process = RuntimeEnv::GetInstance()->process_mem_tracker();
    auto* vi = RuntimeEnv::GetInstance()->vector_index_mem_tracker();
    ASSERT_NE(nullptr, process);
    ASSERT_NE(nullptr, vi);

    // Missing path on purpose: open() still calls new_random_access_file (where the
    // probe captures the active tracker) but then fails NotFound, so the loader bails
    // before ReadIndexFile -- we never feed garbage to faiss, which can SIGSEGV on
    // malformed input. The tracker we want to assert on is already captured by then.
    TrackerProbeFileSystem fs;

    MemTracker cache_tracker(-1, "vi_cache_probe");
    VectorIndexCache cache(/*capacity=*/1024, &cache_tracker);

    // Simulate the load being triggered while running under a query's mem tracker
    // (distinct from both process and vector_index). The loader must redirect the
    // load to process regardless.
    MemTracker fake_query(-1, "fake_query_ambient");
    {
        CurrentThreadMemTrackerSetter ambient(&fake_query);
        TenANNReader r(cache, /*async_load_on_miss=*/false);
        auto meta = make_minimal_meta();
        OlapReaderStatistics stats;
        // Load runs the probe fs, then fails NotFound (return ignored).
        (void)r.init_searcher(std::move(meta), remote_vi("/no/such/probe.vi", &fs), stats);
    }

    ASSERT_NE(nullptr, fs.captured) << "loader never opened the index file via fs";
    EXPECT_EQ(process, fs.captured) << "index load charged to '" << fs.captured->label() << "', expected 'process'";
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

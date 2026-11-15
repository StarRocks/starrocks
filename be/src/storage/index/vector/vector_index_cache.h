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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"

namespace starrocks {
class MemTracker;
class ThreadPool;
class VectorIndexCacheMetrics;
} // namespace starrocks

#ifdef WITH_TENANN

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <ostream>
#include <string>

#include "cache/dynamic_cache.h"
#include "common/statusor.h"
#include "tenann/index/index_cache.h"

namespace starrocks {

enum class VectorIndexCacheEntryState : uint8_t {
    kEmpty,
    kLoading,
    kReady,
};

// Per-entry state transitions and _ref access are protected by _mu. The atomic
// state only supports the best-effort cleanup read in _release_entry().
class VectorIndexCacheEntry {
public:
    using State = VectorIndexCacheEntryState;

    std::unique_lock<std::mutex> guard() { return std::unique_lock<std::mutex>(_mu); }
    bool wait_until_not_loading_until(std::unique_lock<std::mutex>& lock,
                                      std::chrono::steady_clock::time_point deadline) {
        return _cv.wait_until(lock, deadline, [this] { return state(std::memory_order_relaxed) != State::kLoading; });
    }
    void notify_all() noexcept { _cv.notify_all(); }

    State state(std::memory_order order) const { return _state.load(order); }
    void set_state(State state, std::memory_order order = std::memory_order_release) { _state.store(state, order); }
    bool has_ref() const { return _ref != nullptr; }
    tenann::IndexRef ref() const { return _ref; }
    void set_ref(tenann::IndexRef ref) { _ref = std::move(ref); }
    tenann::IndexRef take_ref() { return std::move(_ref); }

private:
    std::mutex _mu;
    std::condition_variable _cv;
    std::atomic<State> _state{State::kEmpty};
    tenann::IndexRef _ref;
};

inline std::ostream& operator<<(std::ostream& os, const VectorIndexCacheEntry&) {
    return os << "VectorIndexCacheEntry";
}

enum class VectorIndexCacheProbeState : uint8_t {
    kReady,
    kLoading,
    kWaitTimeout,
    kMiss,
};

struct VectorIndexCacheProbeResult {
    VectorIndexCacheProbeState state = VectorIndexCacheProbeState::kMiss;
    tenann::IndexCacheHandle handle;
};

class VectorIndexLoadTask;

// SR-owned tenann::IndexCache backed by DynamicCache, with MemTracker attached.
class VectorIndexCache final : public tenann::IndexCache {
public:
    using Cache = DynamicCache<std::string, VectorIndexCacheEntry>;
    using Entry = Cache::Entry;
    using AsyncIndexLoader = std::function<StatusOr<tenann::IndexRef>()>;

    VectorIndexCache(size_t capacity, MemTracker* tracker, VectorIndexCacheMetrics* metrics = nullptr);
    ~VectorIndexCache() override;

    VectorIndexCache(const VectorIndexCache&) = delete;
    VectorIndexCache& operator=(const VectorIndexCache&) = delete;

    [[nodiscard]] bool Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) override;
    // Required by tenann::IndexCache; SR production paths use GetOrCreate.
    void Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) override;
    // Returns false on loader failure (exception or null IndexRef).
    [[nodiscard]] bool GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                                   tenann::IndexCacheHandle* handle) override;

    // Query-only APIs. ProbeForQuery returns kLoading immediately unless the
    // caller requests the synchronous cache contract. TryGetOrSchedule
    // single-flights EMPTY -> LOADING and returns kLoading for both an accepted
    // task and a duplicate request. GetOrCreateForQuery continues a preceding
    // ProbeForQuery miss without counting the same query twice.
    [[nodiscard]] VectorIndexCacheProbeResult ProbeForQuery(const tenann::CacheKey& key, bool wait_for_loading = false);
    [[nodiscard]] VectorIndexCacheProbeResult TryGetOrSchedule(const tenann::CacheKey& key, AsyncIndexLoader loader);
    [[nodiscard]] VectorIndexCacheProbeResult GetOrCreateForQuery(const tenann::CacheKey& key,
                                                                  const IndexLoader& loader, bool wait_for_loading);

    Status init_async_load_pool(int num_threads, int max_queue_size);
    void shutdown_async_load_pool();

    void SetCapacity(size_t new_capacity);
    bool clear_expired(int64_t now = MonotonicMillis());
    size_t capacity() const { return _cache.capacity(); }
    size_t memory_usage() const { return _cache.size(); }
    size_t entry_count() const { return _cache.object_size(); }

    uint64_t lookup_count() const { return _lookup_count.load(std::memory_order_relaxed); }
    uint64_t hit_count() const { return _hit_count.load(std::memory_order_relaxed); }

private:
    friend class VectorIndexLoadTask;
    class LoadingToken;

    tenann::IndexCacheHandle _wrap(Entry* entry, tenann::IndexRef ref);
    void _release_entry(Entry* entry, bool is_ivfpq_list_block = false) noexcept;
    VectorIndexCacheProbeResult _get_or_create(const tenann::CacheKey& key, const IndexLoader& loader,
                                               bool wait_for_loading, bool count_lookup);
    tenann::IndexRef _publish_loaded(LoadingToken* token, tenann::IndexRef loaded, size_t bytes);
    void _update_metrics() const;

    Cache _cache;
    VectorIndexCacheMetrics& _metrics;
    std::atomic<int64_t> _last_clear_expired_ms{0};
    std::atomic<uint64_t> _lookup_count{0};
    std::atomic<uint64_t> _hit_count{0};

    std::mutex _async_pool_mutex;
    std::unique_ptr<ThreadPool> _async_load_pool;
    std::atomic<bool> _accepting_async_loads{false};
};

} // namespace starrocks

#else // !WITH_TENANN

namespace starrocks {

// Stub for non-tenann builds (e.g. Darwin): never constructed, lets callers
// reference accessors without their own WITH_TENANN guards.
class VectorIndexCache {
public:
    VectorIndexCache(size_t, MemTracker*, VectorIndexCacheMetrics* = nullptr) {}
    Status init_async_load_pool(int, int) { return Status::OK(); }
    void shutdown_async_load_pool() {}
    void SetCapacity(size_t) {}
    bool clear_expired(int64_t = 0) { return false; }
    size_t capacity() const { return 0; }
    size_t memory_usage() const { return 0; }
    uint64_t lookup_count() const { return 0; }
    uint64_t hit_count() const { return 0; }
};

} // namespace starrocks

#endif // WITH_TENANN

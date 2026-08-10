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

#ifdef WITH_TENANN

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "common/logging.h"
#include "common/thread/threadpool.h"
#include "runtime/mem_tracker.h"
#include "storage/index/vector/vector_index_cache_metrics.h"

namespace starrocks {

namespace {

constexpr int kAsyncLoadThreadIdleTimeoutSeconds = 60;

int64_t expire_time_ms() {
    const int32_t expire_seconds = config::vector_index_cache_expire_sec;
    if (expire_seconds <= 0) {
        return std::numeric_limits<int64_t>::max();
    }
    return MonotonicMillis() + static_cast<int64_t>(expire_seconds) * 1000;
}

std::chrono::steady_clock::time_point loading_wait_deadline() {
    const int32_t timeout_ms = std::max<int32_t>(0, config::vector_index_cache_loading_wait_timeout_ms);
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
}

} // namespace

class VectorIndexCache::LoadingToken {
public:
    LoadingToken(VectorIndexCache* cache, Entry* entry) noexcept : _cache(cache), _entry(entry) {}
    LoadingToken(const LoadingToken&) = delete;
    LoadingToken& operator=(const LoadingToken&) = delete;
    LoadingToken(LoadingToken&& other) noexcept
            : _cache(std::exchange(other._cache, nullptr)), _entry(std::exchange(other._entry, nullptr)) {}
    LoadingToken& operator=(LoadingToken&& other) noexcept {
        if (this != &other) {
            abort();
            _cache = std::exchange(other._cache, nullptr);
            _entry = std::exchange(other._entry, nullptr);
        }
        return *this;
    }
    ~LoadingToken() noexcept { abort(); }

    Entry* entry() const noexcept { return _entry; }

    void finish() noexcept {
        if (auto* entry = std::exchange(_entry, nullptr); entry != nullptr) {
            _cache->_release_entry(entry);
        }
    }

    Entry* detach_entry() noexcept { return std::exchange(_entry, nullptr); }

    void abort() noexcept {
        auto* entry = std::exchange(_entry, nullptr);
        if (entry == nullptr) {
            return;
        }

        bool notify = false;
        {
            auto lock = entry->value().guard();
            if (entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kLoading) {
                entry->value().set_state(VectorIndexCacheEntryState::kEmpty);
                notify = true;
            }
        }
        if (notify) {
            entry->value().notify_all();
        }
        _cache->_release_entry(entry);
    }

private:
    VectorIndexCache* _cache = nullptr;
    Entry* _entry = nullptr;
};

class VectorIndexLoadTask final : public Runnable {
public:
    VectorIndexLoadTask(VectorIndexCache* cache, VectorIndexCache::LoadingToken token,
                        VectorIndexCache::AsyncIndexLoader loader, std::string key)
            : _cache(cache), _token(std::move(token)), _loader(std::move(loader)), _key(std::move(key)) {
        _cache->_metrics.vector_index_cache_async_load_queued.increment(1);
    }

    void run() noexcept override {
        if (!_queued.exchange(false, std::memory_order_relaxed)) {
            return;
        }
        _cache->_metrics.vector_index_cache_async_load_queued.increment(-1);
        _cache->_metrics.vector_index_cache_async_load_inflight.increment(1);
        DeferOp finish_load([&] {
            _cache->_metrics.vector_index_cache_async_load_inflight.increment(-1);
            _cache->_update_metrics();
        });

        const int64_t start_ns = MonotonicNanos();
        DeferOp record_load_time([&] {
            _cache->_metrics.vector_index_cache_async_load_ns.increment(
                    std::max<int64_t>(0, MonotonicNanos() - start_ns));
        });
        try {
            Status status = run_impl();
            if (status.ok()) {
                _token.finish();
                _cache->_metrics.vector_index_cache_async_load_success.increment(1);
            } else {
                _token.abort();
                _cache->_metrics.vector_index_cache_async_load_failure.increment(1);
                LOG_EVERY_SECOND(WARNING) << "Failed to load vector index into cache asynchronously, key=" << _key
                                          << ", status=" << status;
            }
        } catch (...) {
            _token.abort();
            _cache->_metrics.vector_index_cache_async_load_failure.increment(1);
            LOG_EVERY_SECOND(WARNING) << "Unexpected exception while loading vector index asynchronously, key=" << _key;
        }
    }

    void cancel() noexcept override {
        _leave_queue();
        _token.abort();
        _cache->_update_metrics();
    }

    void abort() noexcept {
        _leave_queue();
        _token.abort();
        _cache->_update_metrics();
    }

private:
    void _leave_queue() noexcept {
        if (_queued.exchange(false, std::memory_order_relaxed)) {
            _cache->_metrics.vector_index_cache_async_load_queued.increment(-1);
        }
    }

    Status run_impl() {
        auto loaded_or = _loader();
        if (!loaded_or.ok()) {
            return loaded_or.status();
        }

        auto loaded = std::move(loaded_or).value();
        if (loaded == nullptr) {
            return Status::InternalError("vector index loader returned a null IndexRef");
        }

        TEST_SYNC_POINT("VectorIndexLoadTask::run_impl:before_estimate");
        const size_t bytes = loaded->EstimateMemoryUsage();
        if (_cache->_publish_loaded(&_token, std::move(loaded), bytes) == nullptr) {
            return Status::InternalError("vector index load lost its cache entry");
        }
        return Status::OK();
    }

    VectorIndexCache* _cache;
    VectorIndexCache::LoadingToken _token;
    VectorIndexCache::AsyncIndexLoader _loader;
    std::string _key;
    std::atomic<bool> _queued{true};
};

VectorIndexCache::VectorIndexCache(size_t capacity, MemTracker* tracker, VectorIndexCacheMetrics* metrics)
        : _cache(capacity), _metrics(*(metrics == nullptr ? VectorIndexCacheMetrics::instance() : metrics)) {
    // The HNSW/tenann index lives in the normal heap, so the global allocator hook
    // already charges its bytes to the process tracker once during load. Accounting
    // them additively on the vector_index tracker (a child of process) would count
    // the same bytes a second time on process and spuriously trip the process
    // mem_limit. Use the excluding-root variant so the vector_index tracker labels
    // the usage without re-adding it to process.
    _cache.set_mem_tracker_excluding_root(tracker);
    _update_metrics();
}

// Stop and join background tasks before draining IndexRefs. IVF-PQ entries can
// hold nested IndexCacheHandles whose deleters call back into this cache, so all
// IndexRef destruction remains outside DynamicCache's global mutex.
VectorIndexCache::~VectorIndexCache() {
    shutdown_async_load_pool();

    auto entries = _cache.get_all_entries();
    std::vector<tenann::IndexRef> stale_refs;
    stale_refs.reserve(entries.size());
    for (auto* entry : entries) {
        auto lock = entry->value().guard();
        stale_refs.emplace_back(entry->value().take_ref());
        entry->value().set_state(VectorIndexCacheEntryState::kEmpty);
    }
    for (auto* entry : entries) {
        entry->value().notify_all();
        _release_entry(entry);
    }
    stale_refs.clear();
}

Status VectorIndexCache::init_async_load_pool(int num_threads, int max_queue_size) {
    if (num_threads <= 0) {
        return Status::InvalidArgument("vector index cache async load threads must be positive");
    }
    if (max_queue_size < 0) {
        return Status::InvalidArgument("vector index cache async load max queue size must not be negative");
    }

    std::lock_guard lock(_async_pool_mutex);
    if (_async_load_pool != nullptr) {
        return Status::OK();
    }

    std::unique_ptr<ThreadPool> pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("vi_cache_load")
                            .set_min_threads(0)
                            .set_max_threads(num_threads)
                            .set_max_queue_size(max_queue_size)
                            .set_idle_timeout(MonoDelta::FromSeconds(kAsyncLoadThreadIdleTimeoutSeconds))
                            .build(&pool));
    _async_load_pool = std::move(pool);
    _accepting_async_loads.store(true, std::memory_order_release);
    return Status::OK();
}

void VectorIndexCache::shutdown_async_load_pool() {
    std::unique_ptr<ThreadPool> pool;
    {
        std::lock_guard lock(_async_pool_mutex);
        _accepting_async_loads.store(false, std::memory_order_release);
        pool = std::move(_async_load_pool);
    }
    if (pool != nullptr) {
        pool->shutdown();
    }
}

bool VectorIndexCache::Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) {
    *handle = tenann::IndexCacheHandle{};
    Entry* entry = _cache.get(key.to_string());
    if (entry == nullptr) {
        return false;
    }

    tenann::IndexRef ref;
    {
        auto lock = entry->value().guard();
        // Lookup preserves the synchronous cache contract. Async queries use
        // the non-waiting ProbeForQuery path instead.
        if (entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kLoading) {
            if (!entry->value().wait_until_not_loading_until(lock, loading_wait_deadline())) {
                lock.unlock();
                _metrics.vector_index_cache_loading_wait_timeout.increment(1);
                _release_entry(entry);
                return false;
            }
        }
        if (entry->value().state(std::memory_order_relaxed) != VectorIndexCacheEntryState::kReady ||
            !entry->value().has_ref()) {
            lock.unlock();
            _release_entry(entry);
            return false;
        }
        ref = entry->value().ref();
    }
    *handle = _wrap(entry, std::move(ref));
    return true;
}

VectorIndexCacheProbeResult VectorIndexCache::ProbeForQuery(const tenann::CacheKey& key, bool wait_for_loading) {
    _lookup_count.fetch_add(1, std::memory_order_relaxed);
    Entry* entry = _cache.get(key.to_string());
    if (entry == nullptr) {
        _update_metrics();
        return {VectorIndexCacheProbeState::kMiss, {}};
    }

    tenann::IndexRef ref;
    VectorIndexCacheEntryState state;
    bool wait_timed_out = false;
    {
        auto lock = entry->value().guard();
        state = entry->value().state(std::memory_order_relaxed);
        if (state == VectorIndexCacheEntryState::kLoading && wait_for_loading) {
            wait_timed_out = !entry->value().wait_until_not_loading_until(lock, loading_wait_deadline());
            if (!wait_timed_out) {
                state = entry->value().state(std::memory_order_relaxed);
            }
        }
        if (state == VectorIndexCacheEntryState::kReady) {
            ref = entry->value().ref();
        }
    }

    if (wait_timed_out) {
        _metrics.vector_index_cache_loading_wait_timeout.increment(1);
        _release_entry(entry);
        _update_metrics();
        return {VectorIndexCacheProbeState::kWaitTimeout, {}};
    }
    if (ref != nullptr) {
        _hit_count.fetch_add(1, std::memory_order_relaxed);
        _update_metrics();
        return {VectorIndexCacheProbeState::kReady, _wrap(entry, std::move(ref))};
    }

    _release_entry(entry);
    _update_metrics();
    return {state == VectorIndexCacheEntryState::kLoading ? VectorIndexCacheProbeState::kLoading
                                                          : VectorIndexCacheProbeState::kMiss,
            {}};
}

VectorIndexCacheProbeResult VectorIndexCache::TryGetOrSchedule(const tenann::CacheKey& key, AsyncIndexLoader loader) {
    DeferOp update_metrics([this] { _update_metrics(); });
    if (!_accepting_async_loads.load(std::memory_order_acquire) || capacity() == 0) {
        _metrics.vector_index_cache_async_load_rejected.increment(1);
        return {VectorIndexCacheProbeState::kMiss, {}};
    }

    const std::string key_string = key.to_string();
    Entry* entry = _cache.get_or_create(key_string);
    auto lock = entry->value().guard();
    const auto state = entry->value().state(std::memory_order_relaxed);
    if (state == VectorIndexCacheEntryState::kReady) {
        auto ref = entry->value().ref();
        lock.unlock();
        if (ref == nullptr) {
            _release_entry(entry);
            return {VectorIndexCacheProbeState::kMiss, {}};
        }
        _hit_count.fetch_add(1, std::memory_order_relaxed);
        _update_metrics();
        return {VectorIndexCacheProbeState::kReady, _wrap(entry, std::move(ref))};
    }
    if (state == VectorIndexCacheEntryState::kLoading) {
        lock.unlock();
        _release_entry(entry);
        return {VectorIndexCacheProbeState::kLoading, {}};
    }

    LoadingToken token(this, entry);
    entry->value().set_state(VectorIndexCacheEntryState::kLoading);
    lock.unlock();

    auto task = std::make_shared<VectorIndexLoadTask>(this, std::move(token), std::move(loader), key_string);

    Status submit_status;
    {
        std::lock_guard pool_lock(_async_pool_mutex);
        if (!_accepting_async_loads.load(std::memory_order_relaxed) || _async_load_pool == nullptr) {
            submit_status = Status::ServiceUnavailable("vector index cache async load pool is stopped");
        } else {
            submit_status = _async_load_pool->submit(task);
        }
    }
    if (!submit_status.ok()) {
        task->abort();
        _metrics.vector_index_cache_async_load_rejected.increment(1);
        VLOG(1) << "Failed to submit vector index cache async load, key=" << key_string << ", status=" << submit_status;
        return {VectorIndexCacheProbeState::kMiss, {}};
    }

    return {VectorIndexCacheProbeState::kLoading, {}};
}

void VectorIndexCache::SetCapacity(size_t new_capacity) {
    _cache.set_capacity(new_capacity);
    _update_metrics();
}

bool VectorIndexCache::clear_expired(int64_t now) {
    const int64_t expire_seconds = config::vector_index_cache_expire_sec;
    if (expire_seconds <= 0) {
        return false;
    }

    const int64_t interval_ms = std::max<int64_t>(1, expire_seconds / 2) * 1000;
    int64_t last_clear_expired_ms = _last_clear_expired_ms.load(std::memory_order_relaxed);
    if (last_clear_expired_ms != 0 && (now <= last_clear_expired_ms || now - last_clear_expired_ms < interval_ms)) {
        return false;
    }
    while (!_last_clear_expired_ms.compare_exchange_weak(last_clear_expired_ms, now, std::memory_order_relaxed)) {
        if (last_clear_expired_ms != 0 && (now <= last_clear_expired_ms || now - last_clear_expired_ms < interval_ms)) {
            return false;
        }
    }

    _cache.clear_expired(now);
    _update_metrics();
    return true;
}

void VectorIndexCache::Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) {
    *handle = tenann::IndexCacheHandle{};
    if (ref == nullptr) {
        return;
    }

    const size_t bytes = ref->EstimateMemoryUsage();
    Entry* entry = _cache.get_or_create(key.to_string());
    tenann::IndexRef stale;
    {
        auto lock = entry->value().guard();
        _cache.update_object_size(entry, bytes);
        stale = entry->value().take_ref();
        entry->value().set_ref(ref);
        entry->value().set_state(VectorIndexCacheEntryState::kReady);
    }
    entry->value().notify_all();
    _update_metrics();
    *handle = _wrap(entry, std::move(ref));
}

bool VectorIndexCache::GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                                   tenann::IndexCacheHandle* handle) {
    auto result = _get_or_create(key, loader, true, true);
    *handle = std::move(result.handle);
    return result.state == VectorIndexCacheProbeState::kReady;
}

VectorIndexCacheProbeResult VectorIndexCache::GetOrCreateForQuery(const tenann::CacheKey& key,
                                                                  const IndexLoader& loader, bool wait_for_loading) {
    // VectorIndexReaderFactory has already counted this query's probe miss.
    // This continuation must not count it a second time.
    return _get_or_create(key, loader, wait_for_loading, false);
}

VectorIndexCacheProbeResult VectorIndexCache::_get_or_create(const tenann::CacheKey& key, const IndexLoader& loader,
                                                             bool wait_for_loading, bool count_lookup) {
    if (count_lookup) {
        _lookup_count.fetch_add(1, std::memory_order_relaxed);
    }
    Entry* entry = _cache.get_or_create(key.to_string());

    const auto wait_deadline = loading_wait_deadline();
    for (;;) {
        auto lock = entry->value().guard();
        const auto state = entry->value().state(std::memory_order_relaxed);
        if (state == VectorIndexCacheEntryState::kReady) {
            auto ref = entry->value().ref();
            if (ref == nullptr) {
                entry->value().set_state(VectorIndexCacheEntryState::kEmpty);
                continue;
            }
            lock.unlock();
            _hit_count.fetch_add(1, std::memory_order_relaxed);
            _update_metrics();
            return {VectorIndexCacheProbeState::kReady, _wrap(entry, std::move(ref))};
        }
        if (state == VectorIndexCacheEntryState::kLoading) {
            if (!wait_for_loading) {
                lock.unlock();
                _release_entry(entry);
                _update_metrics();
                return {VectorIndexCacheProbeState::kLoading, {}};
            }
            if (!entry->value().wait_until_not_loading_until(lock, wait_deadline)) {
                lock.unlock();
                _metrics.vector_index_cache_loading_wait_timeout.increment(1);
                _release_entry(entry);
                _update_metrics();
                return {VectorIndexCacheProbeState::kWaitTimeout, {}};
            }
            continue;
        }

        LoadingToken token(this, entry);
        entry->value().set_state(VectorIndexCacheEntryState::kLoading);
        lock.unlock();
        try {
            auto loaded = loader();
            if (loaded == nullptr) {
                LOG(ERROR) << "VectorIndexCache loader returned null IndexRef for key " << key.to_string();
                _update_metrics();
                return {VectorIndexCacheProbeState::kMiss, {}};
            }

            TEST_SYNC_POINT("VectorIndexCache::_get_or_create:before_estimate");
            const size_t bytes = loaded->EstimateMemoryUsage();
            auto ref = _publish_loaded(&token, std::move(loaded), bytes);
            if (ref == nullptr) {
                _update_metrics();
                return {VectorIndexCacheProbeState::kMiss, {}};
            }

            auto handle = _wrap(token.entry(), std::move(ref));
            token.detach_entry();
            _update_metrics();
            return {VectorIndexCacheProbeState::kReady, std::move(handle)};
        } catch (...) {
            LOG(ERROR) << "VectorIndexCache failed to load or publish index for key " << key.to_string();
            _update_metrics();
            return {VectorIndexCacheProbeState::kMiss, {}};
        }
    }
}

tenann::IndexRef VectorIndexCache::_publish_loaded(LoadingToken* token, tenann::IndexRef loaded, size_t bytes) {
    Entry* entry = token->entry();
    if (entry == nullptr) {
        return nullptr;
    }

    tenann::IndexRef discarded;
    tenann::IndexRef result;
    {
        auto lock = entry->value().guard();
        if (entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kLoading) {
            _cache.update_object_size(entry, bytes);
            entry->value().set_ref(std::move(loaded));
            entry->value().set_state(VectorIndexCacheEntryState::kReady);
        } else {
            discarded = std::move(loaded);
        }
        if (entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kReady) {
            result = entry->value().ref();
        }
    }
    entry->value().notify_all();
    _update_metrics();
    return result;
}

void VectorIndexCache::_release_entry(Entry* entry, bool is_ivfpq_list_block) noexcept {
    // This relaxed read is only an eager cleanup hint. A concurrent transition
    // out of EMPTY stays safe because DynamicCache::remove() drops this pin under
    // its own lock and deletes only when no other external pin remains.
    if (is_ivfpq_list_block || entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kEmpty) {
        _cache.remove(entry);
        _update_metrics();
        return;
    }

    if (_cache.release_with_expire_time_evict_if_over_capacity(entry, expire_time_ms())) {
        _update_metrics();
    }
}

// Deleter captures this as a raw pointer; handles MUST be released before
// StorageEnv::destroy_vector_index_cache() destroys the cache.
tenann::IndexCacheHandle VectorIndexCache::_wrap(Entry* entry, tenann::IndexRef ref) {
    const bool is_ivfpq_list_block =
            ref != nullptr && ref->index_type() == tenann::IndexType::kFaissIvfPqOneInvertedList;
    return tenann::IndexCacheHandle(std::move(ref), std::shared_ptr<void>(entry, [this, is_ivfpq_list_block](void* p) {
                                        _release_entry(static_cast<Entry*>(p), is_ivfpq_list_block);
                                    }));
}

void VectorIndexCache::_update_metrics() const {
    _metrics.update(capacity(), memory_usage(), lookup_count(), hit_count());
}

} // namespace starrocks

#endif // WITH_TENANN

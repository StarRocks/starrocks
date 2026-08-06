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
#include <exception>
#include <limits>
#include <utility>
#include <vector>

#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "common/logging.h"
#include "common/thread/threadpool.h"
#include "runtime/mem_tracker.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_index_cache_metrics.h"
#include "tenann/common/error.h"

namespace starrocks {

namespace {

int64_t expire_time_ms() {
    const int32_t expire_seconds = config::vector_index_cache_expire_sec;
    if (expire_seconds <= 0) {
        return std::numeric_limits<int64_t>::max();
    }
    return MonotonicMillis() + static_cast<int64_t>(expire_seconds) * 1000;
}

} // namespace

class VectorIndexLoadTask final : public Runnable {
public:
    VectorIndexLoadTask(VectorIndexCache* cache, VectorIndexCache::Entry* entry,
                        VectorIndexCache::AsyncIndexLoader loader, std::string key)
            : _cache(cache), _entry(entry), _loader(std::move(loader)), _key(std::move(key)) {}

    void run() noexcept override {
        const int64_t start_ns = MonotonicNanos();
        if (_cache->_metrics != nullptr) {
            _cache->_metrics->vector_index_cache_async_load_inflight.increment(1);
        }

        tenann::IndexRef loaded;
        Status load_status = Status::OK();
        try {
            auto loaded_or = _loader();
            if (!loaded_or.ok()) {
                load_status = loaded_or.status();
            } else {
                loaded = std::move(loaded_or).value();
                if (loaded == nullptr) {
                    load_status = Status::InternalError("vector index loader returned a null IndexRef");
                }
            }
        } catch (const tenann::Error& e) {
            load_status = tenann_error_to_status(e);
        } catch (const std::bad_alloc& e) {
            load_status = Status::MemoryLimitExceeded(e.what());
        } catch (const std::exception& e) {
            load_status = Status::InternalError(e.what());
        } catch (...) {
            load_status = Status::InternalError("unknown vector index loader exception");
        }

        if (loaded != nullptr) {
            const bool published = _cache->_finish_ready_and_release(_entry, std::move(loaded));
            if (_cache->_metrics != nullptr && published) {
                _cache->_metrics->vector_index_cache_async_load_success.increment(1);
            }
        } else {
            _cache->_finish_empty_and_release(_entry);
            if (_cache->_metrics != nullptr) {
                _cache->_metrics->vector_index_cache_async_load_failure.increment(1);
            }
            LOG(WARNING) << "Failed to load vector index into cache asynchronously, key=" << _key
                         << ", status=" << load_status;
        }

        if (_cache->_metrics != nullptr) {
            _cache->_metrics->vector_index_cache_async_load_ns.increment(MonotonicNanos() - start_ns);
            _cache->_metrics->vector_index_cache_async_load_inflight.increment(-1);
        }
    }

    void cancel() noexcept override {
        _cache->_finish_empty_and_release(_entry);
        if (_cache->_metrics != nullptr) {
            _cache->_metrics->vector_index_cache_async_load_cancelled.increment(1);
        }
    }

private:
    VectorIndexCache* _cache;
    VectorIndexCache::Entry* _entry;
    VectorIndexCache::AsyncIndexLoader _loader;
    std::string _key;
};

VectorIndexCache::VectorIndexCache(size_t capacity, MemTracker* tracker, VectorIndexCacheMetrics* metrics)
        : _cache(capacity), _metrics(metrics == nullptr ? VectorIndexCacheMetrics::instance() : metrics) {
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
                            .set_min_threads(num_threads)
                            .set_max_threads(num_threads)
                            .set_max_queue_size(max_queue_size)
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

    if (entry->value().state() == VectorIndexCacheEntryState::kLoading) {
        _release_entry(entry);
        return false;
    }

    tenann::IndexRef ref;
    {
        auto lock = entry->value().guard();
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

VectorIndexCacheProbeResult VectorIndexCache::ProbeForQuery(const tenann::CacheKey& key) {
    const int64_t probe_start_ns = MonotonicNanos();
    DeferOp record_probe([&] {
        if (_metrics != nullptr) {
            _metrics->vector_index_cache_probe_count.increment(1);
            _metrics->vector_index_cache_probe_ns.increment(MonotonicNanos() - probe_start_ns);
        }
    });

    _lookup_count.fetch_add(1, std::memory_order_relaxed);
    Entry* entry = _cache.get(key.to_string());
    if (entry == nullptr) {
        _update_metrics();
        return {VectorIndexCacheProbeState::kMiss, {}};
    }

    if (entry->value().state() == VectorIndexCacheEntryState::kLoading) {
        _release_entry(entry);
        _update_metrics();
        return {VectorIndexCacheProbeState::kLoading, {}};
    }

    const int64_t lock_start_ns = MonotonicNanos();
    auto lock = entry->value().guard();
    if (_metrics != nullptr) {
        _metrics->vector_index_cache_entry_lock_wait_count.increment(1);
        _metrics->vector_index_cache_entry_lock_wait_ns.increment(MonotonicNanos() - lock_start_ns);
    }

    switch (entry->value().state(std::memory_order_relaxed)) {
    case VectorIndexCacheEntryState::kReady: {
        auto ref = entry->value().ref();
        if (ref == nullptr) {
            lock.unlock();
            _release_entry(entry);
            _update_metrics();
            return {VectorIndexCacheProbeState::kMiss, {}};
        }
        lock.unlock();
        _hit_count.fetch_add(1, std::memory_order_relaxed);
        _update_metrics();
        return {VectorIndexCacheProbeState::kReady, _wrap(entry, std::move(ref))};
    }
    case VectorIndexCacheEntryState::kLoading:
        lock.unlock();
        _release_entry(entry);
        _update_metrics();
        return {VectorIndexCacheProbeState::kLoading, {}};
    case VectorIndexCacheEntryState::kEmpty:
        lock.unlock();
        _release_entry(entry);
        _update_metrics();
        return {VectorIndexCacheProbeState::kMiss, {}};
    }
    __builtin_unreachable();
}

VectorIndexCacheProbeResult VectorIndexCache::TryGetOrSchedule(const tenann::CacheKey& key, AsyncIndexLoader loader) {
    if (!_accepting_async_loads.load(std::memory_order_acquire) || capacity() == 0) {
        if (_metrics != nullptr) {
            _metrics->vector_index_cache_async_load_rejected.increment(1);
        }
        return {VectorIndexCacheProbeState::kMiss, {}};
    }

    const std::string key_string = key.to_string();
    Entry* entry = _cache.get_or_create(key_string);
    if (entry->value().state() == VectorIndexCacheEntryState::kLoading) {
        _release_entry(entry);
        if (_metrics != nullptr) {
            _metrics->vector_index_cache_async_load_deduplicated.increment(1);
        }
        return {VectorIndexCacheProbeState::kLoading, {}};
    }

    std::shared_ptr<VectorIndexLoadTask> task;
    try {
        task = std::make_shared<VectorIndexLoadTask>(this, entry, std::move(loader), key_string);
    } catch (...) {
        _release_entry(entry);
        if (_metrics != nullptr) {
            _metrics->vector_index_cache_async_load_rejected.increment(1);
        }
        return {VectorIndexCacheProbeState::kMiss, {}};
    }

    const int64_t lock_start_ns = MonotonicNanos();
    auto lock = entry->value().guard();
    if (_metrics != nullptr) {
        _metrics->vector_index_cache_entry_lock_wait_count.increment(1);
        _metrics->vector_index_cache_entry_lock_wait_ns.increment(MonotonicNanos() - lock_start_ns);
    }
    switch (entry->value().state(std::memory_order_relaxed)) {
    case VectorIndexCacheEntryState::kReady: {
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
    case VectorIndexCacheEntryState::kLoading:
        lock.unlock();
        _release_entry(entry);
        if (_metrics != nullptr) {
            _metrics->vector_index_cache_async_load_deduplicated.increment(1);
        }
        return {VectorIndexCacheProbeState::kLoading, {}};
    case VectorIndexCacheEntryState::kEmpty:
        entry->value().set_state(VectorIndexCacheEntryState::kLoading);
        break;
    }
    lock.unlock();

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
        _finish_empty_and_release(entry);
        if (_metrics != nullptr) {
            _metrics->vector_index_cache_async_load_rejected.increment(1);
        }
        VLOG(1) << "Failed to submit vector index cache async load, key=" << key_string << ", status=" << submit_status;
        return {VectorIndexCacheProbeState::kMiss, {}};
    }

    if (_metrics != nullptr) {
        _metrics->vector_index_cache_async_load_submitted.increment(1);
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
    Entry* entry = _cache.get_or_create(key.to_string());
    if (ref == nullptr) {
        _release_entry(entry);
        return;
    }

    tenann::IndexRef stale;
    {
        auto lock = entry->value().guard();
        stale = entry->value().take_ref();
        entry->value().set_ref(ref);
        _cache.update_object_size(entry, entry->value().memory_usage());
        entry->value().set_state(VectorIndexCacheEntryState::kReady);
    }
    entry->value().notify_all();
    _update_metrics();
    *handle = _wrap(entry, std::move(ref));
}

bool VectorIndexCache::GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                                   tenann::IndexCacheHandle* handle) {
    *handle = tenann::IndexCacheHandle{};
    _lookup_count.fetch_add(1, std::memory_order_relaxed);
    Entry* entry = _cache.get_or_create(key.to_string());

    bool ran_loader = false;
    for (;;) {
        auto lock = entry->value().guard();
        switch (entry->value().state(std::memory_order_relaxed)) {
        case VectorIndexCacheEntryState::kReady: {
            auto ref = entry->value().ref();
            if (ref == nullptr) {
                entry->value().set_state(VectorIndexCacheEntryState::kEmpty);
                continue;
            }
            lock.unlock();
            if (!ran_loader) {
                _hit_count.fetch_add(1, std::memory_order_relaxed);
            }
            _update_metrics();
            *handle = _wrap(entry, std::move(ref));
            return true;
        }
        case VectorIndexCacheEntryState::kLoading:
            entry->value().wait_until_not_loading(lock);
            continue;
        case VectorIndexCacheEntryState::kEmpty:
            entry->value().set_state(VectorIndexCacheEntryState::kLoading);
            lock.unlock();
            break;
        }

        ran_loader = true;
        tenann::IndexRef loaded;
        try {
            loaded = loader();
        } catch (const tenann::Error& e) {
            _finish_empty_and_release(entry);
            _update_metrics();
            LOG(ERROR) << "VectorIndexCache loader threw for key " << key.to_string() << ": " << e.what();
            return false;
        } catch (const std::exception& e) {
            _finish_empty_and_release(entry);
            _update_metrics();
            LOG(ERROR) << "VectorIndexCache loader threw for key " << key.to_string() << ": " << e.what();
            return false;
        } catch (...) {
            _finish_empty_and_release(entry);
            _update_metrics();
            LOG(ERROR) << "VectorIndexCache loader threw an unknown exception for key " << key.to_string();
            return false;
        }
        if (loaded == nullptr) {
            _finish_empty_and_release(entry);
            _update_metrics();
            LOG(ERROR) << "VectorIndexCache loader returned null IndexRef for key " << key.to_string();
            return false;
        }

        tenann::IndexRef discarded;
        lock = entry->value().guard();
        if (entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kLoading) {
            entry->value().set_ref(std::move(loaded));
            _cache.update_object_size(entry, entry->value().memory_usage());
            entry->value().set_state(VectorIndexCacheEntryState::kReady);
        } else {
            discarded = std::move(loaded);
        }
        auto ref = entry->value().ref();
        lock.unlock();
        entry->value().notify_all();
        if (ref == nullptr) {
            _finish_empty_and_release(entry);
            _update_metrics();
            return false;
        }
        _update_metrics();
        *handle = _wrap(entry, std::move(ref));
        return true;
    }
}

bool VectorIndexCache::_finish_ready_and_release(Entry* entry, tenann::IndexRef loaded) noexcept {
    bool published = false;
    tenann::IndexRef discarded;
    {
        auto lock = entry->value().guard();
        if (entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kLoading) {
            entry->value().set_ref(std::move(loaded));
            _cache.update_object_size(entry, entry->value().memory_usage());
            entry->value().set_state(VectorIndexCacheEntryState::kReady);
            published = true;
        } else {
            discarded = std::move(loaded);
        }
    }
    entry->value().notify_all();
    _release_entry(entry);
    return published;
}

void VectorIndexCache::_finish_empty_and_release(Entry* entry) noexcept {
    tenann::IndexRef stale;
    {
        auto lock = entry->value().guard();
        if (entry->value().state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kLoading) {
            stale = entry->value().take_ref();
            if (entry->size() != 0) {
                _cache.update_object_size(entry, 0);
            }
            entry->value().set_state(VectorIndexCacheEntryState::kEmpty);
        }
    }
    entry->value().notify_all();
    _release_entry(entry);
}

void VectorIndexCache::_release_entry(Entry* entry, bool is_ivfpq_list_block) noexcept {
    if (is_ivfpq_list_block) {
        // List blocks belong to the outer IVF-PQ entry. When that entry goes
        // away, release its blocks as a group instead of giving each list an
        // independent TTL.
        _cache.remove(entry);
        _update_metrics();
        return;
    }

    const bool removed = _cache.release_with_expire_time_and_remove_if_unused(
            entry, expire_time_ms(), [](const VectorIndexCacheEntry& value) {
                return value.state(std::memory_order_relaxed) == VectorIndexCacheEntryState::kEmpty;
            });
    if (removed && _metrics != nullptr) {
        _metrics->vector_index_cache_empty_entry_removed.increment(1);
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
    if (_metrics == nullptr) {
        return;
    }
    _metrics->update(capacity(), memory_usage(), lookup_count(), hit_count());
}

} // namespace starrocks

#endif // WITH_TENANN

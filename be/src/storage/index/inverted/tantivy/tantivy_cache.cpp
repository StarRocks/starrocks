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

#include <algorithm>
#include <limits>
#include <thread>
#include <utility>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "storage/index/inverted/tantivy/random_access_bridge.h"
#include "util/dynamic_cache.h"
#include "util/time.h"

namespace starrocks {

TantivyReaderResource::~TantivyReaderResource() {
    if (cache_mem_tracker != nullptr && tracked_bytes > 0) {
        cache_mem_tracker->release_without_root(tracked_bytes);
    }
}

struct TantivyReaderCache::CacheValue {
    ~CacheValue() {
        if (owner == nullptr) {
            return;
        }
        owner->_entries.fetch_sub(1, std::memory_order_relaxed);
        owner->_estimated_resident_bytes.fetch_sub(estimated_bytes, std::memory_order_relaxed);
        if (resident_directory) {
            owner->_resident_directory_entries.fetch_sub(1, std::memory_order_relaxed);
            owner->_resident_directory_bytes.fetch_sub(resident_bytes, std::memory_order_relaxed);
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const CacheValue&) { return os << "TantivyReaderCacheValue"; }

    TantivyReaderCache* owner = nullptr;
    ResourcePtr resource;
    size_t estimated_bytes = 0;
    size_t resident_bytes = 0;
    bool resident_directory = false;
    std::mutex mutex;
};

struct TantivyReaderCache::LoadState {
    bool done = false;
    Status status = Status::OK();
    ResourcePtr resource;
    std::condition_variable cv;
};

struct TantivyReaderCache::LoadingStripe {
    std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<LoadState>> loads;
};

TantivyReaderCache::TantivyReaderCache(size_t capacity, size_t max_entries, size_t max_entry_bytes,
                                       MemTracker* mem_tracker)
        : _cache(std::make_unique<Cache>(capacity)),
          _mem_tracker(mem_tracker),
          _max_entries(std::max<size_t>(1, max_entries)),
          _max_entry_bytes(max_entry_bytes) {
    for (auto& stripe : _loading_stripes) {
        stripe = std::make_unique<LoadingStripe>();
    }
}

TantivyReaderCache::~TantivyReaderCache() {
    // CacheValue deleters update the counters below. Drain the cache while
    // those members are still alive instead of relying on reverse member
    // destruction order.
    _cache.reset();
}

TantivyReaderCache::ResourcePtr TantivyReaderCache::_lookup(const std::string& key, bool update_stats) {
    if (update_stats) {
        _lookup_count.fetch_add(1, std::memory_order_relaxed);
    }
    auto* entry = _cache->get(key);
    if (entry == nullptr) {
        if (update_stats) {
            _miss_count.fetch_add(1, std::memory_order_relaxed);
        }
        return nullptr;
    }
    ResourcePtr resource;
    {
        std::lock_guard lock(entry->value().mutex);
        resource = entry->value().resource;
    }
    _cache->release(entry);
    if (resource == nullptr) {
        if (update_stats) {
            _miss_count.fetch_add(1, std::memory_order_relaxed);
        }
        return nullptr;
    }
    if (update_stats) {
        _hit_count.fetch_add(1, std::memory_order_relaxed);
    }
    return resource;
}

TantivyReaderCache::ResourcePtr TantivyReaderCache::lookup(const TantivyIndexIdentity& identity) {
    return _lookup(identity.encode(), true);
}

void TantivyReaderCache::record_bypass() {
    _bypass_count.fetch_add(1, std::memory_order_relaxed);
}

StatusOr<TantivyReaderCache::ResourcePtr> TantivyReaderCache::get_or_load(const TantivyIndexIdentity& identity,
                                                                          const Loader& loader,
                                                                          const IsCancelled& is_cancelled) {
    const std::string key = identity.encode();
    if (auto resource = _lookup(key, true); resource != nullptr) {
        return resource;
    }

    const size_t stripe_index = std::hash<std::string_view>{}(key) % _loading_stripes.size();
    auto& stripe = *_loading_stripes[stripe_index];
    std::shared_ptr<LoadState> state;
    bool is_loader = false;
    {
        std::unique_lock lock(stripe.mutex);
        if (auto resource = _lookup(key, false); resource != nullptr) {
            _hit_count.fetch_add(1, std::memory_order_relaxed);
            return resource;
        }
        auto [it, inserted] = stripe.loads.try_emplace(key, std::make_shared<LoadState>());
        state = it->second;
        is_loader = inserted;
        if (!is_loader) {
            _duplicate_build_prevented.fetch_add(1, std::memory_order_relaxed);
            _singleflight_waiters.fetch_add(1, std::memory_order_relaxed);
            const auto wait_start = std::chrono::steady_clock::now();
            while (!state->done) {
                if (is_cancelled && is_cancelled()) {
                    return Status::Cancelled("cancelled while waiting for Tantivy reader open");
                }
                state->cv.wait_for(lock, std::chrono::milliseconds(50));
            }
            _singleflight_wait_ns.fetch_add(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - wait_start)
                            .count(),
                    std::memory_order_relaxed);
            if (!state->status.ok()) {
                return state->status;
            }
            return state->resource;
        }
    }

    _build_count.fetch_add(1, std::memory_order_relaxed);
    auto loaded = loader();
    Status status = loaded.ok() ? Status::OK() : loaded.status();
    ResourcePtr resource;
    if (loaded.ok()) {
        resource = std::move(loaded).value();
        _maybe_insert(key, resource);
    } else {
        _build_error_count.fetch_add(1, std::memory_order_relaxed);
    }

    {
        std::lock_guard lock(stripe.mutex);
        state->status = status;
        state->resource = resource;
        state->done = true;
        stripe.loads.erase(key);
    }
    state->cv.notify_all();
    if (!status.ok()) {
        return status;
    }
    return resource;
}

void TantivyReaderCache::_maybe_insert(const std::string& key, const ResourcePtr& resource) {
    const size_t estimated_bytes = std::max<size_t>(1, resource->estimated_bytes + key.size());
    if (!_is_admissible(estimated_bytes)) {
        _oversize_reject_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    const size_t cache_capacity = _cache->capacity();
    const size_t entry_floor = (cache_capacity + _max_entries - 1) / _max_entries;
    const size_t effective_charge = std::max(estimated_bytes, entry_floor);
    resource->effective_charge = effective_charge;
    resource->cache_mem_tracker = _mem_tracker;
    // Buffers leased through TantivyReadBufferPool are tracked at the actual
    // malloc/free boundary. Only account for the remaining reader metadata
    // here so resident file bytes are not charged twice.
    resource->tracked_bytes = estimated_bytes - std::min(estimated_bytes, resource->resident_bytes);
    if (_mem_tracker != nullptr) {
        _mem_tracker->consume_without_root(resource->tracked_bytes);
    }

    auto* entry = _cache->get_or_create(key);
    bool inserted = false;
    {
        std::lock_guard lock(entry->value().mutex);
        if (entry->value().resource == nullptr) {
            entry->value().owner = this;
            entry->value().resource = resource;
            entry->value().estimated_bytes = estimated_bytes;
            entry->value().resident_bytes = resource->resident_bytes;
            entry->value().resident_directory = resource->resident_directory;
            inserted = true;
        }
    }
    if (inserted) {
        _cache->update_object_size(entry, effective_charge);
        _entries.fetch_add(1, std::memory_order_relaxed);
        _estimated_resident_bytes.fetch_add(estimated_bytes, std::memory_order_relaxed);
        if (resource->resident_directory) {
            _resident_directory_entries.fetch_add(1, std::memory_order_relaxed);
            _resident_directory_bytes.fetch_add(resource->resident_bytes, std::memory_order_relaxed);
        }
        _insert_count.fetch_add(1, std::memory_order_relaxed);
    } else {
        resource->cache_mem_tracker = nullptr;
        resource->tracked_bytes = 0;
        if (_mem_tracker != nullptr) {
            _mem_tracker->release_without_root(
                    estimated_bytes - std::min(estimated_bytes, resource->resident_bytes));
        }
    }
    _cache->release(entry);
}

bool TantivyReaderCache::_is_admissible(size_t estimated_bytes) const {
    const size_t cache_capacity = _cache->capacity();
    if (cache_capacity == 0) {
        return false;
    }
    const size_t entry_floor = (cache_capacity + _max_entries - 1) / _max_entries;
    const size_t effective_charge = std::max<size_t>(1, std::max(estimated_bytes, entry_floor));
    return effective_charge <= _max_entry_bytes && effective_charge <= cache_capacity;
}

bool TantivyReaderCache::would_admit(const TantivyIndexIdentity& identity, size_t estimated_bytes) const {
    const auto key = identity.encode();
    const size_t total = estimated_bytes > std::numeric_limits<size_t>::max() - key.size()
                                 ? std::numeric_limits<size_t>::max()
                                 : estimated_bytes + key.size();
    return _is_admissible(total);
}

void TantivyReaderCache::erase(const TantivyIndexIdentity& identity) {
    _cache->try_remove_by_key(identity.encode());
}

void TantivyReaderCache::prune() {
    _cache->clear();
}

void TantivyReaderCache::set_capacity(size_t capacity) {
    _cache->set_capacity(capacity);
}

size_t TantivyReaderCache::capacity() const {
    return _cache->capacity();
}

size_t TantivyReaderCache::memory_usage() const {
    return _cache->size();
}

TantivyReaderCacheStats TantivyReaderCache::stats() const {
    return {
            .lookup = _lookup_count.load(std::memory_order_relaxed),
            .hit = _hit_count.load(std::memory_order_relaxed),
            .miss = _miss_count.load(std::memory_order_relaxed),
            .bypass = _bypass_count.load(std::memory_order_relaxed),
            .insert = _insert_count.load(std::memory_order_relaxed),
            .oversize_reject = _oversize_reject_count.load(std::memory_order_relaxed),
            .build = _build_count.load(std::memory_order_relaxed),
            .build_error = _build_error_count.load(std::memory_order_relaxed),
            .duplicate_build_prevented = _duplicate_build_prevented.load(std::memory_order_relaxed),
            .singleflight_waiters = _singleflight_waiters.load(std::memory_order_relaxed),
            .singleflight_wait_ns = _singleflight_wait_ns.load(std::memory_order_relaxed),
            .entries = _entries.load(std::memory_order_relaxed),
            .estimated_resident_bytes = _estimated_resident_bytes.load(std::memory_order_relaxed),
            .resident_directory_entries = _resident_directory_entries.load(std::memory_order_relaxed),
            .resident_directory_bytes = _resident_directory_bytes.load(std::memory_order_relaxed),
    };
}

struct TantivyQueryCache::CacheValue {
    ~CacheValue() {
        if (owner != nullptr) {
            owner->_entries.fetch_sub(1, std::memory_order_relaxed);
            owner->_estimated_resident_bytes.fetch_sub(estimated_bytes, std::memory_order_relaxed);
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const CacheValue&) { return os << "TantivyQueryCacheValue"; }

    TantivyQueryCache* owner = nullptr;
    BitmapPtr bitmap;
    size_t estimated_bytes = 0;
    std::mutex mutex;
};

TantivyQueryCache::TantivyQueryCache(size_t capacity, size_t max_entry_bytes, size_t max_key_bytes,
                                     double admission_threshold, size_t ghost_entries, MemTracker* mem_tracker)
        : _cache(std::make_unique<Cache>(capacity)),
          _mem_tracker(mem_tracker),
          _max_entry_bytes(max_entry_bytes),
          _max_key_bytes(max_key_bytes),
          _admission_threshold(std::clamp(admission_threshold, 0.0, 1.0)),
          _ghost_entries(ghost_entries) {}

TantivyQueryCache::~TantivyQueryCache() {
    _ghost.clear();
    // CacheValue deleters update this object's counters.
    _cache.reset();
}

TantivyQueryCache::BitmapPtr TantivyQueryCache::lookup(const std::string& key) {
    _lookup_count.fetch_add(1, std::memory_order_relaxed);
    if (key.size() > _max_key_bytes) {
        _key_too_large_count.fetch_add(1, std::memory_order_relaxed);
        _miss_count.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    auto* entry = _cache->get(key);
    if (entry == nullptr) {
        _miss_count.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    BitmapPtr bitmap;
    {
        std::lock_guard lock(entry->value().mutex);
        bitmap = entry->value().bitmap;
    }
    _cache->release(entry);
    if (bitmap == nullptr) {
        _miss_count.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    _hit_count.fetch_add(1, std::memory_order_relaxed);
    return bitmap;
}

void TantivyQueryCache::record_bypass() {
    _bypass_count.fetch_add(1, std::memory_order_relaxed);
}

bool TantivyQueryCache::_admit(const std::string& key) {
    const size_t cache_capacity = _cache->capacity();
    if (cache_capacity == 0 ||
        static_cast<double>(_cache->size()) < static_cast<double>(cache_capacity) * _admission_threshold) {
        return true;
    }
    if (_ghost_entries == 0) {
        return false;
    }

    constexpr int64_t kAdmissionWindowSeconds = 60;
    const uint64_t digest = std::hash<std::string_view>{}(key);
    const int64_t now = MonotonicSeconds();
    std::lock_guard lock(_ghost_mutex);
    auto it = _ghost.find(digest);
    if (it != _ghost.end() && now - it->second <= kAdmissionWindowSeconds) {
        _ghost.erase(it);
        _ghost_admit_count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    if (_ghost.size() >= _ghost_entries) {
        _ghost.erase(_ghost.begin());
    }
    _ghost[digest] = now;
    _ghost_record_count.fetch_add(1, std::memory_order_relaxed);
    return false;
}

void TantivyQueryCache::maybe_insert(const std::string& key, const roaring::Roaring& bitmap) {
    const size_t cache_capacity = _cache->capacity();
    if (key.size() > _max_key_bytes) {
        _key_too_large_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (cache_capacity == 0) {
        _oversize_reject_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (!_admit(key)) {
        return;
    }

    auto* allocation_tracker = _mem_tracker != nullptr ? _mem_tracker->parent() : nullptr;
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(allocation_tracker);
    auto mutable_bitmap = std::make_unique<roaring::Roaring>(bitmap);
    mutable_bitmap->runOptimize();
    mutable_bitmap->shrinkToFit();
    const size_t final_bytes = sizeof(CacheValue) + mutable_bitmap->getSizeInBytes() + key.size();
    if (final_bytes > _max_entry_bytes || final_bytes > cache_capacity) {
        _oversize_reject_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    if (_mem_tracker != nullptr) {
        _mem_tracker->consume_without_root(final_bytes);
    }
    BitmapPtr immutable_bitmap(mutable_bitmap.release(), [tracker = _mem_tracker, allocation_tracker,
                                                          final_bytes](const auto* value) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(allocation_tracker);
        delete value;
        if (tracker != nullptr) {
            tracker->release_without_root(final_bytes);
        }
    });

    auto* entry = _cache->get_or_create(key);
    bool inserted = false;
    {
        std::lock_guard lock(entry->value().mutex);
        if (entry->value().bitmap == nullptr) {
            entry->value().owner = this;
            entry->value().bitmap = std::move(immutable_bitmap);
            entry->value().estimated_bytes = final_bytes;
            inserted = true;
        }
    }
    if (inserted) {
        _cache->update_object_size(entry, final_bytes);
        _entries.fetch_add(1, std::memory_order_relaxed);
        _estimated_resident_bytes.fetch_add(final_bytes, std::memory_order_relaxed);
        _insert_count.fetch_add(1, std::memory_order_relaxed);
    }
    _cache->release(entry);
}

void TantivyQueryCache::erase(const std::string& key) {
    _cache->try_remove_by_key(key);
}

void TantivyQueryCache::prune() {
    _cache->clear();
    std::lock_guard lock(_ghost_mutex);
    _ghost.clear();
}

void TantivyQueryCache::set_capacity(size_t capacity) {
    _cache->set_capacity(capacity);
}

size_t TantivyQueryCache::capacity() const {
    return _cache->capacity();
}

size_t TantivyQueryCache::memory_usage() const {
    return _cache->size();
}

TantivyQueryCacheStats TantivyQueryCache::stats() const {
    return {
            .lookup = _lookup_count.load(std::memory_order_relaxed),
            .hit = _hit_count.load(std::memory_order_relaxed),
            .miss = _miss_count.load(std::memory_order_relaxed),
            .bypass = _bypass_count.load(std::memory_order_relaxed),
            .insert = _insert_count.load(std::memory_order_relaxed),
            .oversize_reject = _oversize_reject_count.load(std::memory_order_relaxed),
            .key_too_large = _key_too_large_count.load(std::memory_order_relaxed),
            .ghost_record = _ghost_record_count.load(std::memory_order_relaxed),
            .ghost_admit = _ghost_admit_count.load(std::memory_order_relaxed),
            .entries = _entries.load(std::memory_order_relaxed),
            .estimated_resident_bytes = _estimated_resident_bytes.load(std::memory_order_relaxed),
    };
}

TantivyCacheManager::TantivyCacheManager(size_t reader_capacity, size_t reader_max_entries,
                                         size_t reader_max_entry_bytes, MemTracker* reader_mem_tracker,
                                         size_t query_capacity, size_t query_max_entry_bytes,
                                         size_t query_max_key_bytes, double query_admission_threshold,
                                         size_t query_ghost_entries, MemTracker* query_mem_tracker)
        : _read_buffer_pool(std::make_shared<TantivyReadBufferPool>(
                  std::min<size_t>(reader_capacity / 8, 256UL * 1024 * 1024), 1024UL * 1024, reader_mem_tracker)),
          _reader_cache(reader_capacity, reader_max_entries, reader_max_entry_bytes, reader_mem_tracker),
          _query_cache(query_capacity, query_max_entry_bytes, query_max_key_bytes, query_admission_threshold,
                       query_ghost_entries, query_mem_tracker) {}

} // namespace starrocks

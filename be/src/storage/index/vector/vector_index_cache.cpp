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

#include <limits>

#include "common/logging.h"
#include "runtime/mem_tracker.h"
#include "storage/index/vector/vector_index_cache_metrics.h"

namespace starrocks {

namespace {

int64_t expire_time_ms(int64_t expire_seconds) {
    if (expire_seconds <= 0) {
        return std::numeric_limits<int64_t>::max();
    }
    const int64_t now = MonotonicMillis();
    constexpr int64_t kMillisPerSecond = 1000;
    if (expire_seconds > (std::numeric_limits<int64_t>::max() - now) / kMillisPerSecond) {
        return std::numeric_limits<int64_t>::max();
    }
    return now + expire_seconds * kMillisPerSecond;
}

} // namespace

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

// Drain IndexRefs outside _cache._lock before ~DynamicCache acquires it.
// IVF-PQ entries hold nested IndexCacheHandles whose deleters call back into
// this cache; freeing them under ~DynamicCache's lock self-deadlocks
// (std::mutex isn't recursive) and stalls BE shutdown.
VectorIndexCache::~VectorIndexCache() {
    auto entries = _cache.get_all_entries();
    for (auto* entry : entries) {
        auto g = entry->value().guard();
        entry->value().set_ref(nullptr);
    }
    for (auto* entry : entries) {
        _cache.release(entry);
    }
}

bool VectorIndexCache::Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) {
    // Counter-silent: this is the warm-path probe in VectorIndexReaderFactory
    // and counting it would double up with the GetOrCreate call right after.
    Entry* entry = _cache.get(key.to_string());
    if (entry == nullptr) return false;

    tenann::IndexRef ref;
    {
        auto g = entry->value().guard();
        if (!entry->value().has_ref()) {
            g.unlock();
            _cache.release(entry);
            return false;
        }
        ref = entry->value().ref();
    }
    *handle = _wrap(entry, std::move(ref));
    return true;
}

void VectorIndexCache::SetCapacity(size_t new_capacity) {
    _cache.set_capacity(new_capacity);
    _update_metrics();
}

void VectorIndexCache::SetExpireSeconds(int64_t expire_seconds) {
    _expire_seconds.store(expire_seconds, std::memory_order_relaxed);
}

bool VectorIndexCache::ClearExpired(int64_t now) {
    if (expire_seconds() <= 0) {
        return false;
    }

    _cache.clear_expired(now);
    _update_metrics();
    return true;
}

void VectorIndexCache::Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) {
    Entry* entry = _cache.get_or_create(key.to_string());
    {
        auto g = entry->value().guard();
        entry->value().set_ref(ref);
        _cache.update_object_size(entry, entry->value().memory_usage());
    }
    _update_metrics();
    *handle = _wrap(entry, std::move(ref));
}

bool VectorIndexCache::GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                                   tenann::IndexCacheHandle* handle) {
    // Per-entry guard single-flights cold loads.
    _lookup_count.fetch_add(1, std::memory_order_relaxed);
    Entry* entry = _cache.get_or_create(key.to_string());
    tenann::IndexRef ref;
    bool warm_hit;
    {
        auto g = entry->value().guard();
        warm_hit = entry->value().has_ref();
        if (!warm_hit) {
            tenann::IndexRef loaded;
            try {
                loaded = loader();
            } catch (const std::exception& e) {
                g.unlock();
                _cache.remove(entry);
                _update_metrics();
                LOG(ERROR) << "VectorIndexCache loader threw for key " << key.to_string() << ": " << e.what();
                return false;
            }
            if (loaded == nullptr) {
                g.unlock();
                _cache.remove(entry);
                _update_metrics();
                LOG(ERROR) << "VectorIndexCache loader returned null IndexRef for key " << key.to_string();
                return false;
            }
            entry->value().set_ref(std::move(loaded));
            _cache.update_object_size(entry, entry->value().memory_usage());
        }
        ref = entry->value().ref();
    }
    if (warm_hit) {
        _hit_count.fetch_add(1, std::memory_order_relaxed);
    }
    _update_metrics();
    *handle = _wrap(entry, std::move(ref));
    return true;
}

// Deleter captures this as a raw pointer; handles MUST be released before
// StorageEnv::destroy_vector_index_cache() runs after query/vector users drain.
tenann::IndexCacheHandle VectorIndexCache::_wrap(Entry* entry, tenann::IndexRef ref) {
    const bool is_ivfpq_list_block =
            ref != nullptr && ref->index_type() == tenann::IndexType::kFaissIvfPqOneInvertedList;
    return tenann::IndexCacheHandle(std::move(ref), std::shared_ptr<void>(entry, [this, is_ivfpq_list_block](void* p) {
                                        _release(static_cast<Entry*>(p), is_ivfpq_list_block);
                                    }));
}

void VectorIndexCache::_release(Entry* entry, bool is_ivfpq_list_block) {
    if (is_ivfpq_list_block) {
        // List blocks belong to the outer IVF-PQ entry. When that entry goes
        // away, release its blocks as a group instead of giving each list an
        // independent TTL.
        _cache.remove(entry);
        _update_metrics();
        return;
    }

    _cache.release_with_expire_time(entry, expire_time_ms(expire_seconds()));
}

void VectorIndexCache::_update_metrics() const {
    if (_metrics == nullptr) {
        return;
    }
    _metrics->update(capacity(), memory_usage(), lookup_count(), hit_count());
}

} // namespace starrocks

#endif // WITH_TENANN

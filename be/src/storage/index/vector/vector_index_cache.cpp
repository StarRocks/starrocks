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

#include "common/logging.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

VectorIndexCache::VectorIndexCache(size_t capacity, MemTracker* tracker) : _cache(capacity) {
    // The HNSW/tenann index lives in the normal heap, so the global allocator hook
    // already charges its bytes to the process tracker once during load. Accounting
    // them additively on the vector_index tracker (a child of process) would count
    // the same bytes a second time on process and spuriously trip the process
    // mem_limit. Use the excluding-root variant so the vector_index tracker labels
    // the usage without re-adding it to process.
    _cache.set_mem_tracker_excluding_root(tracker);
}

// Drain IndexRefs outside _cache._lock before ~DynamicCache acquires it.
// IVF-PQ entries hold nested IndexCacheHandles whose deleters call back into
// _cache.release(); freeing them under ~DynamicCache's lock self-deadlocks
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

void VectorIndexCache::Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) {
    Entry* entry = _cache.get_or_create(key.to_string());
    {
        auto g = entry->value().guard();
        entry->value().set_ref(ref);
        _cache.update_object_size(entry, entry->value().memory_usage());
    }
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
                LOG(ERROR) << "VectorIndexCache loader threw for key " << key.to_string() << ": " << e.what();
                return false;
            }
            if (loaded == nullptr) {
                g.unlock();
                _cache.remove(entry);
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
    *handle = _wrap(entry, std::move(ref));
    return true;
}

// Deleter captures _cache as a raw pointer; handles MUST be released before
// ExecEnv::destroy() — _wait_for_fragments_finish() is the drain boundary.
tenann::IndexCacheHandle VectorIndexCache::_wrap(Entry* entry, tenann::IndexRef ref) {
    Cache* cache = &_cache;
    return tenann::IndexCacheHandle(
            std::move(ref), std::shared_ptr<void>(entry, [cache](void* p) { cache->release(static_cast<Entry*>(p)); }));
}

} // namespace starrocks

#endif // WITH_TENANN

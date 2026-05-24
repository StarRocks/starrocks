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
    _cache.set_mem_tracker(tracker);
}

// IVF-PQ block-cache entries (and any future entry whose value chain re-enters
// this cache) make naive teardown unsafe. The chain on shutdown is:
//
//   top-level IVF-PQ entry
//     -> tenann::IndexRef
//        -> faiss::IndexIVFPQ
//           -> BlockCacheInvertedLists      (owned by faiss::IndexIVFPQ)
//              -> std::vector<IndexCacheHandle>
//                 -> shared_ptr<void> releaser_
//                    -> [cache](void*){cache->release(inner_entry);}
//
// `~DynamicCache` holds `_cache._lock` for the entire iteration and runs
// `delete iobj` in-place. That cascade unwinds straight into the inner-entry
// releaser, which calls `_cache.release` and tries to re-acquire the same
// lock. std::mutex isn't recursive, so the main thread parks on
// FUTEX_WAIT_PRIVATE value=2 forever — the symptom CI's Restart BE step
// reports as "BE exit and gen gcov timeout".
//
// Fix: drain every entry's IndexRef BEFORE the base destructor takes the
// lock. `get_all_entries` only holds `_cache._lock` while copying out the
// list and bumping refs, so the per-entry `set_ref(nullptr)` here — which
// is what actually triggers the BlockCacheInvertedLists destructor and the
// inner releaser callbacks — runs with `_cache._lock` free. By the time
// ~DynamicCache walks the list under the lock, every value()._ref is null
// and `delete iobj` is a no-op cascade.
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
    // Intentionally does not bump _lookup_count / _hit_count. Lookup is used by
    // VectorIndexReaderFactory as an opportunistic probe to skip the OSS HEAD
    // round-trip on warm-cache reads; the actual cache lookup that drives load
    // semantics happens in the GetOrCreate call inside TenANNReader::init_searcher
    // shortly after. Counting both would double the absolute lookup_count / hit_count
    // gauges that production dashboards/alerts compare against pre-PR baselines.
    Entry* entry = _cache.get(key.to_string());
    if (entry == nullptr) return false;

    tenann::IndexRef ref;
    {
        auto g = entry->value().guard();
        if (!entry->value().has_ref()) {
            // Entry is being populated by another caller; report miss.
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
    // Per-entry guard single-flights the cold-load path: only one caller runs
    // loader() and pairs it with update_object_size; others wait and observe
    // has_ref() == true. On failure, remove(entry) drops our caller ref so
    // retries start fresh.
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

// Deleter releases the entry ref when the handle is destroyed. The lambda
// captures _cache as a raw pointer: callers (TenANNReader::_cache_handle,
// etc.) must release every handle before ExecEnv::destroy() runs reset().
// _wait_for_fragments_finish() at shutdown serves as the drain point. Same
// contract as PrimaryIndex/StoragePageCache and the original tenann global
// cache; see DynamicCache::~DynamicCache notes.
tenann::IndexCacheHandle VectorIndexCache::_wrap(Entry* entry, tenann::IndexRef ref) {
    Cache* cache = &_cache;
    return tenann::IndexCacheHandle(
            std::move(ref), std::shared_ptr<void>(entry, [cache](void* p) { cache->release(static_cast<Entry*>(p)); }));
}

} // namespace starrocks

#endif // WITH_TENANN

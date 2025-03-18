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

#include "storage/rowset/metadata_cache.h"

#include "storage/rowset/rowset.h"
#include "util/lru_cache.h"

namespace starrocks {

MetadataCache* MetadataCache::_s_instance = nullptr;

void MetadataCache::create_cache(size_t capacity) {
    if (_s_instance == nullptr) {
        _s_instance = new MetadataCache(capacity);
    }
}

MetadataCache::MetadataCache(size_t capacity) {
    _cache.reset(new_lru_cache(capacity));
}

void MetadataCache::cache_rowset(Rowset* ptr) {
    std::weak_ptr<Rowset>* weak_ptr = new std::weak_ptr<Rowset>(ptr->shared_from_this());
    _insert(ptr->rowset_id_str(), weak_ptr, ptr->segment_memory_usage());
}

void MetadataCache::evict_rowset(Rowset* ptr) {
    _erase(ptr->rowset_id_str());
}

void MetadataCache::refresh_rowset(Rowset* ptr) {
    _warmup(ptr->rowset_id_str());
}

size_t MetadataCache::get_memory_usage() const {
    return _cache->get_memory_usage();
}

void MetadataCache::set_capacity(size_t capacity) {
    _cache->set_capacity(capacity);
}

void MetadataCache::_insert(const std::string& key, std::weak_ptr<Rowset>* ptr, size_t size) {
    Cache::Handle* handle = _cache->insert(CacheKey(key), ptr, size, size, _cache_value_deleter);
    _cache->release(handle);
}

void MetadataCache::_erase(const std::string& key) {
    _cache->erase(CacheKey(key));
}

// This function is used as a deleter for the cache values.
// It is called when a cache entry is evicted or removed.
// The function takes a CacheKey and a void pointer to the value.
// The value is expected to be a std::weak_ptr<Rowset>.
// If the weak_ptr can be locked to a shared_ptr, it calls the close() method on the Rowset.
// Finally, it deletes the weak_ptr.
void MetadataCache::_cache_value_deleter(const CacheKey& /*key*/, void* value) {
    std::weak_ptr<Rowset>* weak_ptr = reinterpret_cast<std::weak_ptr<Rowset>*>(value);
    auto rowset = weak_ptr->lock();
    if (rowset != nullptr) {
        rowset->close();
    }
    delete weak_ptr;
}

void MetadataCache::_warmup(const std::string& key) {
    Cache::Handle* handle = _cache->lookup(CacheKey(key));
    if (handle != nullptr) {
        _cache->release(handle);
    }
}

} // namespace starrocks
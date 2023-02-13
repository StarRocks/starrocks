// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "star_cache/common/lru_container.h"

#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>

#include <rapidjson/document.h>
#include <glog/logging.h>

namespace starrocks::starcache {

uint32_t LRUKey::hash(const char* data, size_t n, uint32_t seed) const {
    // Similar to murmur hash
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char* limit = data + n;
    uint32_t h = seed ^ (n * m);

    // Pick up four bytes at a time
    while (data + 4 <= limit) {
        uint32_t w = _decode_fixed32(data);
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }

    // Pick up remaining bytes
    switch (limit - data) {
    case 3:
        h += static_cast<unsigned char>(data[2]) << 16;

        // fall through
    case 2:
        h += static_cast<unsigned char>(data[1]) << 8;

        // fall through
    case 1:
        h += static_cast<unsigned char>(data[0]);
        h *= m;
        h ^= (h >> r);
        break;

    default:
        break;
    }

    return h;
}

LRUHandle* HandleTable::lookup(const LRUKey& key, uint32_t hash) {
    return *_find_pointer(key, hash);
}

LRUHandle* HandleTable::insert(LRUHandle* h) {
    LRUHandle** ptr = _find_pointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;

    if (old == nullptr) {
        ++_elems;

        if (_elems > _length) {
            // Since each container entry is fairly large, we aim for a small
            // average linked list length (<= 1).
            if (!_resize()) {
                return nullptr;
            }
        }
    }

    return old;
}

LRUHandle* HandleTable::remove(const LRUKey& key, uint32_t hash) {
    LRUHandle** ptr = _find_pointer(key, hash);
    LRUHandle* result = *ptr;

    if (result != nullptr) {
        *ptr = result->next_hash;
        --_elems;
    }

    return result;
}

LRUHandle** HandleTable::_find_pointer(const LRUKey& key, uint32_t hash) {
    LRUHandle** ptr = &_list[hash & (_length - 1)];

    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
        ptr = &(*ptr)->next_hash;
    }

    return ptr;
}

bool HandleTable::_resize() {
    uint32_t new_length = 4;

    while (new_length < _elems) {
        new_length *= 2;
    }

    auto** new_list = new (std::nothrow) LRUHandle*[new_length];

    if (nullptr == new_list) {
        LOG(ERROR) << "failed to malloc new hash list. new_length=" << new_length;
        return false;
    }

    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;

    for (uint32_t i = 0; i < _length; i++) {
        LRUHandle* h = _list[i];

        while (h != nullptr) {
            LRUHandle* next = h->next_hash;
            uint32_t hash = h->hash;
            LRUHandle** ptr = &new_list[hash & (new_length - 1)];
            h->next_hash = *ptr;
            *ptr = h;
            h = next;
            count++;
        }
    }

    if (_elems != count) {
        delete[] new_list;
        LOG(ERROR) << "_elems not match new count. elems=" << _elems << ", count=" << count;
        return false;
    }

    delete[] _list;
    _list = new_list;
    _length = new_length;
    return true;
}

LRUContainer::LRUContainer() {
    // Make empty circular linked list
    _lru.next = &_lru;
    _lru.prev = &_lru;
}

LRUContainer::~LRUContainer() {
    prune();
}

bool LRUContainer::_unref(LRUHandle* e) {
    DCHECK(e->refs > 0);
    e->refs--;
    return e->refs == 0;
}

void LRUContainer::_lru_remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
    e->prev = e->next = nullptr;
}

void LRUContainer::_lru_append(LRUHandle* list, LRUHandle* e) {
    // Make "e" newest entry by inserting just before *list
    e->next = list;
    e->prev = list->prev;
    e->prev->next = e;
    e->next->prev = e;
}

uint64_t LRUContainer::get_lookup_count() {
    std::lock_guard l(_mutex);
    return _lookup_count;
}

size_t LRUContainer::get_usage() {
    std::lock_guard l(_mutex);
    return _usage;
}

LRUHandle* LRUContainer::lookup(const LRUKey& key, uint32_t hash) {
    std::lock_guard l(_mutex);
    ++_lookup_count;
    LRUHandle* e = _table.lookup(key, hash);
    if (e != nullptr) {
        // we get it from _table, so valid must be true
        DCHECK(e->valid);
        if (e->refs == 1) {
            // only in LRU free list, remove it from list
            _lru_remove(e);
        }
        e->refs++;
    }
    return e;
}

void LRUContainer::release(LRUHandle* handle) {
    if (handle == nullptr) {
        return;
    }
    auto* e = handle;
    bool last_ref = false;
    {
        std::lock_guard l(_mutex);
        last_ref = _unref(e);
        if (last_ref) {
            _usage -= e->charge;
        } else if (e->valid && e->refs == 1) {
            // put it to LRU free list
            _lru_append(&_lru, e);
        }
    }

    // free handle out of mutex
    if (last_ref) {
        e->free();
    }
}

void LRUContainer::_evict_from_lru(size_t charge, std::vector<LRUHandle*>* evicted) {
    LRUHandle* cur = &_lru;
    size_t evicted_size = 0;
    while (evicted_size < charge && cur->next != &_lru) {
        LRUHandle* old = cur->next;
        _evict_one_entry(old);
        evicted->push_back(old);
        evicted_size += old->charge;
    }
}

void LRUContainer::_evict_one_entry(LRUHandle* e) {
    DCHECK(e->valid);
    DCHECK(e->refs == 1); // LRU list contains elements which may be evicted
    _lru_remove(e);
    _table.remove(e->key(), e->hash);
    e->valid = false;
    _unref(e);
    _usage -= e->charge;
}

LRUHandle* LRUContainer::insert(const LRUKey& key, uint32_t hash, size_t charge, void* value,
                                void (*deleter)(const LRUKey& key, void* value)) {
    auto* e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->refs = 2; // one for the returned handle, one for LRUContainer.
    e->next = e->prev = nullptr;
    e->valid = true;
    memcpy(e->key_data, key.data(), key.size());
    std::vector<LRUHandle*> last_ref_list;
    {
        std::lock_guard l(_mutex);

        // insert into the container
        auto old = _table.insert(e);
        _usage += charge;
        if (old != nullptr) {
            old->valid = false;
            if (_unref(old)) {
                _usage -= old->charge;
                // old is on LRU because it's in container and its reference count
                // was just 1 (Unref returned 0)
                _lru_remove(old);
                last_ref_list.push_back(old);
            }
        }
    }

    // we free the entries here outside of mutex for
    // performance reasons
    for (auto entry : last_ref_list) {
        entry->free();
    }

    return e;
}

void LRUContainer::erase(const LRUKey& key, uint32_t hash) {
    LRUHandle* e = nullptr;
    bool last_ref = false;
    {
        std::lock_guard l(_mutex);
        e = _table.remove(key, hash);
        if (e != nullptr) {
            last_ref = _unref(e);
            if (last_ref) {
                _usage -= e->charge;
                if (e->valid) {
                    // locate in free list
                    _lru_remove(e);
                }
            }
            e->valid = false;
        }
    }
    // free handle out of mutex, when last_ref is true, e must not be nullptr
    if (last_ref) {
        e->free();
    }
}

void LRUContainer::evict(size_t charge, std::vector<LRUHandle*>* evicted) {
    std::lock_guard l(_mutex);
    _evict_from_lru(charge, evicted);
}

int LRUContainer::prune() {
    std::vector<LRUHandle*> last_ref_list;
    {
        std::lock_guard l(_mutex);
        while (_lru.next != &_lru) {
            LRUHandle* old = _lru.next;
            DCHECK(old->valid);
            DCHECK(old->refs == 1); // LRU list contains elements which may be evicted
            _lru_remove(old);
            _table.remove(old->key(), old->hash);
            old->valid = false;
            _unref(old);
            _usage -= old->charge;
            last_ref_list.push_back(old);
        }
    }
    for (auto entry : last_ref_list) {
        entry->free();
    }
    return last_ref_list.size();
}

inline uint32_t ShardedLRUContainer::_hash_slice(const LRUKey& s) {
    return s.hash(s.data(), s.size(), 0);
}

uint32_t ShardedLRUContainer::_shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
}

LRUHandle* ShardedLRUContainer::insert(const LRUKey& key, size_t charge, void* value,
                                       void (*deleter)(const LRUKey& key, void* value)) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)].insert(key, hash, charge, value, deleter);
}

LRUHandle* ShardedLRUContainer::lookup(const LRUKey& key) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)].lookup(key, hash);
}

void ShardedLRUContainer::release(LRUHandle* handle) {
    _shards[_shard(handle->hash)].release(handle);
}

void ShardedLRUContainer::erase(const LRUKey& key) {
    const uint32_t hash = _hash_slice(key);
    _shards[_shard(hash)].erase(key, hash);
}

void ShardedLRUContainer::evict(size_t charge, std::vector<LRUHandle*>* evicted) {
    for (auto& _shard : _shards) {
        _shard.evict(charge, evicted);
    }
}

void ShardedLRUContainer::evict_for(const LRUKey& key, size_t charge, std::vector<LRUHandle*>* evicted) {
    const uint32_t hash = _hash_slice(key);
    _shards[_shard(hash)].evict(charge, evicted);
}

size_t ShardedLRUContainer::_get_stat(size_t (LRUContainer::*mem_fun)()) {
    size_t n = 0;
    for (auto& shard : _shards) {
        n += (shard.*mem_fun)();
    }
    return n;
}

void ShardedLRUContainer::prune() {
    int num_prune = 0;
    for (auto& _shard : _shards) {
        num_prune += _shard.prune();
    }
    LOG(INFO) << "Successfully prune container, clean " << num_prune << " entries.";
}

size_t ShardedLRUContainer::get_usage() {
    return _get_stat(&LRUContainer::get_usage);
}

size_t ShardedLRUContainer::get_lookup_count() {
    return _get_stat(&LRUContainer::get_lookup_count);
}

void ShardedLRUContainer::get_status(rapidjson::Document* document) {
    for (uint32_t i = 0; i < _shards.size(); ++i) {
        size_t usage = _shards[i].get_usage();
        rapidjson::Value shard_info(rapidjson::kObjectType);
        shard_info.AddMember("usage", static_cast<double>(usage), document->GetAllocator());

        size_t lookup_count = _shards[i].get_lookup_count();
        shard_info.AddMember("lookup_count", static_cast<double>(lookup_count), document->GetAllocator());

        document->PushBack(shard_info, document->GetAllocator());
    }
}

} // namespace starrocks::starcache

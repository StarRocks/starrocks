// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <shared_mutex>
#include <atomic>
#include "util/phmap/phmap.h"

namespace starrocks {

// TODO: A more effective data structure is needed
template <typename K, typename V>
class ThreadSafeHashMap{
public:
    bool insert(const K& key, const V& value) {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        return _kv.insert(std::make_pair(key, value)) > 0;
    }

    void update(const K& key, const V& value) {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        _kv[key] = value;
    }

    bool find(const K& key, V* value) {
        std::shared_lock<std::shared_mutex> read_lock(_mutex);
        auto iter = _kv.find(key);
        if (iter != _kv.end()) {
            *value = iter->second;
            return true;
        }
        return false;
    }

    bool remove(const K& key) {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        return _kv.erase(key) > 0;
    }

private:
    phmap::flat_hash_map<K, V> _kv;
    std::shared_mutex _mutex;
};

} // namespace starrocks

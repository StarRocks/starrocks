// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <shared_mutex>
#include <atomic>
#include "util/phmap/phmap.h"

namespace starrocks::starcache {

// K: key
// V: value
// N: shard bits of the hash map
template <typename K, typename V, uint32_t N = 5>
class ParallelHashMap {
public:
    bool insert(const K& key, const V& value) {
        return _kv.try_emplace_l(key, [](V& ) {}, value);
    }

    void update(const K& key, const V& value) {
        _kv.try_emplace_l(key,
                [&value](V& val) { val = value; }, value);
    }

    bool find(const K& key, V* value) {
        return _kv.if_contains(key,
                [value](const V& val) { *value = val; });
    }

    bool remove(const K& key) {
        return _kv.erase_if(key,
                [](V& v) { return true; });
    }

private:
    using PMap = phmap::parallel_flat_hash_map<K, V, phmap::priv::hash_default_hash<K>,
                                                    phmap::priv::hash_default_eq<K>,
                                                    std::allocator<std::pair<const K, V>>,
                                                    N, std::shared_mutex>;

    PMap _kv;
};

} // namespace starrocks::starcache

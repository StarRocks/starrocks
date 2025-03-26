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

#pragma once

#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <chrono>
#include <mutex>
#include <optional>
#include <shared_mutex>

namespace starrocks {

template <typename K, typename V>
class FixedFIFOCache {
public:
    typedef K key_type;
    typedef V value_type;

protected:
    struct data_node {
        key_type key;
        value_type value;
        std::chrono::time_point<std::chrono::steady_clock> expire_ts;

        data_node(key_type k, value_type v, size_t expire_ms)
                : key(k),
                  value(v),
                  expire_ts(std::chrono::steady_clock::now() + std::chrono::milliseconds(expire_ms)) {}
    };

    // leverage boost::multi_index_container to build the data structure [key, value, expire_ts], indexed
    // by the key and expire_ts
    typedef boost::multi_index_container<
            data_node,
            boost::multi_index::indexed_by<
                    // order by data_node::expire_ts
                    boost::multi_index::ordered_unique<boost::multi_index::member<
                            data_node, std::chrono::time_point<std::chrono::steady_clock>, &data_node::expire_ts>>,
                    // order by data_node::key
                    boost::multi_index::ordered_unique<
                            boost::multi_index::member<data_node, key_type, &data_node::key>>>>
            data_collection;
    typedef typename data_collection::template nth_index<1>::type data_index_by_key_type;

public:
    explicit FixedFIFOCache(size_t capacity, size_t expire_ms)
            : _capacity(capacity), _default_expire_ms(expire_ms), _key_index(_data.template get<1>()) {}
    ~FixedFIFOCache() = default;

    void put(const key_type& key, const value_type& value) {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        auto iter = _key_index.find(key);
        if (iter != _key_index.end()) {
            _key_index.erase(iter);
        }
        _data.insert(data_node(key, value, _default_expire_ms));
        size_t size = _data.size();
        lock.unlock();
        if (size > _capacity) {
            expire_items();
        }
    }

    std::optional<value_type> get(const key_type& key) {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        auto iter = _key_index.find(key);
        if (iter == _key_index.end()) {
            return std::nullopt;
        }
        if (std::chrono::steady_clock::now() > iter->expire_ts) {
            lock.unlock();
            expire_items();
            return std::nullopt;
        }
        return iter->value;
    }

private:
    void expire_items() {
        auto now_ts = std::chrono::steady_clock::now();
        std::unique_lock<std::shared_mutex> lock(_mutex);
        auto& expire_time_index = _data.template get<1>();
        auto iter = expire_time_index.begin();
        while (iter != expire_time_index.end()) {
            if (iter->expire_ts > now_ts && _data.size() <= _capacity) {
                break;
            } else {
                iter = expire_time_index.erase(iter);
            }
        }
    }

private:
    mutable std::shared_mutex _mutex;
    size_t _capacity;
    size_t _default_expire_ms;
    data_collection _data;
    data_index_by_key_type& _key_index;
};

} // namespace starrocks

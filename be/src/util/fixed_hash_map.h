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

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include "column/column_hash.h"
#include "glog/logging.h"
namespace starrocks {

// FixedSizeHashMap
// Key: KeyType integer type eg: uint8 uint16
// value shouldn't be nullptr

template <typename KeyType, typename ValueType, PhmapSeed seed>
class SmallFixedSizeHashMap {
public:
    static_assert(std::is_integral_v<KeyType>);
    static_assert(std::is_pointer_v<ValueType>);
    static constexpr int hash_table_size = 1 << sizeof(KeyType) * 8;

    using key_type = KeyType;
    using search_key_type = typename std::make_unsigned<KeyType>::type;

    SmallFixedSizeHashMap() {
        memset(_hash_table, 0, sizeof(ValueType) * hash_table_size);
        _hash_table[hash_table_size] = reinterpret_cast<ValueType>(0xFFFF);
    }

    struct PPair {
        using Cell = std::pair<KeyType, ValueType>;
        PPair(KeyType key, ValueType value) : _data(key, value) {}
        Cell _data;
        Cell* operator->() { return &_data; }
    };

    class iterator {
    public:
        iterator(ValueType* hash_table_begin, uint32_t hash_table_key)
                : _hash_table_key(hash_table_key), _hash_table_begin(hash_table_begin) {}

        PPair operator->() const { return {static_cast<KeyType>(_hash_table_key), _hash_table_begin[_hash_table_key]}; }

        iterator& operator++() {
            _hash_table_key++;
            skip_empty_value();
            return *this;
        }

        void skip_empty_value() {
            while (_hash_table_begin[_hash_table_key] == nullptr) {
                ++_hash_table_key;
            }
        }

        friend bool operator==(const iterator& a, const iterator& b) { return a._hash_table_key == b._hash_table_key; }
        friend bool operator!=(const iterator& a, const iterator& b) { return !(a == b); }

    private:
        uint32_t _hash_table_key;
        ValueType* _hash_table_begin;
    };

    template <class F>
    iterator lazy_emplace(KeyType key, F&& f) {
        auto search_key = static_cast<search_key_type>(key);
        if (_hash_table[search_key] == nullptr) {
            _size++;
            f([&](KeyType key, ValueType value) {
                DCHECK(value != nullptr);
                _hash_table[search_key] = value;
            });
        }
        return iterator(_hash_table, search_key);
    }

    iterator find(KeyType key) {
        auto search_key = static_cast<search_key_type>(key);
        if (_hash_table[search_key] == nullptr) {
            return end();
        }
        return iterator(_hash_table, search_key);
    }

    iterator begin() {
        auto iter = iterator(_hash_table, 0);
        iter.skip_empty_value();
        return iter;
    }

    iterator end() { return iterator(_hash_table, hash_table_size); }

    void prefetch_hash(size_t hashval) const { __builtin_prefetch(static_cast<const void*>(_hash_table + hashval)); }

    template <class F>
    iterator lazy_emplace_with_hash(KeyType key, size_t& hashval, F&& f) {
        return lazy_emplace(key, f);
    }

    struct HashFunction {
        size_t operator()(KeyType key) { return static_cast<size_t>(key); }
    };

    HashFunction hash_function() { return HashFunction(); }

    size_t bucket_count() { return hash_table_size; }

    size_t size() { return _size; }

    size_t capacity() { return bucket_count(); }

    size_t dump_bound() { return hash_table_size; }

private:
    size_t _size = 0;
    ValueType _hash_table[hash_table_size + 1];
};

template <typename KeyType, PhmapSeed seed>
class SmallFixedSizeHashSet {
public:
    static_assert(std::is_integral_v<KeyType>);
    static constexpr int hash_table_size = 1 << sizeof(KeyType) * 8;

    using key_type = KeyType;
    using search_key_type = typename std::make_unsigned<KeyType>::type;

    class iterator {
    public:
        iterator(uint8_t* hash_table_begin, uint32_t cursor) : _hash_table_begin(hash_table_begin), _cursor(cursor) {}

        KeyType operator*() const { return static_cast<KeyType>(_cursor); }

        iterator& operator++() {
            _cursor++;
            skip_empty_value();
            return *this;
        }

        void skip_empty_value() {
            while (_hash_table_begin[_cursor] == 0) {
                ++_cursor;
            }
        }

        friend bool operator==(const iterator& a, const iterator& b) { return a._cursor == b._cursor; }
        friend bool operator!=(const iterator& a, const iterator& b) { return !(a == b); }

    private:
        uint8_t* _hash_table_begin;
        int32_t _cursor;
    };

    SmallFixedSizeHashSet() {
        memset(_hash_table, 0, sizeof(uint8_t) * hash_table_size);
        _hash_table[hash_table_size] = 0xFF;
    }

    iterator begin() {
        auto iter = iterator(_hash_table, 0);
        iter.skip_empty_value();
        return iter;
    }

    iterator end() { return iterator(_hash_table, hash_table_size); }

    void emplace(KeyType key) {
        _size += _hash_table[static_cast<search_key_type>(key)] == 0;
        _hash_table[static_cast<search_key_type>(key)] = 1;
    }

    bool contains(KeyType key) { return _hash_table[static_cast<search_key_type>(key)]; }

    size_t dump_bound() { return hash_table_size; }

    size_t size() { return _size; }

    size_t capacity() { return hash_table_size; }

private:
    size_t _size = 0;
    uint8_t _hash_table[hash_table_size + 1];
};

} // namespace starrocks

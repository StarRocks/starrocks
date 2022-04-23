// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include "glog/logging.h"
namespace starrocks::vectorized {

// FixedSizeHashMap
// Key: KeyType integer type eg: uint8 uint16
// value shouldn't be nullptr
template <typename KeyType, typename ValueType>
class SmallFixedSizeHashMap {
public:
    static_assert(std::is_integral_v<KeyType>);
    static_assert(std::is_pointer_v<ValueType>);
    static constexpr int hash_table_size = 1 << sizeof(KeyType) * 8;
    class iterator;

    using key_type = KeyType;
    using search_key_type = typename std::make_unsigned<KeyType>::type;
    using iterator = iterator;

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
        if (_hash_table[search_key] != nullptr) {
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

    void prefetch_hash(size_t hashval) const {
        CHECK(false) << "unreachable path in fixed hash map";
        __builtin_unreachable();
    }

    template <class F>
    iterator lazy_emplace_with_hash(KeyType key, size_t& hashval, F&& f) {
        CHECK(false) << "unreachable path in fixed hash map";
        __builtin_unreachable();
    }

    std::hash<KeyType> hash_function() { return std::hash<KeyType>(); }

    size_t bucket_count() { return hash_table_size; }

    size_t size() { return _size; }

    size_t capacity() { return bucket_count(); }

    size_t dump_bound() { return hash_table_size; }

private:
    size_t _size = 0;
    ValueType _hash_table[hash_table_size + 1];
};

} // namespace starrocks::vectorized
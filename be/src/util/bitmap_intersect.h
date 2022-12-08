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

#include "exprs/anyval_util.h"
#include "runtime/datetime_value.h"
#include "runtime/string_value.h"
#include "types/bitmap_value.h"

namespace starrocks {

namespace detail {

const int DATETIME_PACKED_TIME_BYTE_SIZE = 8;
const int DATETIME_TYPE_BYTE_SIZE = 4;

const int DECIMAL_BYTE_SIZE = 16;

// serialize_size start
template <typename T>
int32_t serialize_size(const T& v) {
    return sizeof(T);
}

template <>
inline int32_t serialize_size(const std::string& v) {
    return v.size() + sizeof(uint32_t);
}

template <>
inline int32_t serialize_size(const DateTimeValue& v) {
    return DATETIME_PACKED_TIME_BYTE_SIZE + DATETIME_TYPE_BYTE_SIZE;
}

template <>
inline int32_t serialize_size(const DecimalV2Value& v) {
    return DECIMAL_BYTE_SIZE;
}

template <>
inline int32_t serialize_size(const StringValue& v) {
    return v.len + 4;
}
// serialize_size end

// write_to start
template <typename T>
char* write_to(const T& v, char* dest) {
    size_t type_size = sizeof(T);
    memcpy(dest, &v, type_size);
    dest += type_size;
    return dest;
}

template <>
inline char* write_to(const std::string& v, char* dest) {
    *(uint32_t*)dest = v.size();
    dest += sizeof(uint32_t);
    memcpy(dest, v.c_str(), v.size());
    dest += v.size();
    return dest;
}

// write_to end

// read_from start
template <typename T>
void read_from(const char** src, T* result) {
    size_t type_size = sizeof(T);
    memcpy(result, *src, type_size);
    *src += type_size;
}

template <>
inline void read_from(const char** src, std::string* result) {
    uint32_t length = *(uint32_t*)(*src);
    *src += sizeof(uint32_t);
    *result = std::string((char*)*src, length);
    *src += length;
}
// read_from end

} // namespace detail

// Calculate the intersection of two or more bitmaps
// Usage: intersect_count(bitmap_column_to_count, filter_column, filter_values ...)
// Example: intersect_count(user_id, event, 'A', 'B', 'C'), meaning find the intersect count of user_id in all A/B/C 3 bitmaps
// Todo(kks) Use Array type instead of variable arguments
template <typename T>
struct BitmapIntersect {
public:
    BitmapIntersect() = default;

    explicit BitmapIntersect(const char* src) { deserialize(src); }

    void add_key(const T key) {
        BitmapValue empty_bitmap;
        _bitmaps[key] = empty_bitmap;
    }

    void update(const T& key, const BitmapValue& bitmap) {
        if (_bitmaps.find(key) != _bitmaps.end()) {
            _bitmaps[key] |= bitmap;
        }
    }

    void merge(const BitmapIntersect& other) {
        for (auto& kv : other._bitmaps) {
            if (_bitmaps.find(kv.first) != _bitmaps.end()) {
                _bitmaps[kv.first] |= kv.second;
            } else {
                _bitmaps[kv.first] = kv.second;
            }
        }
    }

    // calculate the intersection for _bitmaps's bitmap values
    int64_t intersect_count() const {
        if (_bitmaps.empty()) {
            return 0;
        }

        BitmapValue result;
        auto it = _bitmaps.begin();
        result |= it->second;
        it++;
        for (; it != _bitmaps.end(); it++) {
            result &= it->second;
        }

        return result.cardinality();
    }

    // the serialize size
    size_t size() {
        size_t size = 4;
        for (auto& kv : _bitmaps) {
            size += detail::serialize_size(kv.first);
            size += kv.second.getSizeInBytes();
        }
        return size;
    }

    //must call size() first
    void serialize(char* dest) {
        char* writer = dest;
        *(int32_t*)writer = _bitmaps.size();
        writer += 4;
        for (auto& kv : _bitmaps) {
            writer = detail::write_to(kv.first, writer);
            kv.second.write(writer);
            writer += kv.second.getSizeInBytes();
        }
    }

    void deserialize(const char* src) {
        const char* reader = src;
        int32_t bitmaps_size = *(int32_t*)reader;
        reader += 4;
        for (int32_t i = 0; i < bitmaps_size; i++) {
            T key;
            detail::read_from(&reader, &key);
            BitmapValue bitmap(reader);
            reader += bitmap.getSizeInBytes();
            _bitmaps[key] = bitmap;
        }
    }

private:
    std::map<T, BitmapValue> _bitmaps;
};

} // namespace starrocks

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

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gutil/endian.h"

namespace starrocks {

// Utility functions for encoding various data types with order-preserving encoding
namespace encoding_utils {

// Endian conversion utilities
template <class UT>
inline UT to_bigendian(UT v) {
    return v;
}

template <>
inline uint8_t to_bigendian(uint8_t v) {
    return v;
}

template <>
inline uint16_t to_bigendian(uint16_t v) {
    return BigEndian::FromHost16(v);
}

template <>
inline uint32_t to_bigendian(uint32_t v) {
    return BigEndian::FromHost32(v);
}

template <>
inline uint64_t to_bigendian(uint64_t v) {
    return BigEndian::FromHost64(v);
}

template <>
inline uint128_t to_bigendian(uint128_t v) {
    return BigEndian::FromHost128(v);
}

// Integral encoding with order-preserving transformation
template <class T>
inline void encode_integral(const T& v, std::string* dest) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = v;
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        uv = to_bigendian(uv);
        dest->append(reinterpret_cast<const char*>(&uv), sizeof(uv));
    } else {
        T nv = to_bigendian(v);
        dest->append(reinterpret_cast<const char*>(&nv), sizeof(nv));
    }
}

// Integral decoding with order-preserving transformation
template <class T>
inline void decode_integral(Slice* src, T* v) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = *(UT*)(src->data);
        uv = to_bigendian(uv);
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        *v = uv;
    } else {
        T nv = *(T*)(src->data);
        *v = to_bigendian(nv);
    }
    src->remove_prefix(sizeof(T));
}

// Slice encoding with SSE optimizations for middle fields
void encode_slice(const Slice& s, std::string* dst, bool is_last);

} // namespace encoding_utils

// Utility methods to encode composite primary key into single binary key, while
// preserving original sort order.
// Currently only bool, integral types(tinyint, smallint, int, bigint, largeint)
// and varchar is supported.
// The encoding method is borrowed from kudu, e.g.
// For a composite key (k1, k2, k3), each key column is encoded and then append
// to the output binary key. Encode method for each type:
// bool, tinyint: append single byte
// smallint, int, bigint, largeint: convert to bigendian and append
// varchar:
//   if this column is the last column: append directly
//   if not: convert each 0x00 inside the string to 0x00 0x01,
//           add a tailing 0x00 0x00, then append
class PrimaryKeyEncoder {
public:
    static bool is_supported(const Field& f);

    static bool is_supported(const Schema& schema, const std::vector<ColumnId>& key_idxes);

    // Return |TYPE_NONE| if no primary key contained in |schema|.
    static LogicalType encoded_primary_key_type(const Schema& schema, const std::vector<ColumnId>& key_idxes);

    // Return -1 if encoded key is not fixed size
    static size_t get_encoded_fixed_size(const Schema& schema);

    // create suitable column to hold encoded key
    //   schema: schema of the table
    //   pcolumn: output column
    //   large_column: some usage may fill the column with more than uint32_max elements, set true to support this
    static Status create_column(const Schema& schema, MutableColumnPtr* pcolumn, bool large_column = false);

    // create suitable column to hold encoded key
    //   schema: schema of the table
    //   pcolumn: output column
    //   key_idxes: indexes of columns for encoding
    //   large_column: some usage may fill the column with more than uint32_max elements, set true to support this
    static Status create_column(const Schema& schema, MutableColumnPtr* pcolumn, const std::vector<ColumnId>& key_idxes,
                                bool large_column = false);

    static void encode(const Schema& schema, const Chunk& chunk, size_t offset, size_t len, Column* dest);

    static Status encode_sort_key(const Schema& schema, const Chunk& chunk, size_t offset, size_t len, Column* dest);

    static void encode_selective(const Schema& schema, const Chunk& chunk, const uint32_t* indexes, size_t len,
                                 Column* dest);

    static bool encode_exceed_limit(const Schema& schema, const Chunk& chunk, size_t offset, size_t len,
                                    size_t limit_size);

    static Status decode(const Schema& schema, const Column& keys, size_t offset, size_t len, Chunk* dest,
                         std::vector<uint8_t>* value_encode_flags = nullptr);
};

} // namespace starrocks

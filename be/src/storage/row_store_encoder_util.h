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
#include "gutil/stringprintf.h"
#include "types/bitmap_value.h"

namespace starrocks {
//declare and defines of template class must be in a file, so we exctract it here

template <class UT>
inline UT to_bigendian(UT v);

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

template <class T>
inline size_t encode_integral(const T& v, std::string* dest) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = v;
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        uv = to_bigendian(uv);
        dest->append(reinterpret_cast<const char*>(&uv), sizeof(uv));
        return sizeof(uv);
    } else {
        T nv = to_bigendian(v);
        dest->append(reinterpret_cast<const char*>(&nv), sizeof(nv));
        return sizeof(nv);
    }
}

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

} // namespace starrocks
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

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "column/vectorized_fwd.h"
#include "storage/uint24.h"

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#include <xmmintrin.h>
#endif

#include "column/type_traits.h"
#include "types/logical_type.h"
#include "util/hash.h"
#include "util/slice.h"

namespace starrocks {

#if defined(__SSE2__) && !defined(ADDRESS_SANITIZER)

// NOTE: This function will access 15 excessive bytes after p1 and p2, which should has padding bytes when allocating
// memory. if withoud pad, please use memequal.
// NOTE: typename T must be uint8_t or int8_t
template <typename T>
typename std::enable_if<sizeof(T) == 1, bool>::type memequal_padded(const T* p1, size_t size1, const T* p2,
                                                                    size_t size2) {
    if (size1 != size2) {
        return false;
    }
    for (size_t offset = 0; offset < size1; offset += 16) {
        uint16_t mask =
                _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1 + offset)),
                                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2 + offset))));
        mask = ~mask;
        if (mask) {
            offset += __builtin_ctz(mask);
            return offset >= size1;
        }
    }
    return true;
}
#else

template <typename T>
typename std::enable_if<sizeof(T) == 1, bool>::type memequal_padded(const T* p1, size_t size1, const T* p2,
                                                                    size_t size2) {
    return (size1 == size2) && (memcmp(p1, p2, size1) == 0);
}
#endif

static constexpr uint16_t SLICE_MEMEQUAL_OVERFLOW_PADDING = 15;
class SliceEqual {
public:
    bool operator()(const Slice& x, const Slice& y) const { return memequal_padded(x.data, x.size, y.data, y.size); }
};

class SliceNormalEqual {
public:
    bool operator()(const Slice& x, const Slice& y) const {
        return (x.size == y.size) && (memcmp(x.data, y.data, x.size) == 0);
    }
};

inline uint64_t crc_hash_uint64(uint64_t value, uint64_t seed) {
#if defined(__x86_64__) && defined(__SSE4_2__)
    return _mm_crc32_u64(seed, value);
#elif defined(__x86_64__)
    return crc32(seed, (const unsigned char*)&value, sizeof(uint64_t));
#elif defined(__aarch64__)
    return __crc32cd(seed, value);
#else
#error "Not supported architecture"
#endif
}

inline uint64_t crc_hash_uint128(uint64_t value0, uint64_t value1, uint64_t seed) {
#if defined(__x86_64__) && defined(__SSE4_2__)
    uint64_t hash = _mm_crc32_u64(seed, value0);
    hash = _mm_crc32_u64(hash, value1);
#elif defined(__x86_64__)
    uint64_t hash = crc32(seed, (const unsigned char*)&value0, sizeof(uint64_t));
    hash = crc32(hash, (const unsigned char*)&value1, sizeof(uint64_t));
#elif defined(__aarch64__)
    uint64_t hash = __crc32cd(seed, value0);
    hash = __crc32cd(hash, value1);
#else
#error "Not supported architecture"
#endif
    return hash;
}

// https://github.com/HowardHinnant/hash_append/issues/7
template <typename T>
inline void hash_combine(uint64_t& seed, const T& val) {
    seed ^= std::hash<T>{}(val) + 0x9e3779b97f4a7c15LLU + (seed << 12) + (seed >> 4);
}

inline uint64_t hash_128(uint64_t seed, int128_t val) {
    auto low = static_cast<size_t>(val);
    auto high = static_cast<size_t>(val >> 64);
    hash_combine(seed, low);
    hash_combine(seed, high);
    return seed;
}

template <PhmapSeed seed>
struct Hash128WithSeed {
    std::size_t operator()(int128_t value) const {
        return phmap_mix_with_seed<sizeof(size_t), seed>()(hash_128(seed, value));
    }
};
template <typename T>
struct HashTypeTraits {
    using HashFunc = StdHashWithSeed<T, PhmapSeed2>;
};
template <>
struct HashTypeTraits<int128_t> {
    using HashFunc = Hash128WithSeed<PhmapSeed2>;
};

template <LogicalType LT, PhmapSeed seed>
struct PhmapDefaultHashFunc {
    std::size_t operator()(const RunTimeCppType<LT>& value) const {
        static_assert(is_supported(), "unsupported logical type");

        if constexpr (lt_is_largeint<LT> || lt_is_decimal128<LT>) {
            return Hash128WithSeed<seed>()(value);
        } else if constexpr (lt_is_fixedlength<LT>) {
            return StdHashWithSeed<RunTimeCppType<LT>, seed>()(value);
        } else if constexpr (lt_is_string<LT> || lt_is_binary<LT>) {
            return SliceHashWithSeed<seed>()(value);
        } else {
            assert(false);
        }
    }

    constexpr static bool is_supported() {
        return lt_is_largeint<LT> || lt_is_decimal128<LT> || lt_is_fixedlength<LT> || lt_is_string<LT> ||
               lt_is_binary<LT>;
    }
};

} // namespace starrocks

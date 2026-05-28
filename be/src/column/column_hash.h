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
#include <cstring>
#include <type_traits>

#include "base/types/int256.h"
#include "base/types/uint24.h"
#include "column/vectorized_fwd.h"

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#include <xmmintrin.h>
#endif

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "base/hash/hash.h"
#include "base/string/slice.h"
#include "column/runtime_type_traits.h"
#include "types/logical_type.h"

namespace starrocks {

#if defined(__AVX2__) && !defined(ADDRESS_SANITIZER)

// AVX2 version: 32 bytes per iteration with early-exit on the first mismatching
// lane. Beats the SSE2 16-byte loop on keys 32..255 bytes (where the wider
// movemask catches the mismatch one iteration sooner); above 256 bytes libc
// memcmp wins (ERMS / REP MOVSB on x86_64), so we short-circuit there.
// Short keys (< 32) skip the AVX2 prologue and use a single SSE2 iteration --
// the AVX2 main loop never executes on them, and the extra branching slows
// down what is already a one-load comparison.
// NOTE: This function reads up to 15 padding bytes past p1/p2 -- every load that
//       can run off the end is a 16-byte SSE2 load (the 32-byte AVX2 loads stay
//       in bounds via `offset + 32 <= size1`). Callers must allocate that
//       trailing slack (see SLICE_MEMEQUAL_OVERFLOW_PADDING).
// NOTE: typename T must be uint8_t or int8_t.
template <typename T>
typename std::enable_if<sizeof(T) == 1, bool>::type memequal_padded(const T* p1, size_t size1, const T* p2,
                                                                    size_t size2) {
    if (size1 != size2) {
        return false;
    }
    // Short-key guard: keys under 32 bytes never enter the AVX2 main loop
    // (its `offset + 32 <= size1` condition is false) -- they would fall
    // straight into the 16-byte tail with its overlap-anchor logic, which is
    // ~1.5x slower on small inputs than a plain SSE2 iteration. So for them
    // we run a plain SSE2 loop. This branch costs ~0.3 ns per call on the
    // mismatch path; the Equal path doesn't pay it because the CPU
    // speculates straight through.
    if (size1 < 32) {
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
    constexpr size_t kMemcmpThreshold = 256;
    if (size1 >= kMemcmpThreshold) {
        return std::memcmp(p1, p2, size1) == 0;
    }
    size_t offset = 0;
    for (; offset + 32 <= size1; offset += 32) {
        __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p1 + offset));
        __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p2 + offset));
        uint32_t mask = ~static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v2)));
        if (mask) {
            return false;
        }
    }
    // Tail: handle up to 31 remaining bytes via one 16-byte SSE2 load and, if
    // more than 16 bytes remain, an overlapping 16-byte load anchored at the
    // end of the buffer.
    if (offset < size1) {
        __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1 + offset));
        __m128i v2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2 + offset));
        uint16_t mask = ~static_cast<uint16_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(v1, v2)));
        if (mask) {
            offset += __builtin_ctz(mask);
            return offset >= size1;
        }
        if (size1 - offset > 16) {
            __m128i v1_tail = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1 + size1 - 16));
            __m128i v2_tail = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2 + size1 - 16));
            uint16_t mask_tail = ~static_cast<uint16_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(v1_tail, v2_tail)));
            if (mask_tail) {
                return false;
            }
        }
    }
    return true;
}

#elif defined(__SSE2__) && !defined(ADDRESS_SANITIZER)

// SSE2 version: 16 bytes per iteration.
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

// Both SIMD memequal_padded paths overrun the buffer by at most 15 bytes: every
// load that can read past the end is a 16-byte SSE2 load (the AVX2 path's
// 32-byte loads are bounded by `offset + 32 <= size1`). Callers must reserve
// this many trailing bytes.
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

inline uint64_t hash_256(uint64_t seed, const int256_t& val) {
    uint64_t parts[4];
    parts[0] = static_cast<uint64_t>(val.low);
    parts[1] = static_cast<uint64_t>(val.low >> 64);
    parts[2] = static_cast<uint64_t>(static_cast<uint128_t>(val.high));
    parts[3] = static_cast<uint64_t>(static_cast<uint128_t>(val.high) >> 64);

    uint64_t hash = seed;
    for (int i = 0; i < 4; ++i) {
        hash_combine(hash, parts[i]);
    }
    return hash;
}

template <PhmapSeed seed>
struct Hash128WithSeed {
    std::size_t operator()(int128_t value) const {
        return phmap_mix_with_seed<sizeof(size_t), seed>()(hash_128(seed, value));
    }
};

template <PhmapSeed seed>
struct Hash256WithSeed {
    std::size_t operator()(const int256_t& value) const {
        return phmap_mix_with_seed<sizeof(size_t), seed>()(hash_256(seed, value));
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

template <>
struct HashTypeTraits<int256_t> {
    using HashFunc = Hash256WithSeed<PhmapSeed2>;
};

template <LogicalType LT, PhmapSeed seed>
struct PhmapDefaultHashFunc {
    std::size_t operator()(const RunTimeCppType<LT>& value) const {
        static_assert(is_supported(), "unsupported logical type");

        if constexpr (lt_is_largeint<LT> || lt_is_decimal128<LT>) {
            return Hash128WithSeed<seed>()(value);
        } else if constexpr (lt_is_decimal256<LT>) {
            return Hash256WithSeed<seed>()(value);
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

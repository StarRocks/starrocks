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

#include <algorithm>
#include <atomic>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "base/container/raw_container.h"
#include "base/simd/multi_version.h"
#include "base/testutil/sync_point.h"
#include "column/sorting/sort_permute.h"
#include "common/logging.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

template <typename T, typename Enable = void>
struct RadixSortTraits {
    static constexpr bool supported = false;
};

template <typename T>
struct RadixSortTraits<T, std::enable_if_t<std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                                           std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>>> {
    static constexpr bool supported = true;
    using UnsignedKey = T;
    static constexpr size_t KEY_BYTES = sizeof(T);

    static inline UnsignedKey to_radix_key(T v, bool asc) noexcept { return asc ? v : static_cast<UnsignedKey>(~v); }
};

template <typename T>
struct RadixSortTraits<T, std::enable_if_t<std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                                           std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>>> {
    static constexpr bool supported = true;
    using UnsignedKey = std::make_unsigned_t<T>;
    static constexpr size_t KEY_BYTES = sizeof(T);
    static constexpr UnsignedKey SIGN_MASK = static_cast<UnsignedKey>(UnsignedKey(1) << (sizeof(T) * 8 - 1));

    static inline UnsignedKey to_radix_key(T v, bool asc) noexcept {
        UnsignedKey u = static_cast<UnsignedKey>(v) ^ SIGN_MASK;
        return asc ? u : static_cast<UnsignedKey>(~u);
    }
};

template <typename T>
struct RadixSortTraits<T, std::enable_if_t<std::is_same_v<T, float> || std::is_same_v<T, double>>> {
    static constexpr bool supported = true;
    using UnsignedKey = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;
    static constexpr size_t KEY_BYTES = sizeof(T);
    static constexpr UnsignedKey SIGN_MASK = static_cast<UnsignedKey>(UnsignedKey(1) << (sizeof(T) * 8 - 1));

    static inline UnsignedKey to_radix_key(T v, bool asc) noexcept {
        UnsignedKey bits = 0;
        if (!std::isnan(v)) {
            std::memcpy(&bits, &v, sizeof(T));
            // Canonicalize -0.0 to +0.0 (and NaN is already canonicalized to 0 above)
            // to match SorterComparator<FloatingPoint>::compare exactly.
            if (bits == SIGN_MASK) {
                bits = 0;
            }
        }
        UnsignedKey mask = (bits & SIGN_MASK) ? ~UnsignedKey(0) : SIGN_MASK;
        UnsignedKey u = bits ^ mask;
        return asc ? u : static_cast<UnsignedKey>(~u);
    }
};

template <>
struct RadixSortTraits<DateValue, void> {
    static constexpr bool supported = true;
    using UnsignedKey = uint32_t;
    static constexpr size_t KEY_BYTES = sizeof(uint32_t);

    static inline UnsignedKey to_radix_key(const DateValue& v, bool asc) noexcept {
        uint32_t u = static_cast<uint32_t>(v.julian()) ^ 0x80000000U;
        return asc ? u : ~u;
    }
};

template <>
struct RadixSortTraits<TimestampValue, void> {
    static constexpr bool supported = true;
    using UnsignedKey = uint64_t;
    static constexpr size_t KEY_BYTES = sizeof(uint64_t);

    static inline UnsignedKey to_radix_key(const TimestampValue& v, bool asc) noexcept {
        uint64_t u = static_cast<uint64_t>(v.timestamp()) ^ 0x8000000000000000ULL;
        return asc ? u : ~u;
    }
};

template <typename T>
concept RadixSortable = RadixSortTraits<T>::supported;

namespace detail {

inline bool is_cancelled(const bool& cancel) noexcept {
    return cancel;
}

inline bool is_cancelled(const std::atomic<bool>& cancel) noexcept {
    return cancel.load(std::memory_order_relaxed);
}

template <typename CancelType>
concept CancelFlag = std::is_same_v<std::remove_cvref_t<CancelType>, bool> ||
                     std::is_same_v<std::remove_cvref_t<CancelType>, std::atomic<bool>>;

template <typename UnsignedKey>
struct
#if defined(__GNUC__) || defined(__clang__)
        __attribute__((__may_alias__))
#endif
        RadixPermuteItem {
    UnsignedKey inline_value;
    uint32_t index_in_chunk;
};

template <RadixSortable T, CancelFlag CancelT>
void radix_sort_core(const CancelT& cancel, InlinePermuteItem<T>* items, InlinePermuteItem<T>* scratch_ptr,
                     InlinePermutation<T>* scratch_vec, size_t n, bool asc) {
    using Traits = RadixSortTraits<T>;
    using UnsignedKey = typename Traits::UnsignedKey;
    using UItem = RadixPermuteItem<UnsignedKey>;

    static_assert(sizeof(InlinePermuteItem<T>) == sizeof(UItem));
    static_assert(alignof(InlinePermuteItem<T>) == alignof(UItem));
    static_assert(offsetof(InlinePermuteItem<T>, inline_value) == offsetof(UItem, inline_value));
    static_assert(offsetof(InlinePermuteItem<T>, index_in_chunk) == offsetof(UItem, index_in_chunk));

    DCHECK_LE(n, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));

    if (n == 0 || is_cancelled(cancel)) {
        return;
    }

    auto* u_items = reinterpret_cast<UItem*>(items);
    auto* u_scratch = reinterpret_cast<UItem*>(scratch_ptr);

    auto load_and_transform_key = [](const UItem& item, bool asc_order) noexcept -> UnsignedKey {
        T raw_val;
        std::memcpy(&raw_val, &item.inline_value, sizeof(T));
        return Traits::to_radix_key(raw_val, asc_order);
    };

    if (n == 1) {
        u_items[0].inline_value = load_and_transform_key(u_items[0], asc);
        return;
    }

    UnsignedKey prev_key = load_and_transform_key(u_items[0], asc);
    u_items[0].inline_value = prev_key;

    bool is_sorted = true;
    bool is_reverse_sorted = true;
    bool has_equal_adj = false;
    size_t monotonic_end = n;

    for (size_t i = 1; i < n; ++i) {
        UnsignedKey key = load_and_transform_key(u_items[i], asc);
        u_items[i].inline_value = key;
        if (prev_key > key) {
            is_sorted = false;
        } else if (prev_key < key) {
            is_reverse_sorted = false;
        } else {
            has_equal_adj = true;
        }
        if (!is_sorted && !is_reverse_sorted) {
            monotonic_end = i + 1;
            break;
        }
        prev_key = key;
    }

    if (is_sorted) {
        return;
    }
    if (is_reverse_sorted) {
        std::reverse(u_items, u_items + n);
        if (has_equal_adj) {
            size_t run_start = 0;
            while (run_start < n) {
                size_t run_end = run_start + 1;
                const UnsignedKey run_key = u_items[run_start].inline_value;
                while (run_end < n && u_items[run_end].inline_value == run_key) {
                    ++run_end;
                }
                if (run_end - run_start > 1) {
                    std::reverse(u_items + run_start, u_items + run_end);
                }
                run_start = run_end;
            }
        }
        return;
    }

    constexpr size_t num_passes = sizeof(UnsignedKey);
    uint32_t histograms[num_passes][256] = {};

    auto record_histogram = [&](UnsignedKey key) noexcept {
        if constexpr (num_passes == 1) {
            histograms[0][key & 0xFFU]++;
        } else if constexpr (num_passes == 2) {
            histograms[0][key & 0xFFU]++;
            histograms[1][(key >> 8) & 0xFFU]++;
        } else if constexpr (num_passes == 4) {
            histograms[0][key & 0xFFU]++;
            histograms[1][(key >> 8) & 0xFFU]++;
            histograms[2][(key >> 16) & 0xFFU]++;
            histograms[3][(key >> 24) & 0xFFU]++;
        } else if constexpr (num_passes == 8) {
            histograms[0][key & 0xFFU]++;
            histograms[1][(key >> 8) & 0xFFU]++;
            histograms[2][(key >> 16) & 0xFFU]++;
            histograms[3][(key >> 24) & 0xFFU]++;
            histograms[4][(key >> 32) & 0xFFU]++;
            histograms[5][(key >> 40) & 0xFFU]++;
            histograms[6][(key >> 48) & 0xFFU]++;
            histograms[7][(key >> 56) & 0xFFU]++;
        }
    };

    for (size_t i = 0; i < monotonic_end; ++i) {
        record_histogram(u_items[i].inline_value);
    }
    for (size_t i = monotonic_end; i < n; ++i) {
        UnsignedKey key = load_and_transform_key(u_items[i], asc);
        u_items[i].inline_value = key;
        record_histogram(key);
    }

    const UnsignedKey first_key = u_items[0].inline_value;
    UItem* src = u_items;
    UItem* dst = u_scratch;

    for (size_t b = 0; b < num_passes; ++b) {
        const size_t shift = b * 8;
        const uint8_t first_byte = static_cast<uint8_t>((first_key >> shift) & 0xFFU);
        if (histograms[b][first_byte] == n) {
            continue;
        }
        if (is_cancelled(cancel)) {
            break;
        }

        if (u_scratch == nullptr) {
            DCHECK(scratch_vec != nullptr);
            if (scratch_vec->size() < n) {
                raw::stl_vector_resize_uninitialized(scratch_vec, n);
            }
            u_scratch = reinterpret_cast<UItem*>(scratch_vec->data());
            dst = u_scratch;
        }

        uint32_t offsets[256];
        uint32_t sum = 0;
        for (size_t k = 0; k < 256; ++k) {
            uint32_t count = histograms[b][k];
            offsets[k] = sum;
            sum += count;
        }

        const UItem* __restrict__ s = src;
        UItem* __restrict__ d = dst;
        size_t i = 0;
        for (; i + 16 < n; i += 4) {
            __builtin_prefetch(s + i + 16, 0, 1);
            const UItem v0 = s[i];
            const UItem v1 = s[i + 1];
            const UItem v2 = s[i + 2];
            const UItem v3 = s[i + 3];
            uint8_t b0 = static_cast<uint8_t>((v0.inline_value >> shift) & 0xFFU);
            uint8_t b1 = static_cast<uint8_t>((v1.inline_value >> shift) & 0xFFU);
            uint8_t b2 = static_cast<uint8_t>((v2.inline_value >> shift) & 0xFFU);
            uint8_t b3 = static_cast<uint8_t>((v3.inline_value >> shift) & 0xFFU);
            d[offsets[b0]++] = v0;
            d[offsets[b1]++] = v1;
            d[offsets[b2]++] = v2;
            d[offsets[b3]++] = v3;
        }
        for (; i + 4 <= n; i += 4) {
            const UItem v0 = s[i];
            const UItem v1 = s[i + 1];
            const UItem v2 = s[i + 2];
            const UItem v3 = s[i + 3];
            uint8_t b0 = static_cast<uint8_t>((v0.inline_value >> shift) & 0xFFU);
            uint8_t b1 = static_cast<uint8_t>((v1.inline_value >> shift) & 0xFFU);
            uint8_t b2 = static_cast<uint8_t>((v2.inline_value >> shift) & 0xFFU);
            uint8_t b3 = static_cast<uint8_t>((v3.inline_value >> shift) & 0xFFU);
            d[offsets[b0]++] = v0;
            d[offsets[b1]++] = v1;
            d[offsets[b2]++] = v2;
            d[offsets[b3]++] = v3;
        }
        for (; i < n; ++i) {
            const UItem v0 = s[i];
            uint8_t b0 = static_cast<uint8_t>((v0.inline_value >> shift) & 0xFFU);
            d[offsets[b0]++] = v0;
        }

        std::swap(src, dst);
        TEST_SYNC_POINT("radix_sort_core::after_scatter_pass");
    }

    if (src != u_items) {
        std::memcpy(u_items, u_scratch, n * sizeof(UItem));
    }
}

template <typename UnsignedKey>
inline void detect_ties_scalar(const RadixPermuteItem<UnsignedKey>* u_sub, size_t first, size_t start_j, size_t n,
                               Tie& tie, bool& has_ties) {
    for (size_t j = start_j; j < n; ++j) {
        uint8_t eq = static_cast<uint8_t>(tie[first + j] != 0) &
                     static_cast<uint8_t>(u_sub[j - 1].inline_value == u_sub[j].inline_value);
        tie[first + j] = eq;
        has_ties |= (eq != 0);
    }
}

#if defined(__GNUC__) && defined(__x86_64__)
inline bool cpu_supports_avx512bw() noexcept {
    static const bool supported = __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512bw");
    return supported;
}

inline bool cpu_supports_avx2() noexcept {
    static const bool supported = __builtin_cpu_supports("avx2");
    return supported;
}

MFV_AVX512BW(size_t detect_ties_avx512_u32(const RadixPermuteItem<uint32_t>* u_sub, size_t first, size_t j, size_t n,
                                           Tie& tie, bool& has_ties) {
    static_assert(sizeof(RadixPermuteItem<uint32_t>) == 8);
    static_assert(offsetof(RadixPermuteItem<uint32_t>, inline_value) == 0);
    for (; j + 8 <= n; j += 8) {
        __m512i vprev = _mm512_loadu_si512(reinterpret_cast<const void*>(&u_sub[j - 1]));
        __m512i vcurr = _mm512_loadu_si512(reinterpret_cast<const void*>(&u_sub[j]));
        __mmask16 cmp_mask = _mm512_cmpeq_epi32_mask(vprev, vcurr);
        for (size_t k = 0; k < 8; ++k) {
            uint8_t eq = static_cast<uint8_t>(((cmp_mask >> (2 * k)) & 1U) & (tie[first + j + k] != 0));
            tie[first + j + k] = eq;
            has_ties |= (eq != 0);
        }
    }
    return j;
})

MFV_AVX512BW(size_t detect_ties_avx512_u64(const RadixPermuteItem<uint64_t>* u_sub, size_t first, size_t j, size_t n,
                                           Tie& tie, bool& has_ties) {
    static_assert(sizeof(RadixPermuteItem<uint64_t>) == 16);
    static_assert(offsetof(RadixPermuteItem<uint64_t>, inline_value) == 0);
    for (; j + 4 <= n; j += 4) {
        __m512i vprev = _mm512_loadu_si512(reinterpret_cast<const void*>(&u_sub[j - 1]));
        __m512i vcurr = _mm512_loadu_si512(reinterpret_cast<const void*>(&u_sub[j]));
        __mmask8 cmp_mask = _mm512_cmpeq_epi64_mask(vprev, vcurr);
        uint8_t eq0 = static_cast<uint8_t>(((cmp_mask >> 0) & 1U) & (tie[first + j] != 0));
        uint8_t eq1 = static_cast<uint8_t>(((cmp_mask >> 2) & 1U) & (tie[first + j + 1] != 0));
        uint8_t eq2 = static_cast<uint8_t>(((cmp_mask >> 4) & 1U) & (tie[first + j + 2] != 0));
        uint8_t eq3 = static_cast<uint8_t>(((cmp_mask >> 6) & 1U) & (tie[first + j + 3] != 0));
        tie[first + j] = eq0;
        tie[first + j + 1] = eq1;
        tie[first + j + 2] = eq2;
        tie[first + j + 3] = eq3;
        has_ties |= ((eq0 | eq1 | eq2 | eq3) != 0);
    }
    return j;
})

MFV_AVX2(size_t detect_ties_avx2_u32(const RadixPermuteItem<uint32_t>* u_sub, size_t first, size_t j, size_t n,
                                     Tie& tie, bool& has_ties) {
    static_assert(sizeof(RadixPermuteItem<uint32_t>) == 8);
    static_assert(offsetof(RadixPermuteItem<uint32_t>, inline_value) == 0);
    for (; j + 4 <= n; j += 4) {
        __m256i vprev = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&u_sub[j - 1]));
        __m256i vcurr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&u_sub[j]));
        __m256i cmp = _mm256_cmpeq_epi32(vprev, vcurr);
        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));
        uint8_t eq0 = static_cast<uint8_t>(((mask >> 0) & 1) & (tie[first + j] != 0));
        uint8_t eq1 = static_cast<uint8_t>(((mask >> 2) & 1) & (tie[first + j + 1] != 0));
        uint8_t eq2 = static_cast<uint8_t>(((mask >> 4) & 1) & (tie[first + j + 2] != 0));
        uint8_t eq3 = static_cast<uint8_t>(((mask >> 6) & 1) & (tie[first + j + 3] != 0));
        tie[first + j] = eq0;
        tie[first + j + 1] = eq1;
        tie[first + j + 2] = eq2;
        tie[first + j + 3] = eq3;
        has_ties |= ((eq0 | eq1 | eq2 | eq3) != 0);
    }
    return j;
})

MFV_AVX2(size_t detect_ties_avx2_u64(const RadixPermuteItem<uint64_t>* u_sub, size_t first, size_t j, size_t n,
                                     Tie& tie, bool& has_ties) {
    static_assert(sizeof(RadixPermuteItem<uint64_t>) == 16);
    static_assert(offsetof(RadixPermuteItem<uint64_t>, inline_value) == 0);
    for (; j + 2 <= n; j += 2) {
        __m256i vprev = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&u_sub[j - 1]));
        __m256i vcurr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&u_sub[j]));
        __m256i cmp = _mm256_cmpeq_epi64(vprev, vcurr);
        int mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
        uint8_t eq0 = static_cast<uint8_t>(((mask >> 0) & 1) & (tie[first + j] != 0));
        uint8_t eq1 = static_cast<uint8_t>(((mask >> 2) & 1) & (tie[first + j + 1] != 0));
        tie[first + j] = eq0;
        tie[first + j + 1] = eq1;
        has_ties |= ((eq0 | eq1) != 0);
    }
    return j;
})
#endif

template <typename UnsignedKey>
void detect_ties_in_range(const RadixPermuteItem<UnsignedKey>* u_sub, size_t first, size_t n, Tie& tie,
                          bool& range_has_ties) {
    bool has_ties = false;
    size_t j = 1;

#if defined(__ARM_NEON) || defined(__aarch64__)
    if constexpr (sizeof(UnsignedKey) == 4) {
        static_assert(sizeof(RadixPermuteItem<UnsignedKey>) == 8);
        for (; j + 4 <= n; j += 4) {
            uint32x4x2_t prev_pair = vld2q_u32(reinterpret_cast<const uint32_t*>(&u_sub[j - 1]));
            uint32x4x2_t curr_pair = vld2q_u32(reinterpret_cast<const uint32_t*>(&u_sub[j]));
            uint32x4_t cmp = vceqq_u32(prev_pair.val[0], curr_pair.val[0]);
            uint32x4_t eq32 = vshrq_n_u32(cmp, 31);
            uint16x4_t eq16 = vmovn_u32(eq32);
            uint8x8_t eq8 = vmovn_u16(vcombine_u16(eq16, vdup_n_u16(0)));

            uint32_t existing_raw = 0;
            std::memcpy(&existing_raw, &tie[first + j], 4);
            uint8x8_t tie_bytes = vreinterpret_u8_u32(vdup_n_u32(existing_raw));
            uint8x8_t tie_nz = vand_u8(vtst_u8(tie_bytes, tie_bytes), vdup_n_u8(1));
            uint8x8_t res8 = vand_u8(eq8, tie_nz);

            uint32_t out_raw = vget_lane_u32(vreinterpret_u32_u8(res8), 0);
            std::memcpy(&tie[first + j], &out_raw, 4);
            has_ties |= (out_raw != 0);
        }
    }
#elif defined(__GNUC__) && defined(__x86_64__)
    if (cpu_supports_avx512bw()) {
        if constexpr (sizeof(UnsignedKey) == 4) {
            j = detect_ties_avx512_u32(reinterpret_cast<const RadixPermuteItem<uint32_t>*>(u_sub), first, j, n, tie,
                                       has_ties);
        } else if constexpr (sizeof(UnsignedKey) == 8) {
            j = detect_ties_avx512_u64(reinterpret_cast<const RadixPermuteItem<uint64_t>*>(u_sub), first, j, n, tie,
                                       has_ties);
        }
    }
    if (cpu_supports_avx2()) {
        if constexpr (sizeof(UnsignedKey) == 4) {
            j = detect_ties_avx2_u32(reinterpret_cast<const RadixPermuteItem<uint32_t>*>(u_sub), first, j, n, tie,
                                     has_ties);
        } else if constexpr (sizeof(UnsignedKey) == 8) {
            j = detect_ties_avx2_u64(reinterpret_cast<const RadixPermuteItem<uint64_t>*>(u_sub), first, j, n, tie,
                                     has_ties);
        }
    }
#endif

    detect_ties_scalar(u_sub, first, j, n, tie, has_ties);
    range_has_ties |= has_ties;
}

} // namespace detail

// CONTRACT: On return (including n == 1, already-sorted/reverse-sorted fast paths, and cancellation),
// items[0..n).inline_value is destructively overwritten in place with the internal bit-transformed
// RadixSortTraits<T>::UnsignedKey representation, NOT the original T values. Only index_in_chunk
// preserves the sorted permutation; callers must re-gather values from the source column if needed.
// If `cancel` becomes true during execution, the range is left partially transformed/unsorted and
// must be discarded by the caller.
template <RadixSortable T, detail::CancelFlag CancelT>
void radix_sort_inlined(const CancelT& cancel, InlinePermuteItem<T>* items, InlinePermuteItem<T>* scratch, size_t n,
                        bool asc) {
    detail::radix_sort_core<T>(cancel, items, scratch, nullptr, n, asc);
}

template <RadixSortable T, detail::CancelFlag CancelT>
void radix_sort_inlined(const CancelT& cancel, InlinePermuteItem<T>* items, InlinePermutation<T>& scratch, size_t n,
                        bool asc) {
    detail::radix_sort_core<T>(cancel, items, nullptr, &scratch, n, asc);
}

// CONTRACT: On return (including n == 1, already-sorted/reverse-sorted fast paths, and cancellation),
// items[first..last).inline_value is destructively overwritten in place with the internal bit-transformed
// RadixSortTraits<T>::UnsignedKey representation, NOT the original T values. Only index_in_chunk
// preserves the sorted permutation; callers must re-gather values from the source column if needed.
// If `cancel` becomes true during execution, the range and `tie` are left partially updated and
// must be discarded by the caller.
template <RadixSortable T, detail::CancelFlag CancelT>
void radix_sort_inlined_and_tie(const CancelT& cancel, InlinePermuteItem<T>* items, InlinePermuteItem<T>* scratch,
                                size_t first, size_t last, bool asc, Tie& tie, bool& range_has_ties) {
    if (last <= first || detail::is_cancelled(cancel)) {
        return;
    }
    const size_t n = last - first;
    InlinePermuteItem<T>* sub_items = items + first;
    detail::radix_sort_core<T>(cancel, sub_items, scratch, nullptr, n, asc);
    if (detail::is_cancelled(cancel) || n <= 1) {
        return;
    }

    using UnsignedKey = typename RadixSortTraits<T>::UnsignedKey;
    using UItem = detail::RadixPermuteItem<UnsignedKey>;
    const auto* u_sub = reinterpret_cast<const UItem*>(sub_items);
    detail::detect_ties_in_range<UnsignedKey>(u_sub, first, n, tie, range_has_ties);
}

template <RadixSortable T, detail::CancelFlag CancelT>
void radix_sort_inlined_and_tie(const CancelT& cancel, InlinePermuteItem<T>* items, InlinePermuteItem<T>* scratch,
                                size_t first, size_t last, bool asc, Tie& tie) {
    bool unused_has_ties = false;
    radix_sort_inlined_and_tie(cancel, items, scratch, first, last, asc, tie, unused_has_ties);
}

template <RadixSortable T, detail::CancelFlag CancelT>
void radix_sort_inlined_and_tie(const CancelT& cancel, InlinePermuteItem<T>* items, InlinePermutation<T>& scratch,
                                size_t first, size_t last, bool asc, Tie& tie, bool& range_has_ties) {
    if (last <= first || detail::is_cancelled(cancel)) {
        return;
    }
    const size_t n = last - first;
    InlinePermuteItem<T>* sub_items = items + first;
    detail::radix_sort_core<T>(cancel, sub_items, nullptr, &scratch, n, asc);
    if (detail::is_cancelled(cancel) || n <= 1) {
        return;
    }

    using UnsignedKey = typename RadixSortTraits<T>::UnsignedKey;
    using UItem = detail::RadixPermuteItem<UnsignedKey>;
    const auto* u_sub = reinterpret_cast<const UItem*>(sub_items);
    detail::detect_ties_in_range<UnsignedKey>(u_sub, first, n, tie, range_has_ties);
}

template <RadixSortable T, detail::CancelFlag CancelT>
void radix_sort_inlined_and_tie(const CancelT& cancel, InlinePermuteItem<T>* items, InlinePermutation<T>& scratch,
                                size_t first, size_t last, bool asc, Tie& tie) {
    bool unused_has_ties = false;
    radix_sort_inlined_and_tie(cancel, items, scratch, first, last, asc, tie, unused_has_ties);
}

} // namespace starrocks

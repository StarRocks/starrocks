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

#include <cstring>
#include <type_traits>
#include <utility>

#ifdef __x86_64__
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/simd_selector.h"
#include "column/sorting/sort_helper.h"
#include "column/sorting/sort_permute.h"
#include "column/sorting/sorting.h"
#include "column/vectorized_fwd.h"
#include "glog/logging.h"
#include "types/datum.h"
#include "types/logical_type.h"

namespace starrocks {

// Compare a column with datum, store result into a vector
// For column-wise compare, only consider rows that are equal at previous columns
// @return number of rows that are equal
template <class DataComparator>
static inline size_t compare_column_helper(CompareVector& cmp_vector, DataComparator cmp) {
    DCHECK(std::all_of(cmp_vector.begin(), cmp_vector.end(), [](int8_t x) { return x == 0 || x == -1 || x == 1; }));

    // For sparse array, use SIMD to skip data.
    // For dense array, just iterate all bytes.
    if (SIMD::count_zero(cmp_vector) > cmp_vector.size() / 8) {
        for (size_t i = 0; i < cmp_vector.size(); i++) {
            // Only consider rows that are queal at previous columns
            if (cmp_vector[i] == 0) {
                cmp_vector[i] = cmp(i);
            }
        }
    } else {
        size_t idx = 0;
        while (true) {
            size_t pos = SIMD::find_zero(cmp_vector, idx);
            if (pos >= cmp_vector.size()) {
                break;
            }

            cmp_vector[pos] = cmp(pos);
            idx = pos + 1;
        }
    }

    return SIMD::count_zero(cmp_vector);
}

template <typename T>
static inline int8_t compare_scalar_with_order(const T& lhs, const T& rhs, int sort_order) {
    int cmp = SorterComparator<T>::compare(lhs, rhs);
    return sort_order >= 0 ? static_cast<int8_t>(cmp) : static_cast<int8_t>(-cmp);
}

template <typename T>
static inline size_t compare_integral_column_simd(CompareVector& cmp_vector, const T* lhs_data, T rhs_data,
                                                  int sort_order) {
    const size_t size = cmp_vector.size();
    if (size == 0) {
        return 0;
    }

    // Fast path: many zeros in cmp_vector -> block-compare and write results.
    const auto scalar_cmp = [&](size_t idx) { return compare_scalar_with_order(lhs_data[idx], rhs_data, sort_order); };

    if (SIMD::count_zero(cmp_vector) > size / 8) {
        size_t i = 0;
        // On AVX-512 builds the compiler auto-vectorises the scalar fallback
        // (the simple `if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp(i);`
        // loop below) to wider AVX-512 mask-merge code that beats the
        // hand-written AVX2 block-skip path. Only opt into the hand-written
        // SIMD on AVX2-only builds.
#if defined(__AVX2__) && !defined(__AVX512F__)
        if constexpr (sizeof(T) == 1) {
            constexpr size_t kBlock = 32;
            const __m256i zero = _mm256_setzero_si256();
            __m256i rhs_vec = _mm256_set1_epi8(static_cast<int8_t>(rhs_data));
            const __m256i sign_mask =
                    std::is_unsigned_v<T> ? _mm256_set1_epi8(static_cast<char>(0x80)) : _mm256_setzero_si256();
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = _mm256_xor_si256(rhs_vec, sign_mask);
            }
            const __m256i ones = _mm256_set1_epi8(1);
            for (; i + kBlock <= size; i += kBlock) {
                __m256i cmp_bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(cmp_vector.data() + i));
                __m256i zeros = _mm256_cmpeq_epi8(cmp_bytes, zero);
                uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(zeros));
                // Mixed block: fall back to scalar for elements still equal.
                if (mask != 0xFFFF'FFFFu) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                __m256i lhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs = _mm256_xor_si256(lhs, sign_mask);
                }

                __m256i gt = _mm256_cmpgt_epi8(lhs, rhs_vec);
                __m256i lt = _mm256_cmpgt_epi8(rhs_vec, lhs);
                __m256i gt01 = _mm256_and_si256(gt, ones);
                __m256i lt01 = _mm256_and_si256(lt, ones);
                __m256i cmp = _mm256_sub_epi8(gt01, lt01);
                if (sort_order < 0) {
                    cmp = _mm256_sub_epi8(zero, cmp);
                }
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(cmp_vector.data() + i), cmp);
            }
        } else if constexpr (sizeof(T) == 2) {
            constexpr size_t kBlock = 16;
            const __m256i zero = _mm256_setzero_si256();
            __m256i rhs_vec = _mm256_set1_epi16(static_cast<int16_t>(rhs_data));
            const __m256i sign_mask =
                    std::is_unsigned_v<T> ? _mm256_set1_epi16(static_cast<int16_t>(0x8000)) : _mm256_setzero_si256();
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = _mm256_xor_si256(rhs_vec, sign_mask);
            }
            const __m256i ones = _mm256_set1_epi16(1);
            const __m128i zero128 = _mm_setzero_si128();
            for (; i + kBlock <= size; i += kBlock) {
                __m128i cmp_bytes = _mm_loadu_si128(reinterpret_cast<const __m128i*>(cmp_vector.data() + i));
                int mask = _mm_movemask_epi8(_mm_cmpeq_epi8(cmp_bytes, zero128));
                // Mixed block: fall back to scalar for elements still equal.
                if (mask != 0xFFFF) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                __m256i lhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs = _mm256_xor_si256(lhs, sign_mask);
                }

                __m256i gt = _mm256_cmpgt_epi16(lhs, rhs_vec);
                __m256i lt = _mm256_cmpgt_epi16(rhs_vec, lhs);
                __m256i gt01 = _mm256_and_si256(gt, ones);
                __m256i lt01 = _mm256_and_si256(lt, ones);
                __m256i cmp16 = _mm256_sub_epi16(gt01, lt01);
                if (sort_order < 0) {
                    cmp16 = _mm256_sub_epi16(zero, cmp16);
                }

                __m128i lo = _mm256_castsi256_si128(cmp16);
                __m128i hi = _mm256_extracti128_si256(cmp16, 1);
                __m128i packed = _mm_packs_epi16(lo, hi);
                _mm_storeu_si128(reinterpret_cast<__m128i*>(cmp_vector.data() + i), packed);
            }
        } else if constexpr (sizeof(T) == 4) {
            constexpr size_t kBlock = 8;
            const __m256i zero = _mm256_setzero_si256();
            __m256i rhs_vec = _mm256_set1_epi32(static_cast<int32_t>(rhs_data));
            const __m256i sign_mask = std::is_unsigned_v<T> ? _mm256_set1_epi32(0x8000'0000) : _mm256_setzero_si256();
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = _mm256_xor_si256(rhs_vec, sign_mask);
            }
            const __m256i ones = _mm256_set1_epi32(1);
            alignas(32) int32_t tmp[8];
            for (; i + kBlock <= size; i += kBlock) {
                uint64_t block_mask = 0;
                std::memcpy(&block_mask, cmp_vector.data() + i, sizeof(block_mask));
                // Mixed block: fall back to scalar for elements still equal.
                if (block_mask != 0) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                __m256i lhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs = _mm256_xor_si256(lhs, sign_mask);
                }

                __m256i gt = _mm256_cmpgt_epi32(lhs, rhs_vec);
                __m256i lt = _mm256_cmpgt_epi32(rhs_vec, lhs);
                __m256i gt01 = _mm256_and_si256(gt, ones);
                __m256i lt01 = _mm256_and_si256(lt, ones);
                __m256i cmp32 = _mm256_sub_epi32(gt01, lt01);
                if (sort_order < 0) {
                    cmp32 = _mm256_sub_epi32(zero, cmp32);
                }
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(tmp), cmp32);
                for (size_t k = 0; k < kBlock; ++k) {
                    cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
                }
            }
        } else if constexpr (sizeof(T) == 8) {
            constexpr size_t kBlock = 4;
            const __m256i zero = _mm256_setzero_si256();
            __m256i rhs_vec = _mm256_set1_epi64x(static_cast<int64_t>(rhs_data));
            const __m256i sign_mask = std::is_unsigned_v<T>
                                              ? _mm256_set1_epi64x(static_cast<int64_t>(0x8000'0000'0000'0000ULL))
                                              : _mm256_setzero_si256();
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = _mm256_xor_si256(rhs_vec, sign_mask);
            }
            const __m256i ones = _mm256_set1_epi64x(1);
            alignas(32) int64_t tmp[4];
            for (; i + kBlock <= size; i += kBlock) {
                uint32_t block_mask = 0;
                std::memcpy(&block_mask, cmp_vector.data() + i, sizeof(block_mask));
                // Mixed block: fall back to scalar for elements still equal.
                if (block_mask != 0) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                __m256i lhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs = _mm256_xor_si256(lhs, sign_mask);
                }

                __m256i gt = _mm256_cmpgt_epi64(lhs, rhs_vec);
                __m256i lt = _mm256_cmpgt_epi64(rhs_vec, lhs);
                __m256i gt01 = _mm256_and_si256(gt, ones);
                __m256i lt01 = _mm256_and_si256(lt, ones);
                __m256i cmp64 = _mm256_sub_epi64(gt01, lt01);
                if (sort_order < 0) {
                    cmp64 = _mm256_sub_epi64(zero, cmp64);
                }
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(tmp), cmp64);
                for (size_t k = 0; k < kBlock; ++k) {
                    cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
                }
            }
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        if constexpr (sizeof(T) == 1) {
            constexpr size_t kBlock = 16;
            const uint8x16_t zero = vdupq_n_u8(0);
            uint8x16_t rhs_vec = vdupq_n_u8(static_cast<uint8_t>(rhs_data));
            const uint8x16_t sign_mask = std::is_unsigned_v<T> ? vdupq_n_u8(0x80) : vdupq_n_u8(0);
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = veorq_u8(rhs_vec, sign_mask);
            }
            const uint8x16_t ones = vdupq_n_u8(1);
            for (; i + kBlock <= size; i += kBlock) {
                uint8x16_t cmp_bytes = vld1q_u8(reinterpret_cast<const uint8_t*>(cmp_vector.data() + i));
                uint8x16_t zeros = vceqq_u8(cmp_bytes, zero);
                // Mixed block: fall back to scalar for elements still equal.
                if (vminvq_u8(zeros) != 0xFF) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                uint8x16_t lhs_u = vld1q_u8(reinterpret_cast<const uint8_t*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs_u = veorq_u8(lhs_u, sign_mask);
                }
                int8x16_t lhs_s = vreinterpretq_s8_u8(lhs_u);
                int8x16_t rhs_s = vreinterpretq_s8_u8(rhs_vec);
                uint8x16_t gt = vcgtq_s8(lhs_s, rhs_s);
                uint8x16_t lt = vcgtq_s8(rhs_s, lhs_s);
                uint8x16_t gt01 = vandq_u8(gt, ones);
                uint8x16_t lt01 = vandq_u8(lt, ones);
                int8x16_t cmp = vreinterpretq_s8_u8(vsubq_u8(gt01, lt01));
                if (sort_order < 0) {
                    cmp = vnegq_s8(cmp);
                }
                vst1q_s8(reinterpret_cast<int8_t*>(cmp_vector.data() + i), cmp);
            }
        } else if constexpr (sizeof(T) == 2) {
            constexpr size_t kBlock = 8;
            uint16x8_t rhs_vec = vdupq_n_u16(static_cast<uint16_t>(rhs_data));
            const uint16x8_t sign_mask = std::is_unsigned_v<T> ? vdupq_n_u16(0x8000) : vdupq_n_u16(0);
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = veorq_u16(rhs_vec, sign_mask);
            }
            const uint16x8_t ones = vdupq_n_u16(1);
            for (; i + kBlock <= size; i += kBlock) {
                uint64_t block_mask = 0;
                std::memcpy(&block_mask, cmp_vector.data() + i, sizeof(block_mask));
                // Mixed block: fall back to scalar for elements still equal.
                if (block_mask != 0) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                uint16x8_t lhs_u = vld1q_u16(reinterpret_cast<const uint16_t*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs_u = veorq_u16(lhs_u, sign_mask);
                }
                int16x8_t lhs_s = vreinterpretq_s16_u16(lhs_u);
                int16x8_t rhs_s = vreinterpretq_s16_u16(rhs_vec);
                uint16x8_t gt = vcgtq_s16(lhs_s, rhs_s);
                uint16x8_t lt = vcgtq_s16(rhs_s, lhs_s);
                uint16x8_t gt01 = vandq_u16(gt, ones);
                uint16x8_t lt01 = vandq_u16(lt, ones);
                int16x8_t cmp = vreinterpretq_s16_u16(vsubq_u16(gt01, lt01));
                if (sort_order < 0) {
                    cmp = vnegq_s16(cmp);
                }
                int8x8_t cmp8 = vqmovn_s16(cmp);
                vst1_s8(reinterpret_cast<int8_t*>(cmp_vector.data() + i), cmp8);
            }
        } else if constexpr (sizeof(T) == 4) {
            constexpr size_t kBlock = 4;
            uint32x4_t rhs_vec = vdupq_n_u32(static_cast<uint32_t>(rhs_data));
            const uint32x4_t sign_mask = std::is_unsigned_v<T> ? vdupq_n_u32(0x8000'0000) : vdupq_n_u32(0);
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = veorq_u32(rhs_vec, sign_mask);
            }
            const uint32x4_t ones = vdupq_n_u32(1);
            alignas(16) int32_t tmp[4];
            for (; i + kBlock <= size; i += kBlock) {
                uint32_t block_mask = 0;
                std::memcpy(&block_mask, cmp_vector.data() + i, sizeof(block_mask));
                // Mixed block: fall back to scalar for elements still equal.
                if (block_mask != 0) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                uint32x4_t lhs_u = vld1q_u32(reinterpret_cast<const uint32_t*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs_u = veorq_u32(lhs_u, sign_mask);
                }
                int32x4_t lhs_s = vreinterpretq_s32_u32(lhs_u);
                int32x4_t rhs_s = vreinterpretq_s32_u32(rhs_vec);
                uint32x4_t gt = vcgtq_s32(lhs_s, rhs_s);
                uint32x4_t lt = vcgtq_s32(rhs_s, lhs_s);
                uint32x4_t gt01 = vandq_u32(gt, ones);
                uint32x4_t lt01 = vandq_u32(lt, ones);
                int32x4_t cmp = vreinterpretq_s32_u32(vsubq_u32(gt01, lt01));
                if (sort_order < 0) {
                    cmp = vnegq_s32(cmp);
                }
                vst1q_s32(tmp, cmp);
                for (size_t k = 0; k < kBlock; ++k) {
                    cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
                }
            }
        } else if constexpr (sizeof(T) == 8) {
            constexpr size_t kBlock = 2;
            uint64x2_t rhs_vec = vdupq_n_u64(static_cast<uint64_t>(rhs_data));
            const uint64x2_t sign_mask = std::is_unsigned_v<T> ? vdupq_n_u64(0x8000'0000'0000'0000ULL) : vdupq_n_u64(0);
            // For unsigned compare, flip sign bit to reuse signed compares.
            if constexpr (std::is_unsigned_v<T>) {
                rhs_vec = veorq_u64(rhs_vec, sign_mask);
            }
            const uint64x2_t ones = vdupq_n_u64(1);
            alignas(16) int64_t tmp[2];
            for (; i + kBlock <= size; i += kBlock) {
                uint16_t block_mask = 0;
                std::memcpy(&block_mask, cmp_vector.data() + i, sizeof(block_mask));
                // Mixed block: fall back to scalar for elements still equal.
                if (block_mask != 0) {
                    for (size_t j = i; j < i + kBlock; ++j) {
                        if (cmp_vector[j] == 0) {
                            cmp_vector[j] = scalar_cmp(j);
                        }
                    }
                    continue;
                }

                uint64x2_t lhs_u = vld1q_u64(reinterpret_cast<const uint64_t*>(lhs_data + i));
                if constexpr (std::is_unsigned_v<T>) {
                    lhs_u = veorq_u64(lhs_u, sign_mask);
                }
                int64x2_t lhs_s = vreinterpretq_s64_u64(lhs_u);
                int64x2_t rhs_s = vreinterpretq_s64_u64(rhs_vec);
                uint64x2_t gt = vcgtq_s64(lhs_s, rhs_s);
                uint64x2_t lt = vcgtq_s64(rhs_s, lhs_s);
                uint64x2_t gt01 = vandq_u64(gt, ones);
                uint64x2_t lt01 = vandq_u64(lt, ones);
                int64x2_t cmp = vreinterpretq_s64_u64(vsubq_u64(gt01, lt01));
                if (sort_order < 0) {
                    cmp = vnegq_s64(cmp);
                }
                vst1q_s64(tmp, cmp);
                for (size_t k = 0; k < kBlock; ++k) {
                    cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
                }
            }
        }
#endif
        for (; i < size; ++i) {
            if (cmp_vector[i] == 0) {
                cmp_vector[i] = scalar_cmp(i);
            }
        }
        return SIMD::count_zero(cmp_vector);
    }

    size_t idx = 0;
    while (true) {
        size_t pos = SIMD::find_zero(cmp_vector, idx);
        if (pos >= size) {
            break;
        }
        cmp_vector[pos] = scalar_cmp(pos);
        idx = pos + 1;
    }

    return SIMD::count_zero(cmp_vector);
}

template <typename T, typename Underlying>
static inline const Underlying* raw_column_as(const T* data) {
    static_assert(std::is_standard_layout_v<T>);
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(sizeof(T) == sizeof(Underlying));
    return reinterpret_cast<const Underlying*>(data);
}

// ColumnCompare compare a chunk with a row
// A plain implementation is iterate all rows in chunk, and compare the left row with right row.
// The problem of this way is the compare overhead, since it must implement as virtual function call.
// So here we introduce the column-wise compare algorithm:
// 1. compare the first left jcolumn with the first value in right row, save result in cmp_vector
// 2. compare the second left column only if the first left column is equal to first right value
// 3. repeat the second step until the last column
//
// In this way, the compare procedure is conducted in column-wise style, which eliminated the compare
// function call.
// And the compare result for each column is represented as a vector of int8_t, which takes only a little
// bit of memory footprint and could support fast navigate.
class ColumnCompare final : public ColumnVisitorAdapter<ColumnCompare> {
public:
    explicit ColumnCompare(CompareVector& cmp_vector, Datum rhs_value, const SortDesc& sort_desc)
            : ColumnVisitorAdapter(this),
              _cmp_vector(cmp_vector),
              _rhs_value(std::move(rhs_value)),
              _sort_order(sort_desc.sort_order),
              _null_first(sort_desc.null_first) {}

    Status do_visit(const NullableColumn& column) {
        // Two step compare:
        // 1. Compare null values, store at temporary result
        // 2. Mask notnull values, and compare not-null values
        const auto null_data = column.immutable_null_column_data();

        int nan_direction = _sort_order * _null_first;

        // Set byte to 0 when it's null/null byte is 1
        // SIMD optimization: null_vector[i] = null_data[i] ^ 1
        CompareVector null_vector(null_data.size());
        size_t i = 0;
        const size_t size = null_data.size();
#ifdef __AVX2__
        const __m256i ones = _mm256_set1_epi8(1);
        for (; i + 32 <= size; i += 32) {
            __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(null_data.data() + i));
            __m256i result = _mm256_xor_si256(data, ones);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(null_vector.data() + i), result);
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        const uint8x16_t ones = vdupq_n_u8(1);
        for (; i + 16 <= size; i += 16) {
            uint8x16_t data = vld1q_u8(null_data.data() + i);
            uint8x16_t result = veorq_u8(data, ones);
            vst1q_s8(null_vector.data() + i, vreinterpretq_s8_u8(result));
        }
#endif
        // SIMD path XORs with 1, equivalent to `(null_data[i]==1)?0:1` only when
        // null_data ∈ {0,1}. NullColumn guarantees this — keep the scalar tail in
        // bit-flip form so both paths are byte-identical.
        for (; i < size; i++) {
            DCHECK(null_data[i] <= 1);
            null_vector[i] = null_data[i] ^ 1;
        }
        auto null_cmp = [&](int lhs_row) -> int {
            DCHECK(null_data[lhs_row] == 1);
            return _rhs_value.is_null() ? 0 : nan_direction;
        };
        size_t null_equal_count = compare_column_helper(null_vector, null_cmp);

        int notnull_equal_count = 0;

        CompareVector cmp_vector(null_data.size());
        auto merge_cmp_vector = [](CompareVector& a, CompareVector& b) {
            DCHECK_EQ(a.size(), b.size());
            SIMD_selector<TYPE_TINYINT>::select_if((uint8_t*)a.data(), a, a, b);
        };
        if (_rhs_value.is_null()) {
            for (size_t i = 0; i < null_data.size(); i++) {
                if (null_data[i] == 0) {
                    cmp_vector[i] = -nan_direction;
                } else {
                    cmp_vector[i] = null_vector[i];
                }
            }
            // merge cmp_vector
            // _cmp_vector[i] = _cmp_vector[i] == 0 ? cmp_vector[i]: _cmp_vector[i];
            merge_cmp_vector(_cmp_vector, cmp_vector);
        } else {
            // 0 means not null, so compare it
            // 1 means null, not compare it for not-null values
            CompareVector notnull_vector(null_data.size());
            for (size_t i = 0; i < null_data.size(); i++) {
                notnull_vector[i] = null_data[i];
            }

            notnull_equal_count = compare_column(column.data_column(), notnull_vector, _rhs_value,
                                                 SortDesc(_sort_order, _null_first));
            for (size_t i = 0; i < null_data.size(); i++) {
                if (null_data[i] == 0) {
                    cmp_vector[i] = notnull_vector[i];
                } else {
                    cmp_vector[i] = null_vector[i];
                }
            }
            merge_cmp_vector(_cmp_vector, cmp_vector);
        }

        _equal_count = null_equal_count + notnull_equal_count;

        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        _equal_count =
                compare_column(column.data_column(), _cmp_vector, _rhs_value, SortDesc(_sort_order, _null_first));

        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        // Convert the datum to a array column
        auto rhs_column = column.clone_empty();
        rhs_column->append_datum(_rhs_value);
        auto cmp = [&](int lhs_index) {
            return column.compare_at(lhs_index, 0, *rhs_column, _null_first) * _sort_order;
        };
        _equal_count = compare_column_helper(_cmp_vector, cmp);
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        // Convert the datum to a array column
        auto rhs_column = column.clone_empty();
        rhs_column->append_datum(_rhs_value);
        auto cmp = [&](int lhs_index) {
            return column.compare_at(lhs_index, 0, *rhs_column, _null_first) * _sort_order;
        };
        _equal_count = compare_column_helper(_cmp_vector, cmp);
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        // Convert the datum to a struct column
        auto rhs_column = column.clone_empty();
        rhs_column->append_datum(_rhs_value);
        auto cmp = [&](int lhs_index) {
            return column.compare_at(lhs_index, 0, *rhs_column, _null_first) * _sort_order;
        };
        _equal_count = compare_column_helper(_cmp_vector, cmp);

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        const auto lhs_datas = column.immutable_data();
        Slice rhs_data = _rhs_value.get<Slice>();

        if (_sort_order == 1) {
            auto cmp = [&](int lhs_row) { return SorterComparator<Slice>::compare(lhs_datas[lhs_row], rhs_data); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        } else {
            auto cmp = [&](int lhs_row) { return -1 * SorterComparator<Slice>::compare(lhs_datas[lhs_row], rhs_data); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        }

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        T rhs_data = _rhs_value.get<T>();
        const auto lhs_data = column.immutable_data();

        if constexpr (std::is_integral_v<T> && sizeof(T) <= 8) {
            _equal_count = compare_integral_column_simd(_cmp_vector, lhs_data.data(), rhs_data, _sort_order);
        } else if constexpr (std::is_same_v<T, DateValue>) {
            using Underlying = std::decay_t<decltype(rhs_data.julian())>;
            const auto* lhs_raw = raw_column_as<T, Underlying>(lhs_data.data());
            _equal_count = compare_integral_column_simd(_cmp_vector, lhs_raw, rhs_data.julian(), _sort_order);
        } else if constexpr (std::is_same_v<T, TimestampValue>) {
            using Underlying = std::decay_t<decltype(rhs_data.timestamp())>;
            const auto* lhs_raw = raw_column_as<T, Underlying>(lhs_data.data());
            _equal_count = compare_integral_column_simd(_cmp_vector, lhs_raw, rhs_data.timestamp(), _sort_order);
        } else {
            if (_sort_order == 1) {
                auto cmp = [&](int lhs_row) { return SorterComparator<T>::compare(lhs_data[lhs_row], rhs_data); };
                _equal_count = compare_column_helper(_cmp_vector, cmp);
            } else {
                auto cmp = [&](int lhs_row) { return -1 * SorterComparator<T>::compare(lhs_data[lhs_row], rhs_data); };
                _equal_count = compare_column_helper(_cmp_vector, cmp);
            }
        }

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        DCHECK(false) << "not support object column sort_and_tie";

        return Status::NotSupported("not support object column sort_and_tie");
    }

    Status do_visit(const AdaptiveNullableColumn& column) {
        // TODO: supported later
        return Status::NotSupported("not support AdaptiveNullableColumn in ColumnCompare");
    }

    Status do_visit(const JsonColumn& column) {
        const auto lhs_data = column.immutable_data();
        const JsonValue& rhs_json = *_rhs_value.get_json();

        if (_sort_order == 1) {
            auto cmp = [&](int lhs_row) { return lhs_data[lhs_row]->compare(rhs_json); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);

        } else {
            auto cmp = [&](int lhs_row) { return -1 * lhs_data[lhs_row]->compare(rhs_json); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        }
        return Status::OK();
    }

    size_t get_equal_count() const { return _equal_count; }

private:
    CompareVector& _cmp_vector;
    Datum _rhs_value;
    int _sort_order;
    int _null_first;

    size_t _equal_count = 0; // Output equal count of compare
};

class ColumnTieBuilder final : public ColumnVisitorAdapter<ColumnTieBuilder> {
public:
    explicit ColumnTieBuilder(ColumnPtr column, Tie* tie)
            : ColumnVisitorAdapter(this), _column(std::move(column)), _tie(tie), _nullable_column(nullptr) {}

    explicit ColumnTieBuilder(ColumnPtr column, Tie* tie, NullColumnPtr nullable_column)
            : ColumnVisitorAdapter(this),
              _column(std::move(column)),
              _tie(tie),
              _nullable_column(std::move(nullable_column)) {}

    Status do_visit(const NullableColumn& column) {
        // TODO: maybe could skip compare rows that contains null
        // 1. Compare the null flag
        // 2. Compare the value if both are not null. Since value for null is just default value,
        //    which are equal, so just compare the value directly
        const auto null_data = column.immutable_null_column_data();
        const size_t col_size = column.size();
        size_t i = 1;
        // SIMD optimization: compare adjacent null flags and AND with tie
#ifdef __AVX2__
        for (; i + 32 <= col_size; i += 32) {
            __m256i prev = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(null_data.data() + i - 1));
            __m256i curr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(null_data.data() + i));
            // Compare equal: 0xFF if equal, 0x00 if not
            __m256i eq = _mm256_cmpeq_epi8(prev, curr);
            __m256i tie_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(_tie->data() + i));
            // tie &= eq (works because tie values are 0 or 1)
            __m256i result = _mm256_and_si256(tie_vec, eq);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(_tie->data() + i), result);
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        for (; i + 16 <= col_size; i += 16) {
            uint8x16_t prev = vld1q_u8(null_data.data() + i - 1);
            uint8x16_t curr = vld1q_u8(null_data.data() + i);
            // Compare equal: 0xFF if equal, 0x00 if not
            uint8x16_t eq = vceqq_u8(prev, curr);
            uint8x16_t tie_vec = vld1q_u8(_tie->data() + i);
            uint8x16_t result = vandq_u8(tie_vec, eq);
            vst1q_u8(_tie->data() + i, result);
        }
#endif
        for (; i < col_size; i++) {
            (*_tie)[i] &= (null_data[i - 1] == null_data[i]);
        }

        build_tie_for_column(column.data_column(), _tie, column.null_column());
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        auto data = column.immutable_data();
        ImmutableNullData null_data;
        if (_nullable_column != nullptr) {
            null_data = _nullable_column->immutable_data();
        }
        for (size_t i = 1; i < column.size(); i++) {
            if ((null_data.empty()) || (null_data[i - 1] != 1 && null_data[i] != 1)) {
                (*_tie)[i] &= SorterComparator<Slice>::compare(data[i - 1], data[i]) == 0;
            }
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        const auto data = column.immutable_data();
        ImmutableNullData null_data;
        if (_nullable_column != nullptr) {
            null_data = _nullable_column->immutable_data();
        }
        for (size_t i = 1; i < column.size(); i++) {
            if ((null_data.empty()) || (null_data[i - 1] != 1 && null_data[i] != 1)) {
                (*_tie)[i] &= SorterComparator<T>::compare(data[i - 1], data[i]) == 0;
            }
        }
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) { return Status::NotSupported("Not support"); }
    Status do_visit(const ArrayColumn& column) { return Status::NotSupported("Not support"); }
    Status do_visit(const MapColumn& column) { return Status::NotSupported("Not support"); }
    Status do_visit(const StructColumn& column) { return Status::NotSupported("Not support"); }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        return Status::NotSupported("not support");
    }

private:
    const ColumnPtr _column;
    Tie* _tie;
    const NullColumnPtr _nullable_column;
};

int compare_column(const ColumnPtr& column, CompareVector& cmp_vector, Datum rhs_value, const SortDesc& desc) {
    ColumnCompare compare(cmp_vector, std::move(rhs_value), desc);

    [[maybe_unused]] Status st = column->accept(&compare);
    DCHECK(st.ok());
    return compare.get_equal_count();
}

void compare_columns(const Columns& columns, CompareVector& cmp_vector, const Buffer<Datum>& rhs_values,
                     const SortDescs& sort_desc) {
    if (columns.empty()) {
        return;
    }
    DCHECK_EQ(columns.size(), rhs_values.size());
    DCHECK_EQ(columns.size(), sort_desc.num_columns());
    DCHECK_EQ(columns[0]->size(), cmp_vector.size());
    DCHECK(std::all_of(cmp_vector.begin(), cmp_vector.end(), [](int8_t x) { return x == 1 || x == -1 || x == 0; }));

    for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
        const Datum& rhs_value = rhs_values[col_idx];

        int equal_count = compare_column(columns[col_idx], cmp_vector, rhs_value, sort_desc.get_column_desc(col_idx));
        if (equal_count == 0) {
            break;
        }
    }
}

void build_tie_for_column(const ColumnPtr& column, Tie* tie, const NullColumnPtr& null_column) {
    DCHECK(!!tie);
    DCHECK_EQ(column->size(), tie->size());

    ColumnTieBuilder tie_builder(column, tie, null_column);
    [[maybe_unused]] Status st = column->accept(&tie_builder);
    DCHECK(st.ok());
}

} // namespace starrocks

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

#include "base/simd/gather.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

namespace starrocks {
namespace {

template <class TB, class TC>
void gather_int16_buckets_impl(TB* b, const int16_t* a, const TC* c, [[maybe_unused]] size_t buckets, int num_rows) {
    int i = 0;
#if defined(__AVX2__)
    if (buckets < SIMDGather::max_process_size) {
        __m256i mask = _mm256_set1_epi32(0xFFFF);
        for (; i + 8 <= num_rows; i += 8) {
            __m256i loaded = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(c + i));
            __m256i gathered = _mm256_i32gather_epi32(reinterpret_cast<const int32_t*>(a), loaded, 2);
            gathered = _mm256_and_si256(gathered, mask);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(b + i), gathered);
        }
        _mm256_zeroupper();
    } else
#endif
    {
        constexpr int kPrefetchDist = 16;
        if (num_rows >= 8192) {
            for (; i + kPrefetchDist + 8 <= num_rows; i += 8) {
                STARROCKS_PREFETCH(&a[c[i + kPrefetchDist + 0]]);
                STARROCKS_PREFETCH(&a[c[i + kPrefetchDist + 2]]);
                STARROCKS_PREFETCH(&a[c[i + kPrefetchDist + 4]]);
                STARROCKS_PREFETCH(&a[c[i + kPrefetchDist + 6]]);

                b[i + 0] = static_cast<TB>(a[c[i + 0]]);
                b[i + 1] = static_cast<TB>(a[c[i + 1]]);
                b[i + 2] = static_cast<TB>(a[c[i + 2]]);
                b[i + 3] = static_cast<TB>(a[c[i + 3]]);
                b[i + 4] = static_cast<TB>(a[c[i + 4]]);
                b[i + 5] = static_cast<TB>(a[c[i + 5]]);
                b[i + 6] = static_cast<TB>(a[c[i + 6]]);
                b[i + 7] = static_cast<TB>(a[c[i + 7]]);
            }
        }
        for (; i + 8 <= num_rows; i += 8) {
            b[i + 0] = static_cast<TB>(a[c[i + 0]]);
            b[i + 1] = static_cast<TB>(a[c[i + 1]]);
            b[i + 2] = static_cast<TB>(a[c[i + 2]]);
            b[i + 3] = static_cast<TB>(a[c[i + 3]]);
            b[i + 4] = static_cast<TB>(a[c[i + 4]]);
            b[i + 5] = static_cast<TB>(a[c[i + 5]]);
            b[i + 6] = static_cast<TB>(a[c[i + 6]]);
            b[i + 7] = static_cast<TB>(a[c[i + 7]]);
        }
    }
    for (; i < num_rows; ++i) {
        b[i] = static_cast<TB>(a[c[i]]);
    }
}

template <size_t ElemSize, typename IndexType>
void gather_bytes_sized(void* dest, const void* src, const IndexType* indexes, size_t num_rows) {
    auto* d = static_cast<uint8_t*>(dest);
    const auto* s = static_cast<const uint8_t*>(src);
    size_t i = 0;
    constexpr size_t kUnroll = 8;
    constexpr size_t kPrefetchDistance = 16;

    if (num_rows >= 8192) {
        for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 0]) * ElemSize);
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 2]) * ElemSize);
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 4]) * ElemSize);
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 6]) * ElemSize);

            for (size_t j = 0; j < kUnroll; ++j) {
                std::memcpy(d + (i + j) * ElemSize, s + static_cast<size_t>(indexes[i + j]) * ElemSize, ElemSize);
            }
        }
    }

    for (; i + kUnroll <= num_rows; i += kUnroll) {
        for (size_t j = 0; j < kUnroll; ++j) {
            std::memcpy(d + (i + j) * ElemSize, s + static_cast<size_t>(indexes[i + j]) * ElemSize, ElemSize);
        }
    }

    for (; i < num_rows; ++i) {
        std::memcpy(d + i * ElemSize, s + static_cast<size_t>(indexes[i]) * ElemSize, ElemSize);
    }
}

template <typename IndexType>
void gather_default(void* dest, const void* src, size_t elem_size, const IndexType* indexes, size_t num_rows) {
    auto* d = reinterpret_cast<uint8_t*>(dest);
    const auto* s = reinterpret_cast<const uint8_t*>(src);
    size_t i = 0;
    constexpr size_t kUnroll = 8;
    constexpr size_t kPrefetchDistance = 16;

    if (num_rows >= 8192) {
        for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 0]) * elem_size);
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 2]) * elem_size);
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 4]) * elem_size);
            STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + 6]) * elem_size);

            for (size_t j = 0; j < kUnroll; ++j) {
                std::memcpy(d + (i + j) * elem_size, s + static_cast<size_t>(indexes[i + j]) * elem_size, elem_size);
            }
        }
    }

    for (; i + kUnroll <= num_rows; i += kUnroll) {
        for (size_t j = 0; j < kUnroll; ++j) {
            std::memcpy(d + (i + j) * elem_size, s + static_cast<size_t>(indexes[i + j]) * elem_size, elem_size);
        }
    }

    for (; i < num_rows; ++i) {
        std::memcpy(d + i * elem_size, s + static_cast<size_t>(indexes[i]) * elem_size, elem_size);
    }
}

template <typename IndexType>
void gather_bytes_impl(void* dest, const void* src, size_t elem_size, const IndexType* indexes, size_t num_rows) {
    switch (elem_size) {
    case 1:
        return gather_bytes_sized<1>(dest, src, indexes, num_rows);
    case 2:
        return gather_bytes_sized<2>(dest, src, indexes, num_rows);
    case 4:
        return gather_bytes_sized<4>(dest, src, indexes, num_rows);
    case 8:
        return gather_bytes_sized<8>(dest, src, indexes, num_rows);
    case 12:
        return gather_bytes_sized<12>(dest, src, indexes, num_rows);
    case 16:
        return gather_bytes_sized<16>(dest, src, indexes, num_rows);
    case 24:
        return gather_bytes_sized<24>(dest, src, indexes, num_rows);
    case 32:
        return gather_bytes_sized<32>(dest, src, indexes, num_rows);
    default:
        return gather_default(dest, src, elem_size, indexes, num_rows);
    }
}

template <size_t ElemSize, typename IndexType>
void gather_bytes_filtered_sized(void* dest, const void* src, const void* zero_val, const IndexType* indexes,
                                 const uint8_t* is_filtered, size_t num_rows) {
    auto* d = static_cast<uint8_t*>(dest);
    const auto* s = static_cast<const uint8_t*>(src);
    size_t i = 0;
    constexpr size_t kUnroll = 8;
    constexpr size_t kPrefetchDistance = 16;

    if (num_rows >= 8192) {
        for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
            for (size_t k = 0; k < kUnroll; k += 2) {
                if (is_filtered[i + kPrefetchDistance + k] == 0) {
                    STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + k]) * ElemSize);
                }
            }

            for (size_t j = 0; j < kUnroll; ++j) {
                if (is_filtered[i + j] == 0) {
                    std::memcpy(d + (i + j) * ElemSize, s + static_cast<size_t>(indexes[i + j]) * ElemSize, ElemSize);
                } else {
                    std::memcpy(d + (i + j) * ElemSize, zero_val, ElemSize);
                }
            }
        }
    }

    for (; i + kUnroll <= num_rows; i += kUnroll) {
        for (size_t j = 0; j < kUnroll; ++j) {
            if (is_filtered[i + j] == 0) {
                std::memcpy(d + (i + j) * ElemSize, s + static_cast<size_t>(indexes[i + j]) * ElemSize, ElemSize);
            } else {
                std::memcpy(d + (i + j) * ElemSize, zero_val, ElemSize);
            }
        }
    }

    for (; i < num_rows; ++i) {
        if (is_filtered[i] == 0) {
            std::memcpy(d + i * ElemSize, s + static_cast<size_t>(indexes[i]) * ElemSize, ElemSize);
        } else {
            std::memcpy(d + i * ElemSize, zero_val, ElemSize);
        }
    }
}

template <typename IndexType>
void gather_filtered_default(void* dest, const void* src, size_t elem_size, const void* zero_val,
                             const IndexType* indexes, const uint8_t* is_filtered, size_t num_rows) {
    auto* d = reinterpret_cast<uint8_t*>(dest);
    const auto* s = reinterpret_cast<const uint8_t*>(src);
    size_t i = 0;
    constexpr size_t kUnroll = 8;
    constexpr size_t kPrefetchDistance = 16;

    if (num_rows >= 8192) {
        for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
            for (size_t k = 0; k < kUnroll; k += 2) {
                if (is_filtered[i + kPrefetchDistance + k] == 0) {
                    STARROCKS_PREFETCH(s + static_cast<size_t>(indexes[i + kPrefetchDistance + k]) * elem_size);
                }
            }

            for (size_t j = 0; j < kUnroll; ++j) {
                if (is_filtered[i + j] == 0) {
                    std::memcpy(d + (i + j) * elem_size, s + static_cast<size_t>(indexes[i + j]) * elem_size,
                                elem_size);
                } else {
                    std::memcpy(d + (i + j) * elem_size, zero_val, elem_size);
                }
            }
        }
    }

    for (; i + kUnroll <= num_rows; i += kUnroll) {
        for (size_t j = 0; j < kUnroll; ++j) {
            if (is_filtered[i + j] == 0) {
                std::memcpy(d + (i + j) * elem_size, s + static_cast<size_t>(indexes[i + j]) * elem_size, elem_size);
            } else {
                std::memcpy(d + (i + j) * elem_size, zero_val, elem_size);
            }
        }
    }

    for (; i < num_rows; ++i) {
        if (is_filtered[i] == 0) {
            std::memcpy(d + i * elem_size, s + static_cast<size_t>(indexes[i]) * elem_size, elem_size);
        } else {
            std::memcpy(d + i * elem_size, zero_val, elem_size);
        }
    }
}

template <typename IndexType>
void gather_bytes_filtered_impl(void* dest, const void* src, size_t elem_size, const void* zero_val,
                                const IndexType* indexes, const uint8_t* is_filtered, size_t num_rows) {
    switch (elem_size) {
    case 1:
        return gather_bytes_filtered_sized<1>(dest, src, zero_val, indexes, is_filtered, num_rows);
    case 2:
        return gather_bytes_filtered_sized<2>(dest, src, zero_val, indexes, is_filtered, num_rows);
    case 4:
        return gather_bytes_filtered_sized<4>(dest, src, zero_val, indexes, is_filtered, num_rows);
    case 8:
        return gather_bytes_filtered_sized<8>(dest, src, zero_val, indexes, is_filtered, num_rows);
    case 12:
        return gather_bytes_filtered_sized<12>(dest, src, zero_val, indexes, is_filtered, num_rows);
    case 16:
        return gather_bytes_filtered_sized<16>(dest, src, zero_val, indexes, is_filtered, num_rows);
    case 24:
        return gather_bytes_filtered_sized<24>(dest, src, zero_val, indexes, is_filtered, num_rows);
    case 32:
        return gather_bytes_filtered_sized<32>(dest, src, zero_val, indexes, is_filtered, num_rows);
    default:
        return gather_filtered_default(dest, src, elem_size, zero_val, indexes, is_filtered, num_rows);
    }
}

} // namespace

namespace detail {

void gather_int16_buckets(uint32_t* b, const int16_t* a, const uint32_t* c, size_t buckets, int num_rows) {
    gather_int16_buckets_impl(b, a, c, buckets, num_rows);
}

void gather_int16_buckets(uint32_t* b, const int16_t* a, const int32_t* c, size_t buckets, int num_rows) {
    gather_int16_buckets_impl(b, a, c, buckets, num_rows);
}

void gather_int16_buckets(int32_t* b, const int16_t* a, const uint32_t* c, size_t buckets, int num_rows) {
    gather_int16_buckets_impl(b, a, c, buckets, num_rows);
}

void gather_int16_buckets(int32_t* b, const int16_t* a, const int32_t* c, size_t buckets, int num_rows) {
    gather_int16_buckets_impl(b, a, c, buckets, num_rows);
}

void gather_bytes(void* dest, const void* src, size_t elem_size, const uint32_t* indexes, size_t num_rows) {
    gather_bytes_impl(dest, src, elem_size, indexes, num_rows);
}

void gather_bytes(void* dest, const void* src, size_t elem_size, const int32_t* indexes, size_t num_rows) {
    gather_bytes_impl(dest, src, elem_size, indexes, num_rows);
}

void gather_bytes_filtered(void* dest, const void* src, size_t elem_size, const void* zero_val, const uint32_t* indexes,
                           const uint8_t* is_filtered, size_t num_rows) {
    gather_bytes_filtered_impl(dest, src, elem_size, zero_val, indexes, is_filtered, num_rows);
}

void gather_bytes_filtered(void* dest, const void* src, size_t elem_size, const void* zero_val, const int32_t* indexes,
                           const uint8_t* is_filtered, size_t num_rows) {
    gather_bytes_filtered_impl(dest, src, elem_size, zero_val, indexes, is_filtered, num_rows);
}

void gather_generic_string(std::string* dest, const std::string* src, const uint32_t* indexes, size_t num_rows) {
    size_t i = 0;
    constexpr size_t kUnroll = 8;
    constexpr size_t kPrefetchDistance = 16;

    if (num_rows >= 8192) {
        for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
            STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 0]]);
            STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 2]]);
            STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 4]]);
            STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + 6]]);

            dest[i + 0] = src[indexes[i + 0]];
            dest[i + 1] = src[indexes[i + 1]];
            dest[i + 2] = src[indexes[i + 2]];
            dest[i + 3] = src[indexes[i + 3]];
            dest[i + 4] = src[indexes[i + 4]];
            dest[i + 5] = src[indexes[i + 5]];
            dest[i + 6] = src[indexes[i + 6]];
            dest[i + 7] = src[indexes[i + 7]];
        }
    }

    for (; i + kUnroll <= num_rows; i += kUnroll) {
        dest[i + 0] = src[indexes[i + 0]];
        dest[i + 1] = src[indexes[i + 1]];
        dest[i + 2] = src[indexes[i + 2]];
        dest[i + 3] = src[indexes[i + 3]];
        dest[i + 4] = src[indexes[i + 4]];
        dest[i + 5] = src[indexes[i + 5]];
        dest[i + 6] = src[indexes[i + 6]];
        dest[i + 7] = src[indexes[i + 7]];
    }

    for (; i < num_rows; ++i) {
        dest[i] = src[indexes[i]];
    }
}

void gather_generic_string_filtered(std::string* dest, const std::string* src, const uint32_t* indexes,
                                    const uint8_t* is_filtered, size_t num_rows) {
    size_t i = 0;
    constexpr size_t kUnroll = 8;
    constexpr size_t kPrefetchDistance = 16;

    if (num_rows >= 8192) {
        for (; i + kPrefetchDistance + kUnroll <= num_rows; i += kUnroll) {
            for (size_t k = 0; k < kUnroll; k += 2) {
                if (is_filtered[i + kPrefetchDistance + k] == 0) {
                    STARROCKS_PREFETCH(&src[indexes[i + kPrefetchDistance + k]]);
                }
            }

            dest[i + 0] = (is_filtered[i + 0] == 0) ? src[indexes[i + 0]] : std::string{};
            dest[i + 1] = (is_filtered[i + 1] == 0) ? src[indexes[i + 1]] : std::string{};
            dest[i + 2] = (is_filtered[i + 2] == 0) ? src[indexes[i + 2]] : std::string{};
            dest[i + 3] = (is_filtered[i + 3] == 0) ? src[indexes[i + 3]] : std::string{};
            dest[i + 4] = (is_filtered[i + 4] == 0) ? src[indexes[i + 4]] : std::string{};
            dest[i + 5] = (is_filtered[i + 5] == 0) ? src[indexes[i + 5]] : std::string{};
            dest[i + 6] = (is_filtered[i + 6] == 0) ? src[indexes[i + 6]] : std::string{};
            dest[i + 7] = (is_filtered[i + 7] == 0) ? src[indexes[i + 7]] : std::string{};
        }
    }

    for (; i + kUnroll <= num_rows; i += kUnroll) {
        dest[i + 0] = (is_filtered[i + 0] == 0) ? src[indexes[i + 0]] : std::string{};
        dest[i + 1] = (is_filtered[i + 1] == 0) ? src[indexes[i + 1]] : std::string{};
        dest[i + 2] = (is_filtered[i + 2] == 0) ? src[indexes[i + 2]] : std::string{};
        dest[i + 3] = (is_filtered[i + 3] == 0) ? src[indexes[i + 3]] : std::string{};
        dest[i + 4] = (is_filtered[i + 4] == 0) ? src[indexes[i + 4]] : std::string{};
        dest[i + 5] = (is_filtered[i + 5] == 0) ? src[indexes[i + 5]] : std::string{};
        dest[i + 6] = (is_filtered[i + 6] == 0) ? src[indexes[i + 6]] : std::string{};
        dest[i + 7] = (is_filtered[i + 7] == 0) ? src[indexes[i + 7]] : std::string{};
    }

    for (; i < num_rows; ++i) {
        dest[i] = (is_filtered[i] == 0) ? src[indexes[i]] : std::string{};
    }
}

} // namespace detail

// Explicit template instantiations for SIMDGather to ensure coverage registration in libBase.a.
// Each instantiation compiles the gather.h inline forwarding wrapper into gather.cpp.o,
// so gcov attributes gather.h line hits to gather.cpp.gcda (under be/src/).

// --- Bucket gather: gather(TB*, const int16_t*, const TC*, size_t, int) ---
// Signed dest + unsigned index
template void SIMDGather::gather<int32_t, uint32_t>(int32_t*, const int16_t*, const uint32_t*, size_t, int);
// Signed dest + signed index
template void SIMDGather::gather<int32_t, int32_t>(int32_t*, const int16_t*, const int32_t*, size_t, int);
// Unsigned dest + unsigned index
template void SIMDGather::gather<uint32_t, uint32_t>(uint32_t*, const int16_t*, const uint32_t*, size_t, int);
// Unsigned dest + signed index (used by parser.cpp)
template void SIMDGather::gather<uint32_t, int32_t>(uint32_t*, const int16_t*, const int32_t*, size_t, int);

// --- Unfiltered gather: gather(DataType*, const DataType*, const IndexType*, size_t) ---
// Trivially copyable types with unsigned 32-bit index
template void SIMDGather::gather<int32_t, uint32_t>(int32_t*, const int32_t*, const uint32_t*, size_t);
template void SIMDGather::gather<uint32_t, uint32_t>(uint32_t*, const uint32_t*, const uint32_t*, size_t);
template void SIMDGather::gather<int64_t, uint32_t>(int64_t*, const int64_t*, const uint32_t*, size_t);
template void SIMDGather::gather<float, uint32_t>(float*, const float*, const uint32_t*, size_t);
template void SIMDGather::gather<double, uint32_t>(double*, const double*, const uint32_t*, size_t);
// Trivially copyable types with signed 32-bit index (covers is_signed_v<IndexType> branch)
template void SIMDGather::gather<int32_t, int32_t>(int32_t*, const int32_t*, const int32_t*, size_t);
// std::string (covers gather_generic_string branch)
template void SIMDGather::gather<std::string, uint32_t>(std::string*, const std::string*, const uint32_t*, size_t);

// --- Filtered gather: gather(DataType*, const DataType*, const IndexType*, const CondType*, size_t) ---
// Trivially copyable types with unsigned 32-bit index + uint8_t filter
template void SIMDGather::gather<int32_t, uint32_t, uint8_t>(int32_t*, const int32_t*, const uint32_t*, const uint8_t*,
                                                             size_t);
template void SIMDGather::gather<uint32_t, uint32_t, uint8_t>(uint32_t*, const uint32_t*, const uint32_t*,
                                                              const uint8_t*, size_t);
template void SIMDGather::gather<int64_t, uint32_t, uint8_t>(int64_t*, const int64_t*, const uint32_t*, const uint8_t*,
                                                             size_t);
// Trivially copyable types with signed 32-bit index + uint8_t filter (covers is_signed_v<IndexType> branch)
template void SIMDGather::gather<int32_t, int32_t, uint8_t>(int32_t*, const int32_t*, const int32_t*, const uint8_t*,
                                                            size_t);
// std::string (covers gather_generic_string_filtered branch)
template void SIMDGather::gather<std::string, uint32_t, uint8_t>(std::string*, const std::string*, const uint32_t*,
                                                                 const uint8_t*, size_t);

} // namespace starrocks

// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>

namespace starrocks::vectorized {
class SIMDUtils {
public:
    template <class T>
    static __m256i set_data(T data) {
        if constexpr (sizeof(T) == 1) {
            return _mm256_set1_epi8(data);
        } else if constexpr (sizeof(T) == 2) {
            return _mm256_set1_epi16(data);
        } else if constexpr (sizeof(T) == 4) {
            return _mm256_set1_epi32(*reinterpret_cast<int32_t*>(&data));
        } else if constexpr (sizeof(T) == 8) {
            return _mm256_set1_epi64x(*reinterpret_cast<int64_t*>(&data));
        } else {
            static_assert(sizeof(T) > 8, "only support sizeof type LE than 8");
        }
    }
};
} // namespace starrocks::vectorized

#endif
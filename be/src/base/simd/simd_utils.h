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

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>

namespace starrocks {
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
} // namespace starrocks

#endif

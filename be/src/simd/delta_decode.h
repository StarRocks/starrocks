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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <cstdint>

namespace starrocks {

#ifdef __AVX2__

// -----------------------------------------------------------------------------
// int32_t / uint32_t version (processes 8 elements per 256‑bit vector)
// -----------------------------------------------------------------------------
inline void delta_decode_chain_avx2_int32(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) noexcept {
    const __m256i v_min_delta = _mm256_set1_epi32(min_delta);
    int32_t running_base = last_value;

    int i = 0;
    constexpr int kVecElems = 8; // 8 × 32‑bit = 256 bit

    // --- vector blocks ---
    for (; i + kVecElems <= n; i += kVecElems) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buf + i));
        v = _mm256_add_epi32(v, v_min_delta); // +min_delta

        // prefix‑sum inside each 128‑bit lane (d0, d0+d1, d0+d1+d2 …)
        __m256i t = _mm256_slli_si256(v, 4);
        v = _mm256_add_epi32(v, t);
        t = _mm256_slli_si256(v, 8);
        v = _mm256_add_epi32(v, t);

        // carry low‑lane sum into high lane
        const int32_t carry = _mm256_extract_epi32(v, 3);
        __m128i hi = _mm256_extracti128_si256(v, 1);
        hi = _mm_add_epi32(hi, _mm_set1_epi32(carry));
        v = _mm256_inserti128_si256(v, hi, 1);

        // add running_base
        v = _mm256_add_epi32(v, _mm256_set1_epi32(running_base));

        _mm256_storeu_si256(reinterpret_cast<__m256i*>(buf + i), v);
        running_base = _mm256_extract_epi32(v, 7);
    }

    // --- scalar tail ---
    for (; i < n; ++i) {
        running_base += buf[i] + min_delta;
        buf[i] = running_base;
    }

    last_value = running_base;
}

// -----------------------------------------------------------------------------
// int64_t / uint64_t version (processes 4 elements per 256‑bit vector)
// -----------------------------------------------------------------------------
inline void delta_decode_chain_avx2_int64(int64_t* buf, int n, int64_t min_delta, int64_t& last_value) noexcept {
    const __m256i v_min_delta = _mm256_set1_epi64x(min_delta);
    int64_t running_base = last_value;

    int i = 0;
    constexpr int kVecElems = 4; // 4 × 64‑bit = 256 bit

    for (; i + kVecElems <= n; i += kVecElems) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buf + i));
        v = _mm256_add_epi64(v, v_min_delta); // +min_delta

        // lane‑local prefix‑sum (128‑bit lane) : shift 1 element
        __m256i t = _mm256_slli_si256(v, 8);
        v = _mm256_add_epi64(v, t);
        // second shift would be beyond lane for 2‑element lane, so done.

        // carry low lane sum into high lane
        const int64_t carry = _mm256_extract_epi64(v, 1);
        __m128i hi = _mm256_extracti128_si256(v, 1);
        hi = _mm_add_epi64(hi, _mm_set1_epi64x(carry));
        v = _mm256_inserti128_si256(v, hi, 1);

        // add running_base
        v = _mm256_add_epi64(v, _mm256_set1_epi64x(running_base));

        _mm256_storeu_si256(reinterpret_cast<__m256i*>(buf + i), v);
        running_base = _mm256_extract_epi64(v, 3);
    }

    // scalar tail
    for (; i < n; ++i) {
        running_base += buf[i] + min_delta;
        buf[i] = running_base;
    }

    last_value = running_base;
}

#endif

// -----------------------------------------------------------------------------
// Optional: trivial scalar fallback (same signature) for reference / testing
// -----------------------------------------------------------------------------
template <typename T>
inline void delta_decode_chain_scalar(T* buf, int n, T min_delta, T& last_value) noexcept {
    T acc = last_value;
    for (int i = 0; i < n; ++i) {
        acc += buf[i] + min_delta;
        buf[i] = acc;
    }
    last_value = acc;
}
} // namespace starrocks
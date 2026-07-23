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

#include "base/simd/delta_decode.h"

#if defined(__x86_64__)
#include <immintrin.h>
#endif

// The AVX2 / AVX-512 kernels below carry per-function `target` attributes, so the compiler makes
// each ISA available for just that function regardless of the global instruction baseline (the
// binary can be built without -mavx2 / -mavx512f and still contain these kernels). The lambdas
// inside the AVX2 kernel repeat the attribute because a lambda's operator() does NOT inherit the
// enclosing function's target attribute. The public dispatchers stay on the baseline and only call
// a kernel after __builtin_cpu_supports confirms the CPU has it, so a single portable binary runs
// AVX-512 at runtime on capable CPUs while still loading on older ones. This replaces the previous
// GCC function-multiversioning layout, which (a) could not be built unless -mavx512f was global and
// (b) collapsed to a compile-time pick of the global baseline (never dispatching at runtime).

namespace starrocks {

#if defined(__x86_64__)

#define DD_TARGET_AVX2 __attribute__((target("avx2")))
// int32/int64 512-bit kernels need only AVX-512F (safe to dispatch on avx512f alone, incl. KNL).
#define DD_TARGET_AVX512F __attribute__((target("avx512f")))
// The alternate 256-bit AVX2 kernel uses a few AVX-512VL ops, so it needs AVX-512VL (implies F).
#define DD_TARGET_AVX512VL __attribute__((target("avx512f,avx512vl")))

// =========================
// reference: https://en.algorithmica.org/hpc/algorithms/prefix/
// int32 / uint32_t version
DD_TARGET_AVX2 void delta_decode_chain_int32_avx2(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    using v4i = __m128i;
    using v8i = __m256i;

    // avx2 instructions.
    const v8i v_min_delta = _mm256_set1_epi32(min_delta);
    auto prefix = [&](int32_t* p) DD_TARGET_AVX2 {
        v8i x = _mm256_loadu_si256((v8i*)p);
        x = _mm256_add_epi32(x, v_min_delta);
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 4));
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 8));
        _mm256_storeu_si256((v8i*)p, x);
    };

    // sse2 instructions.
    auto accumulate = [](int32_t* p, v4i s) DD_TARGET_AVX2 {
        v4i x = _mm_loadu_si128((v4i*)p);
        x = _mm_add_epi32(s, x);
        _mm_storeu_si128((v4i*)p, x);
        return _mm_shuffle_epi32(x, _MM_SHUFFLE(3, 3, 3, 3));
    };

    int sz = (n / 8) * 8;
    if (sz > 0) {
        // two passes, don't mixed use avx2 and sse2.
        for (int i = 0; i < sz; i += 8) {
            prefix(buf + i);
        }
        v4i s = _mm_set1_epi32(last_value);
        for (int i = 0; i < sz; i += 4) {
            s = accumulate(buf + i, s);
        }
        // any index is ok.
        last_value = _mm_extract_epi32(s, 0);
    }

    for (int i = sz; i < n; i++) {
        buf[i] += last_value + min_delta;
        last_value = buf[i];
    }
}

// Though we handle 256bit as a unit, we still use some instructions of avx512f + avx512vl.
DD_TARGET_AVX512VL static __m256i prefix_and_accumulate_int32_avx2(int32_t* p, __m256i s, const __m256i& v_min_delta,
                                                                   const __m256i& v_zero, const __m256i& v_perm7) {
    __m256i x = _mm256_loadu_si256((__m256i*)p);
    x = _mm256_add_epi32(x, v_min_delta);
    x = _mm256_add_epi32(x, _mm256_alignr_epi32(x, v_zero, 8 - 1));
    x = _mm256_add_epi32(x, _mm256_alignr_epi32(x, v_zero, 8 - 2));
    x = _mm256_add_epi32(x, _mm256_alignr_epi32(x, v_zero, 8 - 4));
    // accumulate
    x = _mm256_add_epi32(s, x);
    _mm256_storeu_si256((__m256i*)p, x);
    // return last value.
    return _mm256_permutevar8x32_epi32(x, v_perm7);
}

DD_TARGET_AVX512VL void delta_decode_chain_int32_avx2x(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    using v8i = __m256i;

    // avx2 instructions.
    const v8i v_min_delta = _mm256_set1_epi32(min_delta);
    const v8i v_zero = _mm256_setzero_si256();
    const v8i v_perm7 = _mm256_set1_epi32(7);

    int sz = (n / 8) * 8;
    if (sz > 0) {
        v8i s = _mm256_set1_epi32(last_value);
        for (int i = 0; i < sz; i += 8) {
            s = prefix_and_accumulate_int32_avx2(buf + i, s, v_min_delta, v_zero, v_perm7);
        }
        last_value = _mm256_extract_epi32(s, 0);
    }

    for (int i = sz; i < n; i++) {
        buf[i] += last_value + min_delta;
        last_value = buf[i];
    }
}

// reference: https://www.adms-conf.org/2020-camera-ready/ADMS20_05.pdf
DD_TARGET_AVX512F static __m512i prefix_and_accumulate_int32_avx512(int32_t* p, __m512i s, const __m512i& v_min_delta,
                                                                    const __m512i& v_zero, const __m512i& v_perm15) {
    // prefix
    __m512i x = _mm512_loadu_si512(p);
    x = _mm512_add_epi32(x, v_min_delta);
    x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 1));
    x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 2));
    x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 4));
    x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 8));
    // accumulate
    x = _mm512_add_epi32(s, x);
    _mm512_storeu_si512(p, x);
    // return last value.
    return _mm512_permutexvar_epi32(v_perm15, x);
}

DD_TARGET_AVX512F void delta_decode_chain_int32_avx512(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    using v4i = __m128i;
    using v16i = __m512i;

    // avx512 instructions.
    const v16i v_min_delta = _mm512_set1_epi32(min_delta);
    const v16i v_zero = _mm512_setzero_si512();
    const v16i v_perm15 = _mm512_set1_epi32(15);

    int sz = (n / 16) * 16;
    if (sz > 0) {
        v16i s = _mm512_set1_epi32(last_value);
        for (int i = 0; i < sz; i += 16) {
            s = prefix_and_accumulate_int32_avx512(buf + i, s, v_min_delta, v_zero, v_perm15);
        }
        v4i s2 = _mm512_castsi512_si128(s);
        last_value = _mm_extract_epi32(s2, 0);
    }

    for (int i = sz; i < n; i++) {
        buf[i] += last_value + min_delta;
        last_value = buf[i];
    }
}

// ==========================================

DD_TARGET_AVX512F static __m512i prefix_and_accumulate_int64_avx512(int64_t* p, __m512i s, const __m512i& v_min_delta,
                                                                    const __m512i& v_zero, const __m512i& v_perm7) {
    // prefix
    __m512i x = _mm512_loadu_si512(p);
    x = _mm512_add_epi64(x, v_min_delta);
    x = _mm512_add_epi64(x, _mm512_alignr_epi64(x, v_zero, 8 - 1));
    x = _mm512_add_epi64(x, _mm512_alignr_epi64(x, v_zero, 8 - 2));
    x = _mm512_add_epi64(x, _mm512_alignr_epi64(x, v_zero, 8 - 4));
    // accumulate
    x = _mm512_add_epi64(s, x);
    _mm512_storeu_si512((__m512i*)p, x);
    // return last value.
    return _mm512_permutexvar_epi64(v_perm7, x);
}

DD_TARGET_AVX512F void delta_decode_chain_int64_avx512(int64_t* buf, int n, int64_t min_delta, int64_t& last_value) {
    using v4i = __m128i;
    using v16i = __m512i;

    // avx512 instructions.
    const v16i v_min_delta = _mm512_set1_epi64(min_delta);
    const v16i v_zero = _mm512_setzero_si512();
    const v16i v_perm7 = _mm512_set1_epi64(7);

    int sz = (n / 8) * 8;
    if (sz > 0) {
        v16i s = _mm512_set1_epi64(last_value);
        for (int i = 0; i < sz; i += 8) {
            s = prefix_and_accumulate_int64_avx512(buf + i, s, v_min_delta, v_zero, v_perm7);
        }
        v4i s2 = _mm512_castsi512_si128(s);
        last_value = _mm_extract_epi64(s2, 0);
    }

    for (int i = sz; i < n; i++) {
        buf[i] += last_value + min_delta;
        last_value = buf[i];
    }
}

#undef DD_TARGET_AVX2
#undef DD_TARGET_AVX512F
#undef DD_TARGET_AVX512VL

#endif // defined(__x86_64__)

// ==========================================
// Public entry points: pick the best kernel for the running CPU at runtime.
//
// The AVX-512 path is skipped under ASan: it spills zmm registers to a stack slot ASan aligns to 32
// (not 64) bytes, so the store touches the redzone and traps. See https://github.com/llvm/llvm-project/issues/91565.

void delta_decode_chain_int32(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
#if defined(__x86_64__)
#if !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f")) {
        delta_decode_chain_int32_avx512(buf, n, min_delta, last_value);
        return;
    }
#endif
    if (__builtin_cpu_supports("avx2")) {
        delta_decode_chain_int32_avx2(buf, n, min_delta, last_value);
        return;
    }
#endif
    delta_decode_chain_scalar_prefetch<int32_t>(buf, n, min_delta, last_value);
}

void delta_decode_chain_int64(int64_t* buf, int n, int64_t min_delta, int64_t& last_value) {
#if defined(__x86_64__) && !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f")) {
        delta_decode_chain_int64_avx512(buf, n, min_delta, last_value);
        return;
    }
#endif
    delta_decode_chain_scalar_prefetch<int64_t>(buf, n, min_delta, last_value);
}

} // namespace starrocks

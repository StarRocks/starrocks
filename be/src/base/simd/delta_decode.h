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

#include <cstdint>

#include "base/simd/multi_version.h"

namespace starrocks {

static inline bool contains_negative_value(const int32_t* data, size_t size) {
    size_t i = 0;
#ifdef __AVX2__
    __m256i v_zero = _mm256_setzero_si256();
    for (; i + 8 <= size; i += 8) {
        __m256i v = _mm256_loadu_si256((__m256i*)(data + i));
        __m256i cmp = _mm256_cmpgt_epi32(v_zero, v);
        if (_mm256_movemask_epi8(cmp)) return true;
    }
#endif
    for (; i < size; ++i) {
        if (data[i] < 0) return true;
    }
    return false;
}

template <typename T>
static inline void delta_decode_chain_scalar_prefetch(T* buf, int n, T min_delta, T& last_value) noexcept {
    T acc = last_value;
    int i = 0;
    for (i = 0; (i + 8) < n; ++i) {
        __builtin_prefetch(buf + i + 8);
        acc += buf[i] + min_delta;
        buf[i] = acc;
    }
    for (; i < n; ++i) {
        acc += buf[i] + min_delta;
        buf[i] = acc;
    }
    last_value = acc;
}

// =========================
// reference: https://en.algorithmica.org/hpc/algorithms/prefix/
// int32 / uint32_t version
MFV_AVX2(void delta_decode_chain_int32_avx2(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    using v4i = __m128i;
    using v8i = __m256i;

    // avx2 instructions.
    const v8i v_min_delta = _mm256_set1_epi32(min_delta);
    auto prefix = [&](int32_t* p) {
        v8i x = _mm256_loadu_si256((v8i*)p);
        x = _mm256_add_epi32(x, v_min_delta);
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 4));
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 8));
        _mm256_storeu_si256((v8i*)p, x);
    };

    // sse2 instructions.
    auto accumulate = [](int32_t* p, v4i s) {
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
});

// Though we handle 256bit as a unit, we still use some instructions of avx512f + avx512vl.
MFV_AVX512VL(__m256i prefix_and_accumulate_int32_avx2(int32_t* p, __m256i s, const __m256i& v_min_delta,
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
});

MFV_AVX512VL(void delta_decode_chain_int32_avx2x(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    using v4i = __m128i;
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
});

MFV_AVX512F(__m512i prefix_and_accumulate_int32_avx512(int32_t* p, __m512i s, const __m512i& v_min_delta,
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
});

// reference: https://www.adms-conf.org/2020-camera-ready/ADMS20_05.pdf
MFV_AVX512F(void delta_decode_chain_int32_avx512(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    using v4i = __m128i;
    using v8i = __m256i;
    using v16i = __m512i;

    // avx512 instructions.
    const v16i v_min_delta = _mm512_set1_epi32(min_delta);
    const v16i v_zero = _mm512_setzero_si512();
    const v16i v_perm15 = _mm512_set1_epi32(15);
    // auto prefix = [&](int32_t* p) {
    //     v16i x = _mm512_loadu_si512((v8i*)p);
    //     x = _mm512_add_epi32(x, v_min_delta);
    //     x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 1));
    //     x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 2));
    //     x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 4));
    //     x = _mm512_add_epi32(x, _mm512_alignr_epi32(x, v_zero, 16 - 8));
    //     _mm512_storeu_si512((v16i*)p, x);
    // };

    // auto accumulate = [&](int32_t* p, v16i s) {
    //     v16i x = _mm512_loadu_si512((v16i*)p);
    //     x = _mm512_add_epi32(s, x);
    //     _mm512_storeu_si512((v16i*)p, x);
    //     return _mm512_permutexvar_epi32(v_perm15, x);
    // };

    int sz = (n / 16) * 16;
    if (sz > 0) {
        // for (int i = 0; i < sz; i += 16) {
        //     prefix(buf + i);
        // }
        // v16i s = _mm512_set1_epi32(last_value);
        // for (int i = 0; i < sz; i += 16) {
        //     s = accumulate(buf + i, s);
        // }

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
});

MFV_AVX2(void delta_decode_chain_int32(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    return delta_decode_chain_int32_avx2(buf, n, min_delta, last_value);
});

// https://github.com/llvm/llvm-project/issues/91565
// In ASAN mode, this function will cause SIGSEGV because of touching redzone.
// But according to ChatGPT's explanation, it's a bug of ASAN. ASAN allocas stack space aligned to 32 bytes not 64 bytes,
// so when spill zmm registers, it will touch redzone. and assembly code is like following:
/*
   0x0000000029b7382e <+331>:   je     0x29b7383d <starrocks::delta_decode_chain_int32_avx512(int32_t*, int, int32_t, int32_t&)+346>
   0x0000000029b73830 <+333>:   mov    $0x40,%esi
   0x0000000029b73835 <+338>:   mov    %rax,%rdi
   0x0000000029b73838 <+341>:   call   0x19824bf0 <__asan_report_store_n>
=> 0x0000000029b7383d <+346>:   vmovdqa64 %zmm0,-0x180(%r13)
   0x0000000029b73844 <+353>:   vpxor  %xmm0,%xmm0,%xmm0
   0x0000000029b73848 <+357>:   lea    -0x100(%r13),%rax
   0x0000000029b7384f <+364>:   mov    %rax,%rdx
   0x0000000029b73852 <+367>:   shr    $0x3,%rdx
   0x0000000029b73856 <+371>:   add    $0x7fff8000,%rdx
   */
// to resolve this problem, we have to force stack alignment to 64 bytes, which is not supported in our compiler right now.

#if !defined(ADDRESS_SANITIZER)

MFV_AVX512F(void delta_decode_chain_int32(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    return delta_decode_chain_int32_avx512(buf, n, min_delta, last_value);
});

#endif

MFV_DEFAULT(void delta_decode_chain_int32(int32_t* buf, int n, int32_t min_delta, int32_t& last_value) {
    return delta_decode_chain_scalar_prefetch<int32_t>(buf, n, min_delta, last_value);
});

// ==========================================

MFV_AVX512F(__m512i prefix_and_accumulate_int64_avx512(int64_t* p, __m512i s, const __m512i& v_min_delta,
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
});

MFV_AVX512F(void delta_decode_chain_int64_avx512(int64_t* buf, int n, int64_t min_delta, int64_t& last_value) {
    using v4i = __m128i;
    using v8i = __m256i;
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
});

#if !defined(ADDRESS_SANITIZER)

MFV_AVX512F(void delta_decode_chain_int64(int64_t* buf, int n, int64_t min_delta, int64_t& last_value) {
    return delta_decode_chain_int64_avx512(buf, n, min_delta, last_value);
});

#endif

MFV_DEFAULT(void delta_decode_chain_int64(int64_t* buf, int n, int64_t min_delta, int64_t& last_value) {
    return delta_decode_chain_scalar_prefetch<int64_t>(buf, n, min_delta, last_value);
});

} // namespace starrocks

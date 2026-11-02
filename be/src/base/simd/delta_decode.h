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

#if defined(__x86_64__) && defined(__AVX2__)
#include <immintrin.h>
#endif

namespace starrocks {

static inline bool contains_negative_value(const int32_t* data, size_t size) {
    size_t i = 0;
#ifdef __AVX2__
    for (; i + 8 <= size; i += 8) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        if (_mm256_movemask_ps(_mm256_castsi256_ps(v))) return true;
    }
#endif
    for (; i < size; ++i) {
        if (data[i] < 0) return true;
    }
    return false;
}

template <typename T>
inline void delta_decode_chain_scalar_prefetch(T* buf, int n, T min_delta, T& last_value) noexcept {
    using U = std::make_unsigned_t<T>;
    U acc = static_cast<U>(last_value);
    const U u_min_delta = static_cast<U>(min_delta);
    int i = 0;
    for (i = 0; (i + 8) < n; ++i) {
        __builtin_prefetch(buf + i + 8);
        acc += static_cast<U>(buf[i]) + u_min_delta;
        buf[i] = static_cast<T>(acc);
    }
    for (; i < n; ++i) {
        acc += static_cast<U>(buf[i]) + u_min_delta;
        buf[i] = static_cast<T>(acc);
    }
    last_value = static_cast<T>(acc);
}

// Portable scalar reference implementations, defined in delta_decode.cpp.
void delta_decode_chain_int32_scalar(int32_t* buf, int n, int32_t min_delta, int32_t& last_value);
void delta_decode_chain_int64_scalar(int64_t* buf, int n, int64_t min_delta, int64_t& last_value);

// ISA-specific implementations, defined in delta_decode.cpp. Each kernel carries a per-function
// `target` attribute so its AVX2 / AVX-512 intrinsics are available even when the global build
// targets a lower baseline (e.g. USE_AVX512=OFF). Declared here for the runtime dispatchers below
// and the microbenchmark.
//
// This replaces the previous GCC function-multiversioning (FMV) layout in this header. FMV emitted
// these as a same-named IFUNC family gated behind `#if defined(__AVX512F__)`, so the AVX-512 path
// only existed when the whole binary was built with -mavx512f and otherwise collapsed to a
// compile-time pick of the global baseline (it never dispatched at runtime).
void delta_decode_chain_int32_avx2(int32_t* buf, int n, int32_t min_delta, int32_t& last_value);
void delta_decode_chain_int32_avx2x(int32_t* buf, int n, int32_t min_delta, int32_t& last_value);
void delta_decode_chain_int32_avx512(int32_t* buf, int n, int32_t min_delta, int32_t& last_value);
void delta_decode_chain_int64_avx512(int64_t* buf, int n, int64_t min_delta, int64_t& last_value);

// Public entry points. Defined in delta_decode.cpp; they pick the best implementation for the
// running CPU at runtime via __builtin_cpu_supports (explicit dispatch, not FMV/IFUNC).
void delta_decode_chain_int32(int32_t* buf, int n, int32_t min_delta, int32_t& last_value);
void delta_decode_chain_int64(int64_t* buf, int n, int64_t min_delta, int64_t& last_value);

} // namespace starrocks

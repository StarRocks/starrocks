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
#include <cstdint>
#include <limits>

#include "base/simd/multi_version.h"

namespace starrocks {

// ============================================================================
// Trivial std::fill_n wrappers. The compiler emits an AVX2 / AVX-512 / NEON
// broadcast-store sequence from these at -O3; no hand-rolled SIMD variant.
// ============================================================================

inline void simd_fill_int16(int16_t* __restrict dst, int16_t value, int32_t count) {
    std::fill_n(dst, count, value);
}

inline void simd_fill_int32(int32_t* __restrict dst, int32_t value, int32_t count) {
    std::fill_n(dst, count, value);
}

// ============================================================================
// Scalar fallbacks. Kept file-local in each TU that includes this header so
// callers and unit tests can exercise the scalar path directly without going
// through the dispatcher. The dispatcher in rle_simd.cpp also calls its own
// file-local copy.
// ============================================================================

MFV_AUTOVEC_STATIC(void simd_minmax_int32_default(const int32_t* __restrict data, int32_t count, int32_t& out_min,
                                                  int32_t& out_max) {
    // Reduce into locals so the compiler can hoist running min/max into
    // registers; writing to the reference parameters in the loop blocks
    // auto-vectorisation because `data` may alias `&out_min`/`&out_max`.
    int32_t local_min = std::numeric_limits<int32_t>::max();
    int32_t local_max = std::numeric_limits<int32_t>::min();
    for (int32_t i = 0; i < count; ++i) {
        local_min = std::min(local_min, data[i]);
        local_max = std::max(local_max, data[i]);
    }
    out_min = local_min;
    out_max = local_max;
})

MFV_AUTOVEC_STATIC(void simd_dict_gather_int32_default(int32_t* __restrict dest, const int32_t* __restrict dict,
                                                       const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = dict[indices[i]];
})

MFV_AUTOVEC_STATIC(void simd_dict_gather_int64_default(int64_t* __restrict dest, const int64_t* __restrict dict,
                                                       const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = dict[indices[i]];
})

MFV_AUTOVEC_STATIC(void simd_dict_gather_float_default(float* __restrict dest, const float* __restrict dict,
                                                       const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = dict[indices[i]];
})

MFV_AUTOVEC_STATIC(void simd_dict_gather_double_default(double* __restrict dest, const double* __restrict dict,
                                                        const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = dict[indices[i]];
})

MFV_AUTOVEC_STATIC(void simd_widen_int8_to_int32_default(int32_t* __restrict dest, const int8_t* __restrict src,
                                                         int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
})

MFV_AUTOVEC_STATIC(void simd_widen_int16_to_int32_default(int32_t* __restrict dest, const int16_t* __restrict src,
                                                          int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
})

// ============================================================================
// Public dispatchers. Implementations and their hand-rolled MFV_AVX2 /
// MFV_AVX512F helpers live in rle_simd.cpp so intrinsic headers stay out of
// the inclusion graph for callers.
// ============================================================================

void simd_minmax_int32(const int32_t* __restrict data, int32_t count, int32_t& out_min, int32_t& out_max);

void simd_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                            const uint32_t* __restrict indices, int32_t count);
void simd_dict_gather_int64(int64_t* __restrict dest, const int64_t* __restrict dict,
                            const uint32_t* __restrict indices, int32_t count);
void simd_dict_gather_float(float* __restrict dest, const float* __restrict dict, const uint32_t* __restrict indices,
                            int32_t count);
void simd_dict_gather_double(double* __restrict dest, const double* __restrict dict, const uint32_t* __restrict indices,
                             int32_t count);

void simd_widen_int8_to_int32(int32_t* __restrict dest, const int8_t* __restrict src, int32_t count);
void simd_widen_int16_to_int32(int32_t* __restrict dest, const int16_t* __restrict src, int32_t count);

} // namespace starrocks

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

// Only x86 support function multiversion.
// https://gcc.gnu.org/wiki/FunctionMultiVersioning
// TODO(GoHalo) Support aarch64 platform.
#if defined(__GNUC__) && defined(__x86_64__)
#include <x86intrin.h>

// Variadic forwarding so the macro accepts function definitions whose bodies
// contain top-level commas (e.g. aggregate-initialised LUTs inside `{ ... }`).
#define MFV_IMPL(ATTR, ...)                                                                \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wunused-function\"") \
            ATTR static __VA_ARGS__ _Pragma("GCC diagnostic pop")

#if defined(__SSE4_2__)
#define MFV_SSE42(...) MFV_IMPL(__attribute__((target("sse4.2"))), __VA_ARGS__)
#endif

#if defined(__AVX2__)
#define MFV_AVX2(...) MFV_IMPL(__attribute__((target("avx2"))), __VA_ARGS__)
#endif

#if defined(__AVX512F__)
#define MFV_AVX512F(...) MFV_IMPL(__attribute__((target("avx512f"))), __VA_ARGS__)
#endif

#if defined(__AVX512BW__)
#define MFV_AVX512BW(...) MFV_IMPL(__attribute__((target("avx512bw"))), __VA_ARGS__)
#endif

#if defined(__AVX512VL__)
#define MFV_AVX512VL(...) MFV_IMPL(__attribute__((target("avx512vl"))), __VA_ARGS__)
#endif

#if defined(__AVX512F__) && defined(__AVX512BW__) && defined(__AVX512VL__)
#define MFV_AVX512VLBW(...) MFV_IMPL(__attribute__((target("avx512f,avx512vl,avx512bw"))), __VA_ARGS__)
#endif

// Use this for the "default" version of a function multi-version *family*
// (same function name as the sibling MFV_SSE42/MFV_AVX2/MFV_AVX512* variants).
// GCC's FMV requires every version to carry a `target` attribute so the IFUNC
// resolver can distinguish them.
#define MFV_DEFAULT(...) MFV_IMPL(__attribute__((target("default"))), __VA_ARGS__)

// Use this for a stand-alone scalar fallback with a UNIQUE name (e.g. the
// `simd_*_default` helpers in rle_simd.h that are picked by a compile-time
// `#if defined(__AVX2__)` dispatcher, not by FMV). It inherits the build's
// global -m flags so the compiler can auto-vectorise the body to AVX2 on
// USE_AVX512=OFF and to AVX-512 on USE_AVX512=ON. Using MFV_DEFAULT here
// instead would attach `target("default")` to the function, overriding the
// global flags and silently emitting SSE2-only code, making the "fallback"
// slower than an undecorated loop on the same build.
#define MFV_AUTOVEC_STATIC(...) MFV_IMPL(/* inherit global -m flags */, __VA_ARGS__)

#endif // end of defined(__GNUC__) && defined(__x86_64__)

#if !defined(MFV_SSE42)
#define MFV_SSE42(...)
#endif

#if !defined(MFV_AVX2)
#define MFV_AVX2(...)
#endif

#if !defined(MFV_AVX512F)
#define MFV_AVX512F(...)
#endif

#if !defined(MFV_AVX512BW)
#define MFV_AVX512BW(...)
#endif

#if !defined(MFV_AVX512VL)
#define MFV_AVX512VL(...)
#endif

#if !defined(MFV_AVX512VLBW)
#define MFV_AVX512VLBW(...)
#endif

#if !defined(MFV_DEFAULT)
// Non-x86 fallback: mark as `static` to match the x86 MFV_IMPL linkage, so that
// helpers defined via MFV_DEFAULT in headers (e.g. base/simd/rle_simd.h,
// base/simd/delta_decode.h) don't trigger duplicate-definition link errors when
// included from multiple translation units on aarch64 / other targets.
#define MFV_DEFAULT(...) static __VA_ARGS__
#endif

#if !defined(MFV_AUTOVEC_STATIC)
#define MFV_AUTOVEC_STATIC(...) static __VA_ARGS__
#endif

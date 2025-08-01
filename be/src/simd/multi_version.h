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

#define MFV_IMPL(IMPL, ATTR)                                                               \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wunused-function\"") \
            ATTR static IMPL _Pragma("GCC diagnostic pop")

#if defined(__SSE4_2__)
#define MFV_SSE42(IMPL) MFV_IMPL(IMPL, __attribute__((target("sse4.2"))))
#endif

#if defined(__AVX2__)
#define MFV_AVX2(IMPL) MFV_IMPL(IMPL, __attribute__((target("avx2"))))
#endif

#if defined(__AVX512F__)
#define MFV_AVX512F(IMPL) MFV_IMPL(IMPL, __attribute__((target("avx512f"))))
#endif

#if defined(__AVX512BW__)
#define MFV_AVX512BW(IMPL) MFV_IMPL(IMPL, __attribute__((target("avx512bw"))))
#endif

#if defined(__AVX512VL__)
#define MFV_AVX512VL(IMPL) MFV_IMPL(IMPL, __attribute__((target("avx512vl"))))
#endif

#if defined(__AVX512F__) && defined(__AVX512BW__) && defined(__AVX512VL__)
#define MFV_AVX512VLBW(IMPL) MFV_IMPL(IMPL, __attribute__((target("avx512f,avx512vl,avx512bw"))))
#endif

#define MFV_DEFAULT(IMPL) MFV_IMPL(IMPL, __attribute__((target("default"))))

#endif // end of defined(__GNUC__) && defined(__x86_64__)

#if !defined(MFV_SSE42)
#define MFV_SSE42(IMPL)
#endif

#if !defined(MFV_AVX2)
#define MFV_AVX2(IMPL)
#endif

#if !defined(MFV_AVX512F)
#define MFV_AVX512F(IMPL)
#endif

#if !defined(MFV_AVX512BW)
#define MFV_AVX512BW(IMPL)
#endif

#if !defined(MFV_AVX512VL)
#define MFV_AVX512VL(IMPL)
#endif

#if !defined(MFV_AVX512VLBW)
#define MFV_AVX512VLBW(IMPL)
#endif

#if !defined(MFV_DEFAULT)
#define MFV_DEFAULT(IMPL) IMPL
#endif

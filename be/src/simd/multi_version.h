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
            ATTR static inline IMPL _Pragma("GCC diagnostic pop")

#define MFV_SSE42(IMPL) MFV_IMPL(IMPL, __attribute__((target("sse4.2"))))
#define MFV_AVX2(IMPL) MFV_IMPL(IMPL, __attribute__((target("avx2"))))
#define MFV_AVX512(IMPL) MFV_IMPL(IMPL, __attribute__((target("avx512f,avx512bw"))))
#define MFV_DEFAULT(IMPL) MFV_IMPL(IMPL, __attribute__((target("default"))))

#else

#define MFV_SSE42(IMPL)
#define MFV_AVX2(IMPL)
#define MFV_AVX512(IMPL)
#define MFV_DEFAULT(IMPL) IMPL

#endif

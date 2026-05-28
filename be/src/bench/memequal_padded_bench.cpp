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

// Per-size microbench for memequal_padded vs the pre-PR SSE2 form and vs libc
// std::memcmp. The SSE2 reference is kept inline so we can compare it against
// the shipping AVX2 implementation in the same binary; the production
// memequal_padded picks whichever path the target macros select.
//
// Five scenarios per size:
//   * equal       -- buffers identical, full sweep (worst case for SIMD loop)
//   * short_diff  -- diverge at byte 1 (easiest for both, mostly tests overhead)
//   * mid_diff    -- diverge at byte (size/2), typical hash-join collision
//   * tail_diff   -- diverge at byte (size-1), worst case for early-exit logic
//   * len_diff    -- size1 != size2, instant false
//
// The 256-byte boundary is the cutoff where memequal_padded short-circuits to
// std::memcmp (libc ERMS beats the hand AVX2 loop on bulk equality, ~1.6x at
// 4096 bytes). The 32-byte boundary is where the function leaves the SSE2
// fallback path and enters the AVX2 main loop. Sizes are chosen to straddle
// both.

#include <benchmark/benchmark.h>

#include <cstring>
#include <random>
#include <type_traits>
#include <vector>

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

#include "column/column_hash.h"

namespace starrocks {

namespace {

#if defined(__SSE2__)
// Pre-PR SSE2 baseline, kept here verbatim so the bench can compare against
// the shipping AVX2 implementation in one binary. x86-only -- the _mm_*
// intrinsics do not exist on non-SSE2 targets (e.g. aarch64).
template <typename T>
typename std::enable_if<sizeof(T) == 1, bool>::type sse2_memequal_padded(const T* p1, size_t size1, const T* p2,
                                                                         size_t size2) {
    if (size1 != size2) return false;
    for (size_t offset = 0; offset < size1; offset += 16) {
        uint16_t mask =
                _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1 + offset)),
                                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2 + offset))));
        mask = ~mask;
        if (mask) {
            offset += __builtin_ctz(mask);
            return offset >= size1;
        }
    }
    return true;
}
#endif

enum Scenario { kEqual = 0, kShortDiff = 1, kMidDiff = 2, kTailDiff = 3, kLenDiff = 4 };

struct Buffers {
    std::vector<uint8_t> a;
    std::vector<uint8_t> b;
    size_t s1;
    size_t s2;
    Buffers(size_t size, Scenario sc)
            : a(size + SLICE_MEMEQUAL_OVERFLOW_PADDING, 0),
              b(size + SLICE_MEMEQUAL_OVERFLOW_PADDING, 0),
              s1(size),
              s2(size) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> g(0, 255);
        for (size_t i = 0; i < size; ++i) {
            a[i] = b[i] = static_cast<uint8_t>(g(rng));
        }
        switch (sc) {
        case kEqual:
            break;
        case kShortDiff:
            if (size > 1) {
                b[1] ^= 0xFFu;
            } else if (size > 0) {
                b[0] ^= 0xFFu;
            }
            break;
        case kMidDiff:
            if (size > 0) b[size / 2] ^= 0xFFu;
            break;
        case kTailDiff:
            if (size > 0) b[size - 1] ^= 0xFFu;
            break;
        case kLenDiff:
            s2 = size + 1;
            break;
        }
    }
};

} // namespace

#if defined(__SSE2__)
template <Scenario sc>
static void BM_SSE2(benchmark::State& state) {
    Buffers buf(state.range(0), sc);
    bool acc = false;
    for (auto _ : state) {
        acc ^= sse2_memequal_padded(buf.a.data(), buf.s1, buf.b.data(), buf.s2);
        benchmark::DoNotOptimize(acc);
    }
}
#endif

template <Scenario sc>
static void BM_AVX2(benchmark::State& state) {
    Buffers buf(state.range(0), sc);
    bool acc = false;
    for (auto _ : state) {
        acc ^= memequal_padded(buf.a.data(), buf.s1, buf.b.data(), buf.s2);
        benchmark::DoNotOptimize(acc);
    }
}

template <Scenario sc>
static void BM_Memcmp(benchmark::State& state) {
    Buffers buf(state.range(0), sc);
    bool acc = false;
    for (auto _ : state) {
        acc ^= (buf.s1 == buf.s2) && (std::memcmp(buf.a.data(), buf.b.data(), buf.s1) == 0);
        benchmark::DoNotOptimize(acc);
    }
}

// clang-format off
static void register_all() {
    static constexpr int kSizes[] = {4, 8, 16, 24, 31, 32, 33, 48, 64, 96, 127, 128, 256, 1024, 4096};
    auto add = [](auto* b) {
        for (int s : kSizes) b->Arg(s);
    };
#if defined(__SSE2__)
    // x86-only pre-PR baseline.
    add(benchmark::RegisterBenchmark("SSE2_Equal",       BM_SSE2<kEqual>));
    add(benchmark::RegisterBenchmark("SSE2_ShortDiff",   BM_SSE2<kShortDiff>));
    add(benchmark::RegisterBenchmark("SSE2_MidDiff",     BM_SSE2<kMidDiff>));
    add(benchmark::RegisterBenchmark("SSE2_TailDiff",    BM_SSE2<kTailDiff>));
    add(benchmark::RegisterBenchmark("SSE2_LenDiff",     BM_SSE2<kLenDiff>));
#endif
    add(benchmark::RegisterBenchmark("AVX2_Equal",       BM_AVX2<kEqual>));
    add(benchmark::RegisterBenchmark("AVX2_ShortDiff",   BM_AVX2<kShortDiff>));
    add(benchmark::RegisterBenchmark("AVX2_MidDiff",     BM_AVX2<kMidDiff>));
    add(benchmark::RegisterBenchmark("AVX2_TailDiff",    BM_AVX2<kTailDiff>));
    add(benchmark::RegisterBenchmark("AVX2_LenDiff",     BM_AVX2<kLenDiff>));
    add(benchmark::RegisterBenchmark("Memcmp_Equal",     BM_Memcmp<kEqual>));
    add(benchmark::RegisterBenchmark("Memcmp_ShortDiff", BM_Memcmp<kShortDiff>));
    add(benchmark::RegisterBenchmark("Memcmp_MidDiff",   BM_Memcmp<kMidDiff>));
    add(benchmark::RegisterBenchmark("Memcmp_TailDiff",  BM_Memcmp<kTailDiff>));
    add(benchmark::RegisterBenchmark("Memcmp_LenDiff",   BM_Memcmp<kLenDiff>));
}
// clang-format on

static int registered = (register_all(), 0);

} // namespace starrocks

BENCHMARK_MAIN();

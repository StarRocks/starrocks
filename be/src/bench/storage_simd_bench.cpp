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

// Microbenchmarks for the storage-layer SIMD paths added by PR #73292
// ([Enhancement] Add SIMD optimizations for storage layer).
//
// Covers the path unique to this PR:
//   * persistent_index :: get_matched_tag_idxes
//       SSE2 16-byte cmpeq -> AVX2 32-byte cmpeq for primary-key tag scan
//
// (padding_char_column's adaptive sparse-null branch reuses the same
// count_nonzero / > N/8 pattern already exercised by agg_simd_bench in
// PR #73290 and rle_page's batch-decode is covered by the existing
// parquet_dict_decode_bench end-to-end.)

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__SSE2__)
// scalar_get_matched_sse2() uses _mm_* intrinsics under #if defined(__SSE2__);
// on `-msse2 -mno-avx2` builds __SSE2__ is set but __AVX2__ is not, so the
// AVX2-gated include above isn't enough. Pull in <emmintrin.h> for that case.
#include <emmintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

namespace starrocks {

// =====================================================================
// persistent_index :: get_matched_tag_idxes
// =====================================================================
//
// Used by the persistent-PK index to find positions in a per-bucket tag
// vector that equal a probe tag. ntag is a multiple of 16 (one bucket).
// state.range(0) is the number of matches expected, in percent of ntag.

static constexpr size_t kBucketTags = 256; // covers AVX2 8x32B and NEON 16x16B

static size_t scalar_get_matched_sse2(const uint8_t* tags, size_t ntag, uint8_t tag, uint8_t* out) {
    size_t nmatched = 0;
#if defined(__SSE2__)
    auto tests = _mm_set1_epi8(tag);
    for (size_t i = 0; i < ntag; i += 16) {
        auto tags16 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tags + i));
        auto eqs = _mm_cmpeq_epi8(tags16, tests);
        uint16_t mask = static_cast<uint16_t>(_mm_movemask_epi8(eqs));
        while (mask != 0) {
            uint32_t match_pos = __builtin_ctz(mask);
            if (i + match_pos < ntag) out[nmatched++] = static_cast<uint8_t>(i + match_pos);
            mask &= (mask - 1);
        }
    }
#else
    for (size_t i = 0; i < ntag; ++i) {
        if (tags[i] == tag) out[nmatched++] = static_cast<uint8_t>(i);
    }
#endif
    return nmatched;
}

static size_t simd_get_matched(const uint8_t* tags, size_t ntag, uint8_t tag, uint8_t* out) {
    size_t nmatched = 0;
    size_t i = 0;
#ifdef __AVX2__
    auto tests = _mm256_set1_epi8(tag);
    for (; i + 32 <= ntag; i += 32) {
        auto tags32 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(tags + i));
        auto eqs = _mm256_cmpeq_epi8(tags32, tests);
        auto mask = static_cast<uint32_t>(_mm256_movemask_epi8(eqs));
        while (mask != 0) {
            uint32_t match_pos = __builtin_ctz(mask);
            out[nmatched++] = static_cast<uint8_t>(i + match_pos);
            mask &= (mask - 1);
        }
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    uint8x16_t tests = vdupq_n_u8(tag);
    for (; i + 16 <= ntag; i += 16) {
        uint8x16_t tags16 = vld1q_u8(tags + i);
        uint8x16_t eqs = vceqq_u8(tags16, tests);
        uint64_t lo = vgetq_lane_u64(vreinterpretq_u64_u8(eqs), 0);
        uint64_t hi = vgetq_lane_u64(vreinterpretq_u64_u8(eqs), 1);
        while (lo != 0) {
            int bit_pos = __builtin_ctzll(lo);
            out[nmatched++] = static_cast<uint8_t>(i + (bit_pos >> 3));
            lo &= ~(0xFFULL << bit_pos);
        }
        while (hi != 0) {
            int bit_pos = __builtin_ctzll(hi);
            out[nmatched++] = static_cast<uint8_t>(i + 8 + (bit_pos >> 3));
            hi &= ~(0xFFULL << bit_pos);
        }
    }
#endif
    for (; i < ntag; ++i) {
        if (tags[i] == tag) out[nmatched++] = static_cast<uint8_t>(i);
    }
    return nmatched;
}

static void prepare_tags(std::vector<uint8_t>& tags, int match_ratio_percent) {
    tags.resize(kBucketTags);
    std::mt19937_64 rng(0xA15C0DE);
    std::uniform_int_distribution<int> d(0, 99);
    for (auto& v : tags) v = (d(rng) < match_ratio_percent) ? 0xA5 : 0x00;
}

static void BM_MatchTags_SSE2(benchmark::State& state) {
    std::vector<uint8_t> tags;
    prepare_tags(tags, static_cast<int>(state.range(0)));
    std::vector<uint8_t> out(kBucketTags);
    for (auto _ : state) {
        size_t n = scalar_get_matched_sse2(tags.data(), tags.size(), 0xA5, out.data());
        benchmark::DoNotOptimize(n);
    }
}

static void BM_MatchTags_SIMD(benchmark::State& state) {
    std::vector<uint8_t> tags;
    prepare_tags(tags, static_cast<int>(state.range(0)));
    std::vector<uint8_t> out(kBucketTags);
    for (auto _ : state) {
        size_t n = simd_get_matched(tags.data(), tags.size(), 0xA5, out.data());
        benchmark::DoNotOptimize(n);
    }
}

BENCHMARK(BM_MatchTags_SSE2)->Arg(0)->Arg(1)->Arg(10)->Arg(50)->Arg(100);
BENCHMARK(BM_MatchTags_SIMD)->Arg(0)->Arg(1)->Arg(10)->Arg(50)->Arg(100);

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/storage_simd_bench`.
// =====================================================================

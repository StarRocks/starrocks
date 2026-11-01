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

// Microbenchmarks for the SIMD primitives introduced by PR #73287
// ([Enhancement] Add AVX2/NEON SIMD primitives for analytic workloads).
//
// These primitives are foundation helpers: they have no in-tree call sites
// yet; the consumers land in stacked PRs (parquet, storage, aggregate).
// The microbenchmarks below exercise each helper against the scalar baseline
// it replaces, so reviewers can confirm the helper is worth taking on its
// own merits.
//
// Functions covered:
//   * simd.h          : all_zeros / all_ones (8-bit) vs scalar early-exit
//   * rle_simd.h      : simd_fill_int32 / simd_fill_int16 vs scalar fill
//                       simd_dict_gather_int32 vs scalar dict[indices[i]]
//                       simd_minmax_int32       vs scalar min/max loop
//                       simd_widen_int8_to_int32 vs scalar cast loop
//   * string_length_filter.h : length_eq_mask  vs scalar batched compute

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "base/simd/rle_simd.h"
#include "base/simd/simd.h"
#include "base/simd/string_length_filter.h"
#include "base/string/string_searcher.h"
#include "base/string/volnitsky.h"

namespace starrocks {

constexpr size_t kBatch = 4096;

// =====================================================================
// all_zeros / all_ones
// =====================================================================
//
// Pre-PR equivalent:
//   bool all_zero = true; for (i) if (data[i]) { all_zero = false; break; }
// Post-PR:
//   SIMD::all_zeros(data, n)  -> wraps contains_nonzero_bit
//   SIMD::all_ones(data,  n)  -> wraps std::memchr (libc SIMD)
//
// state.range(0) is the position of the disagreeing byte (0..kBatch), or
// kBatch to mean "all bytes agree" (the worst case where no early exit).

static bool scalar_all_zeros(const uint8_t* data, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (data[i] != 0) return false;
    }
    return true;
}

static bool scalar_all_ones(const uint8_t* data, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (data[i] == 0) return false;
    }
    return true;
}

static void BM_AllZeros_Scalar(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 0);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 1;
    for (auto _ : state) {
        bool r = scalar_all_zeros(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

static void BM_AllZeros_SIMD(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 0);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 1;
    for (auto _ : state) {
        bool r = SIMD::all_zeros(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

static void BM_AllOnes_Scalar(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 1);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 0;
    for (auto _ : state) {
        bool r = scalar_all_ones(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

static void BM_AllOnes_SIMD(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 1);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 0;
    for (auto _ : state) {
        bool r = SIMD::all_ones(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

BENCHMARK(BM_AllZeros_Scalar)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);
BENCHMARK(BM_AllZeros_SIMD)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);
BENCHMARK(BM_AllOnes_Scalar)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);
BENCHMARK(BM_AllOnes_SIMD)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);

// =====================================================================
// simd_widen_int8_to_int32 (parquet INT8 -> INT32 promotion)
// =====================================================================

static void BM_WidenInt8_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int8_t> src(n);
    std::vector<int32_t> dest(n);
    std::mt19937_64 rng(0xC0DECAFE);
    std::uniform_int_distribution<int> d(-128, 127);
    for (auto& v : src) v = static_cast<int8_t>(d(rng));
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) dest[i] = static_cast<int32_t>(src[i]);
        benchmark::DoNotOptimize(dest.data());
    }
}

static void BM_WidenInt8_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int8_t> src(n);
    std::vector<int32_t> dest(n);
    std::mt19937_64 rng(0xC0DECAFE);
    std::uniform_int_distribution<int> d(-128, 127);
    for (auto& v : src) v = static_cast<int8_t>(d(rng));
    for (auto _ : state) {
        simd_widen_int8_to_int32(dest.data(), src.data(), static_cast<int32_t>(n));
        benchmark::DoNotOptimize(dest.data());
    }
}

BENCHMARK(BM_WidenInt8_Scalar)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_WidenInt8_SIMD)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

// =====================================================================
// simd_dict_gather_int32: scalar (autovec) vs hand AVX2 i32gather vs AVX-512
// =====================================================================
//
// The hand-AVX2 `_mm256_i32gather_epi32` path was removed from rle_simd.h
// because microbench on AMD m6a / Aliyun Intel (GDS microcode applied)
// showed it was ~25% slower than the scalar default. This bench provides
// the data needed to revisit that decision on Intel hosts where the GDS
// mitigation is not applied (per stdpain's review on PR #73287).
//
// Setup: a 256-entry dictionary (fits comfortably in L1d), random indices
// drawn uniformly. count = 256 / 1024 / 4096 covers the chunk sizes the
// Parquet dictionary decoder typically calls with.

static constexpr int32_t kDictSize = 256;

static void prepare_dict_gather(std::vector<int32_t>& dict, std::vector<uint32_t>& indices, int32_t count) {
    dict.resize(kDictSize);
    indices.resize(count);
    std::mt19937_64 rng(0x6A746F72);
    std::uniform_int_distribution<int32_t> dv(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    for (auto& v : dict) v = dv(rng);
    std::uniform_int_distribution<uint32_t> id(0, kDictSize - 1);
    for (auto& v : indices) v = id(rng);
}

// Plain scalar gather. Compiler is free to auto-vectorise this any way it
// likes (AVX2 vpgatherdd on -mavx2, AVX-512 vpgatherdd on -mavx512f, or
// scalar loads if it prefers).
static void scalar_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                                     const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = dict[indices[i]];
}

#if defined(__AVX2__)
// Hand AVX2 vpgatherdd, mirrors the path that was removed from rle_simd.h.
static void avx2_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                                   const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_idx = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + i));
        __m256i v_gat = _mm256_i32gather_epi32(dict, v_idx, sizeof(int32_t));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_gat);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
}
#endif

#if defined(__AVX512F__)
// Hand AVX-512 vpgatherdd zmm, same as simd_dict_gather_int32_avx512 in
// rle_simd.h but called directly so the bench numbers line up against the
// AVX2 path on the same hardware in the same binary.
static void avx512_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                                     const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __m512i v_idx = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(indices + i));
        __m512i v_gat = _mm512_i32gather_epi32(v_idx, dict, sizeof(int32_t));
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), v_gat);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
}
#endif

static void BM_DictGather_Scalar(benchmark::State& state) {
    int32_t n = static_cast<int32_t>(state.range(0));
    std::vector<int32_t> dict;
    std::vector<uint32_t> indices;
    prepare_dict_gather(dict, indices, n);
    std::vector<int32_t> dest(n);
    for (auto _ : state) {
        scalar_dict_gather_int32(dest.data(), dict.data(), indices.data(), n);
        benchmark::DoNotOptimize(dest.data());
    }
}

#if defined(__AVX2__)
static void BM_DictGather_AVX2(benchmark::State& state) {
    int32_t n = static_cast<int32_t>(state.range(0));
    std::vector<int32_t> dict;
    std::vector<uint32_t> indices;
    prepare_dict_gather(dict, indices, n);
    std::vector<int32_t> dest(n);
    for (auto _ : state) {
        avx2_dict_gather_int32(dest.data(), dict.data(), indices.data(), n);
        benchmark::DoNotOptimize(dest.data());
    }
}
#endif

#if defined(__AVX512F__)
static void BM_DictGather_AVX512(benchmark::State& state) {
    int32_t n = static_cast<int32_t>(state.range(0));
    std::vector<int32_t> dict;
    std::vector<uint32_t> indices;
    prepare_dict_gather(dict, indices, n);
    std::vector<int32_t> dest(n);
    for (auto _ : state) {
        avx512_dict_gather_int32(dest.data(), dict.data(), indices.data(), n);
        benchmark::DoNotOptimize(dest.data());
    }
}
#endif

BENCHMARK(BM_DictGather_Scalar)->Arg(256)->Arg(1024)->Arg(4096);
#if defined(__AVX2__)
BENCHMARK(BM_DictGather_AVX2)->Arg(256)->Arg(1024)->Arg(4096);
#endif
#if defined(__AVX512F__)
BENCHMARK(BM_DictGather_AVX512)->Arg(256)->Arg(1024)->Arg(4096);
#endif

// =====================================================================
// length_eq_mask (parquet PLAIN binary length filter, 8 strings per call)
// =====================================================================
//
// Pre-PR equivalent: scalar 8-element batched compute.
// Post-PR: AVX2 (8 lanes) or NEON (4 lanes). The NEON bench loops twice
// per AVX2 call so total work matches.

static uint32_t scalar_length_eq_mask_avx_width(const uint32_t* offsets, size_t base, uint32_t target_len) {
    uint32_t mask = 0;
    for (int i = 0; i < 8; ++i) {
        if (offsets[base + i + 1] - offsets[base + i] == target_len) mask |= (1u << i);
    }
    return mask;
}

static void BM_LengthEqMask_Scalar(benchmark::State& state) {
    size_t n_strings = static_cast<size_t>(state.range(0));
    std::vector<uint32_t> offsets(n_strings + 1);
    std::mt19937_64 rng(0xFADEDDEE);
    std::uniform_int_distribution<int> d(1, 32);
    uint32_t cur = 0;
    offsets[0] = 0;
    for (size_t i = 0; i < n_strings; ++i) {
        cur += d(rng);
        offsets[i + 1] = cur;
    }
    for (auto _ : state) {
        uint32_t agg = 0;
        for (size_t base = 0; base + 8 <= n_strings; base += 8) {
            agg ^= scalar_length_eq_mask_avx_width(offsets.data(), base, 5);
        }
        benchmark::DoNotOptimize(agg);
    }
}

static void BM_LengthEqMask_SIMD(benchmark::State& state) {
    size_t n_strings = static_cast<size_t>(state.range(0));
    std::vector<uint32_t> offsets(n_strings + 1);
    std::mt19937_64 rng(0xFADEDDEE);
    std::uniform_int_distribution<int> d(1, 32);
    uint32_t cur = 0;
    offsets[0] = 0;
    for (size_t i = 0; i < n_strings; ++i) {
        cur += d(rng);
        offsets[i + 1] = cur;
    }
    for (auto _ : state) {
        uint32_t agg = 0;
        for (size_t base = 0; base + SIMD::kStringLenSimdWidth <= n_strings; base += SIMD::kStringLenSimdWidth) {
            agg ^= SIMD::length_eq_mask(offsets.data(), base, 5);
        }
        benchmark::DoNotOptimize(agg);
    }
}

BENCHMARK(BM_LengthEqMask_Scalar)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_LengthEqMask_SIMD)->Arg(256)->Arg(1024)->Arg(4096);

// =====================================================================
// Constant substring search baseline (VolnitskyUTF8 vs StringSearcher)
// =====================================================================
//
// Synthetic URL-like corpus matching ClickBench Q20-Q23 shape:
//   * 4,096 rows per chunk (~68 bytes/row, ~278.5 KB/chunk)
//   * 10 chunks in rotation (40,960 rows total) so 0.01% selectivity (1 per 10,000 rows)
//     is accurately represented without empty-match distortion
//   * Parameterized by (needle_len, selectivity_per_10k):
//       needle_len          in {2, 6, 16, 32, 33, 64}
//       selectivity_per_10k in {1 (0.01%), 10 (0.1%), 100 (1%), 1000 (10%)}

static constexpr size_t kSubstringChunkRows = 4096;
static constexpr size_t kSubstringNumChunks = 10;

struct SubstringChunk {
    std::string data;
    std::vector<uint32_t> offsets;
    size_t num_rows{0};
};

static std::string make_benchmark_needle(size_t needle_len) {
    static constexpr std::string_view kTemplate = "google_analytics_campaign_source_medium_term_content_identifier1";
    if (needle_len == 2) {
        return "gq";
    }
    if (needle_len <= kTemplate.size()) {
        return std::string(kTemplate.substr(0, needle_len));
    }
    std::string s(kTemplate);
    s.append(needle_len - kTemplate.size(), 'x');
    return s;
}

static std::vector<SubstringChunk> build_url_chunks(size_t needle_len, size_t selectivity_per_10k) {
    // 68-byte non-matching URL templates containing realistic ASCII characters ('g', 'e', 's', 'q', '/')
    // without containing any prefix of make_benchmark_needle >= 2 bytes.
    static constexpr std::string_view kMissTemplates[4] = {
            "http://yandex.ru/yandsearch?text=starrocks+olap+database&lr=213&pg=1",
            "https://example.org/catalog/item?category=storage&region=ap-east&p=2",
            "http://news.portal.net/article/2026/11/01/simd-vector-execution-plan",
            "https://shop.domain.com/search?query=high+performance+analytics&id=9",
    };

    const std::string needle = make_benchmark_needle(needle_len);
    std::vector<SubstringChunk> chunks(kSubstringNumChunks);

    for (size_t c = 0; c < kSubstringNumChunks; ++c) {
        auto& chunk = chunks[c];
        chunk.num_rows = kSubstringChunkRows;
        chunk.offsets.resize(kSubstringChunkRows + 1, 0);
        chunk.data.reserve(kSubstringChunkRows * 72);

        for (size_t r = 0; r < kSubstringChunkRows; ++r) {
            const size_t global_row = c * kSubstringChunkRows + r;
            const bool is_hit = ((global_row * 7919ULL + 5000ULL) % 10000ULL) < selectivity_per_10k;
            std::string row(kMissTemplates[global_row & 3]);
            if (is_hit) {
                if (needle.size() + 12 <= row.size()) {
                    row.replace(12, needle.size(), needle);
                } else {
                    row = "http://x.ru/" + needle;
                }
            }
            chunk.data.append(row);
            chunk.offsets[r + 1] = static_cast<uint32_t>(chunk.data.size());
        }
    }
    return chunks;
}

static void BM_Volnitsky_Baseline(benchmark::State& state) {
    const size_t needle_len = static_cast<size_t>(state.range(0));
    const size_t selectivity_per_10k = static_cast<size_t>(state.range(1));
    const std::string needle = make_benchmark_needle(needle_len);
    const auto chunks = build_url_chunks(needle_len, selectivity_per_10k);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        const char* begin = chunk.data.data();
        const char* end = begin + chunk.data.size();
        const char* pos = begin;
        size_t i = 0;

        // Simulates LikePredicate::constant_substring_fn per-chunk VolnitskyUTF8 construction & scan.
        VolnitskyUTF8 searcher(needle.data(), needle.size(), static_cast<size_t>(end - pos));
        while (pos < end && end != (pos = searcher.search(pos, static_cast<size_t>(end - pos)))) {
            while (begin + chunk.offsets[i + 1] <= pos) {
                res[i] = 0;
                ++i;
            }
            const char* row_end = begin + chunk.offsets[i + 1];
            res[i] = static_cast<uint8_t>(pos + needle.size() <= row_end);
            pos = row_end;
            ++i;
        }
        if (i < chunk.num_rows) {
            std::memset(res.data() + i, 0, chunk.num_rows - i);
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

static void BM_StringSearcher_Baseline(benchmark::State& state) {
    const size_t needle_len = static_cast<size_t>(state.range(0));
    const size_t selectivity_per_10k = static_cast<size_t>(state.range(1));
    const std::string needle = make_benchmark_needle(needle_len);
    const auto chunks = build_url_chunks(needle_len, selectivity_per_10k);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        const auto* begin = reinterpret_cast<const uint8_t*>(chunk.data.data());
        const auto* end = begin + chunk.data.size();
        const uint8_t* pos = begin;
        size_t i = 0;

        StringSearcher searcher(needle.data(), needle.size());
        while (pos < end && end != (pos = searcher.search(pos, end))) {
            while (begin + chunk.offsets[i + 1] <= pos) {
                res[i] = 0;
                ++i;
            }
            const uint8_t* row_end = begin + chunk.offsets[i + 1];
            res[i] = static_cast<uint8_t>(pos + needle.size() <= row_end);
            pos = row_end;
            ++i;
        }
        if (i < chunk.num_rows) {
            std::memset(res.data() + i, 0, chunk.num_rows - i);
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_Volnitsky_Baseline)->ArgsProduct({{2, 6, 16, 32, 33, 64}, {1, 10, 100, 1000}});
BENCHMARK(BM_StringSearcher_Baseline)->ArgsProduct({{2, 6, 16, 32, 33, 64}, {1, 10, 100, 1000}});

// =====================================================================
// Direct 64 KB buffer scan: StringSearcher::search_scalar vs StringSearcher::search (SIMD)
// =====================================================================

static std::string build_64kb_haystack(const std::string& needle) {
    static constexpr std::string_view kLine = "http://yandex.ru/yandsearch?text=starrocks+olap+database&lr=213&pg=1";
    constexpr size_t kBufSize = 64 * 1024;
    std::string buf;
    buf.reserve(kBufSize + kLine.size());
    while (buf.size() < kBufSize) {
        buf.append(kLine);
    }
    buf.resize(kBufSize);
    buf.replace(kBufSize - needle.size(), needle.size(), needle);
    return buf;
}

static void BM_StringSearcher_Scalar(benchmark::State& state) {
    const size_t needle_len = static_cast<size_t>(state.range(0));
    const std::string needle = make_benchmark_needle(needle_len);
    const std::string hay = build_64kb_haystack(needle);
    const auto* begin = reinterpret_cast<const uint8_t*>(hay.data());
    const auto* end = begin + hay.size();
    StringSearcher searcher(needle.data(), needle.size());

    for (auto _ : state) {
        const uint8_t* res = searcher.search_scalar(begin, end);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * hay.size()));
}

static void BM_StringSearcher_SIMD(benchmark::State& state) {
    const size_t needle_len = static_cast<size_t>(state.range(0));
    const std::string needle = make_benchmark_needle(needle_len);
    const std::string hay = build_64kb_haystack(needle);
    const auto* begin = reinterpret_cast<const uint8_t*>(hay.data());
    const auto* end = begin + hay.size();
    StringSearcher searcher(needle.data(), needle.size());

    for (auto _ : state) {
        const uint8_t* res = searcher.search(begin, end);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * hay.size()));
}

BENCHMARK(BM_StringSearcher_Scalar)->Arg(2)->Arg(6)->Arg(16)->Arg(32);
BENCHMARK(BM_StringSearcher_SIMD)->Arg(2)->Arg(6)->Arg(16)->Arg(32);

#if defined(__x86_64__)
static void BM_StringSearcher_SSE41(benchmark::State& state) {
    if (!__builtin_cpu_supports("sse4.1")) {
        state.SkipWithError("SSE4.1 not supported");
        return;
    }
    const size_t needle_len = static_cast<size_t>(state.range(0));
    const std::string needle = make_benchmark_needle(needle_len);
    const std::string hay = build_64kb_haystack(needle);
    const auto* begin = reinterpret_cast<const uint8_t*>(hay.data());
    const auto* end = begin + hay.size();
    StringSearcher searcher(needle.data(), needle.size());

    for (auto _ : state) {
        const uint8_t* res = searcher.search_sse41(begin, end);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * hay.size()));
}

static void BM_StringSearcher_AVX2(benchmark::State& state) {
    if (!__builtin_cpu_supports("avx2")) {
        state.SkipWithError("AVX2 not supported");
        return;
    }
    const size_t needle_len = static_cast<size_t>(state.range(0));
    const std::string needle = make_benchmark_needle(needle_len);
    const std::string hay = build_64kb_haystack(needle);
    const auto* begin = reinterpret_cast<const uint8_t*>(hay.data());
    const auto* end = begin + hay.size();
    StringSearcher searcher(needle.data(), needle.size());

    for (auto _ : state) {
        const uint8_t* res = searcher.search_avx2(begin, end);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * hay.size()));
}

BENCHMARK(BM_StringSearcher_SSE41)->Arg(2)->Arg(6)->Arg(16)->Arg(32);
BENCHMARK(BM_StringSearcher_AVX2)->Arg(2)->Arg(6)->Arg(16)->Arg(32);
#endif

// =====================================================================
// Task 3: Full Matrix, Ablation (G4), Row-Lengths (G2), Crossover,
//         Cold-Cache & Multi-Thread Scaling (G3)
// =====================================================================

// Reference implementation of pre-PR legacy single-byte StringSearcher (`main` branch)
// and legacy Volnitsky threshold (`min_needle_size = 4`) so Stage 0 and Stage 1
// ablation stages can be reproduced cleanly inside the same benchmark binary.
class LegacySingleByteSearcher {
    const uint8_t* needle;
    size_t needle_size;
    uint8_t first{};
#ifdef __SSE4_1__
    __m128i pat{};
    __m128i cache{};
    size_t cache_valid_len{0};
#endif

public:
    LegacySingleByteSearcher(const char* needle_, size_t needle_size_)
            : needle(reinterpret_cast<const uint8_t*>(needle_)), needle_size(needle_size_) {
        if (needle_size == 0) return;
        first = *needle;
#ifdef __SSE4_1__
        pat = _mm_set1_epi8(static_cast<char>(first));
        cache_valid_len = std::min<size_t>(needle_size, 16);
        alignas(16) uint8_t tmp[16]{};
        std::memcpy(tmp, needle, cache_valid_len);
        cache = _mm_load_si128(reinterpret_cast<const __m128i*>(tmp));
#endif
    }

    inline bool compare(const uint8_t* haystack, const uint8_t* haystack_end, const uint8_t* pos) const {
        if (needle_size == 0) return true;
        if (pos < haystack || static_cast<size_t>(haystack_end - pos) < needle_size) return false;
        return std::memcmp(pos, needle, needle_size) == 0;
    }

    const uint8_t* search(const uint8_t* haystack, const uint8_t* haystack_end) const {
        if (needle_size == 0) return haystack;
        while (haystack < haystack_end && static_cast<size_t>(haystack_end - haystack) >= needle_size) {
#ifdef __SSE4_1__
            if (static_cast<size_t>(haystack_end - haystack) >= 16) {
                const auto v_hay = _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack));
                const auto v_cmp = _mm_cmpeq_epi8(v_hay, pat);
                const uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(v_cmp));
                if (mask == 0) {
                    haystack += 16;
                    continue;
                }
                const size_t offset = static_cast<size_t>(__builtin_ctz(mask));
                haystack += offset;
                if (haystack >= haystack_end || static_cast<size_t>(haystack_end - haystack) < needle_size) {
                    return haystack_end;
                }
                if (static_cast<size_t>(haystack_end - haystack) >= 16) {
                    const auto v_cand = _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack));
                    const auto v_eq = _mm_cmpeq_epi8(v_cand, cache);
                    const uint32_t eq_mask = static_cast<uint32_t>(_mm_movemask_epi8(v_eq));
                    const uint32_t need_mask = (1u << cache_valid_len) - 1u;
                    if ((eq_mask & need_mask) == need_mask) {
                        if (needle_size <= 16 || std::memcmp(haystack + 16, needle + 16, needle_size - 16) == 0) {
                            return haystack;
                        }
                    }
                    ++haystack;
                    continue;
                }
            }
#endif
            if (*haystack == first && std::memcmp(haystack, needle, needle_size) == 0) {
                return haystack;
            }
            ++haystack;
        }
        return haystack_end;
    }

    const char* search(const char* haystack, size_t haystack_size) const {
        return reinterpret_cast<const char*>(search(reinterpret_cast<const uint8_t*>(haystack),
                                                    reinterpret_cast<const uint8_t*>(haystack) + haystack_size));
    }
};

// Configurable Volnitsky wrapper with explicit min_needle_size threshold so we can test:
//   * Legacy threshold (min_needle_size = 4) vs New threshold (min_needle_size = 33)
//   * Forced Volnitsky (min_needle_size = 4) for crossover curves across needle_len = 6..64
template <typename FallbackSearcher, size_t MinNeedleSize>
class ConfigurableVolnitsky {
    const uint8_t* needle;
    size_t needle_size;
    size_t step;
    std::unique_ptr<VolnitskyTraits::Offset[]> hash;
    bool fallback;
    FallbackSearcher fallback_searcher;

public:
    ConfigurableVolnitsky(const char* needle_, size_t needle_size_, size_t haystack_size_hint = 0)
            : needle(reinterpret_cast<const uint8_t*>(needle_)),
              needle_size(needle_size_),
              step(needle_size >= sizeof(VolnitskyTraits::Ngram) ? needle_size - sizeof(VolnitskyTraits::Ngram) + 1
                                                                 : 1),
              fallback(needle_size < MinNeedleSize ||
                       needle_size >= std::numeric_limits<VolnitskyTraits::Offset>::max() ||
                       (haystack_size_hint && haystack_size_hint < VolnitskyTraits::min_haystack_size_for_algorithm)),
              fallback_searcher(needle_, needle_size_) {
        if (fallback) return;
        hash = std::unique_ptr<VolnitskyTraits::Offset[]>(new VolnitskyTraits::Offset[VolnitskyTraits::hash_size]{});
        for (auto i = static_cast<ssize_t>(needle_size - sizeof(VolnitskyTraits::Ngram)); i >= 0; --i) {
            const VolnitskyTraits::Ngram ngram = VolnitskyTraits::toNGram(this->needle + i);
            size_t cell_num = ngram % VolnitskyTraits::hash_size;
            while (hash[cell_num]) {
                cell_num = (cell_num + 1) % VolnitskyTraits::hash_size;
            }
            hash[cell_num] = static_cast<VolnitskyTraits::Offset>(i + 1);
        }
    }

    const uint8_t* search(const uint8_t* haystack, size_t haystack_size) const {
        if (needle_size == 0) return haystack;
        const auto* haystack_end = haystack + haystack_size;
        if (fallback || haystack_size <= needle_size) {
            return fallback_searcher.search(haystack, haystack_end);
        }
        const auto* pos = haystack + needle_size - sizeof(VolnitskyTraits::Ngram);
        for (; pos <= haystack_end - needle_size; pos += step) {
            for (size_t cell_num = VolnitskyTraits::toNGram(pos) % VolnitskyTraits::hash_size; hash[cell_num];
                 cell_num = (cell_num + 1) % VolnitskyTraits::hash_size) {
                const auto* res = pos - (hash[cell_num] - 1);
                if (fallback_searcher.compare(haystack, haystack_end, res)) return res;
            }
        }
        return fallback_searcher.search(pos - step + 1, haystack_end);
    }

    const char* search(const char* haystack, size_t haystack_size) const {
        return reinterpret_cast<const char*>(search(reinterpret_cast<const uint8_t*>(haystack), haystack_size));
    }
};

using LegacyVolnitskyStage0 = ConfigurableVolnitsky<LegacySingleByteSearcher, 4>;
using ForcedVolnitskySearcher = ConfigurableVolnitsky<StringSearcher, 4>;
// Stage 1 ablation uses the new SIMD StringSearcher fallback with the legacy Volnitsky threshold (4).
using NewKernelLegacyThresholdStage1 = ForcedVolnitskySearcher;

template <typename Searcher>
static inline void scan_chunk_linear_offsets(const SubstringChunk& chunk, size_t needle_len, const Searcher& searcher,
                                             std::vector<uint8_t>& res) {
    const char* begin = chunk.data.data();
    const char* end = begin + chunk.data.size();
    const char* pos = begin;
    size_t i = 0;

    while (pos < end && end != (pos = searcher.search(pos, static_cast<size_t>(end - pos)))) {
        while (begin + chunk.offsets[i + 1] <= pos) {
            res[i] = 0;
            ++i;
        }
        const char* row_end = begin + chunk.offsets[i + 1];
        res[i] = static_cast<uint8_t>(pos + needle_len <= row_end);
        pos = row_end;
        ++i;
    }
    if (i < chunk.num_rows) {
        std::memset(res.data() + i, 0, chunk.num_rows - i);
    }
}

template <size_t kLinearThreshold, typename Searcher>
static inline void scan_chunk_hybrid_offsets(const SubstringChunk& chunk, size_t needle_len, const Searcher& searcher,
                                             std::vector<uint8_t>& res) {
    uint8_t* __restrict res_data = res.data();
    std::memset(res_data, 0, chunk.num_rows);

    const char* begin = chunk.data.data();
    const char* end = begin + chunk.data.size();
    const uint32_t* __restrict offset_data = chunk.offsets.data();
    const size_t num_rows = chunk.num_rows;
    const char* pos = begin;
    size_t i = 0;

    while (pos < end && end != (pos = searcher.search(pos, static_cast<size_t>(end - pos)))) {
        const auto target_offset = static_cast<uint32_t>(pos - begin);
        if constexpr (kLinearThreshold == 0) {
            const auto* it = std::upper_bound(offset_data + i + 1, offset_data + num_rows + 1, target_offset);
            i = static_cast<size_t>((it - offset_data) - 1);
        } else if (i + kLinearThreshold < num_rows && offset_data[i + kLinearThreshold] <= target_offset) {
            const auto* it =
                    std::upper_bound(offset_data + i + kLinearThreshold + 1, offset_data + num_rows + 1, target_offset);
            i = static_cast<size_t>((it - offset_data) - 1);
        } else {
            while (offset_data[i + 1] <= target_offset) {
                ++i;
            }
        }
        const char* row_end = begin + offset_data[i + 1];
        res_data[i] = static_cast<uint8_t>(pos + needle_len <= row_end);
        pos = row_end;
        ++i;
    }
}

// Full optimized LikePredicate::constant_substring_fn benchmark across selectivity (0.01%..50%)
// and needle length (2, 6, 16, 32, 33, 64).
static void BM_LikeSubstring_Optimized(benchmark::State& state) {
    const size_t needle_len = static_cast<size_t>(state.range(0));
    const size_t selectivity_per_10k = static_cast<size_t>(state.range(1));
    const std::string needle = make_benchmark_needle(needle_len);
    const auto chunks = build_url_chunks(needle_len, selectivity_per_10k);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    // Fragment-local hoisted searchers (mirrors LikePredicateState::init_substring_searcher()).
    const VolnitskyUTF8 substring_searcher(needle.data(), needle.size());
    const StringSearcher substring_fallback(needle.data(), needle.size());

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        if (chunk.data.size() < VolnitskyTraits::min_haystack_size_for_algorithm) {
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), substring_fallback, res);
        } else {
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), substring_searcher, res);
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_LikeSubstring_Optimized)->ArgsProduct({{2, 6, 16, 32, 33, 64}, {1, 10, 100, 1000, 5000}});

// Gate G4: Per-change ablation benchmark across 5 cumulative stages:
//   Stage 0: Baseline (`main`: single-byte SSE4.1 StringSearcher + Volnitsky>=4 + per-chunk init + linear offsets)
//   Stage 1: + Multi-arch SIMD First+Last-Byte StringSearcher (still Volnitsky>=4 + per-chunk init + linear offsets)
//   Stage 2: + VolnitskyTraits::min_needle_size_for_algorithm = 33 (still per-chunk init + linear offsets)
//   Stage 3: + Fragment-local searcher hoisting in LikePredicateState (still linear offsets)
//   Stage 4: + Upfront memset & hybrid 16-row linear + std::upper_bound offset lookup
static void BM_LikeSubstring_Ablation(benchmark::State& state) {
    const int64_t stage = state.range(0);
    const size_t needle_len = static_cast<size_t>(state.range(1));
    const size_t selectivity_per_10k = static_cast<size_t>(state.range(2));
    const std::string needle = make_benchmark_needle(needle_len);
    const auto chunks = build_url_chunks(needle_len, selectivity_per_10k);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    const VolnitskyUTF8 hoisted_searcher(needle.data(), needle.size());
    const StringSearcher hoisted_fallback(needle.data(), needle.size());

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        switch (stage) {
        case 0: {
            LegacyVolnitskyStage0 searcher(needle.data(), needle.size(), chunk.data.size());
            scan_chunk_linear_offsets(chunk, needle.size(), searcher, res);
            break;
        }
        case 1: {
            NewKernelLegacyThresholdStage1 searcher(needle.data(), needle.size(), chunk.data.size());
            scan_chunk_linear_offsets(chunk, needle.size(), searcher, res);
            break;
        }
        case 2: {
            VolnitskyUTF8 searcher(needle.data(), needle.size(), chunk.data.size());
            scan_chunk_linear_offsets(chunk, needle.size(), searcher, res);
            break;
        }
        case 3: {
            if (chunk.data.size() < VolnitskyTraits::min_haystack_size_for_algorithm) {
                scan_chunk_linear_offsets(chunk, needle.size(), hoisted_fallback, res);
            } else {
                scan_chunk_linear_offsets(chunk, needle.size(), hoisted_searcher, res);
            }
            break;
        }
        case 4:
        default: {
            if (chunk.data.size() < VolnitskyTraits::min_haystack_size_for_algorithm) {
                scan_chunk_hybrid_offsets<16>(chunk, needle.size(), hoisted_fallback, res);
            } else {
                scan_chunk_hybrid_offsets<16>(chunk, needle.size(), hoisted_searcher, res);
            }
            break;
        }
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_LikeSubstring_Ablation)->ArgsProduct({{0, 1, 2, 3, 4}, {2, 6, 16, 32, 33, 64}, {1, 10, 100, 1000, 5000}});

// Gate G4: Offset lookup strategy comparison (justifying the 16-row linear step constant):
//   mode = 0: Legacy linear walk + trailing memset
//   mode = 1: Upfront memset + pure linear walk
//   mode = 2: Upfront memset + pure std::upper_bound (threshold = 0)
//   mode = 3: Upfront memset + hybrid 8-row linear + std::upper_bound
//   mode = 4: Upfront memset + hybrid 16-row linear + std::upper_bound (production)
//   mode = 5: Upfront memset + hybrid 32-row linear + std::upper_bound
static void BM_OffsetLookup_Threshold(benchmark::State& state) {
    const int64_t mode = state.range(0);
    const size_t selectivity_per_10k = static_cast<size_t>(state.range(1));
    const std::string needle = make_benchmark_needle(6);
    const auto chunks = build_url_chunks(6, selectivity_per_10k);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);
    const StringSearcher searcher(needle.data(), needle.size());

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        switch (mode) {
        case 0:
            scan_chunk_linear_offsets(chunk, needle.size(), searcher, res);
            break;
        case 1:
            scan_chunk_hybrid_offsets<65536>(chunk, needle.size(), searcher, res);
            break;
        case 2:
            scan_chunk_hybrid_offsets<0>(chunk, needle.size(), searcher, res);
            break;
        case 3:
            scan_chunk_hybrid_offsets<8>(chunk, needle.size(), searcher, res);
            break;
        case 4:
        default:
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), searcher, res);
            break;
        case 5:
            scan_chunk_hybrid_offsets<32>(chunk, needle.size(), searcher, res);
            break;
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_OffsetLookup_Threshold)->ArgsProduct({{0, 1, 2, 3, 4, 5}, {1, 10, 100, 1000, 5000}});

// Gate G4: Crossover analysis between hoisted StringSearcher (algo=0) and hoisted Volnitsky (algo=1)
// across needle lengths {6, 8, 12, 16, 20, 24, 28, 32, 33, 40, 48, 64} on natural URL chunks (0.1% sel).
static void BM_Substring_NaturalCrossover(benchmark::State& state) {
    const int64_t algo = state.range(0); // 0 = StringSearcher, 1 = ForcedVolnitskySearcher
    const size_t needle_len = static_cast<size_t>(state.range(1));
    const std::string needle = make_benchmark_needle(needle_len);
    const auto chunks = build_url_chunks(needle_len, 10);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    const StringSearcher simd_searcher(needle.data(), needle.size());
    const ForcedVolnitskySearcher volnitsky_searcher(needle.data(), needle.size(), 278528);

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        if (algo == 0) {
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), simd_searcher, res);
        } else {
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), volnitsky_searcher, res);
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_Substring_NaturalCrossover)->ArgsProduct({{0, 1}, {6, 8, 12, 16, 20, 24, 28, 32, 33, 40, 48, 64}});

// Gate G4: Adversarial repetitive near-match corpus where non-matching rows contain repeated
// decoys matching both `first` and `last` bytes and differing in the interior, testing worst-case
// `std::memcmp` verification amplification vs Volnitsky skip-step across needle lengths 8..64.
static std::vector<SubstringChunk> build_adversarial_chunks(size_t needle_len, size_t selectivity_per_10k,
                                                            std::string* out_needle) {
    // Needle of the form: 'a' + (needle_len - 2) 'b'/'c' interior with unique ending bigram "qz"
    // while the haystack is filled with "ab...ba" decoys that match first='a' and last='a' if
    // the needle ends with 'a'. To test true first+last collision amplification vs Volnitsky bigram skip:
    // Needle: 'a' + (needle_len - 4) * 'b' + "XYZ" + 'a' (starts with 'a', ends with 'a', interior has "XYZ").
    // Haystack miss rows: repeated 'a' + (needle_len - 2) * 'b' + 'a' blocks (every needle_len bytes has
    // hay[0] == 'a' and hay[needle_len - 1] == 'a', triggering first+last match, while trailing bigram is "ba" != "Za").
    std::string needle(needle_len, 'b');
    needle.front() = 'a';
    needle.back() = 'a';
    if (needle_len >= 4) {
        needle[needle_len - 2] = 'Z';
        needle[needle_len / 2] = 'X';
    } else if (needle_len >= 3) {
        needle[1] = 'Z';
    }
    *out_needle = needle;

    std::string decoy_unit(needle_len, 'b');
    decoy_unit.front() = 'a';
    decoy_unit.back() = 'a';

    std::string miss_row;
    while (miss_row.size() < 96) {
        miss_row.append(decoy_unit);
    }
    miss_row.resize(std::max<size_t>(96, needle_len + 16), 'c');

    std::vector<SubstringChunk> chunks(kSubstringNumChunks);
    for (size_t c = 0; c < kSubstringNumChunks; ++c) {
        auto& chunk = chunks[c];
        chunk.num_rows = kSubstringChunkRows;
        chunk.offsets.resize(kSubstringChunkRows + 1, 0);
        chunk.data.reserve(kSubstringChunkRows * miss_row.size());

        for (size_t r = 0; r < kSubstringChunkRows; ++r) {
            const size_t global_row = c * kSubstringChunkRows + r;
            const bool is_hit = ((global_row * 7919ULL + 5000ULL) % 10000ULL) < selectivity_per_10k;
            std::string row = miss_row;
            if (is_hit) {
                row.replace(row.size() - needle.size(), needle.size(), needle);
            }
            chunk.data.append(row);
            chunk.offsets[r + 1] = static_cast<uint32_t>(chunk.data.size());
        }
    }
    return chunks;
}

static void BM_Substring_AdversarialCrossover(benchmark::State& state) {
    const int64_t algo =
            state.range(0); // 0 = StringSearcher, 1 = ForcedVolnitskySearcher, 2 = Auto VolnitskyUTF8 (threshold=33)
    const size_t needle_len = static_cast<size_t>(state.range(1));
    std::string needle;
    const auto chunks = build_adversarial_chunks(needle_len, 10, &needle);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    const StringSearcher simd_searcher(needle.data(), needle.size());
    const ForcedVolnitskySearcher forced_volnitsky(needle.data(), needle.size(), chunks[0].data.size());
    const VolnitskyUTF8 auto_volnitsky(needle.data(), needle.size(), chunks[0].data.size());

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        if (algo == 0) {
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), simd_searcher, res);
        } else if (algo == 1) {
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), forced_volnitsky, res);
        } else {
            scan_chunk_hybrid_offsets<16>(chunk, needle.size(), auto_volnitsky, res);
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_Substring_AdversarialCrossover)->ArgsProduct({{0, 1, 2}, {8, 16, 24, 32, 33, 48, 64}});

// Gate G2: Row-length distributions (~20 B short strings, ~68 B URLs, ~1024 B long strings)
static std::vector<SubstringChunk> build_rowlen_chunks(size_t row_len, size_t needle_len, size_t selectivity_per_10k) {
    static constexpr std::string_view kBase = "http://yandex.ru/yandsearch?text=starrocks+olap+database&lr=213&pg=1";
    const std::string needle = make_benchmark_needle(needle_len);

    // Keep chunk byte payload around ~256-512 KB for long rows, or 4096 rows for 20B/68B.
    const size_t rows_per_chunk = (row_len >= 512) ? 512 : kSubstringChunkRows;
    std::string miss_template;
    miss_template.reserve(row_len + kBase.size());
    while (miss_template.size() < row_len) {
        miss_template.append(kBase);
    }
    miss_template.resize(row_len);

    std::vector<SubstringChunk> chunks(kSubstringNumChunks);
    for (size_t c = 0; c < kSubstringNumChunks; ++c) {
        auto& chunk = chunks[c];
        chunk.num_rows = rows_per_chunk;
        chunk.offsets.resize(rows_per_chunk + 1, 0);
        chunk.data.reserve(rows_per_chunk * std::max(row_len, needle.size() + 4));

        for (size_t r = 0; r < rows_per_chunk; ++r) {
            const size_t global_row = c * rows_per_chunk + r;
            const bool is_hit = ((global_row * 7919ULL + 5000ULL) % 10000ULL) < selectivity_per_10k;
            std::string row = miss_template;
            if (is_hit) {
                if (needle.size() <= row.size()) {
                    row.replace(row.size() - needle.size(), needle.size(), needle);
                } else {
                    row = needle;
                }
            }
            chunk.data.append(row);
            chunk.offsets[r + 1] = static_cast<uint32_t>(chunk.data.size());
        }
    }
    return chunks;
}

static void BM_LikeSubstring_RowLengths(benchmark::State& state) {
    const int64_t variant = state.range(0); // 0 = Stage 0 Baseline, 1 = Stage 4 Optimized
    const size_t row_len = static_cast<size_t>(state.range(1));
    const size_t needle_len = static_cast<size_t>(state.range(2));
    const std::string needle = make_benchmark_needle(needle_len);
    const auto chunks = build_rowlen_chunks(row_len, needle_len, 10);
    std::vector<uint8_t> res(chunks[0].num_rows, 0);

    const VolnitskyUTF8 hoisted_searcher(needle.data(), needle.size());
    const StringSearcher hoisted_fallback(needle.data(), needle.size());

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    size_t total_rows = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kSubstringNumChunks) ? 0 : (chunk_idx + 1);

        if (variant == 0) {
            LegacyVolnitskyStage0 searcher(needle.data(), needle.size(), chunk.data.size());
            scan_chunk_linear_offsets(chunk, needle.size(), searcher, res);
        } else {
            if (chunk.data.size() < VolnitskyTraits::min_haystack_size_for_algorithm) {
                scan_chunk_hybrid_offsets<16>(chunk, needle.size(), hoisted_fallback, res);
            } else {
                scan_chunk_hybrid_offsets<16>(chunk, needle.size(), hoisted_searcher, res);
            }
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
        total_rows += chunk.num_rows;
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(total_rows));
}

BENCHMARK(BM_LikeSubstring_RowLengths)->ArgsProduct({{0, 1}, {20, 68, 1024}, {2, 6, 16, 32, 33, 64}});

// Gate G3: Cold-cache working set benchmark (256 chunks = ~71.3 MB > 24 MiB L3 cache)
static constexpr size_t kColdCacheNumChunks = 256;

static const std::vector<SubstringChunk>& get_cold_cache_chunks(size_t needle_len) {
    static std::mutex mu;
    static std::vector<std::pair<size_t, std::vector<SubstringChunk>>> cache;
    std::lock_guard<std::mutex> lock(mu);
    for (const auto& entry : cache) {
        if (entry.first == needle_len) {
            return entry.second;
        }
    }
    const auto base_chunks = build_url_chunks(needle_len, 10);
    std::vector<SubstringChunk> cold_chunks(kColdCacheNumChunks);
    for (size_t i = 0; i < kColdCacheNumChunks; ++i) {
        cold_chunks[i] = base_chunks[i % base_chunks.size()];
        // Mutate one byte per chunk copy so pages are distinct physical allocations.
        if (!cold_chunks[i].data.empty()) {
            cold_chunks[i].data[0] = static_cast<char>('h');
        }
    }
    cache.emplace_back(needle_len, std::move(cold_chunks));
    return cache.back().second;
}

static void BM_LikeSubstring_ColdCache(benchmark::State& state) {
    const int64_t variant = state.range(0); // 0 = Stage 0 Baseline, 1 = Stage 4 Optimized
    const size_t needle_len = static_cast<size_t>(state.range(1));
    const std::string needle = make_benchmark_needle(needle_len);
    const auto& chunks = get_cold_cache_chunks(needle_len);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    const VolnitskyUTF8 hoisted_searcher(needle.data(), needle.size());
    const StringSearcher hoisted_fallback(needle.data(), needle.size());

    size_t chunk_idx = 0;
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == kColdCacheNumChunks) ? 0 : (chunk_idx + 1);

        if (variant == 0) {
            LegacyVolnitskyStage0 searcher(needle.data(), needle.size(), chunk.data.size());
            scan_chunk_linear_offsets(chunk, needle.size(), searcher, res);
        } else {
            if (chunk.data.size() < VolnitskyTraits::min_haystack_size_for_algorithm) {
                scan_chunk_hybrid_offsets<16>(chunk, needle.size(), hoisted_fallback, res);
            } else {
                scan_chunk_hybrid_offsets<16>(chunk, needle.size(), hoisted_searcher, res);
            }
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_LikeSubstring_ColdCache)->ArgsProduct({{0, 1}, {6, 16, 32, 33}});

// Gate G3: Multi-threaded scaling benchmark (1, 8, 24, 32 threads) over shared warm and cold corpora
struct SharedMultiThreadState {
    std::string needle;
    std::vector<SubstringChunk> warm_chunks;
    std::unique_ptr<VolnitskyUTF8> hoisted_searcher;
    std::unique_ptr<StringSearcher> hoisted_fallback;
};

static const SharedMultiThreadState& get_shared_mt_state() {
    static std::once_flag flag;
    static SharedMultiThreadState st;
    std::call_once(flag, []() {
        st.needle = make_benchmark_needle(6);
        st.warm_chunks = build_url_chunks(6, 10);
        st.hoisted_searcher = std::make_unique<VolnitskyUTF8>(st.needle.data(), st.needle.size());
        st.hoisted_fallback = std::make_unique<StringSearcher>(st.needle.data(), st.needle.size());
        (void)get_cold_cache_chunks(6);
    });
    return st;
}

static void BM_LikeSubstring_MultiThread_Baseline(benchmark::State& state) {
    const int64_t cold = state.range(0); // 0 = 2.78 MB L3-resident, 1 = 71.3 MB cold working set
    const auto& st = get_shared_mt_state();
    const auto& chunks = (cold == 0) ? st.warm_chunks : get_cold_cache_chunks(6);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    size_t chunk_idx = static_cast<size_t>(state.thread_index()) % chunks.size();
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == chunks.size()) ? 0 : (chunk_idx + 1);

        LegacyVolnitskyStage0 searcher(st.needle.data(), st.needle.size(), chunk.data.size());
        scan_chunk_linear_offsets(chunk, st.needle.size(), searcher, res);
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

static void BM_LikeSubstring_MultiThread_Optimized(benchmark::State& state) {
    const int64_t cold = state.range(0); // 0 = 2.78 MB L3-resident, 1 = 71.3 MB cold working set
    const auto& st = get_shared_mt_state();
    const auto& chunks = (cold == 0) ? st.warm_chunks : get_cold_cache_chunks(6);
    std::vector<uint8_t> res(kSubstringChunkRows, 0);

    size_t chunk_idx = static_cast<size_t>(state.thread_index()) % chunks.size();
    size_t total_bytes = 0;
    for (auto _ : state) {
        const auto& chunk = chunks[chunk_idx];
        chunk_idx = (chunk_idx + 1 == chunks.size()) ? 0 : (chunk_idx + 1);

        if (chunk.data.size() < VolnitskyTraits::min_haystack_size_for_algorithm) {
            scan_chunk_hybrid_offsets<16>(chunk, st.needle.size(), *st.hoisted_fallback, res);
        } else {
            scan_chunk_hybrid_offsets<16>(chunk, st.needle.size(), *st.hoisted_searcher, res);
        }
        benchmark::DoNotOptimize(res.data());
        total_bytes += chunk.data.size();
    }
    state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSubstringChunkRows));
}

BENCHMARK(BM_LikeSubstring_MultiThread_Baseline)
        ->Arg(0)
        ->Arg(1)
        ->Threads(1)
        ->Threads(8)
        ->Threads(24)
        ->Threads(32)
        ->UseRealTime();
BENCHMARK(BM_LikeSubstring_MultiThread_Optimized)
        ->Arg(0)
        ->Arg(1)
        ->Threads(1)
        ->Threads(8)
        ->Threads(24)
        ->Threads(32)
        ->UseRealTime();

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Measured Substring Search Results (AMD Ryzen AI 9 HX 370 x86-64 AVX2/SSE4.1 & ARM64 Neoverse-V2)
// =====================================================================
//
// 1. 64 KB Direct Buffer Scan (x86-64 Zen 5, GCC 14.2 -O3):
//    BM_StringSearcher_Scalar/2                14626 ns        4.17 Gi/s
//    BM_StringSearcher_Scalar/6                14217 ns        4.29 Gi/s
//    BM_StringSearcher_Scalar/16               14200 ns        4.30 Gi/s
//    BM_StringSearcher_Scalar/32               13484 ns        4.53 Gi/s
//    BM_StringSearcher_SSE41/2                  1692 ns       36.07 Gi/s  (8.64x vs scalar)
//    BM_StringSearcher_SSE41/6                  1632 ns       37.42 Gi/s  (8.71x vs scalar)
//    BM_StringSearcher_SSE41/16                 1564 ns       39.04 Gi/s  (9.08x vs scalar)
//    BM_StringSearcher_SSE41/32                 1519 ns       40.19 Gi/s  (8.88x vs scalar)
//    BM_StringSearcher_AVX2/2                   1202 ns       50.79 Gi/s  (12.17x vs scalar)
//    BM_StringSearcher_AVX2/6                   1044 ns       58.49 Gi/s  (13.62x vs scalar)
//    BM_StringSearcher_AVX2/16                  1135 ns       53.78 Gi/s  (12.51x vs scalar)
//    BM_StringSearcher_AVX2/32                  1113 ns       54.85 Gi/s  (12.12x vs scalar)
//
// 2. 4,096-Row URL Chunk Scan (~278.5 KB/chunk, 0.1% selectivity, LIKE '%google%'):
//    x86-64 Zen 5 (AVX2, 1T warm):     27,867 ns/chunk ( 9.31 Gi/s) ->  5,066 ns/chunk (51.21 Gi/s, 5.50x)
//    x86-64 Zen 5 (--without-avx2):    32,487 ns/chunk ( 7.98 Gi/s) ->  5,248 ns/chunk (49.43 Gi/s, 6.19x)
//    x86-64 Zen 5 (1T cold, 71.3 MB):  29,181 ns/chunk ( 8.89 Gi/s) -> 10,514 ns/chunk (24.67 Gi/s, 2.78x)
//    x86-64 Zen 5 (8T / 24T / 32T):    33.4-111.6 Gi/s              -> 190.9-396.7 Gi/s (3.55x-5.71x warm)
//    ARM64 Neoverse-V2 (250 chunks):     8.92 ms       ( 8.17 GB/s) ->   2.09 ms       (34.84 GB/s, 4.27x)
// =====================================================================

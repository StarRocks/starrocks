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

// Definition levels -> NullInfos for an optional column:
//   BM_DefLevelsToNulls: the flag pass alone, the previous loop (reads back is_nulls[i - 1] to count runs)
//                        against assign_nulls_from_def_levels
//   BM_DecodeDefLevels:  a whole RLE page, int16 levels + flag pass against LevelDecoder::decode_nulls
//
//   ./parquet_def_level_bench --benchmark_repetitions=5 --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>

#include <random>
#include <vector>

#include "base/bit/rle_encoding.h"
#include "base/string/faststring.h"
#include "base/string/slice.h"
#include "formats/parquet/level_codec.h"
#include "formats/parquet/utils.h"

namespace starrocks::parquet {

static constexpr size_t kBatch = 4096;

// The loop OptionalStoredColumnReader used before assign_nulls_from_def_levels.
static void assign_nulls_reference(int16_t* __restrict levels, uint16_t def_level, size_t n,
                                   uint8_t* __restrict is_nulls, size_t* ranges_dst, size_t* num_nulls_dst) {
    size_t num_ranges = 1;
    size_t num_nulls{};
    int16_t max_def_level = def_level;
    if (n > 0) {
        is_nulls[0] = levels[0] < max_def_level;
        num_nulls += is_nulls[0];
    }
    for (size_t i = 1; i < n; ++i) {
        is_nulls[i] = levels[i] < max_def_level;
        num_nulls += is_nulls[i];
        num_ranges += is_nulls[i] != is_nulls[i - 1];
    }
    *ranges_dst = num_ranges;
    *num_nulls_dst = num_nulls;
}

static void BM_DefLevelsToNulls(benchmark::State& state) {
    const bool use_new = state.range(0) != 0;
    const int null_pct = static_cast<int>(state.range(1));
    std::mt19937 rng(7);
    std::uniform_int_distribution<int> pct(0, 99);
    std::vector<int16_t> levels(kBatch);
    for (auto& l : levels) {
        l = pct(rng) < null_pct ? 0 : 1;
    }
    NullInfos infos;
    infos.reset_with_capacity(kBatch);
    size_t ref_ranges = 0, ref_nulls = 0;
    assign_nulls_reference(levels.data(), 1, kBatch, infos.nulls_data(), &ref_ranges, &ref_nulls);
    assign_nulls_from_def_levels(levels.data(), 1, kBatch, &infos);
    CHECK_EQ(ref_ranges, infos.num_ranges);
    CHECK_EQ(ref_nulls, infos.num_nulls);

    for (auto _ : state) {
        if (use_new) {
            assign_nulls_from_def_levels(levels.data(), 1, kBatch, &infos);
        } else {
            assign_nulls_reference(levels.data(), 1, kBatch, infos.nulls_data(), &infos.num_ranges, &infos.num_nulls);
        }
        benchmark::DoNotOptimize(infos);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kBatch);
    state.SetLabel(use_new ? "new" : "reference");
}

// A whole page of 1-bit RLE definition levels to null flags, 4096 rows per batch:
//   OLD: LevelDecoder::get_avail_levels (RLE -> int16 levels) + assign_nulls_from_def_levels
//   NEW: LevelDecoder::decode_nulls (RLE -> uint8 flags directly)
// pattern 0: random nulls at null_pct; 1: runs of 97 rows, every third run null
static void BM_DecodeDefLevels(benchmark::State& state) {
    const bool use_new = state.range(0) != 0;
    const int pattern = static_cast<int>(state.range(1));
    const int null_pct = static_cast<int>(state.range(2));
    constexpr size_t kPageLevels = 64 * 1024;
    std::mt19937 rng(11);
    std::uniform_int_distribution<int> pct(0, 99);
    faststring encoded;
    RleEncoder<int16_t> encoder(&encoded, 1);
    for (size_t i = 0; i < kPageLevels; i++) {
        encoder.Put(pattern == 0 ? (pct(rng) >= null_pct) : ((i / 97) % 3 != 0));
    }
    encoder.Flush();
    int64_t timer = 0;
    LevelDecoder decoder(&timer);
    NullInfos infos;
    infos.reset_with_capacity(kBatch);

    for (auto _ : state) {
        Slice slice(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        CHECK(decoder.parse_v2(encoded.size(), 1, kPageLevels, &slice).ok());
        for (size_t begin = 0; begin < kPageLevels; begin += kBatch) {
            if (use_new) {
                CHECK(decoder.decode_nulls(kBatch, &infos));
            } else {
                level_t* levels = nullptr;
                decoder.get_avail_levels(kBatch, &levels);
                decoder.consume_levels(kBatch);
                infos.reset_with_capacity(kBatch);
                assign_nulls_from_def_levels(levels, 1, kBatch, &infos);
                decoder.reset();
            }
            benchmark::DoNotOptimize(infos);
        }
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kPageLevels);
    state.SetLabel(use_new ? "new" : "old");
}

// Skipping a whole page of 1-bit RLE levels, counting the non-null values (OptionalStoredColumnReader::
// _lazy_skip_values), 8192 rows per batch:
//   OLD: get_avail_levels (int16 levels) + a count of levels == 1, then reset()
//   NEW: LevelDecoder::skip_count_non_null
static void BM_SkipDefLevels(benchmark::State& state) {
    const bool use_new = state.range(0) != 0;
    const int null_pct = static_cast<int>(state.range(1));
    constexpr size_t kPageLevels = 64 * 1024;
    constexpr size_t kSkipBatch = 8192;
    std::mt19937 rng(13);
    std::uniform_int_distribution<int> pct(0, 99);
    faststring encoded;
    RleEncoder<int16_t> encoder(&encoded, 1);
    for (size_t i = 0; i < kPageLevels; i++) encoder.Put(pct(rng) >= null_pct);
    encoder.Flush();
    int64_t timer = 0;
    LevelDecoder decoder(&timer);
    for (auto _ : state) {
        Slice slice(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        CHECK(decoder.parse_v2(encoded.size(), 1, kPageLevels, &slice).ok());
        size_t non_null = 0;
        if (use_new) {
            CHECK(decoder.skip_count_non_null(kPageLevels, &non_null));
        } else {
            for (size_t begin = 0; begin < kPageLevels; begin += kSkipBatch) {
                level_t* levels = nullptr;
                decoder.get_avail_levels(kSkipBatch, &levels);
                for (size_t i = 0; i < kSkipBatch; i++) non_null += levels[i] == 1;
                decoder.consume_levels(kSkipBatch);
                decoder.reset();
            }
        }
        benchmark::DoNotOptimize(non_null);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kPageLevels);
    state.SetLabel(use_new ? "new" : "old");
}

BENCHMARK(BM_SkipDefLevels)
        ->ArgNames({"new", "null_pct"})
        ->ArgsProduct({{0, 1}, {5, 50}})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DecodeDefLevels)
        ->ArgNames({"new", "pattern", "null_pct"})
        ->Args({0, 0, 5})
        ->Args({1, 0, 5})
        ->Args({0, 0, 50})
        ->Args({1, 0, 50})
        ->Args({0, 1, 0})
        ->Args({1, 1, 0})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DefLevelsToNulls)
        ->ArgNames({"new", "null_pct"})
        ->ArgsProduct({{0, 1}, {0, 5, 50}})
        ->Unit(benchmark::kNanosecond);

} // namespace starrocks::parquet

BENCHMARK_MAIN();

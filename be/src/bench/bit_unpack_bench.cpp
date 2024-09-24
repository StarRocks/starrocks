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

#include "gutil/integral_types.h"
#include "util/bit_stream_utils.inline.h"
#ifdef __AVX2__
#include <arrow/util/bpacking_avx2.h>
#endif

#include <benchmark/benchmark.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <memory>
#include <random>
#include <vector>

#include "bench.h"
#include "bench/bit_copy.h"
#include "util/bit_packing.h"
#include "util/bit_packing_adapter.h"
#include "util/bit_packing_simd.h"

namespace starrocks {

class BitUnpackBench {
public:
    BitUnpackBench() { SetUp(); }
    ~BitUnpackBench() = default;

    static BitUnpackBench* get_instance() {
        static BitUnpackBench bench;
        return &bench;
    }

#define DO_BENCH_DECLARE(WIDTH)                     \
    void do_bench_##WIDTH##_default(int bit_width); \
    void do_bench_##WIDTH##_avx2(int bit_width);    \
    void do_bench_##WIDTH##_bmi(int bit_width);

    DO_BENCH_DECLARE(32);
    DO_BENCH_DECLARE(16);
    DO_BENCH_DECLARE(8);

private:
    void SetUp();
    void populateBitPacked();

    std::vector<uint32_t> randomInts_u32;
    // Array of bit packed representations of randomInts_u32. The array at index i
    // is packed i bits wide and the values come from the low bits of
    std::vector<std::vector<uint64_t>> bitPackedData;

    std::vector<uint32_t> result32_;
    std::vector<uint16_t> result16_;
    std::vector<uint8_t> result8_;

    static const uint64_t kNumValues = 1024 * 1024 * 4;
};

void BitUnpackBench::populateBitPacked() {
    bitPackedData.resize(33);
    for (auto bitWidth = 1; bitWidth <= 32; ++bitWidth) {
        auto numWords = (randomInts_u32.size() * bitWidth + 64 - 1) / 64;
        bitPackedData[bitWidth].resize(numWords);
        auto source = reinterpret_cast<uint64_t*>(randomInts_u32.data());
        auto destination = reinterpret_cast<uint64_t*>(bitPackedData[bitWidth].data());
        for (auto i = 0; i < randomInts_u32.size(); ++i) {
            BitCopy::copyBits(source, i * 32, destination, i * bitWidth, bitWidth);
        }
    }
}

void BitUnpackBench::SetUp() {
    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<uint32_t> distr(0, std::numeric_limits<uint32_t>::max());
    for (int32_t i = 0; i < kNumValues; i++) {
        auto randomInt = distr(gen);
        randomInts_u32.push_back(randomInt);
    }
    populateBitPacked();
    result32_.resize(kNumValues);
    result16_.resize(kNumValues);
    result8_.resize(kNumValues);
}

#define DO_BENCH_DEFINE(WIDTH)                                                                                      \
    void BitUnpackBench::do_bench_##WIDTH##_default(int bit_width) {                                                \
        auto source = bitPackedData[bit_width];                                                                     \
        starrocks::BitPacking::UnpackValues(bit_width, reinterpret_cast<uint8_t*>(source.data()),                   \
                                            source.size() * sizeof(uint64_t), kNumValues, result##WIDTH##_.data()); \
    }                                                                                                               \
                                                                                                                    \
    void BitUnpackBench::do_bench_##WIDTH##_avx2(int bit_width) {                                                   \
        auto source = bitPackedData[bit_width];                                                                     \
        starrocks::BitPackingAdapter::UnpackValues_ARROW(bit_width, reinterpret_cast<uint8_t*>(source.data()),      \
                                                         source.size() * sizeof(uint64_t), kNumValues,              \
                                                         result##WIDTH##_.data());                                  \
    }                                                                                                               \
                                                                                                                    \
    void BitUnpackBench::do_bench_##WIDTH##_bmi(int bit_width) {                                                    \
        auto source = bitPackedData[bit_width];                                                                     \
        starrocks::util::unpack(bit_width, reinterpret_cast<uint8_t*>(source.data()),                               \
                                source.size() * sizeof(uint64_t), kNumValues, result##WIDTH##_.data());             \
    }

DO_BENCH_DEFINE(32);
DO_BENCH_DEFINE(16);
DO_BENCH_DEFINE(8);

#define BM_BENCH_DEFINE(WIDTH)                                  \
    static void BM_##WIDTH##_DEFAULT(benchmark::State& state) { \
        int bit_width = state.range(0);                         \
        auto* instance = BitUnpackBench::get_instance();        \
                                                                \
        for (auto _ : state) {                                  \
            state.ResumeTiming();                               \
            instance->do_bench_##WIDTH##_default(bit_width);    \
            state.PauseTiming();                                \
        }                                                       \
    }                                                           \
                                                                \
    static void BM_##WIDTH##_AVX2(benchmark::State& state) {    \
        int bit_width = state.range(0);                         \
        auto* instance = BitUnpackBench::get_instance();        \
                                                                \
        for (auto _ : state) {                                  \
            state.ResumeTiming();                               \
            instance->do_bench_##WIDTH##_avx2(bit_width);       \
            state.PauseTiming();                                \
        }                                                       \
    }                                                           \
                                                                \
    static void BM_##WIDTH##_BMI(benchmark::State& state) {     \
        int bit_width = state.range(0);                         \
        auto* instance = BitUnpackBench::get_instance();        \
                                                                \
        for (auto _ : state) {                                  \
            state.ResumeTiming();                               \
            instance->do_bench_##WIDTH##_bmi(bit_width);        \
            state.PauseTiming();                                \
        }                                                       \
    }

BM_BENCH_DEFINE(32);
BM_BENCH_DEFINE(16);
BM_BENCH_DEFINE(8);

BENCHMARK(BM_32_DEFAULT)->DenseRange(1, 32, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_32_AVX2)->DenseRange(1, 32, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_32_BMI)->DenseRange(1, 32, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_16_DEFAULT)->DenseRange(1, 16, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_16_AVX2)->DenseRange(1, 16, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_16_BMI)->DenseRange(1, 16, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_8_DEFAULT)->DenseRange(1, 8, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_8_AVX2)->DenseRange(1, 8, 1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_8_BMI)->DenseRange(1, 8, 1)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();

/*
 * final conclusion:
 * BMI is faster.
 *
BM_32_DEFAULT/1        2.08 ms         2.08 ms          334
BM_32_DEFAULT/2        2.13 ms         2.13 ms          327
BM_32_DEFAULT/3        1.65 ms         1.65 ms          420
BM_32_DEFAULT/4        2.24 ms         2.24 ms          305
BM_32_DEFAULT/5        2.32 ms         2.32 ms          300
BM_32_DEFAULT/6        2.89 ms         2.89 ms          241
BM_32_DEFAULT/7        2.61 ms         2.61 ms          266
BM_32_DEFAULT/8        2.68 ms         2.68 ms          258
BM_32_DEFAULT/9        2.47 ms         2.47 ms          282
BM_32_DEFAULT/10       2.55 ms         2.55 ms          274
BM_32_DEFAULT/11       2.76 ms         2.76 ms          254
BM_32_DEFAULT/12       3.17 ms         3.17 ms          225
BM_32_DEFAULT/13       2.94 ms         2.94 ms          237
BM_32_DEFAULT/14       3.02 ms         3.02 ms          229
BM_32_DEFAULT/15       6.38 ms         6.38 ms           97
BM_32_DEFAULT/16       7.58 ms         7.58 ms           84
BM_32_DEFAULT/17       6.99 ms         6.99 ms           89
BM_32_DEFAULT/18       7.38 ms         7.37 ms           86
BM_32_DEFAULT/19       7.99 ms         7.99 ms           77
BM_32_DEFAULT/20       8.38 ms         8.38 ms           76
BM_32_DEFAULT/21       8.39 ms         8.39 ms           75
BM_32_DEFAULT/22       8.87 ms         8.87 ms           72
BM_32_DEFAULT/23       9.24 ms         9.23 ms           69
BM_32_DEFAULT/24       9.94 ms         9.94 ms           64
BM_32_DEFAULT/25      10.00 ms        10.00 ms           63
BM_32_DEFAULT/26       10.4 ms         10.4 ms           62
BM_32_DEFAULT/27       10.8 ms         10.8 ms           59
BM_32_DEFAULT/28       11.3 ms         11.3 ms           57
BM_32_DEFAULT/29       11.4 ms         11.4 ms           56
BM_32_DEFAULT/30       11.9 ms         11.9 ms           54
BM_32_DEFAULT/31       12.2 ms         12.2 ms           53
BM_32_DEFAULT/32       12.2 ms         12.2 ms           53
BM_32_AVX2/1          0.796 ms        0.796 ms          866
BM_32_AVX2/2          0.900 ms        0.900 ms          767
BM_32_AVX2/3          0.995 ms        0.995 ms          680
BM_32_AVX2/4           1.05 ms         1.05 ms          658
BM_32_AVX2/5           1.64 ms         1.64 ms          423
BM_32_AVX2/6           1.63 ms         1.63 ms          423
BM_32_AVX2/7           2.25 ms         2.25 ms          307
BM_32_AVX2/8           1.47 ms         1.47 ms          467
BM_32_AVX2/9           2.92 ms         2.92 ms          237
BM_32_AVX2/10          3.03 ms         3.03 ms          228
BM_32_AVX2/11          3.14 ms         3.14 ms          220
BM_32_AVX2/12          3.28 ms         3.28 ms          211
BM_32_AVX2/13          3.30 ms         3.30 ms          210
BM_32_AVX2/14          3.38 ms         3.38 ms          206
BM_32_AVX2/15          6.71 ms         6.70 ms           92
BM_32_AVX2/16          5.95 ms         5.95 ms          106
BM_32_AVX2/17          7.34 ms         7.34 ms           86
BM_32_AVX2/18          7.71 ms         7.71 ms           82
BM_32_AVX2/19          8.08 ms         8.08 ms           78
BM_32_AVX2/20          8.34 ms         8.34 ms           76
BM_32_AVX2/21          8.76 ms         8.76 ms           71
BM_32_AVX2/22          9.16 ms         9.16 ms           69
BM_32_AVX2/23          9.54 ms         9.54 ms           67
BM_32_AVX2/24          9.67 ms         9.66 ms           66
BM_32_AVX2/25          10.3 ms         10.3 ms           62
BM_32_AVX2/26          10.7 ms         10.7 ms           60
BM_32_AVX2/27          11.0 ms         11.0 ms           58
BM_32_AVX2/28          11.3 ms         11.3 ms           57
BM_32_AVX2/29          11.6 ms         11.6 ms           55
BM_32_AVX2/30          12.2 ms         12.2 ms           54
BM_32_AVX2/31          11.8 ms         11.8 ms           54
BM_32_AVX2/32          12.1 ms         12.1 ms           52
BM_32_BMI/1           0.761 ms        0.761 ms          900
BM_32_BMI/2           0.864 ms        0.864 ms          799
BM_32_BMI/3           0.960 ms        0.960 ms          719
BM_32_BMI/4            1.05 ms         1.05 ms          652
BM_32_BMI/5            1.14 ms         1.14 ms          600
BM_32_BMI/6            1.25 ms         1.25 ms          544
BM_32_BMI/7            1.34 ms         1.34 ms          503
BM_32_BMI/8            1.46 ms         1.46 ms          477
BM_32_BMI/9            1.64 ms         1.64 ms          427
BM_32_BMI/10           1.76 ms         1.76 ms          397
BM_32_BMI/11           1.86 ms         1.86 ms          372
BM_32_BMI/12           2.00 ms         2.00 ms          343
BM_32_BMI/13           2.11 ms         2.12 ms          328
BM_32_BMI/14           2.23 ms         2.23 ms          310
BM_32_BMI/15           5.62 ms         5.62 ms          109
BM_32_BMI/16           5.95 ms         5.95 ms          106
BM_32_BMI/17           6.53 ms         6.52 ms           97
BM_32_BMI/18           6.88 ms         6.88 ms           92
BM_32_BMI/19           7.24 ms         7.24 ms           87
BM_32_BMI/20           7.61 ms         7.61 ms           84
BM_32_BMI/21           8.01 ms         8.01 ms           79
BM_32_BMI/22           8.41 ms         8.41 ms           75
BM_32_BMI/23           8.81 ms         8.80 ms           72
BM_32_BMI/24           9.19 ms         9.19 ms           69
BM_32_BMI/25           9.62 ms         9.62 ms           67
BM_32_BMI/26           9.98 ms         9.98 ms           63
BM_32_BMI/27           10.5 ms         10.5 ms           61
BM_32_BMI/28           10.9 ms         10.8 ms           59
BM_32_BMI/29           11.1 ms         11.1 ms           58
BM_32_BMI/30           11.5 ms         11.5 ms           55
BM_32_BMI/31           12.0 ms         12.0 ms           54
BM_32_BMI/32           12.7 ms         12.6 ms           50
BM_16_DEFAULT/1        1.70 ms         1.70 ms          409
BM_16_DEFAULT/2        1.82 ms         1.82 ms          382
BM_16_DEFAULT/3        1.83 ms         1.83 ms          379
BM_16_DEFAULT/4        2.05 ms         2.05 ms          336
BM_16_DEFAULT/5        1.99 ms         1.99 ms          349
BM_16_DEFAULT/6        2.80 ms         2.80 ms          248
BM_16_DEFAULT/7        2.14 ms         2.14 ms          321
BM_16_DEFAULT/8        2.54 ms         2.54 ms          275
BM_16_DEFAULT/9        2.29 ms         2.29 ms          305
BM_16_DEFAULT/10       2.36 ms         2.36 ms          296
BM_16_DEFAULT/11       2.44 ms         2.44 ms          278
BM_16_DEFAULT/12       2.53 ms         2.53 ms          275
BM_16_DEFAULT/13       2.62 ms         2.62 ms          264
BM_16_DEFAULT/14       2.72 ms         2.72 ms          255
BM_16_DEFAULT/15       5.86 ms         5.85 ms          104
BM_16_DEFAULT/16       6.68 ms         6.68 ms           94
BM_16_AVX2/1          0.519 ms        0.519 ms         1333
BM_16_AVX2/2          0.631 ms        0.631 ms         1087
BM_16_AVX2/3           1.13 ms         1.13 ms          611
BM_16_AVX2/4          0.813 ms        0.813 ms          831
BM_16_AVX2/5           1.82 ms         1.82 ms          379
BM_16_AVX2/6           1.80 ms         1.80 ms          382
BM_16_AVX2/7           2.46 ms         2.46 ms          282
BM_16_AVX2/8           1.24 ms         1.24 ms          554
BM_16_AVX2/9           3.10 ms         3.10 ms          225
BM_16_AVX2/10          3.22 ms         3.22 ms          216
BM_16_AVX2/11          3.33 ms         3.33 ms          209
BM_16_AVX2/12          3.46 ms         3.46 ms          202
BM_16_AVX2/13          3.47 ms         3.47 ms          200
BM_16_AVX2/14          3.56 ms         3.56 ms          194
BM_16_AVX2/15          6.69 ms         6.69 ms           91
BM_16_AVX2/16          5.55 ms         5.55 ms          113
BM_16_BMI/1           0.424 ms        0.424 ms         1652
BM_16_BMI/2           0.542 ms        0.542 ms         1268
BM_16_BMI/3           0.630 ms        0.630 ms         1097
BM_16_BMI/4           0.745 ms        0.745 ms          924
BM_16_BMI/5           0.844 ms        0.844 ms          815
BM_16_BMI/6           0.944 ms        0.944 ms          729
BM_16_BMI/7            1.05 ms         1.05 ms          657
BM_16_BMI/8            1.13 ms         1.14 ms          602
BM_16_BMI/9            1.32 ms         1.32 ms          526
BM_16_BMI/10           1.43 ms         1.43 ms          480
BM_16_BMI/11           1.54 ms         1.54 ms          448
BM_16_BMI/12           1.65 ms         1.65 ms          419
BM_16_BMI/13           1.77 ms         1.77 ms          392
BM_16_BMI/14           1.88 ms         1.88 ms          369
BM_16_BMI/15           5.03 ms         5.03 ms          119
BM_16_BMI/16           6.04 ms         6.04 ms          108
BM_8_DEFAULT/1         1.66 ms         1.66 ms          421
BM_8_DEFAULT/2         1.77 ms         1.77 ms          392
BM_8_DEFAULT/3         2.07 ms         2.07 ms          335
BM_8_DEFAULT/4         2.00 ms         2.00 ms          346
BM_8_DEFAULT/5         2.23 ms         2.23 ms          314
BM_8_DEFAULT/6         2.25 ms         2.25 ms          307
BM_8_DEFAULT/7         2.35 ms         2.35 ms          295
BM_8_DEFAULT/8         1.70 ms         1.70 ms          403
BM_8_AVX2/1           0.526 ms        0.526 ms         1318
BM_8_AVX2/2           0.614 ms        0.614 ms         1143
BM_8_AVX2/3            1.18 ms         1.18 ms          594
BM_8_AVX2/4           0.765 ms        0.765 ms          900
BM_8_AVX2/5            1.90 ms         1.90 ms          368
BM_8_AVX2/6            1.87 ms         1.87 ms          369
BM_8_AVX2/7            2.50 ms         2.50 ms          276
BM_8_AVX2/8            1.19 ms         1.19 ms          584
BM_8_BMI/1            0.369 ms        0.369 ms         1891
BM_8_BMI/2            0.453 ms        0.453 ms         1508
BM_8_BMI/3            0.540 ms        0.540 ms         1278
BM_8_BMI/4            0.642 ms        0.642 ms         1078
BM_8_BMI/5            0.728 ms        0.728 ms          940
BM_8_BMI/6            0.839 ms        0.840 ms          817
BM_8_BMI/7            0.943 ms        0.943 ms          726
BM_8_BMI/8             1.05 ms         1.05 ms          661
 *
 */

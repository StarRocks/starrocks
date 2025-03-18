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

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <cstdint>
#include <random>
#include <vector>

#include "column/vectorized_fwd.h"
#include "gutil/endian.h"
#ifdef __AVX2__
#include <emmintrin.h>
#endif

#include "column/column_helper.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks {

enum class FilterType {
    ORDINARY,
    COLLECT_ASSIGN,
    COMPRESS,
    CUSTOM,
};

template <typename T>
static inline size_t ordinary_filter(const Filter& filter, T* data) {
    auto result_offset = 0;
    for (auto i = 0; i < filter.size(); i++) {
        if (filter[i]) {
            *(data + result_offset) = *(data + i);
            result_offset++;
        }
    }
    return result_offset;
}

template <typename T>
static inline size_t collect_assign_filter(const Filter& filter, T* data, std::vector<T>& res_data) {
    auto res = 0;
    for (auto i = 0; i < filter.size(); i++) {
        res += filter[i] ? 1 : 0;
    }

    std::vector<size_t> pos(res);
    auto index = 0;
    for (auto i = 0; index < res; i++) {
        pos[index] = i;
        index += filter[i] ? 1 : 0;
    }
    res_data.resize(pos.size());
    for (auto i = 0; i < pos.size(); i++) {
        res_data[i] = *(data + pos[i]);
    }
    return res;
}

template <typename T>
static inline size_t filter_simd_compress(const Filter& filter, T* data) {
    std::vector<uint8> bit_mask(filter.size() / 8);

    constexpr size_t filter_batch_size = 64;
    auto mask_offset = 0;
#ifdef __AVX512BW__
    const __m512i zero64 = _mm512_setzero_si512();
    for (size_t i = 0; i < filter.size(); i += filter_batch_size) {
        int64 m = _mm512_cmpneq_epi8_mask(_mm512_loadu_si512(reinterpret_cast<const __m512i*>(filter.data() + i)),
                                          zero64);
        // int64 m_order = ghtonll(m);
        memcpy(bit_mask.data() + mask_offset, &m, 8);
        mask_offset += 8;
    }
#endif
    constexpr size_t batch_size = 512 / (sizeof(T) * 8);
    constexpr size_t mask_batch_size = batch_size / 8;
    size_t res = 0;
    size_t batches = bit_mask.size() * 8 / batch_size;
    if constexpr (sizeof(T) * 8 == 32) {
        for (size_t i = 0; i < batches; i++) {
#ifdef __AVX512F__
            __mmask16 m = *(reinterpret_cast<const uint16_t*>(bit_mask.data() + i * mask_batch_size));
            __m512i src = _mm512_loadu_epi32(data + i * batch_size);
            _mm512_mask_compressstoreu_epi32(data + res, m, src);
            res += __builtin_popcount(m);
#endif
        }
    } else if constexpr (sizeof(T) * 8 == 64) {
        for (size_t i = 0; i < batches; i++) {
#ifdef __AVX512F__
            __mmask8 m = bit_mask[i * mask_batch_size];
            __m512i src = _mm512_loadu_epi64(data + i * batch_size);
            _mm512_mask_compressstoreu_epi64(data + res, m, src);
            res += __builtin_popcount(m);
#endif
        }
    } else {
        // pass
    }

    return res;
}

template <typename T>
class FilterDataBench {
public:
    void SetUp();
    void TearDown() {}

    FilterDataBench(size_t ratio) : _ratio(ratio) {}

    template <FilterType type>
    size_t do_bench();

    // called before filter, used to check the filter result.
    T expect_result() {
        T sum = 0;
        for (size_t i = 0; i < _data.size(); i++) {
            if (_filter[i]) {
                sum += _data[i];
            }
        }
        return sum;
    }

    // called after filter, used to check the filter result.
    template <FilterType type>
    T final_result(size_t size) {
        T sum = 0;
        if constexpr (type != FilterType::COLLECT_ASSIGN) {
            for (size_t i = 0; i < size; i++) {
                sum += _data[i];
            }
        } else {
            for (size_t i = 0; i < size; i++) {
                sum += _res_data[i];
            }
        }
        return sum;
    }

private:
    size_t _ratio = 0;
    static constexpr size_t _num_rows = 4096;
    Filter _filter;
    std::vector<T> _data;
    std::vector<T> _res_data;
};

template <typename T>
void FilterDataBench<T>::SetUp() {
    _filter.resize(_num_rows);
    _data.resize(_num_rows);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<T> dist(0, 999);

    for (size_t i = 0; i < _num_rows; i++) {
        int random_number = dist(rng);
        _filter[i] = random_number < _ratio ? 0 : 1;
        _data[i] = random_number;
    }
}

template <typename T>
template <FilterType type>
size_t FilterDataBench<T>::do_bench() {
    if constexpr (type == FilterType::ORDINARY) {
        return ordinary_filter<T>(_filter, _data.data());
    } else if constexpr (type == FilterType::COLLECT_ASSIGN) {
        return collect_assign_filter<T>(_filter, _data.data(), _res_data);
    } else if constexpr (type == FilterType::COMPRESS) {
        return filter_simd_compress<T>(_filter, _data.data());
    } else {
        return ColumnHelper::filter<T>(_filter, _data.data());
    }
}

template <FilterType type>
static void BM_FilterData_T(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        FilterDataBench<uint64_t> bench(state.range(0));
        bench.SetUp();
        size_t sum = bench.expect_result();
        size_t res = 0;
        state.ResumeTiming();
        res = bench.do_bench<type>();
        state.PauseTiming();
        ASSERT_EQ(sum, bench.final_result<type>(res));
    }
}

static void BM_FilterData_Custom(benchmark::State& state) {
    BM_FilterData_T<FilterType::CUSTOM>(state);
}

static void BM_FilterData_Ordinary(benchmark::State& state) {
    BM_FilterData_T<FilterType::ORDINARY>(state);
}

static void BM_FilterData_CAndA(benchmark::State& state) {
    BM_FilterData_T<FilterType::COLLECT_ASSIGN>(state);
}

static void BM_FilterData_Compress(benchmark::State& state) {
    BM_FilterData_T<FilterType::COMPRESS>(state);
}

BENCHMARK(BM_FilterData_Custom)->ArgsProduct({{0, 20, 40, 60, 80, 100, 300, 500, 700, 900, 920, 940, 960, 980, 1000}});

#if defined(__AVX512F__) && defined(__AVX512BW__)
BENCHMARK(BM_FilterData_Compress)
        ->ArgsProduct({{0, 20, 40, 60, 80, 100, 300, 500, 700, 900, 920, 940, 960, 980, 1000}});
#endif

BENCHMARK(BM_FilterData_Ordinary)
        ->ArgsProduct({{0, 20, 40, 60, 80, 100, 300, 500, 700, 900, 920, 940, 960, 980, 1000}});

BENCHMARK(BM_FilterData_CAndA)->ArgsProduct({{0, 20, 40, 60, 80, 100, 300, 500, 700, 900, 920, 940, 960, 980, 1000}});

} //namespace starrocks

BENCHMARK_MAIN();

/*
----------------------------------------------------------------------
Benchmark                            Time             CPU   Iterations
----------------------------------------------------------------------
BM_FilterData_Custom/0            2402 ns         2357 ns       297359
BM_FilterData_Custom/20           3490 ns         3461 ns       202131
BM_FilterData_Custom/40           2930 ns         2885 ns       242048
BM_FilterData_Custom/60           2431 ns         2394 ns       292127
BM_FilterData_Custom/80           2197 ns         2158 ns       324461
BM_FilterData_Custom/100          2086 ns         2045 ns       342555
BM_FilterData_Custom/300          1963 ns         1915 ns       365499
BM_FilterData_Custom/500          1946 ns         1898 ns       369684
BM_FilterData_Custom/700          1922 ns         1881 ns       372513
BM_FilterData_Custom/900          2686 ns         2634 ns       265878
BM_FilterData_Custom/920          2999 ns         2940 ns       238365
BM_FilterData_Custom/940          3303 ns         3250 ns       214932
BM_FilterData_Custom/960          3563 ns         3506 ns       199595
BM_FilterData_Custom/980          3248 ns         3203 ns       218570
BM_FilterData_Custom/1000         1170 ns         1124 ns       623176
BM_FilterData_Ordinary/0          7758 ns         7709 ns        90928
BM_FilterData_Ordinary/20         9250 ns         9194 ns        76172
BM_FilterData_Ordinary/40        10758 ns        10706 ns        65369
BM_FilterData_Ordinary/60        12280 ns        12233 ns        57226
BM_FilterData_Ordinary/80        13818 ns        13771 ns        50804
BM_FilterData_Ordinary/100       15376 ns        15330 ns        45678
BM_FilterData_Ordinary/300       31681 ns        31643 ns        22294
BM_FilterData_Ordinary/500       40954 ns        40914 ns        17095
BM_FilterData_Ordinary/700       32627 ns        32590 ns        21308
BM_FilterData_Ordinary/900       16672 ns        16626 ns        42062
BM_FilterData_Ordinary/920       14805 ns        14750 ns        47332
BM_FilterData_Ordinary/940       12853 ns        12803 ns        54537
BM_FilterData_Ordinary/960       10846 ns        10795 ns        64791
BM_FilterData_Ordinary/980        8736 ns         8686 ns        80564
BM_FilterData_Ordinary/1000       6678 ns         6629 ns       105723
BM_FilterData_CAndA/0            14344 ns        14313 ns        48456
BM_FilterData_CAndA/20           14187 ns        14153 ns        49442
BM_FilterData_CAndA/40           14032 ns        13996 ns        50019
BM_FilterData_CAndA/60           13906 ns        13866 ns        50418
BM_FilterData_CAndA/80           13832 ns        13795 ns        50918
BM_FilterData_CAndA/100          13692 ns        13660 ns        51185
BM_FilterData_CAndA/300          12234 ns        12198 ns        57389
BM_FilterData_CAndA/500          10867 ns        10835 ns        64551
BM_FilterData_CAndA/700           9461 ns         9422 ns        74335
BM_FilterData_CAndA/900           8028 ns         7978 ns        87790
BM_FilterData_CAndA/920           7903 ns         7846 ns        89225
BM_FilterData_CAndA/940           7753 ns         7703 ns        90843
BM_FilterData_CAndA/960           7604 ns         7554 ns        92631
BM_FilterData_CAndA/980           7473 ns         7421 ns        94427
BM_FilterData_CAndA/1000          1405 ns         1356 ns       517270
 */

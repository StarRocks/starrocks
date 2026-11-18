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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <random>
#include <type_traits>
#include <vector>

#include "base/orlp/pdqsort.h"
#include "base/simd/simd.h"
#include "column/column_sorter_comparator.h"
#include "column/sorting/radix_sort.h"
#include "column/sorting/sort_permute.h"

namespace starrocks {
namespace {

enum class Distribution : int {
    UNIFORM_RANDOM = 0,
    ALREADY_SORTED = 1,
    REVERSE_SORTED = 2,
    LOW_CARDINALITY = 3,
    NON_NEGATIVE_LOW_CARD = 4,
    NEARLY_SORTED = 5,
    SORTED_PREFIX_THEN_RANDOM = 6,
    ZIPFIAN = 7,
};

template <typename T>
std::vector<T> generate_data(size_t n, Distribution dist) {
    std::mt19937_64 rng(0x5EED1234ULL);
    std::vector<T> data(n);

    switch (dist) {
    case Distribution::UNIFORM_RANDOM: {
        if constexpr (std::is_integral_v<T>) {
            std::uniform_int_distribution<T> d(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
            for (size_t i = 0; i < n; ++i) {
                data[i] = d(rng);
            }
        } else {
            std::uniform_real_distribution<T> d(static_cast<T>(-1e6), static_cast<T>(1e6));
            for (size_t i = 0; i < n; ++i) {
                data[i] = d(rng);
            }
        }
        break;
    }
    case Distribution::ALREADY_SORTED: {
        for (size_t i = 0; i < n; ++i) {
            data[i] = static_cast<T>(static_cast<int64_t>(i) - static_cast<int64_t>(n / 2));
        }
        break;
    }
    case Distribution::REVERSE_SORTED: {
        for (size_t i = 0; i < n; ++i) {
            data[i] = static_cast<T>(static_cast<int64_t>(n / 2) - static_cast<int64_t>(i));
        }
        break;
    }
    case Distribution::LOW_CARDINALITY: {
        std::uniform_int_distribution<int> d(0, 99);
        for (size_t i = 0; i < n; ++i) {
            data[i] = static_cast<T>(d(rng) - 50);
        }
        break;
    }
    case Distribution::NON_NEGATIVE_LOW_CARD: {
        std::uniform_int_distribution<int> d(0, 99);
        for (size_t i = 0; i < n; ++i) {
            data[i] = static_cast<T>(d(rng));
        }
        break;
    }
    case Distribution::NEARLY_SORTED: {
        for (size_t i = 0; i < n; ++i) {
            data[i] = static_cast<T>(static_cast<int64_t>(i) - static_cast<int64_t>(n / 2));
        }
        if (n > 1) {
            const size_t swaps = std::max<size_t>(1, n / 100);
            std::uniform_int_distribution<size_t> idx_dist(0, n - 1);
            for (size_t s = 0; s < swaps; ++s) {
                std::swap(data[idx_dist(rng)], data[idx_dist(rng)]);
            }
        }
        break;
    }
    case Distribution::SORTED_PREFIX_THEN_RANDOM: {
        const size_t prefix = (n * 9) / 10;
        for (size_t i = 0; i < prefix; ++i) {
            data[i] = static_cast<T>(static_cast<int64_t>(i) - static_cast<int64_t>(n / 2));
        }
        if constexpr (std::is_integral_v<T>) {
            std::uniform_int_distribution<T> d(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
            for (size_t i = prefix; i < n; ++i) {
                data[i] = d(rng);
            }
        } else {
            std::uniform_real_distribution<T> d(static_cast<T>(-1e6), static_cast<T>(1e6));
            for (size_t i = prefix; i < n; ++i) {
                data[i] = d(rng);
            }
        }
        break;
    }
    case Distribution::ZIPFIAN: {
        for (size_t i = 0; i < n; ++i) {
            if ((rng() % 5) != 0) {
                data[i] = static_cast<T>(rng() & 3ULL);
            } else {
                data[i] = static_cast<T>(rng() & 1023ULL);
            }
        }
        break;
    }
    }
    return data;
}

template <typename T>
InlinePermutation<T> make_initial_permutation(const std::vector<T>& values) {
    const size_t n = values.size();
    InlinePermutation<T> items(n);
    for (size_t i = 0; i < n; ++i) {
        items[i].inline_value = values[i];
        items[i].index_in_chunk = static_cast<uint32_t>(i);
    }
    return items;
}

template <typename T>
inline void pdqsort_and_tie(InlinePermuteItem<T>* items, size_t n, Tie& tie) {
    auto lesser = [](const InlinePermuteItem<T>& lhs, const InlinePermuteItem<T>& rhs) {
        return SorterComparator<T>::compare(lhs.inline_value, rhs.inline_value) < 0;
    };
    ::pdqsort(items, items + n, lesser);
    tie[0] = 0;
    for (size_t i = 1; i < n; ++i) {
        tie[i] &= static_cast<uint8_t>(SorterComparator<T>::compare(items[i - 1].inline_value, items[i].inline_value) ==
                                       0);
    }
}

inline void stabilize_ties_by_index_in_chunk(SmallPermutation& small_perm, const Tie& tie) {
    const size_t n = tie.size();
    size_t inner_first = 0;
    while (inner_first < n) {
        if (inner_first == 0 && tie[0] == 1) {
            // Start from 0
        } else {
            inner_first = SIMD::find_nonzero(tie, inner_first + 1, n - (inner_first + 1));
            if (inner_first >= n) {
                break;
            }
            inner_first--;
        }
        size_t inner_last = SIMD::find_zero(tie, inner_first + 1, n - inner_first + 1);
        inner_last = std::min(inner_last, n);
        if (inner_last - inner_first > 1) {
            ::pdqsort(
                    small_perm.begin() + inner_first, small_perm.begin() + inner_last,
                    [](SmallPermuteItem lhs, SmallPermuteItem rhs) { return lhs.index_in_chunk < rhs.index_in_chunk; });
        }
        inner_first = inner_last;
    }
}

template <typename T, Distribution Dist>
void BM_PdqSort(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    const auto values = generate_data<T>(n, Dist);
    const auto initial_items = make_initial_permutation(values);
    InlinePermutation<T> items(n);
    Tie tie(n, 1);

    for (auto _ : state) {
        std::memcpy(items.data(), initial_items.data(), n * sizeof(InlinePermuteItem<T>));
        std::memset(tie.data(), 1, n);
        pdqsort_and_tie(items.data(), n, tie);
        benchmark::DoNotOptimize(items.data());
        benchmark::DoNotOptimize(tie.data());
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(n * sizeof(T)));
}

template <typename T, Distribution Dist>
void BM_RadixSort(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    const auto values = generate_data<T>(n, Dist);
    const auto initial_items = make_initial_permutation(values);
    InlinePermutation<T> items(n);
    InlinePermutation<T> scratch(n);
    Tie tie(n, 1);
    const bool cancel = false;

    for (auto _ : state) {
        std::memcpy(items.data(), initial_items.data(), n * sizeof(InlinePermuteItem<T>));
        std::memset(tie.data(), 1, n);
        tie[0] = 0;
        bool range_has_ties = false;
        radix_sort_inlined_and_tie<T>(cancel, items.data(), scratch.data(), 0, n, true, tie, range_has_ties);
        benchmark::DoNotOptimize(items.data());
        benchmark::DoNotOptimize(tie.data());
        benchmark::DoNotOptimize(range_has_ties);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(n * sizeof(T)));
}

template <typename T, Distribution Dist>
void BM_RadixSortColdScratch(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    const auto values = generate_data<T>(n, Dist);
    const auto initial_items = make_initial_permutation(values);
    InlinePermutation<T> items(n);
    Tie tie(n, 1);
    const bool cancel = false;

    for (auto _ : state) {
        std::memcpy(items.data(), initial_items.data(), n * sizeof(InlinePermuteItem<T>));
        std::memset(tie.data(), 1, n);
        tie[0] = 0;
        bool range_has_ties = false;
        InlinePermutation<T> scratch;
        radix_sort_inlined_and_tie<T>(cancel, items.data(), scratch, 0, n, true, tie, range_has_ties);
        benchmark::DoNotOptimize(items.data());
        benchmark::DoNotOptimize(tie.data());
        benchmark::DoNotOptimize(range_has_ties);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(n * sizeof(T)));
}

template <typename T>
void BM_PdqSortWithStabilization(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    const auto values = generate_data<T>(n, Distribution::LOW_CARDINALITY);
    const auto initial_items = make_initial_permutation(values);
    InlinePermutation<T> items(n);
    SmallPermutation small_perm(n);
    Tie tie(n, 1);

    for (auto _ : state) {
        std::memcpy(items.data(), initial_items.data(), n * sizeof(InlinePermuteItem<T>));
        std::memset(tie.data(), 1, n);
        pdqsort_and_tie(items.data(), n, tie);
        restore_inline_permutation(items, small_perm);
        stabilize_ties_by_index_in_chunk(small_perm, tie);
        benchmark::DoNotOptimize(small_perm.data());
        benchmark::DoNotOptimize(tie.data());
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(n * sizeof(T)));
}

template <typename T>
void BM_RadixSortStable(benchmark::State& state) {
    const size_t n = static_cast<size_t>(state.range(0));
    const auto values = generate_data<T>(n, Distribution::LOW_CARDINALITY);
    const auto initial_items = make_initial_permutation(values);
    InlinePermutation<T> items(n);
    InlinePermutation<T> scratch(n);
    SmallPermutation small_perm(n);
    Tie tie(n, 1);
    const bool cancel = false;

    for (auto _ : state) {
        std::memcpy(items.data(), initial_items.data(), n * sizeof(InlinePermuteItem<T>));
        std::memset(tie.data(), 1, n);
        tie[0] = 0;
        bool range_has_ties = false;
        radix_sort_inlined_and_tie<T>(cancel, items.data(), scratch.data(), 0, n, true, tie, range_has_ties);
        restore_inline_permutation(items, small_perm);
        benchmark::DoNotOptimize(small_perm.data());
        benchmark::DoNotOptimize(tie.data());
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(n * sizeof(T)));
}

#define REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, DIST, DIST_NAME)    \
    BENCHMARK_TEMPLATE(BM_PdqSort, TYPE, Distribution::DIST)              \
            ->Name("BM_PdqSort_" TYPE_NAME "_" DIST_NAME)                 \
            ->Arg(256)                                                    \
            ->Arg(1024)                                                   \
            ->Arg(4096)                                                   \
            ->Arg(65536)                                                  \
            ->Arg(1000000);                                               \
    BENCHMARK_TEMPLATE(BM_RadixSort, TYPE, Distribution::DIST)            \
            ->Name("BM_RadixSort_" TYPE_NAME "_" DIST_NAME)               \
            ->Arg(256)                                                    \
            ->Arg(1024)                                                   \
            ->Arg(4096)                                                   \
            ->Arg(65536)                                                  \
            ->Arg(1000000);                                               \
    BENCHMARK_TEMPLATE(BM_RadixSortColdScratch, TYPE, Distribution::DIST) \
            ->Name("BM_RadixSortColdScratch_" TYPE_NAME "_" DIST_NAME)    \
            ->Arg(256)                                                    \
            ->Arg(1024)                                                   \
            ->Arg(4096)                                                   \
            ->Arg(65536)                                                  \
            ->Arg(1000000)

#define REGISTER_SORT_BENCH_FOR_TYPE(TYPE, TYPE_NAME)                                           \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, UNIFORM_RANDOM, "UniformRandom");             \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, ALREADY_SORTED, "AlreadySorted");             \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, REVERSE_SORTED, "ReverseSorted");             \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, LOW_CARDINALITY, "LowCardinality");           \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, NON_NEGATIVE_LOW_CARD, "NonNegLowCard");      \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, NEARLY_SORTED, "NearlySorted");               \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, SORTED_PREFIX_THEN_RANDOM, "SortedPrefix90"); \
    REGISTER_SORT_BENCH_FOR_DIST(TYPE, TYPE_NAME, ZIPFIAN, "Zipfian")

REGISTER_SORT_BENCH_FOR_TYPE(int32_t, "Int32");
REGISTER_SORT_BENCH_FOR_TYPE(int64_t, "Int64");
REGISTER_SORT_BENCH_FOR_TYPE(float, "Float");
REGISTER_SORT_BENCH_FOR_TYPE(double, "Double");

#define REGISTER_STABLE_SORT_BENCH(TYPE, TYPE_NAME)                     \
    BENCHMARK_TEMPLATE(BM_PdqSortWithStabilization, TYPE)               \
            ->Name("BM_PdqSortWithStabilization_" TYPE_NAME "_LowCard") \
            ->Arg(256)                                                  \
            ->Arg(1024)                                                 \
            ->Arg(4096)                                                 \
            ->Arg(65536)                                                \
            ->Arg(1000000);                                             \
    BENCHMARK_TEMPLATE(BM_RadixSortStable, TYPE)                        \
            ->Name("BM_RadixSortStable_" TYPE_NAME "_LowCard")          \
            ->Arg(256)                                                  \
            ->Arg(1024)                                                 \
            ->Arg(4096)                                                 \
            ->Arg(65536)                                                \
            ->Arg(1000000)

REGISTER_STABLE_SORT_BENCH(int32_t, "Int32");
REGISTER_STABLE_SORT_BENCH(int64_t, "Int64");
REGISTER_STABLE_SORT_BENCH(float, "Float");
REGISTER_STABLE_SORT_BENCH(double, "Double");

} // namespace
} // namespace starrocks

BENCHMARK_MAIN();

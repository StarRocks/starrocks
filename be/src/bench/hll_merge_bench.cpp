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

// Microbench for the HyperLogLog merge / serialize hot paths.
//
// Compares, in a single binary, the legacy temp-object merge against the
// in-place HyperLogLog::merge(const Slice&) FULL fast path, and the legacy
// per-row HLL construction in convert_to_serialize_format against direct
// explicit-record emit. Run with:
//   --benchmark_filter=BM_Hll

#include <benchmark/benchmark.h>

#include <random>
#include <string>
#include <vector>

#include "base/coding.h"
#include "base/string/slice.h"
#include "types/constexpr.h"
#include "types/hll.h"

namespace starrocks {

// Build a HLL that ends up in the FULL register format (>> HLL_EXPLICLIT_INT64_NUM
// distinct hashes), then serialize it to a heap buffer.
static std::string make_serialized_full(int n_distinct, uint64_t seed) {
    HyperLogLog hll;
    std::mt19937_64 rng(seed);
    for (int i = 0; i < n_distinct; ++i) {
        hll.update(rng());
    }
    std::string buf;
    buf.resize(hll.max_serialized_size());
    size_t n = hll.serialize(reinterpret_cast<uint8_t*>(buf.data()));
    buf.resize(n);
    return buf;
}

// Build a HLL that stays in the EXPLICIT format (<= HLL_EXPLICLIT_INT64_NUM hashes).
static std::string make_serialized_explicit(int n_distinct, uint64_t seed) {
    HyperLogLog hll;
    std::mt19937_64 rng(seed);
    for (int i = 0; i < n_distinct; ++i) {
        hll.update(rng());
    }
    std::string buf;
    buf.resize(hll.max_serialized_size());
    size_t n = hll.serialize(reinterpret_cast<uint8_t*>(buf.data()));
    buf.resize(n);
    return buf;
}

static constexpr int kSources = 256;

template <bool kDirect>
static void do_merge(benchmark::State& state, bool full) {
    const int n_distinct = full ? 50000 : 64;
    std::vector<std::string> sources;
    sources.reserve(kSources);
    for (int i = 0; i < kSources; ++i) {
        sources.emplace_back(full ? make_serialized_full(n_distinct, i + 1)
                                  : make_serialized_explicit(n_distinct, i + 1));
    }

    for (auto _ : state) {
        HyperLogLog dst;
        for (const auto& s : sources) {
            Slice slice(s);
            if constexpr (kDirect) {
                dst.merge(slice);
            } else {
                // Legacy path: materialize a temporary HLL, then merge it in.
                HyperLogLog tmp(slice);
                dst.merge(tmp);
            }
        }
        benchmark::DoNotOptimize(dst.estimate_cardinality());
    }
    state.SetItemsProcessed(state.iterations() * kSources);
}

// Group-by merge: each serialized HLL is merged into its group's state, cycling
// through n_groups distinct destinations instead of one global state. This is the
// `ndv ... GROUP BY` shape, where the destination registers cycle and turn cache-
// cold (vs the single-state path above, where the one destination stays hot).
// Sources come from a small fixed pool so setup stays cheap.
template <bool kDirect>
static void do_merge_groupby(benchmark::State& state) {
    const int n_rows = state.range(0);
    const int n_groups = state.range(1);
    static const std::vector<std::string> pool = [] {
        std::vector<std::string> p;
        p.reserve(kSources);
        for (int i = 0; i < kSources; ++i) {
            p.emplace_back(make_serialized_full(50000, i + 1)); // FULL on the wire
        }
        return p;
    }();

    for (auto _ : state) {
        std::vector<HyperLogLog> groups(n_groups);
        for (int i = 0; i < n_rows; ++i) {
            HyperLogLog& dst = groups[i % n_groups];
            Slice slice(pool[i % kSources]);
            if constexpr (kDirect) {
                dst.merge(slice);
            } else {
                HyperLogLog tmp(slice);
                dst.merge(tmp);
            }
        }
        for (auto& g : groups) {
            benchmark::DoNotOptimize(g.estimate_cardinality());
        }
    }
    state.SetItemsProcessed(state.iterations() * n_rows);
}

static void BM_HllMergeFullSingle_Legacy(benchmark::State& state) {
    do_merge<false>(state, /*full=*/true);
}
static void BM_HllMergeFullSingle_Direct(benchmark::State& state) {
    do_merge<true>(state, /*full=*/true);
}
static void BM_HllMergeExplicitSingle_Legacy(benchmark::State& state) {
    do_merge<false>(state, /*full=*/false);
}
static void BM_HllMergeExplicitSingle_Direct(benchmark::State& state) {
    do_merge<true>(state, /*full=*/false);
}
static void BM_HllMergeFullGroupBy_Legacy(benchmark::State& state) {
    do_merge_groupby<false>(state);
}
static void BM_HllMergeFullGroupBy_Direct(benchmark::State& state) {
    do_merge_groupby<true>(state);
}

// Mirrors HllNdvAggregateFunction::convert_to_serialize_format for a chunk of
// scalar values. Legacy constructs a HyperLogLog (heap-allocates a flat_hash_set
// for one element) per row; direct emits the explicit record straight into bytes.
template <bool kDirect>
static void do_convert(benchmark::State& state) {
    const int chunk_size = state.range(0);
    std::vector<uint64_t> values(chunk_size);
    std::mt19937_64 rng(12345);
    for (auto& v : values) {
        v = rng();
    }

    for (auto _ : state) {
        std::vector<uint8_t> bytes;
        bytes.reserve(chunk_size * 10);
        size_t old_size = 0;
        for (int i = 0; i < chunk_size; ++i) {
            uint64_t value = values[i];
            if constexpr (kDirect) {
                if (value == 0) {
                    bytes.resize(old_size + 1);
                    bytes[old_size] = HLL_DATA_EMPTY;
                    old_size += 1;
                } else {
                    bytes.resize(old_size + 10);
                    uint8_t* p = bytes.data() + old_size;
                    p[0] = HLL_DATA_EXPLICIT;
                    p[1] = 1;
                    encode_fixed64_le(p + 2, value);
                    old_size += 10;
                }
            } else {
                HyperLogLog hll;
                if (value != 0) {
                    hll.update(value);
                }
                size_t new_size = old_size + hll.max_serialized_size();
                bytes.resize(new_size);
                hll.serialize(bytes.data() + old_size);
                old_size = new_size;
            }
        }
        benchmark::DoNotOptimize(bytes.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * chunk_size);
}

static void BM_HllConvert_Legacy(benchmark::State& state) {
    do_convert<false>(state);
}
static void BM_HllConvert_Direct(benchmark::State& state) {
    do_convert<true>(state);
}

BENCHMARK(BM_HllMergeFullSingle_Legacy);
BENCHMARK(BM_HllMergeFullSingle_Direct);
BENCHMARK(BM_HllMergeExplicitSingle_Legacy);
BENCHMARK(BM_HllMergeExplicitSingle_Direct);
BENCHMARK(BM_HllMergeFullGroupBy_Legacy)->Args({4096, 64})->Args({4096, 512});
BENCHMARK(BM_HllMergeFullGroupBy_Direct)->Args({4096, 64})->Args({4096, 512});
BENCHMARK(BM_HllConvert_Legacy)->Arg(1024)->Arg(4096);
BENCHMARK(BM_HllConvert_Direct)->Arg(1024)->Arg(4096);

} // namespace starrocks

BENCHMARK_MAIN();

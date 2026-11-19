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

#include "types/tdigest.h"

namespace starrocks {

// Model percentile_approx merging the small states produced by case-level aggregation.
static void BM_TDigestMergeSingleton(benchmark::State& state) {
    std::vector<TDigest> inputs(1024);
    for (size_t i = 0; i < inputs.size(); ++i) {
        inputs[i].add((i * 7919) % 100003);
    }

    const auto num_merges = state.range(0);
    for (auto _ : state) {
        TDigest digest(10000);
        for (int64_t i = 0; i < num_merges; ++i) {
            digest.merge(&inputs[i % inputs.size()]);
        }
        benchmark::DoNotOptimize(digest.quantile(0.5));
        benchmark::DoNotOptimize(digest.quantile(0.9));
    }
    state.SetItemsProcessed(state.iterations() * num_merges);
}

BENCHMARK(BM_TDigestMergeSingleton)->Arg(10000)->Arg(100000)->Arg(1000000)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();

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

#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "types/json_value.h"

// Benchmark for JsonValue::parse_json_or_string, which is on the hot path of
// both casting a string column to JSON and the get_json_*() functions
// (_string_json parses the input string column row by row). Profiling showed
// the per-row Parser/Builder allocation dominating this function.
//
// It compares two variants:
//   - OneShot: the original behavior, where every call allocates a fresh
//     velocypack Parser/Builder/Buffer internally (Parser::fromJson).
//   - Reused: a single parser is created per batch and reused across all rows,
//     so the Builder/Buffer is allocated once and its capacity is amortized.
//
// Run with e.g.:
//   ./json_parse_bench --benchmark_filter=BM_ParseJson
//
// Sample results (4096 rows/batch, Release build, 32-core host):
//   Benchmark (rows/width)        OneShot      Reused     Speedup
//   4096 / 1                      1230 us       491 us      2.51x
//   4096 / 8                      4133 us      3217 us      1.28x
//   4096 / 32                    16068 us     14712 us      1.09x
// Reuse wins most for small objects where per-row Parser/Builder allocation
// dominates; for wide objects the byte-level parse cost amortizes the gain.

namespace starrocks {

// Build `rows` JSON object strings. `width` controls how many key/value pairs
// each object carries, which drives the per-row parsing cost.
static std::vector<std::string> make_json_rows(int rows, int width) {
    std::vector<std::string> data;
    data.reserve(rows);
    for (int r = 0; r < rows; ++r) {
        std::string s = "{";
        for (int c = 0; c < width; ++c) {
            if (c > 0) {
                s += ",";
            }
            s += "\"key_" + std::to_string(c) + "\":";
            // Mix value kinds so the parser exercises several code paths.
            switch (c % 4) {
            case 0:
                s += std::to_string(r * 31 + c);
                break;
            case 1:
                s += "\"value_" + std::to_string(r) + "_" + std::to_string(c) + "\"";
                break;
            case 2:
                s += (c % 2 == 0) ? "true" : "false";
                break;
            default:
                s += std::to_string((r + c) * 0.5);
                break;
            }
        }
        s += "}";
        data.emplace_back(std::move(s));
    }
    return data;
}

// Baseline: allocate a parser per row (Parser::fromJson under the hood).
static void BM_ParseJson_OneShot(benchmark::State& state) {
    const int rows = state.range(0);
    const int width = state.range(1);
    const auto data = make_json_rows(rows, width);

    size_t total = 0;
    for (auto _ : state) {
        for (const auto& s : data) {
            auto res = JsonValue::parse_json_or_string(Slice(s));
            benchmark::DoNotOptimize(res);
            if (res.ok()) {
                total += res.value().serialize_size();
            }
        }
    }
    benchmark::DoNotOptimize(total);
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
}

// Optimized: reuse one parser for the whole batch.
static void BM_ParseJson_Reused(benchmark::State& state) {
    const int rows = state.range(0);
    const int width = state.range(1);
    const auto data = make_json_rows(rows, width);

    size_t total = 0;
    for (auto _ : state) {
        auto parser = JsonValue::make_reusable_parser();
        for (const auto& s : data) {
            auto res = JsonValue::parse_json_or_string(Slice(s), parser.get());
            benchmark::DoNotOptimize(res);
            if (res.ok()) {
                total += res.value().serialize_size();
            }
        }
    }
    benchmark::DoNotOptimize(total);
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
}

// Args: {rows, width}. Vary both the batch size and per-row complexity.
BENCHMARK(BM_ParseJson_OneShot)->Args({4096, 1})->Args({4096, 8})->Args({4096, 32})->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ParseJson_Reused)->Args({4096, 1})->Args({4096, 8})->Args({4096, 32})->Unit(benchmark::kMicrosecond);

} // namespace starrocks

BENCHMARK_MAIN();

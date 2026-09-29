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

// Filtered copy of a column that must stay intact (the parquet lazy column triggered during predicate
// evaluation, still referenced by the read chunk and the slot cache):
//   MUTATE:    Column::mutate(col) + filter   (the old code; with COW off it deep-copies twice)
//   CLONE:     col->clone() + filter
//   SELECTIVE: col->clone_empty() + append_selective(surviving rows)
//
//   ./filter_copy_column_bench --benchmark_repetitions=5 --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>

#include <random>
#include <vector>

#include "column/binary_column.h"
#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks {

static constexpr size_t kRows = 4096;

enum CopyMode { MUTATE = 0, CLONE = 1, SELECTIVE = 2 };
enum ColType { INT32 = 0, INT64 = 1, STRING = 2 };

static ColumnPtr make_column(ColType type, std::mt19937& rng) {
    auto nulls = NullColumn::create();
    MutableColumnPtr data;
    if (type == INT32) {
        auto c = Int32Column::create();
        for (size_t i = 0; i < kRows; i++) c->append(static_cast<int32_t>(rng()));
        data = std::move(c);
    } else if (type == INT64) {
        auto c = Int64Column::create();
        for (size_t i = 0; i < kRows; i++) c->append(static_cast<int64_t>(rng()));
        data = std::move(c);
    } else {
        auto c = BinaryColumn::create();
        for (size_t i = 0; i < kRows; i++) {
            std::string s(8 + rng() % 24, static_cast<char>('a' + rng() % 26));
            c->append(Slice(s));
        }
        data = std::move(c);
    }
    for (size_t i = 0; i < kRows; i++) nulls->append(rng() % 20 == 0);
    return NullableColumn::create(std::move(data), std::move(nulls));
}

MutableColumnPtr filtered_copy(CopyMode mode, const ColumnPtr& col, const Filter& filter,
                               std::vector<uint32_t>* indexes) {
    if (mode == MUTATE) {
        ColumnPtr shared = col;
        auto res = Column::mutate(std::move(shared));
        res->filter(filter);
        return res;
    }
    if (mode == CLONE) {
        auto res = col->clone();
        res->filter(filter);
        return res;
    }
    indexes->clear();
    for (uint32_t i = 0; i < filter.size(); i++) {
        if (filter[i]) indexes->push_back(i);
    }
    auto res = col->clone_empty();
    res->append_selective(*col, indexes->data(), 0, static_cast<uint32_t>(indexes->size()));
    return res;
}

static void BM_FilterCopy(benchmark::State& state) {
    auto mode = static_cast<CopyMode>(state.range(0));
    auto type = static_cast<ColType>(state.range(1));
    int keep_pct = static_cast<int>(state.range(2));
    std::mt19937 rng(11);
    ColumnPtr col = make_column(type, rng);
    // a second reference, as the read chunk and slot cache both hold the column
    ColumnPtr holder = col;
    Filter filter(kRows);
    for (auto& f : filter) f = static_cast<int>(rng() % 100) < keep_pct;
    std::vector<uint32_t> indexes;
    indexes.reserve(kRows);

    auto check = filtered_copy(mode, col, filter, &indexes);
    auto expect = filtered_copy(CLONE, col, filter, &indexes);
    CHECK_EQ(check->size(), expect->size());
    for (size_t i = 0; i < expect->size(); i++) CHECK_EQ(0, check->compare_at(i, i, *expect, -1));

    for (auto _ : state) {
        auto res = filtered_copy(mode, col, filter, &indexes);
        benchmark::DoNotOptimize(res->size());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kRows);
    static const char* kModes[] = {"mutate", "clone", "selective"};
    static const char* kTypes[] = {"int32", "int64", "string"};
    state.SetLabel(std::string(kModes[mode]) + "/" + kTypes[type]);
}

BENCHMARK(BM_FilterCopy)
        ->ArgNames({"mode", "type", "keep_pct"})
        ->ArgsProduct({{MUTATE, CLONE, SELECTIVE}, {INT32, INT64, STRING}, {5, 50, 95}})
        ->Unit(benchmark::kMicrosecond);

} // namespace starrocks

BENCHMARK_MAIN();

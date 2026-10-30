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

// Several array/map functions decide per row which elements survive and then copy them. There are
// two ways to do the copying, and which one is faster is not obvious from the source:
//
//   per_element : dst->append(src, i, 1) for each survivor, as the decision loop walks
//   selective   : record the survivors' indices, then one dst->append_selective(src, indices)
//
// append_selective() sizes the destination in one pass and then copies in bulk, but it reads the
// indices back from memory and gathers; appending one element at a time redoes the per-element
// offset bookkeeping but streams. The trade depends on three things, so the bench sweeps all of
// them: the element type (a BinaryColumn carries offsets plus bytes, an Int32Column does not), how
// many elements survive, and whether the survivors are visited in ascending order (array_distinct
// keeps the input order) or scattered within the row (array_top_n copies in sorted order).
//
// Run: ./selective_copy_bench --benchmark_filter=.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <random>
#include <string>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

enum CopyMode { kPerElement = 1, kSelective = 2 };
enum ElementType { kVarchar = 1, kInt32 = 2 };

// One "row" of a map/array column, i.e. the unit inside which array_top_n scatters its picks.
constexpr size_t kElementsPerRow = 20;
constexpr size_t kRows = 4096;
constexpr size_t kElements = kRows * kElementsPerRow;

static ColumnPtr make_source(ElementType type) {
    std::mt19937 rng(20260916);
    if (type == kInt32) {
        auto column = Int32Column::create();
        for (size_t i = 0; i < kElements; i++) {
            column->append(static_cast<int32_t>(rng()));
        }
        return column;
    }
    auto column = BinaryColumn::create();
    std::string str;
    for (size_t i = 0; i < kElements; i++) {
        str.assign(8 + rng() % 24, static_cast<char>('a' + rng() % 26));
        column->append_string(str);
    }
    return column;
}

// |keep_pct| survivors per row. Ascending order models array_distinct, which keeps the elements in
// the order it met them; scattered models array_top_n, which copies them in sorted order.
static Buffer<uint32_t> make_indexes(int keep_pct, bool scatter) {
    const size_t per_row = std::max<size_t>(1, kElementsPerRow * keep_pct / 100);
    std::mt19937 rng(982451653);
    Buffer<uint32_t> indexes;
    indexes.reserve(kRows * per_row);
    std::vector<uint32_t> row(kElementsPerRow);
    for (size_t r = 0; r < kRows; r++) {
        const uint32_t base = static_cast<uint32_t>(r * kElementsPerRow);
        for (size_t i = 0; i < kElementsPerRow; i++) {
            row[i] = base + static_cast<uint32_t>(i);
        }
        if (scatter) {
            std::shuffle(row.begin(), row.end(), rng);
            indexes.insert(indexes.end(), row.begin(), row.begin() + per_row);
        } else {
            // Ascending within the row: take the first |per_row| positions.
            indexes.insert(indexes.end(), row.begin(), row.begin() + per_row);
        }
    }
    return indexes;
}

static void bench_func(benchmark::State& state) {
    const auto mode = static_cast<CopyMode>(state.range(0));
    const auto type = static_cast<ElementType>(state.range(1));
    const int keep_pct = static_cast<int>(state.range(2));
    const bool scatter = state.range(3) != 0;

    const ColumnPtr src = make_source(type);
    const Buffer<uint32_t> selected = make_indexes(keep_pct, scatter);

    size_t copied = 0;
    for (auto _ : state) {
        auto dst = src->clone_empty();
        if (mode == kPerElement) {
            for (uint32_t idx : selected) {
                dst->append(*src, idx, 1);
            }
        } else {
            // The decision loop records indices instead of appending, so the record itself is part
            // of what this mode pays for.
            Buffer<uint32_t> indexes;
            indexes.reserve(selected.size());
            for (uint32_t idx : selected) {
                indexes.emplace_back(idx);
            }
            dst->append_selective(*src, indexes);
        }
        copied += dst->size();
        benchmark::DoNotOptimize(dst);
    }
    state.SetItemsProcessed(static_cast<int64_t>(copied));
    state.SetLabel(std::string(mode == kPerElement ? "per_element" : "selective ") +
                   (type == kVarchar ? " varchar" : " int32  ") + " keep=" + std::to_string(keep_pct) + "%" +
                   (scatter ? " scattered" : " ascending"));
}

static void process_args(benchmark::internal::Benchmark* b) {
    for (int type : {kVarchar, kInt32}) {
        for (int keep : {100, 25, 5}) {
            for (int scatter : {0, 1}) {
                b->Args({kPerElement, type, keep, scatter});
                b->Args({kSelective, type, keep, scatter});
            }
        }
    }
    b->Iterations(200);
}

BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();

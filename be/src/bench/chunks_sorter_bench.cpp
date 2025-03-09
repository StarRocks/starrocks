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

#include <memory>
#include <numeric>
#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "exec/chunks_sorter.h"
#include "exec/chunks_sorter_full_sort.h"
#include "exec/chunks_sorter_heap_sort.h"
#include "exec/chunks_sorter_topn.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sorting.h"
#include "exprs/column_ref.h"
#include "runtime/chunk_cursor.h"
#include "runtime/runtime_state.h"
#include "runtime/sorted_chunks_merger.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"

namespace starrocks {

inline int kTestChunkSize = 4096;

class ChunkSorterBase {
public:
    void SetUp() {
        config::vector_chunk_size = 4096;

        _runtime_state = _create_runtime_state();
    }

    void TearDown() { _runtime_state.reset(); }

    static std::tuple<ColumnPtr, std::unique_ptr<ColumnRef>> build_sorted_column(const TypeDescriptor& type_desc,
                                                                                 int slot_index, bool low_card,
                                                                                 bool nullable) {
        DCHECK_EQ(TYPE_INT, type_desc.type);
        using UniformInt = std::uniform_int_distribution<std::mt19937::result_type>;

        MutableColumnPtr column = ColumnHelper::create_column(type_desc, nullable);
        auto expr = std::make_unique<ColumnRef>(type_desc, slot_index);

        std::random_device dev;
        std::mt19937 rng(dev());
        UniformInt uniform_int;
        uniform_int.param(UniformInt::param_type(1, 100'000 * std::pow(2, slot_index)));

        int null_count = config::vector_chunk_size / 100;
        std::vector<int32_t> elements(config::vector_chunk_size - null_count);
        std::generate(elements.begin(), elements.end(), [&]() { return uniform_int(rng); });
        std::sort(elements.begin(), elements.end());

        column->append_nulls(null_count);
        for (int32_t x : elements) {
            column->append_datum(Datum((int32_t)x));
        }
        down_cast<NullableColumn*>(column.get())->update_has_null();

        return {column, std::move(expr)};
    }

    static std::tuple<ColumnPtr, std::unique_ptr<ColumnRef>> build_column(const TypeDescriptor& type_desc,
                                                                          int slot_index, bool low_card,
                                                                          bool nullable) {
        using UniformInt = std::uniform_int_distribution<std::mt19937::result_type>;
        using PoissonInt = std::poisson_distribution<std::mt19937::result_type>;
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, nullable);
        auto expr = std::make_unique<ColumnRef>(type_desc, slot_index);

        std::random_device dev;
        std::mt19937 rng(dev());
        UniformInt uniform_int;
        std::uniform_real_distribution<> niform_real(1, 10);
        if (low_card) {
            uniform_int.param(UniformInt::param_type(1, 100 * std::pow(2, slot_index)));
        } else {
            uniform_int.param(UniformInt::param_type(1, 100'000 * std::pow(2, slot_index)));
        }
        PoissonInt poisson_int(100'000);
        static std::string alphanum =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";

        auto gen_rand_str = [&]() {
            int str_len = uniform_int(rng) % 20;
            int str_start = std::min(poisson_int(rng) % alphanum.size(), alphanum.size() - str_len);
            Slice rand_str(alphanum.c_str() + str_start, str_len);
            return rand_str;
        };

        for (int i = 0; i < config::vector_chunk_size; i++) {
            if (nullable) {
                int32_t x = uniform_int(rng);
                if (x % 1000 == 0) {
                    column->append_nulls(1);
                    continue;
                }
            }
            if (type_desc.type == TYPE_INT) {
                int32_t x = uniform_int(rng);
                column->append_datum(Datum(x));
            } else if (type_desc.type == TYPE_VARCHAR) {
                column->append_datum(Datum(gen_rand_str()));
            } else if (type_desc.type == TYPE_DOUBLE) {
                column->append_datum(Datum(niform_real(rng)));
            } else {
                std::cerr << "not supported" << std::endl;
            }
        }

        return {column, std::move(expr)};
    }

    std::shared_ptr<RuntimeState> _create_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = config::vector_chunk_size;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        runtime_state->init_instance_mem_tracker();
        return runtime_state;
    }

    std::shared_ptr<RuntimeState> _runtime_state;
};

enum SortAlgorithm : int {
    FullSort = 1,  // ChunksSorterFullSort
    HeapSort = 2,  // HeapSoreter
    MergeSort = 3, // ChunksSorterTopN
};

struct SortParameters {
    int limit = -1;
    bool low_card = false;
    bool nullable = false;
    int max_buffered_chunks = ChunksSorterTopn::kDefaultBufferedChunks;

    SortParameters() = default;

    static SortParameters with_limit(int limit) {
        SortParameters params;
        params.limit = limit;
        return params;
    }

    static SortParameters with_low_card(bool low_card) {
        SortParameters params;
        params.low_card = low_card;
        return params;
    }

    static SortParameters with_nullable(bool nullable) {
        SortParameters params;
        params.nullable = nullable;
        return params;
    }

    static SortParameters with_max_buffered_chunks(int max_buffered_chunks) {
        SortParameters params;
        params.max_buffered_chunks = max_buffered_chunks;
        return params;
    }
};

static void do_bench(benchmark::State& state, SortAlgorithm sorter_algo, LogicalType data_type, int num_chunks,
                     int num_columns, SortParameters params = SortParameters()) {
    // state.PauseTiming();
    ChunkSorterBase suite;
    suite.SetUp();

    TypeDescriptor type_desc;
    if (data_type == TYPE_INT) {
        type_desc = TypeDescriptor(TYPE_INT);
    } else if (data_type == TYPE_VARCHAR) {
        type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    } else if (data_type == TYPE_DOUBLE) {
        type_desc = TypeDescriptor(TYPE_DOUBLE);
    } else {
        ASSERT_TRUE(false) << "not support type: " << data_type;
    }

    Columns columns;
    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool> asc_arr;
    std::vector<bool> null_first;
    Chunk::SlotHashMap map;

    for (int i = 0; i < num_columns; i++) {
        auto [column, expr] = suite.build_column(type_desc, i, params.low_card, params.nullable);
        columns.push_back(column);
        exprs.emplace_back(std::move(expr));
        auto sort_expr = new ExprContext(exprs.back().get());
        ASSERT_OK(sort_expr->prepare(suite._runtime_state.get()));
        ASSERT_OK(sort_expr->open(suite._runtime_state.get()));
        sort_exprs.push_back(sort_expr);
        asc_arr.push_back(true);
        null_first.push_back(true);
        map[i] = i;
    }
    auto chunk = std::make_shared<Chunk>(columns, map);

    RuntimeState* runtime_state = suite._runtime_state.get();
    int64_t item_processed = 0;
    int64_t data_size = 0;
    int64_t mem_usage = 0;
    const int64_t max_buffered_rows = 1024 * 1024;
    const int64_t max_buffered_bytes = max_buffered_rows * 256;
    const std::vector<SlotId> early_materialized_slots;

    for (auto _ : state) {
        state.PauseTiming();
        RuntimeProfile profile("dummy");
        std::unique_ptr<ChunksSorter> sorter;
        size_t expected_rows = 0;
        size_t total_rows = chunk->num_rows() * num_chunks;
        int limit_rows = params.limit == -1 ? total_rows : std::min(params.limit, (int)total_rows);

        switch (sorter_algo) {
        case FullSort: {
            sorter = std::make_unique<ChunksSorterFullSort>(suite._runtime_state.get(), &sort_exprs, &asc_arr,
                                                            &null_first, "", max_buffered_rows, max_buffered_bytes,
                                                            early_materialized_slots);
            expected_rows = total_rows;
            break;
        }
        case HeapSort: {
            sorter = std::make_unique<ChunksSorterHeapSort>(suite._runtime_state.get(), &sort_exprs, &asc_arr,
                                                            &null_first, "", 0, limit_rows);
            expected_rows = limit_rows;
            break;
        }
        case MergeSort: {
            sorter = std::make_unique<ChunksSorterTopn>(suite._runtime_state.get(), &sort_exprs, &asc_arr, &null_first,
                                                        "", 0, limit_rows, TTopNType::ROW_NUMBER, max_buffered_rows,
                                                        max_buffered_bytes params.max_buffered_chunks);
            expected_rows = limit_rows;
            break;
        }
        default:
            ASSERT_TRUE(false) << "unknown algorithm " << (int)sorter_algo;
        }
        sorter->setup_runtime(suite._runtime_state.get(), &profile, suite._runtime_state->instance_mem_tracker());

        int64_t iteration_data_size = 0;
        for (int i = 0; i < num_chunks; i++) {
            // Clone is necessary for HeapSorter
            auto cloned = chunk->clone_empty();
            cloned->append_safe(*chunk);
            ChunkPtr ck(cloned.release());

            // TopN Sorter needs timing when updating
            iteration_data_size += ck->bytes_usage();
            state.ResumeTiming();
            ASSERT_TRUE(sorter->update(runtime_state, ck).ok());
            state.PauseTiming();
            mem_usage = std::max(mem_usage, sorter->mem_usage());
        }
        data_size = std::max(data_size, iteration_data_size);

        state.ResumeTiming();
        ASSERT_TRUE(sorter->done(suite._runtime_state.get()).ok());
        item_processed += total_rows;
        state.PauseTiming();
        mem_usage = std::max(mem_usage, sorter->mem_usage());

        bool eos = false;
        size_t actual_rows = 0;
        while (!eos) {
            ChunkPtr page;
            ASSERT_TRUE(sorter->get_next(&page, &eos).ok());
            if (eos) break;
            actual_rows += page->num_rows();
        }
        ASSERT_TRUE(eos);
        ASSERT_EQ(expected_rows, actual_rows);
        ASSERT_TRUE(sorter->done(suite._runtime_state.get()).ok());
    }
    state.counters["rows_sorted"] += item_processed;
    state.counters["data_size"] += data_size;
    state.counters["mem_usage"] = mem_usage;
    state.SetItemsProcessed(item_processed);

    suite.TearDown();
}

static void do_heap_merge(benchmark::State& state, int num_runs, bool use_merger = true) {
    constexpr int num_columns = 3;
    constexpr int num_chunks_per_run = 1;

    ChunkSorterBase suite;
    suite.SetUp();
    RuntimeState* runtime_state = suite._runtime_state.get();

    Columns columns;
    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool> asc_arr;
    std::vector<bool> null_first;
    Chunk::SlotHashMap map;
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);

    for (int i = 0; i < num_columns; i++) {
        auto [column, expr] = suite.build_sorted_column(type_desc, i, false, false);
        columns.push_back(column);
        exprs.emplace_back(std::move(expr));
        auto sort_expr = new ExprContext(exprs.back().get());
        ASSERT_OK(sort_expr->prepare(suite._runtime_state.get()));
        ASSERT_OK(sort_expr->open(suite._runtime_state.get()));
        sort_exprs.push_back(sort_expr);

        asc_arr.push_back(true);
        null_first.push_back(true);
        map[i] = i;
    }
    ChunkPtr base_chunk = std::make_shared<Chunk>(columns, map);

    int64_t num_rows = 0;
    for (auto _ : state) {
        ChunkSuppliers chunk_suppliers;
        ChunkProbeSuppliers chunk_probe_suppliers;
        ChunkHasSuppliers chunk_has_suppliers;
        std::vector<int> chunk_input_index(num_runs, 0);
        std::vector<int> chunk_probe_index(num_runs, 0);

        for (int i = 0; i < num_runs; i++) {
            ChunkSupplier chunk_supplier = [&, i](Chunk** output) -> Status {
                if (output) {
                    if (++chunk_input_index[i] > num_chunks_per_run) {
                        *output = nullptr;
                    } else {
                        *output = base_chunk->clone_unique().release();
                    }
                }
                return Status::OK();
            };
            ChunkProbeSupplier chunk_probe_supplier = [&, i](Chunk** output) -> bool {
                if (output) {
                    if (++chunk_probe_index[i] > num_chunks_per_run) {
                        *output = nullptr;
                        return false;
                    } else {
                        *output = base_chunk->clone_unique().release();
                    }
                }
                return true;
            };
            ChunkHasSupplier chunk_has_supplier = [&]() -> bool { return true; };
            chunk_suppliers.emplace_back(chunk_supplier);
            chunk_probe_suppliers.emplace_back(chunk_probe_supplier);
            chunk_has_suppliers.emplace_back(chunk_has_supplier);
        }

        if (use_merger) {
            SortedChunksMerger merger(runtime_state, true);
            ASSERT_OK(merger.init_for_pipeline(chunk_suppliers, chunk_probe_suppliers, chunk_has_suppliers, &sort_exprs,
                                               &asc_arr, &null_first));
            merger.is_data_ready();
            std::atomic<bool> eos{false};
            bool should_exit{false};
            while (!eos && !should_exit) {
                ChunkPtr chunk;
                ASSERT_OK(merger.get_next_for_pipeline(&chunk, &eos, &should_exit));
                num_rows += chunk->num_rows();
            }
        } else {
            CHECK(false) << "TODO";
        }
    }
    state.SetItemsProcessed(num_rows);

    suite.TearDown();
}

static void do_merge_columnwise(benchmark::State& state, int num_runs, bool nullable) {
    ChunkSorterBase suite;
    suite.SetUp();

    constexpr int num_columns = 3;
    Columns columns;
    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool> asc_arr;
    std::vector<bool> null_first;
    Chunk::SlotHashMap map;
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);

    for (int i = 0; i < num_columns; i++) {
        auto [column, expr] = suite.build_sorted_column(type_desc, i, false, nullable);
        columns.push_back(column);
        exprs.emplace_back(std::move(expr));
        auto sort_expr = new ExprContext(exprs.back().get());
        ASSERT_OK(sort_expr->prepare(suite._runtime_state.get()));
        ASSERT_OK(sort_expr->open(suite._runtime_state.get()));
        sort_exprs.push_back(sort_expr);
        asc_arr.push_back(true);
        null_first.push_back(true);
        map[i] = i;
    }
    ChunkPtr chunk1 = std::make_shared<Chunk>(columns, map);
    ChunkPtr chunk2 = std::make_shared<Chunk>(columns, map);

    int64_t num_rows = 0;
    SortDescs sort_desc(std::vector<int>{1, 1, 1}, std::vector<int>{-1, -1, -1});
    for (auto _ : state) {
        std::vector<ChunkUniquePtr> inputs;
        size_t input_rows = num_runs * chunk1->num_rows();
        for (int i = 0; i < num_runs; i++) {
            if (i % 2 == 0) {
                inputs.push_back(chunk1->clone_unique());
            } else {
                inputs.push_back(chunk2->clone_unique());
            }
        }
        SortedRuns merged;
        ASSERT_TRUE(merge_sorted_chunks(sort_desc, &sort_exprs, inputs, &merged).ok());
        ASSERT_EQ(input_rows, merged.num_rows());

        num_rows += merged.num_rows();
    }

    state.SetItemsProcessed(num_rows);
    suite.TearDown();
}

// Sort full data: ORDER BY
static void BM_fullsort_notnull(benchmark::State& state) {
    do_bench(state, FullSort, TYPE_INT, state.range(0), state.range(1));
}
static void BM_fullsort_float_notnull(benchmark::State& state) {
    do_bench(state, FullSort, TYPE_DOUBLE, state.range(0), state.range(1));
}
static void BM_fullsort_nullable(benchmark::State& state) {
    do_bench(state, FullSort, TYPE_INT, state.range(0), state.range(1), SortParameters::with_nullable(true));
}
static void BM_fullsort_varchar_column_incr(benchmark::State& state) {
    do_bench(state, FullSort, TYPE_VARCHAR, state.range(0), state.range(1));
}

// Low cardinality
static void BM_fullsort_low_card_colinc(benchmark::State& state) {
    do_bench(state, FullSort, TYPE_INT, state.range(0), state.range(1), SortParameters::with_low_card(true));
}
static void BM_fullsort_low_card_nullable(benchmark::State& state) {
    SortParameters params = SortParameters::with_low_card(true);
    params.nullable = true;
    do_bench(state, FullSort, TYPE_INT, state.range(0), state.range(1), params);
}

// Sort partial data: ORDER BY xxx LIMIT
static void BM_topn_limit_heapsort(benchmark::State& state) {
    do_bench(state, HeapSort, TYPE_INT, state.range(0), state.range(1), SortParameters::with_limit(state.range(2)));
}
static void BM_topn_limit_mergesort_notnull(benchmark::State& state) {
    do_bench(state, MergeSort, TYPE_INT, state.range(0), state.range(1), SortParameters::with_limit(state.range(2)));
}
static void BM_topn_limit_mergesort_nullable(benchmark::State& state) {
    SortParameters params = SortParameters::with_limit(state.range(2));
    params.nullable = true;
    do_bench(state, MergeSort, TYPE_INT, state.range(0), state.range(1), params);
}
static void BM_topn_buffered_chunks(benchmark::State& state) {
    SortParameters params;
    params.max_buffered_chunks = state.range(0);
    params.limit = state.range(1);
    do_bench(state, MergeSort, TYPE_INT, 4096, 2, params);
}
static void BM_topn_buffered_chunks_tunned(benchmark::State& state) {
    SortParameters params;
    params.limit = state.range(1);
    params.max_buffered_chunks = ChunksSorterTopn::tunning_buffered_chunks(params.limit);
    do_bench(state, MergeSort, TYPE_INT, 4096, 2, params);
}

// Merge sorted runs
static void BM_merge_heap(benchmark::State& state) {
    do_heap_merge(state, state.range(0), true);
}
static void BM_merge_columnwise(benchmark::State& state) {
    do_merge_columnwise(state, state.range(0), false);
}
static void BM_merge_columnwise_nullable(benchmark::State& state) {
    do_merge_columnwise(state, state.range(0), true);
}

static void CustomArgsFull(benchmark::internal::Benchmark* b) {
    // num_chunks
    for (int num_chunks = 64; num_chunks <= 32768; num_chunks *= 8) {
        // num_columns
        for (int num_columns = 1; num_columns <= 4; num_columns++) {
            b->Args({num_chunks, num_columns});
        }
    }
}
static void CustomArgsLimit(benchmark::internal::Benchmark* b) {
    // num_chunks
    for (int num_chunks = 1024; num_chunks <= 32768; num_chunks *= 4) {
        // num_columns
        for (int num_columns = 1; num_columns <= 4; num_columns++) {
            // limit
            for (int limit = 1; limit <= num_chunks * kTestChunkSize / 8; limit *= 8) {
                b->Args({num_chunks, num_columns, limit});
            }
        }
    }
}

// Full sort
BENCHMARK(BM_fullsort_notnull)->Apply(CustomArgsFull);
BENCHMARK(BM_fullsort_nullable)->Apply(CustomArgsFull);
BENCHMARK(BM_fullsort_float_notnull)->Apply(CustomArgsFull);
BENCHMARK(BM_fullsort_varchar_column_incr)->Apply(CustomArgsFull);

// Low-Cardinality Sort
BENCHMARK(BM_fullsort_low_card_colinc)->Apply(CustomArgsFull);
BENCHMARK(BM_fullsort_low_card_nullable)->Apply(CustomArgsFull);

// TopN sort
BENCHMARK(BM_topn_limit_heapsort)->Apply(CustomArgsLimit);
BENCHMARK(BM_topn_limit_mergesort_notnull)->Apply(CustomArgsLimit);
BENCHMARK(BM_topn_limit_mergesort_nullable)->Apply(CustomArgsLimit);

// Tunning the parameter buffered_chunks of TopN
BENCHMARK(BM_topn_buffered_chunks)->RangeMultiplier(4)->Ranges({{10, 10'000}, {100, 100'000}});
BENCHMARK(BM_topn_buffered_chunks_tunned)->RangeMultiplier(4)->Ranges({{10, 10'000}, {100, 100'000}});

// Merge
BENCHMARK(BM_merge_heap)->DenseRange(2, 64, 4);
BENCHMARK(BM_merge_columnwise)->DenseRange(2, 64, 4);
BENCHMARK(BM_merge_columnwise_nullable)->DenseRange(2, 64, 4);

static size_t plain_find_zero(const std::vector<uint8_t>& bytes) {
    for (size_t i = 0; i < bytes.size(); i++) {
        if (bytes[i] == 0) {
            return i;
        }
    }
    return bytes.size();
}

static void BM_find_zero_plain(benchmark::State& state) {
    int len = state.range(0);
    std::vector<uint8_t> bytes(len, 1);
    bytes[len - 3] = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(plain_find_zero(bytes));
    }
}
BENCHMARK(BM_find_zero_plain)->Range(8, 8 << 12);

static void BM_find_zero_simd(benchmark::State& state) {
    int len = state.range(0);
    std::vector<uint8_t> bytes(len, 1);
    bytes[len - 3] = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(SIMD::find_zero(bytes, 0));
    }
}
BENCHMARK(BM_find_zero_simd)->Range(8, 8 << 12);

static void BM_find_zero_stdfind(benchmark::State& state) {
    int len = state.range(0);
    std::vector<uint8_t> bytes(len, 1);
    bytes[len - 3] = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(std::find(bytes.begin(), bytes.end(), 0));
    }
}
BENCHMARK(BM_find_zero_stdfind)->Range(8, 8 << 12);

static void BM_find_zero_memchr(benchmark::State& state) {
    int len = state.range(0);
    std::vector<uint8_t> bytes(len, 1);
    bytes[len - 3] = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(memchr(bytes.data(), 0, bytes.size()));
    }
}
BENCHMARK(BM_find_zero_memchr)->Range(8, 8 << 12);

} // namespace starrocks

BENCHMARK_MAIN();

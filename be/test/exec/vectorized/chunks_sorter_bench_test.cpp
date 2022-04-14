// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

#include <random>

#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"
#include "exec/vectorized/chunk_sorter_heapsorter.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/slot_ref.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

inline int kTestChunkSize = 4096;

class ChunkSorterBase {
public:
    void SetUp() {
        config::vector_chunk_size = 4096;

        _runtime_state = _create_runtime_state();
    }

    static std::tuple<ColumnPtr, std::unique_ptr<SlotRef>> build_column(TypeDescriptor type_desc, int slot_index) {
        ColumnPtr column = ColumnHelper::create_column(type_desc, false);
        auto expr = std::make_unique<SlotRef>(type_desc, 0, slot_index);

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> uniform_int(1, 1'000'000 * std::pow(2, slot_index));
        std::poisson_distribution<std::mt19937::result_type> poisson_int(1000000);
        static std::string alphanum =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";

        auto gen_rand_str = [&]() {
            int str_len = uniform_int(rng) % 10;
            int str_start = std::min(poisson_int(rng) % alphanum.size(), alphanum.size() - str_len);
            Slice rand_str(alphanum.c_str() + str_start, str_len);
            return rand_str;
        };

        for (int i = 0; i < config::vector_chunk_size; i++) {
            if (type_desc.type == TYPE_INT) {
                column->append_datum(Datum(int32_t(uniform_int(rng))));
            } else if (type_desc.type == TYPE_VARCHAR) {
                column->append_datum(Datum(gen_rand_str()));
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

static void do_bench(benchmark::State& state, SortAlgorithm sorter_algo, CompareStrategy strategy, int num_chunks,
                     int num_columns, int limit = -1) {
    ChunkSorterBase suite;
    suite.SetUp();

    // TODO more data type
    const auto& int_type_desc = TypeDescriptor(TYPE_INT);
    // const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    Columns columns;
    std::vector<std::unique_ptr<SlotRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool> asc_arr;
    std::vector<bool> null_first;
    Chunk::SlotHashMap map;

    for (int i = 0; i < num_columns; i++) {
        auto [column, expr] = suite.build_column(int_type_desc, i);
        columns.push_back(column);
        exprs.emplace_back(std::move(expr));
        sort_exprs.push_back(new ExprContext(exprs.back().get()));
        asc_arr.push_back(true);
        null_first.push_back(true);
        map[i] = i;
    }
    auto chunk = std::make_shared<Chunk>(columns, map);

    RuntimeState* runtime_state = suite._runtime_state.get();
    int64_t item_processed = 0;
    int64_t data_size = 0;
    int64_t mem_usage = 0;
    for (auto _ : state) {
        state.PauseTiming();

        std::unique_ptr<ChunksSorter> sorter;
        size_t expected_rows = 0;
        size_t total_rows = chunk->num_rows() * num_chunks;
        int limit_rows = limit == -1 ? total_rows : std::min(limit, (int)total_rows);

        switch (sorter_algo) {
        case FullSort: {
            sorter.reset(new ChunksSorterFullSort(suite._runtime_state.get(), &sort_exprs, &asc_arr, &null_first, "",
                                                  config::vector_chunk_size));
            expected_rows = total_rows;
            break;
        }
        case HeapSort: {
            sorter.reset(new HeapChunkSorter(suite._runtime_state.get(), &sort_exprs, &asc_arr, &null_first, "", 0,
                                             limit_rows, config::vector_chunk_size));
            expected_rows = limit_rows;
            break;
        }
        case MergeSort: {
            sorter.reset(new ChunksSorterTopn(suite._runtime_state.get(), &sort_exprs, &asc_arr, &null_first, "", 0,
                                              limit_rows));
            expected_rows = limit_rows;
            break;
        }
        default:
            ASSERT_TRUE(false) << "unknown algorithm " << (int)sorter_algo;
        }
        sorter->set_compare_strategy(strategy);

        int64_t iteration_data_size = 0;
        for (int i = 0; i < num_chunks; i++) {
            // Clone is necessary for HeapSorter
            auto cloned = chunk->clone_empty();
            cloned->append_safe(*chunk);
            ChunkPtr ck(cloned.release());

            // TopN Sorter needs timing when updating
            iteration_data_size += ck->bytes_usage();
            state.ResumeTiming();
            sorter->update(runtime_state, ck);
            state.PauseTiming();
            mem_usage = std::max(mem_usage, sorter->mem_usage());
        }
        data_size = std::max(data_size, iteration_data_size);

        state.ResumeTiming();
        sorter->done(suite._runtime_state.get());
        item_processed += total_rows;
        state.PauseTiming();
        mem_usage = std::max(mem_usage, sorter->mem_usage());

        bool eos = false;
        size_t actual_rows = 0;
        while (!eos) {
            ChunkPtr page;
            sorter->get_next(&page, &eos);
            if (eos) break;
            actual_rows += page->num_rows();
        }
        ASSERT_TRUE(eos);
        ASSERT_EQ(expected_rows, actual_rows);
    }
    state.counters["rows_sorted"] += item_processed;
    state.counters["data_size"] += data_size;
    state.counters["mem_usage"] = mem_usage;
    state.SetItemsProcessed(item_processed);
}

// Sort full data: ORDER BY
static void BM_fullsort_row_wise(benchmark::State& state) {
    do_bench(state, FullSort, RowWise, state.range(0), state.range(1));
}
static void BM_fullsort_column_wise(benchmark::State& state) {
    do_bench(state, FullSort, ColumnWise, state.range(0), state.range(1));
}
static void BM_heapsort_row_wise(benchmark::State& state) {
    do_bench(state, HeapSort, RowWise, state.range(0), state.range(1));
}
static void BM_mergesort_row_wise(benchmark::State& state) {
    do_bench(state, MergeSort, RowWise, state.range(0), state.range(1));
}

// Sort partial data: ORDER BY xxx LIMIT
static void BM_topn_limit_heapsort(benchmark::State& state) {
    do_bench(state, HeapSort, RowWise, state.range(0), state.range(1), state.range(2));
}
static void BM_topn_limit_mergesort(benchmark::State& state) {
    do_bench(state, MergeSort, RowWise, state.range(0), state.range(1), state.range(2));
}

static void CustomArgsFull(benchmark::internal::Benchmark* b) {
    // num_chunks
    for (int num_chunks = 64; num_chunks <= 32768; num_chunks *= 8) {
        // num_columns
        for (int num_columns = 1; num_columns <= 8; num_columns++) {
            b->Args({num_chunks, num_columns});
        }
    }
}
static void CustomArgsLimit(benchmark::internal::Benchmark* b) {
    // num_chunks
    for (int num_chunks = 1024; num_chunks <= 32768; num_chunks *= 4) {
        // num_columns
        for (int num_columns = 1; num_columns <= 8; num_columns++) {
            // limit
            for (int limit = 1; limit <= num_chunks * kTestChunkSize; limit *= 8) {
                b->Args({num_chunks, num_columns, limit});
            }
        }
    }
}

BENCHMARK(BM_fullsort_row_wise)->Apply(CustomArgsFull);
BENCHMARK(BM_fullsort_column_wise)->Apply(CustomArgsFull);
BENCHMARK(BM_heapsort_row_wise)->Apply(CustomArgsFull);
BENCHMARK(BM_mergesort_row_wise)->Apply(CustomArgsFull);

BENCHMARK(BM_topn_limit_heapsort)->Apply(CustomArgsLimit);
BENCHMARK(BM_topn_limit_mergesort)->Apply(CustomArgsLimit);

} // namespace starrocks::vectorized

BENCHMARK_MAIN();

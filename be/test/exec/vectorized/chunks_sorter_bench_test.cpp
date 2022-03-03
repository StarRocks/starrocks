// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

#include <random>

#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"
#include "exec/vectorized/chunk_sorter_heapsorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/slot_ref.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

class ChunkSorterBase {
public:
    void SetUp() {
        config::vector_chunk_size = 1024;

        _runtime_state = _create_runtime_state();
    }

    static std::tuple<ColumnPtr, std::unique_ptr<SlotRef>> build_column(TypeDescriptor type_desc, int slot_index) {
        ColumnPtr column = ColumnHelper::create_column(type_desc, false);
        auto expr = std::make_unique<SlotRef>(type_desc, 0, slot_index);

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> uniform_int(1,
                                                                             1000000); // distribution in range [1, 6]
        std::poisson_distribution<std::mt19937::result_type> poisson_int(1000000);     // distribution in range [1, 6]
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

static void do_bench(benchmark::State& state, int sorter_type, int sort_algo, int num_chunks, int num_columns) {
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
    for (auto _ : state) {
        state.PauseTiming();

        std::unique_ptr<ChunksSorter> sorter;
        size_t expected_rows = 0;
        size_t total_rows = chunk->num_rows() * num_chunks;

        switch (sorter_type) {
        case 1: {
            sorter.reset(new ChunksSorterFullSort(suite._runtime_state.get(), &sort_exprs, &asc_arr, &null_first,
                                                  config::vector_chunk_size));
            ((ChunksSorterFullSort*)sorter.get())->set_compare_strategy(sort_algo);
            expected_rows = total_rows;
            break;
        }
        case 2: {
            // TODO limit
            int limit_rows = total_rows / 2;
            sorter.reset(new HeapChunkSorter(suite._runtime_state.get(), &sort_exprs, &asc_arr, &null_first, 0,
                                             limit_rows, config::vector_chunk_size));
            expected_rows = limit_rows;
            break;
        }
        default:
            ASSERT_TRUE(false) << "unknown algorithm " << sort_algo;
        }

        for (int i = 0; i < num_chunks; i++) {
            // Clone is necessary for HeapSorter
            auto cloned = chunk->clone_empty();
            cloned->append_safe(*chunk);
            ChunkPtr ck(cloned.release());

            // TopN Sorter needs timing when updating
            state.ResumeTiming();
            sorter->update(runtime_state, ck);
            state.PauseTiming();
        }

        state.ResumeTiming();
        sorter->done(suite._runtime_state.get());
        item_processed += total_rows;
        state.PauseTiming();

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
    state.SetItemsProcessed(item_processed);
}

static void Bench_fullsort_row_wise(benchmark::State& state) {
    do_bench(state, 1, 1, state.range(0), state.range(1));
}
static void Bench_fullsort_column_wise(benchmark::State& state) {
    do_bench(state, 1, 2, state.range(0), state.range(1));
}
static void Bench_topn_row_wise(benchmark::State& state) {
    do_bench(state, 2, 2, state.range(0), state.range(1));
}
static void CustomArgs(benchmark::internal::Benchmark* b) {
    // num_chunks
    for (int num_chunks = 64; num_chunks <= 8192; num_chunks *= 8) {
        // num_columns
        for (int num_columns = 1; num_columns <= 8; num_columns++) {
            b->Args({num_chunks, num_columns});
        }
    }
}

BENCHMARK(Bench_fullsort_row_wise)->Apply(CustomArgs);
BENCHMARK(Bench_fullsort_column_wise)->Apply(CustomArgs);
BENCHMARK(Bench_topn_row_wise)->Apply(CustomArgs);

} // namespace starrocks::vectorized

BENCHMARK_MAIN();
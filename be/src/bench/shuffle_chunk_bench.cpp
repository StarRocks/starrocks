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
#include <testutil/assert.h>

#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"
#include "runtime/chunk_cursor.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks {

class ShuffleChunkPerf {
public:
    void SetUp() {}
    void TearDown() {}

    ShuffleChunkPerf(int chunk_count, int column_count, int node_count, int src_chunk_size, int null_percent)
            : _chunk_count(chunk_count),
              _column_count(column_count),
              _node_count(node_count),
              _src_chunk_size(src_chunk_size),
              _null_percent(null_percent) {}

    void init_types();
    ChunkPtr init_src_chunk();
    void init_dest_chunks();
    void reset_dest_chunks();
    ChunkPtr init_dest_chunk();
    ColumnPtr init_src_column(const TypeDescriptor& type);
    ColumnPtr init_src_key_column(const TypeDescriptor& type);
    ColumnPtr init_dest_column(const TypeDescriptor& type);
    void do_hash(const ColumnPtr& col);
    void do_shuffle(const Chunk& src_chunk);
    void do_bench(benchmark::State& state);

private:
    int _chunk_count = 400;
    int _column_count = 400;
    int _node_count = 140;
    int _src_chunk_size = 4096;
    int _dest_chunk_size = 4096;
    int _null_percent = 100;
    std::vector<TypeDescriptor> _types;
    std::vector<ChunkPtr> _src_chunks;
    std::vector<ChunkPtr> _dest_chunks;
    std::vector<uint32_t> _shuffle_idxs;
    std::vector<uint32_t> _select_idxs;
};

void ShuffleChunkPerf::init_types() {
    _types.resize(_column_count);
    for (int i = 0; i < _column_count; i++) {
        _types[i] = TypeDescriptor::create_varchar_type(1024);
    }
}

ColumnPtr ShuffleChunkPerf::init_src_column(const TypeDescriptor& type) {
    ColumnPtr c1 = ColumnHelper::create_column(type, true);
    c1->reserve(_src_chunk_size);
    auto* nullable_col = down_cast<NullableColumn*>(c1.get());
    for (int k = 0; k < _src_chunk_size; k++) {
        int v = rand();
        if (v % 100 < _null_percent) {
            nullable_col->append_default();
        } else {
            auto val = "str123" + std::to_string(v);
            nullable_col->append_datum(Datum(Slice(val)));
        }
    }
    return c1;
}

ColumnPtr ShuffleChunkPerf::init_src_key_column(const TypeDescriptor& type) {
    ColumnPtr c1 = ColumnHelper::create_column(type, true);
    c1->reserve(_src_chunk_size);
    auto* nullable_col = down_cast<NullableColumn*>(c1.get());
    for (int k = 0; k < _src_chunk_size; k++) {
        int v = rand();
        auto val = "str123" + std::to_string(v);
        nullable_col->append_datum(Datum(Slice(val)));
    }
    return c1;
}

ColumnPtr ShuffleChunkPerf::init_dest_column(const TypeDescriptor& type) {
    auto c1 = ColumnHelper::create_column(type, true);
    c1->reserve(_dest_chunk_size);
    return c1;
}

ChunkPtr ShuffleChunkPerf::init_src_chunk() {
    auto chunk = std::make_unique<Chunk>();
    auto col = init_src_key_column(_types[0]);
    chunk->append_column(col, 0);
    for (int i = 1; i < _column_count; i++) {
        col = init_src_column(_types[i]);
        chunk->append_column(col, i);
    }
    return chunk;
}

void ShuffleChunkPerf::init_dest_chunks() {
    for (int i = 0; i < _node_count; i++) {
        if (_dest_chunks[i] == nullptr) {
            _dest_chunks[i] = init_dest_chunk();
        }
    }
}

void ShuffleChunkPerf::reset_dest_chunks() {
    for (int i = 0; i < _node_count; i++) {
        if (_dest_chunks[i]->num_rows() > _dest_chunk_size) {
            _dest_chunks[i].reset();
        }
    }
}

ChunkPtr ShuffleChunkPerf::init_dest_chunk() {
    auto chunk = std::make_unique<Chunk>();
    for (int i = 0; i < _column_count; i++) {
        auto col = init_dest_column(_types[i]);
        chunk->append_column(col, i);
    }
    return chunk;
}

void ShuffleChunkPerf::do_hash(const ColumnPtr& col) {
    _shuffle_idxs.resize(_src_chunk_size);

    col->crc32_hash(&_shuffle_idxs[0], 0, _src_chunk_size);
    for (int i = 0; i < _src_chunk_size; i++) {
        _shuffle_idxs[i] = _shuffle_idxs[i] % _node_count;
    }
}

void ShuffleChunkPerf::do_shuffle(const Chunk& src_chunk) {
    for (int n = 0; n < _node_count; n++) {
        _select_idxs.resize(0);
        for (int i = 0; i < _shuffle_idxs.size(); i++) {
            if (_shuffle_idxs[i] == n) {
                _select_idxs.emplace_back(i);
            }
        }
        _dest_chunks[n]->append_selective(src_chunk, _select_idxs.data(), 0, _select_idxs.size());
    }
}

void ShuffleChunkPerf::do_bench(benchmark::State& state) {
    init_types();
    _dest_chunks.resize(_node_count, nullptr);

    state.ResumeTiming();

    for (int i = 0; i < _chunk_count; i++) {
        state.PauseTiming();
        ChunkPtr src_chunk = init_src_chunk();
        state.ResumeTiming();

        do_hash(src_chunk->columns()[0]);

        init_dest_chunks();

        do_shuffle(*src_chunk);

        reset_dest_chunks();
    }
    state.PauseTiming();
}

static void bench_func(benchmark::State& state) {
    int chunk_count = state.range(0);
    int column_count = state.range(1);
    int node_count = state.range(2);
    int src_chunk_size = state.range(3);
    int null_percent = state.range(4);

    ShuffleChunkPerf perf(chunk_count, column_count, node_count, src_chunk_size, null_percent);
    perf.do_bench(state);
}

static void process_args(benchmark::internal::Benchmark* b) {
    // chunk_count, column_count, node_count, src_chunk_size, null percent
    b->Args({400, 400, 140, 4096, 80});

    // node
    b->Args({400, 400, 70, 4096, 80});
    b->Args({400, 400, 20, 4096, 80});
    b->Args({400, 400, 10, 4096, 80});
    b->Args({400, 400, 3, 4096, 80});

    // column
    b->Args({800, 200, 140, 4096, 80});
    b->Args({1600, 100, 140, 4096, 80});
    b->Args({16000, 10, 140, 4096, 80});

    // chunk size
    b->Args({200, 400, 140, 8192, 80});
    b->Args({100, 400, 140, 16384, 80});
    b->Args({50, 400, 140, 32768, 80});
    b->Args({25, 400, 140, 65536, 80});

    // null percent
    b->Args({400, 400, 140, 4096, 0});
    b->Args({400, 400, 70, 4096, 0});
    b->Args({400, 400, 20, 4096, 0});
    b->Args({400, 400, 10, 4096, 0});
    b->Args({400, 400, 3, 4096, 0});
}

BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();

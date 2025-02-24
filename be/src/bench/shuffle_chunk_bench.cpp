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
#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "runtime/types.h"
#include "storage/chunk_helper.h"
#include "types/logical_type.h"

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
    MutableColumnPtr c1 = ColumnHelper::create_column(type, true);
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
    MutableColumnPtr c1 = ColumnHelper::create_column(type, true);
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

// Benchmark SegmentedColumn::clone_selective && Chunk::append_selective function
class SegmentedChunkPerf {
public:
    SegmentedChunkPerf() = default;

    void prepare_bench_segmented_chunk_clone(benchmark::State& state) {
        // std::cerr << "chunk_size: " << _dest_chunk_size << std::endl;
        // std::cerr << "segment_size: " << _segment_size << std::endl;
        // std::cerr << "segmented_chunk_size: " << _segment_chunk_size << std::endl;
        SegmentedChunkPtr seg_chunk = prepare_chunk();
        CHECK_EQ(seg_chunk->num_rows(), _segment_chunk_size);

        // random select
        random_select(select, _dest_chunk_size, seg_chunk->num_rows());
    }

    void prepare_bench_chunk_clone(benchmark::State& state) {
        ChunkPtr chunk = build_chunk(_segment_size);
        CHECK_EQ(chunk->num_rows(), _segment_size);
        random_select(select, _dest_chunk_size, chunk->num_rows());
    }

    void prepare(benchmark::State& state) {
        state.PauseTiming();

        _column_count = state.range(0);
        _data_type = state.range(1);
        _num_segments = state.range(2);
        _types.clear();

        prepare_bench_chunk_clone(state);
        prepare_bench_segmented_chunk_clone(state);

        state.ResumeTiming();
    }

    void do_bench_segmented_chunk_clone(benchmark::State& state) {
        SegmentedChunkPtr seg_chunk = prepare_chunk();
        // clone_selective
        size_t items = 0;
        for (auto _ : state) {
            for (auto& column : seg_chunk->columns()) {
                auto cloned = column->clone_selective(select.data(), 0, select.size());
            }
            items += select.size();
        }
        state.SetItemsProcessed(items);
    }

    void do_bench_chunk_clone(benchmark::State& state) {
        ChunkPtr chunk = prepare_big_chunk();
        size_t items = 0;
        for (auto _ : state) {
            ChunkPtr empty = chunk->clone_empty();
            empty->append_selective(*chunk, select.data(), 0, select.size());
            items += select.size();
        }
        state.SetItemsProcessed(items);
    }

    ChunkPtr prepare_big_chunk() {
        if (_big_chunk) {
            return _big_chunk;
        }
        _big_chunk = build_chunk(_segment_chunk_size);
        return _big_chunk;
    }

    SegmentedChunkPtr prepare_chunk() {
        if (_seg_chunk) {
            return _seg_chunk;
        }
        ChunkPtr chunk = build_chunk(_dest_chunk_size);

        for (int i = 0; i < (_segment_chunk_size / _dest_chunk_size); i++) {
            if (!_seg_chunk) {
                _seg_chunk = SegmentedChunk::create(_segment_size);
                ChunkPtr chunk = build_chunk(_dest_chunk_size);
                auto map = chunk->get_slot_id_to_index_map();
                for (auto entry : map) {
                    _seg_chunk->append_column(chunk->get_column_by_slot_id(entry.first), entry.first);
                }
                _seg_chunk->build_columns();
            } else {
                // std::cerr << " append " << chunk->num_rows() << "rows, become " << _seg_chunk->num_rows() << std::endl;
                _seg_chunk->append_chunk(chunk);
            }
        }
        return _seg_chunk;
    }

    void random_select(std::vector<uint32_t>& select, size_t count, size_t range) {
        select.resize(count);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, range - 1);
        std::generate(select.begin(), select.end(), [&]() { return dis(gen); });
    }

    ChunkPtr build_chunk(size_t chunk_size) {
        if (_types.empty()) {
            for (int i = 0; i < _column_count; i++) {
                if (_data_type == 0) {
                    _types.emplace_back(TypeDescriptor::create_varchar_type(128));
                } else if (_data_type == 1) {
                    _types.emplace_back(LogicalType::TYPE_INT);
                } else {
                    CHECK(false) << "data type not supported: " << _data_type;
                }
            }
        }

        auto chunk = std::make_unique<Chunk>();
        for (int i = 0; i < _column_count; i++) {
            auto col = init_dest_column(_types[i], chunk_size);
            chunk->append_column(col, i);
        }
        return chunk;
    }

    ColumnPtr init_dest_column(const TypeDescriptor& type, size_t chunk_size) {
        auto c1 = ColumnHelper::create_column(type, true);
        c1->reserve(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            if (type.is_string_type()) {
                std::string str = fmt::format("str{}", i);
                c1->append_datum(Slice(str));
            } else if (type.is_integer_type()) {
                c1->append_datum(i);
            } else {
                CHECK(false) << "data type not supported";
            }
        }
        return c1;
    }

private:
    int _column_count = 4;
    int _data_type = 0;
    size_t _dest_chunk_size = 4096;
    size_t _segment_size = 65536;
    size_t _num_segments = 10;
    size_t _segment_chunk_size = _segment_size * _num_segments;

    SegmentedChunkPtr _seg_chunk;
    ChunkPtr _big_chunk;
    std::vector<uint32_t> select;
    std::vector<TypeDescriptor> _types;
};

static void BenchSegmentedChunkClone(benchmark::State& state) {
    google::InstallFailureSignalHandler();
    auto perf = std::make_unique<SegmentedChunkPerf>();
    perf->prepare(state);
    perf->do_bench_segmented_chunk_clone(state);
}

static void BenchChunkClone(benchmark::State& state) {
    google::InstallFailureSignalHandler();
    auto perf = std::make_unique<SegmentedChunkPerf>();
    perf->prepare(state);
    perf->do_bench_chunk_clone(state);
}

static std::vector<std::vector<int64_t>> chunk_clone_args() {
    return {
            {1, 2, 3, 4},  // num columns
            {0, 1},        // data type
            {1, 4, 16, 64} // num_segments
    };
}

BENCHMARK(BenchSegmentedChunkClone)->ArgsProduct(chunk_clone_args());
BENCHMARK(BenchChunkClone)->ArgsProduct(chunk_clone_args());

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

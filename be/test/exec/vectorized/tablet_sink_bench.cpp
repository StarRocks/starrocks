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
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "runtime/vectorized/chunk_cursor.h"
#include "storage/async_tablet_sink_chunk_split_executor.h"
#include "storage/storage_engine.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/execution_queue.h>
DIAGNOSTIC_POP

namespace starrocks {

const int64_t kRetryIntervalMs = 50;

class ChunkHelper {
public:
    void SetUp() {}
    void TearDown() {}

    ChunkHelper(int column_count, int src_chunk_size, int dest_chunk_size, int null_percent)
            : _column_count(column_count),
              _src_chunk_size(src_chunk_size),
              _dest_chunk_size(dest_chunk_size),
              _null_percent(null_percent) {
        init_types();
    }

    void init_types() {
        _types.resize(_column_count);
        for (int i = 0; i < _column_count; i++) {
            _types[i] = TypeDescriptor::create_varchar_type(1024);
        }
    }

    vectorized::ChunkPtr next_src_chunk() {
        auto chunk = std::make_unique<vectorized::Chunk>();
        auto col = init_src_key_column(_types[0]);
        chunk->append_column(col, 0);
        for (int i = 1; i < _column_count; i++) {
            col = init_src_column(_types[i]);
            chunk->append_column(col, i);
        }
        return chunk;
    }

    vectorized::ChunkPtr init_dest_chunk() {
        auto chunk = std::make_unique<vectorized::Chunk>();
        for (int i = 0; i < _column_count; i++) {
            auto col = init_dest_column(_types[i]);
            chunk->append_column(col, i);
        }
        return chunk;
    }

    ColumnPtr init_src_column(const TypeDescriptor& type) {
        ColumnPtr c1 = vectorized::ColumnHelper::create_column(type, true);
        c1->reserve(_src_chunk_size);
        auto* nullable_col = down_cast<vectorized::NullableColumn*>(c1.get());
        for (int k = 0; k < _src_chunk_size; k++) {
            int v = rand();
            if (v % 100 < _null_percent) {
                nullable_col->append_default();
            } else {
                string s = "str123" + std::to_string(v);
                Slice slice(s);
                vectorized::Datum datum(slice);
                nullable_col->append_datum(datum);
            }
        }
        return c1;
    }

    ColumnPtr init_src_key_column(const TypeDescriptor& type) {
        ColumnPtr c1 = vectorized::ColumnHelper::create_column(type, true);
        c1->reserve(_src_chunk_size);
        auto* nullable_col = down_cast<vectorized::NullableColumn*>(c1.get());
        for (int k = 0; k < _src_chunk_size; k++) {
            int v = rand();
            string s = "str123" + std::to_string(v);
            Slice slice(s);
            vectorized::Datum datum(slice);
            nullable_col->append_datum(datum);
        }
        return c1;
    }

    ColumnPtr init_dest_column(const TypeDescriptor& type) {
        auto c1 = vectorized::ColumnHelper::create_column(type, true);
        c1->reserve(_dest_chunk_size);
        return c1;
    }

private:
    int _column_count = 400;
    int _src_chunk_size = 4096;
    int _dest_chunk_size = 4096;
    int _null_percent = 100;
    std::vector<TypeDescriptor> _types;
};

class BenchOlapTableSink {
public:
    BenchOlapTableSink(ChunkHelper& chunk_helper, int node_count, int dest_chunk_size)
            : _chunk_helper(chunk_helper),
              _node_count(node_count),
              _dest_chunks(_node_count, nullptr),
              _dest_chunk_size(dest_chunk_size){};

    ~BenchOlapTableSink(){};

    virtual void init() {
        _dest_chunks.resize(_node_count);
        for (int i = 0; i < _node_count; i++) {
            if (_dest_chunks[i] == nullptr) {
                _dest_chunks[i] = _chunk_helper.init_dest_chunk();
            }
        }
    }

    void do_hash(const vectorized::ColumnPtr& col, vector<uint32_t>& shuffle_idxs) {
        int size = col->size();
        shuffle_idxs.resize(size);

        col->crc32_hash(&shuffle_idxs[0], 0, size);
        for (int i = 0; i < size; i++) {
            shuffle_idxs[i] = shuffle_idxs[i] % _node_count;
        }
    }

    virtual void send_chunk(vectorized::Chunk* chunk) {
        do_hash(chunk->columns()[0], _shuffle_idxs);
        for (int n = 0; n < _node_count; n++) {
            _node_select_idx.resize(0);
            for (int i = 0; i < _shuffle_idxs.size(); i++) {
                if (_shuffle_idxs[i] == n) {
                    _node_select_idx.emplace_back(i);
                }
            }
            _dest_chunks[n]->append_selective(*chunk, _node_select_idx.data(), 0, _node_select_idx.size());
            if (_dest_chunks[n]->num_rows() > _dest_chunk_size) {
                _dest_chunks[n].reset();
                _dest_chunks[n] = _chunk_helper.init_dest_chunk();
            }
        }
    }

    virtual void close() {}

    ChunkHelper _chunk_helper;
    int _node_count;
    std::vector<vectorized::ChunkPtr> _dest_chunks;
    int _dest_chunk_size;

    std::vector<uint32_t> _node_select_idx;
    std::vector<uint32_t> _shuffle_idxs;
};

class BenchParallelOlapTableSink : public BenchOlapTableSink {
    struct TabletSinkAddChunkTask {
        vectorized::Chunk* chunk = nullptr;
        BenchParallelOlapTableSink* sink = nullptr;
        std::vector<uint32_t>* shuffle_index;
        std::atomic<int32_t>* pending_processing_num_channel;

        int n;
    };

    struct TabletSinkCachedElement {
        TabletSinkCachedElement(std::unique_ptr<vectorized::Chunk>&& chunk, int32_t num_channel)
                : chunk(std::move(chunk)), pending_processing_num_channel(num_channel) {}

        std::unique_ptr<vectorized::Chunk> chunk;
        std::atomic<int32_t> pending_processing_num_channel;
        std::vector<uint32_t> shuffle_index;
    };

public:
    BenchParallelOlapTableSink(ChunkHelper& chunk_helper, int node_count, int dest_chunk_size,
                               int tablet_sink_split_chunk_dop)
            : BenchOlapTableSink(chunk_helper, node_count, dest_chunk_size),
              _tablet_sink_split_chunk_dop(tablet_sink_split_chunk_dop) {}

    ~BenchParallelOlapTableSink() {}

    static int _execute_node_channel_add_chunk(void* meta, bthread::TaskIterator<TabletSinkAddChunkTask>& iter) {
        if (iter.is_queue_stopped()) {
            return 0;
        }
        for (; iter; ++iter) {
            BenchParallelOlapTableSink* sink = iter->sink;
            vectorized::Chunk* chunk = iter->chunk;
            std::vector<uint32_t>& shuffle_index = *iter->shuffle_index;
            int n = iter->n;

            std::vector<uint32_t> node_select_idx;
            for (int i = 0; i < shuffle_index.size(); i++) {
                if (shuffle_index[i] == n) {
                    node_select_idx.emplace_back(i);
                }
            }

            int from = 0;
            int size = node_select_idx.size();

            sink->_dest_chunks[n]->append_selective(*chunk, node_select_idx.data(), from, size);
            if (sink->_dest_chunks[n]->num_rows() >= sink->_dest_chunk_size) {
                sink->_dest_chunks[n].reset();
                sink->_dest_chunks[n] = sink->_chunk_helper.init_dest_chunk();
            }
            (*iter->pending_processing_num_channel)--;
        }
        return 0;
    }

    void init() override {
        _dest_chunks.resize(_node_count);
        for (int i = 0; i < _node_count; i++) {
            if (_dest_chunks[i] == nullptr) {
                _dest_chunks[i] = _chunk_helper.init_dest_chunk();
            }
        }

        bthread::ExecutionQueueOptions opts;
        config::number_tablet_sink_chunk_split_threads = 16;
        executor.init();
        opts.executor = &executor;
        for (int i = 0; i < _tablet_sink_split_chunk_dop; i++) {
            _queue_ids.emplace_back();
            bthread::execution_queue_start(&_queue_ids[_queue_ids.size() - 1], &opts, _execute_node_channel_add_chunk,
                                           nullptr);
        }
    }

    void send_chunk(vectorized::Chunk* chunk) override {
        _cached_element_queue.emplace(chunk->clone_unique(), _node_count);
        do_hash(chunk->columns()[0], _cached_element_queue.back().shuffle_index);
        for (int n = 0; n < _node_count; n++) {
            TabletSinkAddChunkTask task;
            task.chunk = _cached_element_queue.back().chunk.get();
            task.sink = this;
            task.n = n;
            task.pending_processing_num_channel = &_cached_element_queue.back().pending_processing_num_channel;
            task.shuffle_index = &_cached_element_queue.back().shuffle_index;
            int queue_index = n % _tablet_sink_split_chunk_dop;
            bthread::execution_queue_execute(_queue_ids[queue_index], task);
        }
    }

    void close() override {
        while (!_cached_element_queue.empty()) {
            while (_cached_element_queue.front().pending_processing_num_channel > 0) {
            }
            _cached_element_queue.pop();
        }
        for (const auto& queue_id : _queue_ids) {
            bthread::execution_queue_stop(queue_id);
            bthread::execution_queue_join(queue_id);
        }
    }

    int _tablet_sink_split_chunk_dop;

    AsyncTabletSinkChunkSplitExecutor executor;
    std::vector<bthread::ExecutionQueueId<TabletSinkAddChunkTask>> _queue_ids;
    std::queue<TabletSinkCachedElement> _cached_element_queue;
};

static void bench_func(benchmark::State& state) {
    int chunk_count = state.range(0);
    int column_count = state.range(1);
    int node_count = state.range(2);
    int src_chunk_size = state.range(3);
    int dest_chunk_size = state.range(4);
    int null_percent = state.range(5);
    int tablet_sink_split_chunk_dop = state.range(6);
    int use_parallel = state.range(7);

    ChunkHelper chunk_helper(column_count, src_chunk_size, dest_chunk_size, null_percent);
    state.ResumeTiming();
    std::unique_ptr<BenchOlapTableSink> olapTableSink = nullptr;
    if (!use_parallel) {
        olapTableSink = std::make_unique<BenchOlapTableSink>(chunk_helper, node_count, dest_chunk_size);
    } else {
        olapTableSink = std::make_unique<BenchParallelOlapTableSink>(chunk_helper, node_count, dest_chunk_size,
                                                                     tablet_sink_split_chunk_dop);
    }
    olapTableSink->init();
    for (int i = 0; i < chunk_count; i++) {
        state.PauseTiming();
        vectorized::ChunkPtr src_chunk = chunk_helper.next_src_chunk();
        state.ResumeTiming();
        olapTableSink->send_chunk(src_chunk.get());
    }
    olapTableSink->close();
    state.PauseTiming();
}

static void process_args(benchmark::internal::Benchmark* b) {
    // chunk_count, column_count, node_count, src_chunk_size, dest_chunk_size, null percent, tablet_sink_split_chunk_dop, use_parallel
    b->Args({400, 400, 200, 4096, 4096, 80, 16, 1});
    b->Args({400, 400, 200, 4096, 4096, 80, 8, 1});
    b->Args({400, 400, 200, 4096, 4096, 80, 4, 1});
    b->Args({400, 400, 200, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 200, 4096, 4096, 80, 1, 1});
    b->Args({400, 400, 200, 4096, 4096, 80, 1, 0});

    // node
    b->Args({400, 400, 100, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 100, 4096, 4096, 80, 1, 0});
    b->Args({400, 400, 50, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 50, 4096, 4096, 80, 1, 0});
    b->Args({400, 400, 10, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 10, 4096, 4096, 80, 1, 0});
    b->Args({400, 400, 5, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 5, 4096, 4096, 80, 1, 0});
    b->Args({400, 400, 3, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 3, 4096, 4096, 80, 1, 0});
    b->Args({400, 400, 1, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 1, 4096, 4096, 80, 1, 0});

    // column
    b->Args({400, 400, 200, 4096, 4096, 80, 2, 1});
    b->Args({400, 400, 200, 4096, 4096, 80, 1, 0});
    b->Args({800, 200, 200, 4096, 4096, 80, 2, 1});
    b->Args({800, 200, 200, 4096, 4096, 80, 1, 0});
    b->Args({1600, 100, 200, 4096, 4096, 80, 2, 1});
    b->Args({1600, 100, 200, 4096, 4096, 80, 1, 0});
    b->Args({16000, 10, 200, 4096, 4096, 80, 2, 1});
    b->Args({16000, 10, 200, 4096, 4096, 80, 1, 0});
}
BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();
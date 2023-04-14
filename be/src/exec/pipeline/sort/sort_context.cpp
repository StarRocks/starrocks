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

#include "exec/pipeline/sort/sort_context.h"

#include <algorithm>
#include <mutex>
#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sorting.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/chunk_cursor.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

void SortContext::close(RuntimeState* state) {
    _chunks_sorter_partitions.clear();
}

void SortContext::add_partition_chunks_sorter(const std::shared_ptr<ChunksSorter>& chunks_sorter) {
    _chunks_sorter_partitions.push_back(chunks_sorter);
}

void SortContext::finish_partition(uint64_t partition_rows) {
    _total_rows.fetch_add(partition_rows, std::memory_order_relaxed);
    _num_partition_finished.fetch_add(1, std::memory_order_release);
}

bool SortContext::is_partition_sort_finished() const {
    return _num_partition_finished.load(std::memory_order_acquire) == _num_partition_sinkers;
}

bool SortContext::is_output_finished() const {
    return is_partition_sort_finished() && _merger_inited && _required_rows == 0;
}

// TODO: optimize this
bool SortContext::is_partition_ready() const {
    return std::all_of(_chunks_sorter_partitions.begin(), _chunks_sorter_partitions.end(), [](const auto& sorter) {
        return !sorter->spiller() || !sorter->spiller()->spilled() || sorter->spiller()->has_output_data();
    });
}

void SortContext::cancel() {}

StatusOr<ChunkPtr> SortContext::pull_chunk() {
    _init_merger();

    while (_required_rows > 0 && !_merger.is_eos()) {
        if (_current_chunk.empty()) {
            auto chunk = _merger.try_get_next();
            // Input cursor maye short circuit
            if (!chunk) {
                if (_merger.is_eos()) {
                    _required_rows = 0;
                }
                return nullptr;
            }
            _current_chunk.reset(std::move(chunk));
        }

        // Skip some rows before return it
        if (_offset > 0) {
            _offset -= _current_chunk.skip(_offset);
            if (_current_chunk.empty()) {
                continue;
            }
        }

        // Return required rows and cutoff the big chunk
        size_t required_rows = std::min<size_t>(_required_rows, _state->chunk_size());
        ChunkPtr res = _current_chunk.cutoff(required_rows);
        _required_rows -= res->num_rows();
        return res;
    }
    return nullptr;
}

void SortContext::set_runtime_filter_collector(RuntimeFilterHub* hub, int32_t plan_node_id,
                                               std::unique_ptr<RuntimeFilterCollector>&& collector) {
    std::call_once(_set_collector_flag,
                   [&collector, plan_node_id, hub]() { hub->set_collector(plan_node_id, std::move(collector)); });
}

Status SortContext::_init_merger() {
    if (_merger_inited) {
        return Status::OK();
    }

    // Keep all the data if topn type is RANK or DENSE_RANK
    _required_rows = _total_rows;
    if (_topn_type == TTopNType::ROW_NUMBER) {
        _required_rows = ((_limit < 0) ? _total_rows.load() : std::min<int64_t>(_limit + _offset, _total_rows));
    }

    _partial_cursors.reserve(_num_partition_sinkers);
    for (int i = 0; i < _num_partition_sinkers; i++) {
        ChunkProvider provider = [i, this](ChunkUniquePtr* out_chunk, bool* eos) -> bool {
            // data ready
            if (out_chunk == nullptr || eos == nullptr) {
                return true;
            }
            auto& partition_sorter = _chunks_sorter_partitions[i];
            ChunkPtr chunk;
            Status st = partition_sorter->get_next(&chunk, eos);
            if (!st.ok() || *eos || chunk == nullptr) {
                return false;
            }
            *out_chunk = chunk->clone_unique();
            return true;
        };
        _partial_cursors.push_back(std::make_unique<SimpleChunkSortCursor>(std::move(provider), &_sort_exprs));
    }

    RETURN_IF_ERROR(_merger.init(_sort_desc, std::move(_partial_cursors)));
    CHECK(_merger.is_data_ready()) << "data must be ready";
    _merger_inited = true;

    return Status::OK();
}

SortContextFactory::SortContextFactory(RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
                                       std::vector<ExprContext*> sort_exprs, const std::vector<bool>& is_asc_order,
                                       const std::vector<bool>& is_null_first,
                                       [[maybe_unused]] const std::vector<TExpr>& partition_exprs, int64_t offset,
                                       int64_t limit, const std::string& sort_keys,
                                       const std::vector<OrderByType>& order_by_types,
                                       const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters)
        : _state(state),
          _topn_type(topn_type),
          _is_merging(is_merging),
          _offset(offset),
          _limit(limit),
          _sort_exprs(std::move(sort_exprs)),
          _sort_descs(is_asc_order, is_null_first),
          _build_runtime_filters(build_runtime_filters) {}

SortContextPtr SortContextFactory::create(int32_t idx) {
    size_t actual_idx = _is_merging ? 0 : idx;
    if (auto it = _sort_contexts.find(actual_idx); it != _sort_contexts.end()) {
        return it->second;
    }

    auto ctx = std::make_shared<SortContext>(_state, _topn_type, _offset, _limit, _sort_exprs, _sort_descs,
                                             _build_runtime_filters);
    _sort_contexts.emplace(actual_idx, ctx);
    return ctx;
}
} // namespace starrocks::pipeline

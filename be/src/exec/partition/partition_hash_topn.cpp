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

#include "exec/partition/partition_hash_topn.h"

#include <algorithm>

#include "exec/sorting/sort_helper.h"

namespace starrocks {

PartitionHashTopn::PartitionHashTopn(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>& is_asc_order,
                                     const std::vector<bool>& is_null_first, size_t offset, size_t limit)
        : _sort_exprs(sort_exprs),
          _sort_descs(is_asc_order, is_null_first),
          _limit(limit),
          _partition_heap(PartitionComparator(&_sort_descs)) {}

bool PartitionHashTopn::is_valid(size_t partition_idx) {
    DCHECK_LT(partition_idx, _partition_infos.size());
    auto& partition_info = _partition_infos[partition_idx];
    return partition_info.is_valid();
}

Status PartitionHashTopn::new_partition(size_t partition_idx) {
    _partition_infos.emplace_back();
    return Status::OK();
}

Status PartitionHashTopn::offer(size_t partition_idx, const ChunkPtr& chunk) {
    VLOG_ROW << "PartitionHashTopn offer, chunk size: " << chunk->num_rows() << ", partition_idx: " << partition_idx
             << ", heap size: " << _partition_heap.size();
    for (auto i = 0; i < chunk->num_rows(); i++) {
        VLOG_ROW << "PartitionHashTopn offer, row " << i << ": " << chunk->debug_row(i)
                 << ", partition_idx: " << partition_idx;
    }

    // Get or create partition info
    DCHECK_LT(partition_idx, _partition_infos.size());
    auto& partition_info = _partition_infos[partition_idx];
    if (!partition_info.is_valid()) {
        // the partition is not valid, no need to compare with the new row
        return Status::OK();
    }
    // NOTE: order by exprs are the same for partitions, so we can use the 1st row to compare with the new row
    // for each partition, only use the 1st row to compare with the new row
    partition_info.chunks.emplace_back(chunk);

    bool is_new_partition = partition_info.is_new_partition;
    if (is_new_partition) {
        RETURN_IF_ERROR(_evaluate_sort_columns(chunk, &partition_info.sort_columns));
        partition_info.is_new_partition = false;
        _partition_heap.push(partition_info);
    }

    // Keep only top N partitions (by their best row)
    size_t heap_size = _partition_heap.size();
    if (heap_size % _limit == 0 && heap_size > _limit) {
        while (heap_size > _limit) {
            auto& top_partition = _partition_heap.top();
            // mark the partition as invalid, no need to compare with the new row
            top_partition.chunks.clear();
            top_partition.sort_columns.clear();
            _partition_heap.pop();
        }
    }
    return Status::OK();
}

Status PartitionHashTopn::done() {
    // Keep only top N partitions (by their best row)
    while (_partition_heap.size() > _limit) {
        auto& top_partition = _partition_heap.top();
        // mark the partition as invalid, no need to compare with the new row
        top_partition.chunks.clear();
        top_partition.sort_columns.clear();
        _partition_heap.pop();
    }
    _is_done = true;
    return Status::OK();
}

StatusOr<ChunkPtr> PartitionHashTopn::get_next_chunk(size_t expected_rows) {
    DCHECK(_is_done) << "PartitionHashTopn is not done";
    if (_partition_heap.empty()) {
        return ChunkPtr(nullptr);
    }
    // clone the 1st chunk of the 1st partition
    auto& top_partition = _partition_heap.top();
    DCHECK(top_partition.is_valid());
    auto output = top_partition.chunks[0]->clone_empty_with_slot();

    size_t produced = 0;
    // start to produce the result chunk from _result_pos if _result_pos is not 0
    VLOG_ROW << "PartitionHashTopn start to produce result chunk, output size: " << output->num_rows()
             << ", heap size: " << _partition_heap.size() << ", result chunk idx: " << _result_chunk_idx
             << ", result chunk pos: " << _result_chunk_pos << ", produced: " << produced
             << ", expected rows: " << expected_rows << ", top partition valid: " << top_partition.is_valid();
    while (!_partition_heap.empty() && produced < expected_rows) {
        auto& partition_info = _partition_heap.top();
        DCHECK(partition_info.is_valid());
        size_t chunk_size = partition_info.chunks.size();
        for (; _result_chunk_idx < chunk_size; ++_result_chunk_idx) {
            auto& chunk = partition_info.chunks[_result_chunk_idx];
            size_t num_rows = chunk->num_rows() - _result_chunk_pos;
            if (num_rows == 0) {
                _result_chunk_pos = 0;
                continue;
            }
            // we must ensure the output result size is not larger than the expected rows
            if (produced + num_rows > expected_rows) {
                num_rows = expected_rows - produced;
                output->append(*chunk, _result_chunk_pos, num_rows);
                _result_chunk_pos += num_rows;
                produced += num_rows;
                // Break from the inner loop since we've reached expected_rows
                break;
            } else {
                output->append(*chunk, _result_chunk_pos, num_rows);
                _result_chunk_pos = 0;
                produced += num_rows;
                // If we've reached expected_rows, break from the inner loop
                if (produced >= expected_rows) {
                    break;
                }
            }
        }
        // pop the partition, and reset the result chunk index and position
        if (_result_chunk_pos == 0 && _result_chunk_idx == chunk_size) {
            _partition_heap.pop();
            _result_chunk_idx = 0;
        }
        // If we've reached expected_rows, break from the outer loop
        if (produced >= expected_rows) {
            break;
        }
    }
    VLOG_ROW << "PartitionHashTopn get_next_chunk, output size: " << output->num_rows()
             << ", heap size: " << _partition_heap.size() << ", result chunk idx: " << _result_chunk_idx
             << ", result chunk pos: " << _result_chunk_pos << ", produced: " << produced
             << ", expected rows: " << expected_rows;
    for (auto i = 0; i < output->num_rows(); i++) {
        VLOG_ROW << "PartitionHashTopn get_next_chunk, row " << i << ": " << output->debug_row(i);
    }
    return output;
}

Status PartitionHashTopn::_evaluate_sort_columns(const ChunkPtr& chunk, Columns* columns) {
    columns->reserve(_sort_exprs->size());
    for (auto* expr : *_sort_exprs) {
        // TODO: only needs the 1st row of the sort columns for each partition, so we can optimize the evaluation here
        ASSIGN_OR_RETURN(ColumnPtr col, expr->evaluate(chunk.get()));
        columns->emplace_back(col);
    }
    return Status::OK();
}

} // namespace starrocks

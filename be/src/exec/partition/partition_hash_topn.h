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

#pragma once

#include <queue>
#include <unordered_map>
#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sorting.h"
#include "exprs/expr_context.h"

namespace starrocks {

// A lightweight hash-aware topN helper.
// It keeps the best (offset + limit) rows according to the sort keys.
// The data is kept by referencing input chunks, so the caller must keep
// the chunks alive until done() is called.
class PartitionHashTopn {
public:
    PartitionHashTopn(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>& is_asc_order,
                      const std::vector<bool>& is_null_first, size_t offset, size_t limit);

    bool is_valid(size_t partition_idx);

    // Feed one chunk. The chunk must outlive the topN instance until done() is called.
    Status offer(size_t partition_idx, const ChunkPtr& chunk);

    // New a partition.
    Status new_partition(size_t partition_idx);

    // Finalize and make the internal result iterable.
    Status done();

    // Return next chunk of results. nullptr chunk means EOS.
    StatusOr<ChunkPtr> get_next_chunk(size_t expected_rows);

    size_t result_size() const { return _partition_heap.size(); }
    bool exhausted() const { return _is_done && _partition_heap.empty(); }

private:
    // PartitionInfo is used to keep all chunks for a partition
    // which is the same for each row compared by the sort/partition keys.
    struct PartitionInfo {
        // chunks in this partition
        // NOTE: chunks should not empty since each partition should have at least one chunk.
        // but we use a hack tech here, when chunks is empty after pop, the partition is considered as invalid.
        mutable std::vector<ChunkPtr> chunks;
        // TODO: only keep the 1st row of the sort columns for each partition.
        mutable Columns sort_columns;
        // whether the partition is new, only valid for the first chunk of the partition
        bool is_new_partition = true;
        // whether the partition is valid, if the partition is not valid, no need to compare with the new row
        bool is_valid() const { return is_new_partition || !chunks.empty(); }
    };

    struct PartitionComparator {
        explicit PartitionComparator(const SortDescs* sort_descs) : sort_descs(sort_descs) {}

        bool operator()(const PartitionInfo& lhs, const PartitionInfo& rhs) const {
            const auto& l_sort_columns = lhs.sort_columns;
            const auto& r_sort_columns = rhs.sort_columns;
            int cmp = compare_chunk_row(*sort_descs, l_sort_columns, r_sort_columns, 0, 0);
            // priority_queue keeps the "largest" on top. We want the worst partition on top,
            // so return true if lhs partition is better (has smaller best row) than rhs.
            return cmp > 0;
        }

        const SortDescs* sort_descs;
    };

    Status _evaluate_sort_columns(const ChunkPtr& chunk, Columns* columns);

    const std::vector<ExprContext*>* _sort_exprs;
    SortDescs _sort_descs;
    const size_t _limit;

    // Store all rows per partition
    std::vector<PartitionInfo> _partition_infos;
    // Heap to maintain top N partitions (by their best row)
    std::priority_queue<PartitionInfo, std::vector<PartitionInfo>, PartitionComparator> _partition_heap;
    // The position of the next row to be returned in the result chunk
    size_t _result_chunk_idx = 0;
    size_t _result_chunk_pos = 0;

    bool _is_done = false;
};

} // namespace starrocks

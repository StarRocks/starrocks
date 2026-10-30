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

#include <cstddef>
#include <memory>
#include <vector>

#include "compute_env/spill/block_group.h"

namespace starrocks {

class ChunkIterator;
using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;

/**
 * Encapsulates a single merge input unit for pipeline execution.
 *
 * WHY THIS STRUCT: Groups all resources needed for one merge task to be executed
 * independently in a pipeline operator. The block_groups ownership is critical.
 */
struct LoadSpillMergeInputBatch {
    // LIFETIME MANAGEMENT: Holds shared ownership of block groups to prevent premature
    // destruction. The merge_itr reads from these block groups asynchronously during
    // pipeline execution, so block groups must outlive the iterator. Without this,
    // we'd have use-after-free bugs when blocks are reclaimed before iterator finishes.
    std::vector<spill::BlockGroupPtr> block_groups;

    // Iterator that performs the actual merge of spilled data chunks. Supports both
    // sorted merge (for aggregation/ordering) and union (for DUP_KEYS tables).
    ChunkIteratorPtr merge_itr;

    // Smallest slot_idx (original memtable flush order) among this batch's block groups. Batches are
    // formed from a contiguous, monotonically increasing slot range, so this gives each batch a stable
    // order key. The result-consolidation step sorts batches by it so segments/del files are merged in
    // flush order regardless of the (concurrent, hence unordered) order in which tasks were registered.
    int64_t slot_idx = -1;

    // Metrics for monitoring merge workload distribution across pipeline tasks.
    // Used to ensure roughly equal work distribution and for performance analysis.
    size_t total_block_groups = 0;
    size_t total_block_bytes = 0;

    // Release block group in advance to free load spill disk space
    void release_block_groups() { block_groups.clear(); }
};

} // namespace starrocks

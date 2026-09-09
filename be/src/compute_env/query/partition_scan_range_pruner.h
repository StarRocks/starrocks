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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/status.h"
#include "column/vectorized_fwd.h"
#include "common/runtime_profile.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "types/type_descriptor.h"

namespace starrocks {

class RuntimeFilterProbeCollector;
class RuntimeState;

// Materialize partition column candidate values described by `column_range` into a Column.
// Supports either a list of literal `list_values` or an inclusive integer/date `[begin_key, end_key]` range.
// Used by the backend-side dynamic partition pruning path in both the shared-nothing OlapScanNode and
// the shared-data LakeDataSourceProvider.
StatusOr<ColumnPtr> build_partition_col_values(const SlotDescriptor* slot_desc, const TKeyRange& column_range,
                                               ObjectPool* obj_pool, RuntimeState* state);

// Drop scan ranges whose partition values cannot satisfy any of the single-column
// `partition_conjunct_ctxs`. The conjunct contexts must have been prepared and opened by the
// caller. The tuple descriptor is used to resolve partition column names to slots. On success
// `pruned_scan_ranges` is populated with the retained scan ranges.
Status prune_scan_ranges_by_partition_conjuncts(RuntimeState* state, const TupleDescriptor* tuple_desc,
                                                const std::vector<ExprContext*>& partition_conjunct_ctxs,
                                                const std::vector<TScanRangeParams>& scan_ranges,
                                                std::vector<TScanRangeParams>* pruned_scan_ranges);

struct RuntimeFilterPartitionBoundary {
    std::vector<int64_t> physical_partition_ids;
    SlotId slot_id = 0;
    TypeDescriptor column_type;
    // LIST partition: the partition's value set; NULL membership travels in contains_null.
    ColumnPtr list_values;
    // RANGE partition: [lower_bound, upper_bound); either side may be null for unbounded.
    ColumnPtr lower_bound;
    ColumnPtr upper_bound;
    bool contains_null = false;
    // Multi-column RANGE only: its first-column projection has a closed upper bound.
    bool upper_bound_closed = false;

    bool is_range() const { return lower_bound != nullptr || upper_bound != nullptr; }
};

using RuntimeFilterPartitionBoundaries = std::vector<RuntimeFilterPartitionBoundary>;
using RuntimeFilterPartitionBoundaryMap = std::unordered_map<SlotId, RuntimeFilterPartitionBoundaries>;

RuntimeFilterPartitionBoundaryMap parse_partition_boundaries(const TupleDescriptor* tuple_desc,
                                                             const std::vector<TPartitionBoundary>& thrift_boundaries);

// Pruning state for one scan node, shared by all of its drivers.
class RuntimeFilterPartitionPruner {
public:
    explicit RuntimeFilterPartitionPruner(RuntimeFilterPartitionBoundaryMap boundaries);

    int64_t candidate_partition_count() const { return _candidate_partition_count; }

    // Judge the exact Runtime IN filters; call when the local runtime filters become ready.
    void prune_by_in_filters(const std::vector<ExprContext*>& runtime_in_filters);
    // Per checkpoint: consume newly published bloom filters; a late one is retried.
    void prune_by_bloom_filters(RuntimeFilterProbeCollector& runtime_bloom_filters, RuntimeState* state);
    bool is_partition_pruned(int64_t partition_id) const {
        return _pruned_partitions.load(std::memory_order_acquire)->count(partition_id) != 0;
    }
    int64_t pruned_partition_count() const {
        return static_cast<int64_t>(_pruned_partitions.load(std::memory_order_acquire)->size());
    }

private:
    using PrunedPartitions = std::unordered_set<int64_t>;
    using PrunedPartitionsPtr = std::shared_ptr<const PrunedPartitions>;

    // Requires _update_mutex.
    void publish_pruned_partitions(const PrunedPartitions& current, const PrunedPartitions& newly_pruned_partitions);

    const RuntimeFilterPartitionBoundaryMap _boundaries;
    int64_t _candidate_partition_count = 0;

    // Readers load an immutable snapshot without taking _update_mutex.
    std::atomic<PrunedPartitionsPtr> _pruned_partitions{std::make_shared<PrunedPartitions>()};
    // Terminal when all bloom filters are consumed or all candidates are pruned.
    std::atomic<bool> _bloom_pruning_complete{false};

    // IN waits for the lock; bloom skips checkpoints on contention.
    std::mutex _update_mutex;
    // Guarded by _update_mutex.
    std::unordered_set<int32_t> _processed_filter_ids;
};

// Every driver records a snapshot of the shared pruned set: the high water mark keeps concurrent snapshots
// from rolling back, and drivers and instances merge by max.
inline RuntimeProfile::Counter* add_rf_partitions_pruned_counter(RuntimeProfile* profile) {
    return profile->AddHighWaterMarkCounter(
            "RuntimeFilterPartitionsPruned", TUnit::UNIT,
            RuntimeProfile::Counter::create_strategy(TCounterAggregateType::MAX, TCounterMergeType::MERGE_ALL, 0,
                                                     TCounterMinMaxType::SKIP_ALL));
}

} // namespace starrocks

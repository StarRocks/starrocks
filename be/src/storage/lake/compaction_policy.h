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

#include <unordered_set>
#include <vector>

#include "common/statusor.h"
#include "storage/compaction_utils.h"

namespace starrocks {
class TabletMetadataPB;
}

namespace starrocks::lake {

class Rowset;
using RowsetPtr = std::shared_ptr<Rowset>;
class CompactionPolicy;
using CompactionPolicyPtr = std::shared_ptr<CompactionPolicy>;
class TabletManager;

// Partial compaction state for a tablet (Design Doc Section 6.3.3)
// Used to support the Selector Strategy Extension for autonomous compaction
struct PartialCompactionState {
    int64_t tablet_id = 0;
    bool has_pending_results = false;      // Some tablets have finished compaction
    bool has_running_tasks = false;        // Some tablets still running compaction
    int pending_result_count = 0;          // Number of pending results
    int running_task_count = 0;            // Number of running tasks
    
    // Check if this tablet is in partial completion state
    // A tablet is in partial state if it has pending results but also running tasks
    bool is_partial() const {
        return has_pending_results && has_running_tasks;
    }
    
    // Check if tablet should trigger publish
    // Returns true if there are pending results that can be published
    bool should_trigger_publish() const {
        return has_pending_results;
    }
};

// Compaction policy for lake tablet
//
// Enhanced with Partial Compaction Support (Design Doc Section 6.3.3):
// - Detects partial completion states
// - Supports publish even when only some tablets have results
// - Maintains version consistency using force_publish
class CompactionPolicy {
public:
    virtual ~CompactionPolicy();

    virtual StatusOr<std::vector<RowsetPtr>> pick_rowsets() = 0;

    // Pick rowsets with data volume limit and rowset exclusion support for autonomous compaction
    // @param max_bytes: maximum total data size to compact in one task
    // @param exclude_rowsets: rowset IDs currently being compacted (should be excluded)
    // @return: selected rowsets for compaction
    virtual StatusOr<std::vector<RowsetPtr>> pick_rowsets_with_limit(int64_t max_bytes,
                                                                      const std::unordered_set<uint32_t>& exclude_rowsets);

    virtual StatusOr<CompactionAlgorithm> choose_compaction_algorithm(const std::vector<RowsetPtr>& rowsets);

    static StatusOr<CompactionPolicyPtr> create(TabletManager* tablet_mgr,
                                                std::shared_ptr<const TabletMetadataPB> tablet_metadata,
                                                bool force_base_compaction);

    static bool is_real_time_compaction_strategy(const std::shared_ptr<const TabletMetadataPB>& metadata);

protected:
    explicit CompactionPolicy(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> tablet_metadata,
                              bool force_base_compaction)
            : _tablet_mgr(tablet_mgr),
              _tablet_metadata(std::move(tablet_metadata)),
              _force_base_compaction(force_base_compaction) {
        CHECK(_tablet_mgr != nullptr) << "tablet_mgr is null";
        CHECK(_tablet_metadata != nullptr) << "tablet metadata is null";
    }

    TabletManager* _tablet_mgr;
    std::shared_ptr<const TabletMetadataPB> _tablet_metadata;
    bool _force_base_compaction;
};

double compaction_score(TabletManager* tablet_mgr, const std::shared_ptr<const TabletMetadataPB>& metadata);

// ============== Partial Compaction Support (Section 6.3.3) ==============

// Selector that supports partial compaction states for autonomous compaction
// 
// Design Doc Section 6.3.3: Necessity of Selector Extension
// Why must Selector support "partial compaction"?
// Scenario: In autonomous compaction mode:
// - Some tablets finish early (local results exist)
// - Others are still running
// Selector must:
// - Detect partial completion states
// - Trigger publish when some results exist
// - Use force_publish to maintain version consistency
class PartialCompactionSelector {
public:
    // Check if tablets in a partition have partial compaction state
    // @param tablet_ids: list of tablet IDs to check
    // @param compacting_rowsets_map: map of tablet_id -> currently compacting rowset IDs
    // @param pending_results_tablets: set of tablet IDs with pending results
    // @return: partial compaction state for the partition
    static PartialCompactionState check_partition_state(
            const std::vector<int64_t>& tablet_ids,
            const std::unordered_map<int64_t, std::unordered_set<uint32_t>>& compacting_rowsets_map,
            const std::unordered_set<int64_t>& pending_results_tablets);

    // Determine if a partition should trigger publish based on partial compaction state
    // Returns true if:
    // 1. Some tablets have pending results
    // 2. The number of tablets with results exceeds threshold (configurable)
    // 3. Or force_publish is requested
    static bool should_publish_partial(const PartialCompactionState& state, 
                                       int min_tablets_with_results = 1,
                                       bool force_publish = false);

    // Build publish request for partial compaction
    // - Includes all tablets in the partition
    // - Sets force_publish flag to maintain version consistency
    // - Only tablets with results will have actual compaction applied
    static void prepare_partial_publish(const std::vector<int64_t>& tablet_ids,
                                        const std::unordered_set<int64_t>& tablets_with_results,
                                        bool* force_publish_out,
                                        std::vector<int64_t>* tablets_to_publish_out);
};

} // namespace starrocks::lake

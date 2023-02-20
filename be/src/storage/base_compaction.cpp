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

#include "storage/base_compaction.h"

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/trace.h"

namespace starrocks {

BaseCompaction::BaseCompaction(MemTracker* mem_tracker, TabletSharedPtr tablet)
        : Compaction(mem_tracker, std::move(tablet)) {}

BaseCompaction::~BaseCompaction() = default;

Status BaseCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return Status::InvalidArgument("base compaction input parameter error.");
    }

    std::unique_lock lock(_tablet->get_base_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OK();
    }
    TRACE("got base compaction lock");

    // 1. pick rowsets to compact
    RETURN_IF_ERROR(pick_rowsets_to_compact());
    TRACE("rowsets picked");
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());

    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker);
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    // 2. do base compaction, merge rowsets
    RETURN_IF_ERROR(do_compaction());
    TRACE("compaction finished");

    // 3. set state to success
    _state = CompactionState::SUCCESS;

    // 4. add metric to base compaction
    StarRocksMetrics::instance()->base_compaction_deltas_total.increment(_input_rowsets.size());
    StarRocksMetrics::instance()->base_compaction_bytes_total.increment(_input_rowsets_size);
    TRACE("save base compaction metrics");

    return Status::OK();
}

Status BaseCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _input_rowsets.clear();
    _tablet->pick_candicate_rowsets_to_base_compaction(&candidate_rowsets);
    if (candidate_rowsets.size() <= 1) {
        return Status::NotFound("base compaction no suitable version error.");
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    RETURN_IF_ERROR(check_version_continuity(candidate_rowsets));
    RETURN_IF_ERROR(_check_rowset_overlapping(candidate_rowsets));

    if (candidate_rowsets.size() == 2 && candidate_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return Status::NotFound("base compaction no suitable version error.");
    }

    std::vector<RowsetSharedPtr> transient_rowsets;
    size_t compaction_score = 0;

    for (auto& rowset : candidate_rowsets) {
        if (compaction_score >= config::max_base_compaction_num_singleton_deltas) {
            // got enough segments
            break;
        }
        compaction_score += rowset->rowset_meta()->get_compaction_score();
        transient_rowsets.push_back(rowset);
    }

    if (!transient_rowsets.empty()) {
        _input_rowsets = transient_rowsets;
    }

    if (compaction_score >= config::min_base_compaction_num_singleton_deltas) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                  << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                  << ", min_base_compaction_num_singleton_deltas=" << config::min_base_compaction_num_singleton_deltas;
        return Status::OK();
    }

    // 2. the ratio between base rowset and all input cumulative rowsets reachs the threshold
    int64_t base_size = 0;
    int64_t cumulative_total_size = 0;
    for (auto& rowset : _input_rowsets) {
        if (rowset->start_version() != 0) {
            cumulative_total_size += rowset->data_disk_size();
        } else {
            base_size = rowset->data_disk_size();
        }
    }

    double base_cumulative_delta_ratio = config::base_cumulative_delta_ratio;
    if (base_size == 0) {
        // base_size == 0 means this may be a base version [0-1], which has no data.
        // set to 1 to void devide by zero
        base_size = 1;
    }
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;

    if (cumulative_base_ratio > base_cumulative_delta_ratio) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                  << ", cumulative_total_size=" << cumulative_total_size << ", base_size=" << base_size
                  << ", cumulative_base_ratio=" << cumulative_base_ratio
                  << ", policy_ratio=" << base_cumulative_delta_ratio;
        return Status::OK();
    }

    // 3. the interval since last base compaction reachs the threshold
    int64_t base_creation_time = _input_rowsets[0]->creation_time();
    int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_base_compaction = time(nullptr) - base_creation_time;
    if (interval_since_last_base_compaction > interval_threshold) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                  << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction
                  << ", interval_threshold=" << interval_threshold;
        return Status::OK();
    }

    LOG(INFO) << "don't satisfy the base compaction policy. tablet=" << _tablet->full_name()
              << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
              << ", cumulative_base_ratio=" << cumulative_base_ratio
              << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction;
    return Status::NotFound("base compaction no suitable version error.");
}

Status BaseCompaction::_check_rowset_overlapping(const std::vector<RowsetSharedPtr>& rowsets) {
    for (auto& rs : rowsets) {
        if (rs->rowset_meta()->is_segments_overlapping()) {
            return Status::InternalError("base compaction segments overlapping error.");
        }
    }
    return Status::OK();
}

} // namespace starrocks

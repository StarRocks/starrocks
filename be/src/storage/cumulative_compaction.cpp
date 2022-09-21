// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/cumulative_compaction.h"

#include "runtime/current_thread.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks::vectorized {

CumulativeCompaction::CumulativeCompaction(MemTracker* mem_tracker, TabletSharedPtr tablet)
        : Compaction(mem_tracker, std::move(tablet)) {}

CumulativeCompaction::~CumulativeCompaction() {}

Status CumulativeCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return Status::InvalidArgument("cumulative compaction input parameter error.");
    }

    std::unique_lock lock(_tablet->get_cumulative_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OK();
    }
    TRACE("got cumulative compaction lock");

    // 1.calculate cumulative point
    _tablet->calculate_cumulative_point();
    TRACE("calculated cumulative point");

    // 2. pick rowsets to compact
    RETURN_IF_ERROR(pick_rowsets_to_compact());
    TRACE("rowsets picked");
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());

    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);

    // 3. do cumulative compaction, merge rowsets
    RETURN_IF_ERROR(do_compaction());
    TRACE("compaction finished");

    // 4. set state to success
    _state = CompactionState::SUCCESS;

    // 5. set cumulative point
    _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);

    // 6. add metric to cumulative compaction
    StarRocksMetrics::instance()->cumulative_compaction_deltas_total.increment(_input_rowsets.size());
    StarRocksMetrics::instance()->cumulative_compaction_bytes_total.increment(_input_rowsets_size);
    TRACE("save cumulative compaction metrics");

    return Status::OK();
}

Status CumulativeCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(&candidate_rowsets);

    if (candidate_rowsets.empty()) {
        return Status::NotFound("cumulative compaction no suitable version error.");
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);

    RETURN_IF_ERROR(check_version_continuity_with_cumulative_point(candidate_rowsets));
    RETURN_IF_ERROR(check_version_continuity(candidate_rowsets));

    std::vector<RowsetSharedPtr> transient_rowsets;
    size_t compaction_score = 0;
    // the last delete version we meet when traversing candidate_rowsets
    Version last_delete_version{-1, -1};

    for (auto rowset : candidate_rowsets) {
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            last_delete_version = rowset->version();
            if (!transient_rowsets.empty()) {
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                _input_rowsets = transient_rowsets;
                break;
            }

            // we meet a delete version, and no other versions before, skip it and continue
            transient_rowsets.clear();
            compaction_score = 0;
            continue;
        }

        if (compaction_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            // got enough segments
            break;
        }

        compaction_score += rowset->rowset_meta()->get_compaction_score();
        transient_rowsets.push_back(rowset);
    }

    // if we have a sufficient number of segments,
    // or have other versions before encountering the delete version, we should process the compaction.
    if (compaction_score >= config::min_cumulative_compaction_num_singleton_deltas ||
        (last_delete_version.first != -1 && !transient_rowsets.empty())) {
        _input_rowsets = transient_rowsets;
    }

    // Cumulative compaction will process with at least 1 rowset.
    // So when there is no rowset being chosen, we should return Status::NotFound("cumulative compaction no suitable version error.");
    if (_input_rowsets.empty()) {
        if (last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doen't matter.
            _tablet->set_cumulative_layer_point(last_delete_version.first + 1);
            return Status::NotFound("cumulative compaction no suitable version error.");
        }

        // we did not meet any delete version. which means compaction_score is not enough to do cumulative compaction.
        // We should wait until there are more rowsets to come, and keep the cumulative point unchanged.
        // But in order to avoid the stall of compaction because no new rowset arrives later, we should increase
        // the cumulative point after waiting for a long time, to ensure that the base compaction can continue.

        // check both last success time of base and cumulative compaction
        int64_t now = UnixMillis();
        int64_t last_cumu = _tablet->last_cumu_compaction_success_time();
        int64_t last_base = _tablet->last_base_compaction_success_time();
        if (last_cumu != 0 || last_base != 0) {
            int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation * 1000;
            int64_t cumu_interval = now - last_cumu;
            int64_t base_interval = now - last_base;
            if (cumu_interval > interval_threshold && base_interval > interval_threshold) {
                // before increasing cumulative point, we should make sure all rowsets are non-overlapping.
                // if at least one rowset is overlapping, we should compact them first.
                CHECK(candidate_rowsets.size() == transient_rowsets.size())
                        << "tablet: " << _tablet->full_name() << ", " << candidate_rowsets.size() << " vs. "
                        << transient_rowsets.size();
                for (auto& rs : candidate_rowsets) {
                    if (rs->rowset_meta()->is_segments_overlapping()) {
                        _input_rowsets = candidate_rowsets;
                        return Status::OK();
                    }
                }

                // all candicate rowsets are non-overlapping, increase the cumulative point
                _tablet->set_cumulative_layer_point(candidate_rowsets.back()->start_version() + 1);
            }
        } else {
            // init the compaction success time for first time
            if (last_cumu == 0) {
                _tablet->set_last_cumu_compaction_success_time(now);
            }

            if (last_base == 0) {
                _tablet->set_last_base_compaction_success_time(now);
            }
        }

        return Status::NotFound("cumulative compaction no suitable version error.");
    }

    return Status::OK();
}

Status CumulativeCompaction::check_version_continuity_with_cumulative_point(
        const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return Status::OK();
    }

    auto start_version = rowsets.front()->start_version();
    auto cumulative_point = _tablet->cumulative_layer_point();

    if (start_version == cumulative_point) return Status::OK();

    LOG(WARNING) << "candidate rowsets misses version behind cumulative point, which may be fixed "
                    "by clone later. cumulative point: "
                 << cumulative_point << " rowset start version: " << start_version
                 << " rowset end version: " << rowsets.back()->end_version();
    return Status::InternalError("cumulative compaction miss version error.");
}

} // namespace starrocks::vectorized

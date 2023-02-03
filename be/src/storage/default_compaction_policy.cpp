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

#include "storage/default_compaction_policy.h"

#include "runtime/current_thread.h"
#include "storage/compaction_task_factory.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

bool DefaultCumulativeBaseCompactionPolicy::need_compaction(double* score, CompactionType* type) {
    if (_tablet->tablet_state() != TABLET_RUNNING) {
        return false;
    }

    _tablet->calculate_cumulative_point();
    auto cumu_st = _pick_rowsets_to_cumulative_compact(&_cumulative_rowsets, &_cumulative_score);
    auto base_st = _pick_rowsets_to_base_compact(&_base_rowsets, &_base_score);

    if (cumu_st.ok() && base_st.ok()) {
        if (_cumulative_score >= _base_score) {
            _compaction_type = CUMULATIVE_COMPACTION;
            *score = _cumulative_score;
        } else {
            _compaction_type = BASE_COMPACTION;
            *score = _base_score;
        }
    } else if (cumu_st.ok()) {
        _compaction_type = CUMULATIVE_COMPACTION;
        *score = _cumulative_score;
    } else if (base_st.ok()) {
        _compaction_type = BASE_COMPACTION;
        *score = _base_score;
    } else {
        _compaction_type = INVALID_COMPACTION;
        *score = 0;
    }
    *type = _compaction_type;

    return _compaction_type != INVALID_COMPACTION;
}

std::shared_ptr<CompactionTask> DefaultCumulativeBaseCompactionPolicy::create_compaction(TabletSharedPtr tablet) {
    if (_compaction_type == CUMULATIVE_COMPACTION) {
        Version output_version;
        output_version.first = (*_cumulative_rowsets.begin())->start_version();
        output_version.second = (*_cumulative_rowsets.rbegin())->end_version();

        CompactionTaskFactory factory(output_version, tablet, std::move(_cumulative_rowsets), _cumulative_score,
                                      _compaction_type);
        std::shared_ptr<CompactionTask> compaction_task = factory.create_compaction_task();
        _compaction_type = INVALID_COMPACTION;
        return compaction_task;
    } else if (_compaction_type == BASE_COMPACTION) {
        Version output_version;
        output_version.first = (*_base_rowsets.begin())->start_version();
        output_version.second = (*_base_rowsets.rbegin())->end_version();

        CompactionTaskFactory factory(output_version, tablet, std::move(_base_rowsets), _base_score, _compaction_type);
        std::shared_ptr<CompactionTask> compaction_task = factory.create_compaction_task();
        _compaction_type = INVALID_COMPACTION;
        return compaction_task;
    } else {
        return nullptr;
    }
}

bool DefaultCumulativeBaseCompactionPolicy::_fit_compaction_condition(const std::vector<RowsetSharedPtr>& rowsets,
                                                                      double compaction_score) {
    if (!rowsets.empty()) {
        if (compaction_score >= config::min_cumulative_compaction_num_singleton_deltas) {
            return true;
        }
        if (rowsets.front()->start_version() == _tablet->cumulative_layer_point()) {
            return true;
        }
    }

    return false;
}

Status DefaultCumulativeBaseCompactionPolicy::_pick_rowsets_to_cumulative_compact(
        std::vector<RowsetSharedPtr>* input_rowsets, double* score) {
    input_rowsets->clear();
    *score = 0;
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(&candidate_rowsets);

    if (candidate_rowsets.empty()) {
        return Status::NotFound("cumulative compaction no suitable version error.");
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    RETURN_IF_ERROR(_check_version_overlapping(candidate_rowsets));

    std::vector<RowsetSharedPtr> transient_rowsets;
    size_t compaction_score = 0;
    // the last delete version we meet when traversing candidate_rowsets
    Version last_delete_version{-1, -1};

    // | means cumulative point
    // 6 means version 6 is singleton version
    // 1-2 means version 1-2 is non singleton version
    // (5) means version 5 is delete version
    // <4,5> means version 4,5 selected for cumulative compaction
    int64_t prev_end_version = _tablet->cumulative_layer_point() - 1;
    bool only_cal_score = false;
    for (auto rowset : candidate_rowsets) {
        if (only_cal_score) {
            compaction_score += rowset->rowset_meta()->get_compaction_score();
            continue;
        }
        // meet missed version
        if (rowset->start_version() != prev_end_version + 1) {
            if (!transient_rowsets.empty() &&
                compaction_score >= config::min_cumulative_compaction_num_singleton_deltas) {
                // min_cumulative_compaction_num_singleton_deltas = 2
                // 7,6,<4,3>|2-1 -> <4,3>
                *input_rowsets = transient_rowsets;
                only_cal_score = true;
                continue;
            } else {
                // 7,6,<3>|2-1
                compaction_score = 0;
                transient_rowsets.clear();
            }
        }
        // meet a delete version
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            last_delete_version = rowset->version();
            if (_fit_compaction_condition(transient_rowsets, compaction_score)) {
                // 7,6,(5),<4,3>|2-1 -> <4,3>
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                *input_rowsets = transient_rowsets;
                only_cal_score = true;
                continue;
            } else {
                // 7,6,5,4,(3)|2-1 -> 7,6,5,4|(3),2-1
                // we meet a delete version, and there were no other versions before.
                // we can increase the cumulative point when delete version next to cumulative_layer_point
                // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doen't matter.
                if (_tablet->cumulative_layer_point() == last_delete_version.first) {
                    _tablet->set_cumulative_layer_point(last_delete_version.first + 1);
                }
                compaction_score = 0;
                transient_rowsets.clear();
                prev_end_version = rowset->end_version();
                continue;
            }
        }
        // meet already compaction rowset
        if (!rowset->rowset_meta()->is_singleton_delta()) {
            // 6-5,<4,3>|2-1 -> <4,3>
            if (_fit_compaction_condition(transient_rowsets, compaction_score)) {
                *input_rowsets = transient_rowsets;
                only_cal_score = true;
                continue;
            } else {
                // 6,5,4-3|2-1 -> 6,5|4-3,2-1
                if (_tablet->cumulative_layer_point() == rowset->start_version()) {
                    _tablet->set_cumulative_layer_point(rowset->end_version() + 1);
                }
                // 9-8,<7>|2-1
                compaction_score = 0;
                transient_rowsets.clear();
                prev_end_version = rowset->end_version();
                continue;
            }
        }

        if (compaction_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            // got enough segments
            *input_rowsets = transient_rowsets;
            break;
        }

        compaction_score += rowset->rowset_meta()->get_compaction_score();
        transient_rowsets.push_back(rowset);
        prev_end_version = rowset->end_version();
    }

    *score = compaction_score;
    if (input_rowsets->empty() && compaction_score >= config::min_cumulative_compaction_num_singleton_deltas) {
        *input_rowsets = transient_rowsets;
    }

    // Cumulative compaction will process with at least 1 rowset.
    // So when there is no rowset being chosen, we should return Status::NotFound("cumulative compaction no suitable version error.");
    if (input_rowsets->empty()) {
        // check both last success time of base and cumulative compaction
        int64_t now = UnixMillis();
        int64_t last_cumu = _tablet->last_cumu_compaction_success_time();
        int64_t last_base = _tablet->last_base_compaction_success_time();
        if (last_cumu != 0 || last_base != 0) {
            int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation * 1000;
            int64_t cumu_interval = now - last_cumu;
            int64_t base_interval = now - last_base;
            if (cumu_interval > interval_threshold && base_interval > interval_threshold) {
                if (_check_version_continuity_with_cumulative_point(candidate_rowsets).ok() &&
                    _check_version_continuity(candidate_rowsets).ok()) {
                    // before increasing cumulative point, we should make sure all rowsets are non-overlapping.
                    // if at least one rowset is overlapping, we should compact them first.
                    for (auto& rs : candidate_rowsets) {
                        if (rs->rowset_meta()->is_segments_overlapping()) {
                            *input_rowsets = candidate_rowsets;
                            return Status::OK();
                        }
                    }

                    // all candicate rowsets are non-overlapping, increase the cumulative point
                    _tablet->set_cumulative_layer_point(candidate_rowsets.back()->start_version() + 1);
                }
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

        input_rowsets->clear();
        return Status::NotFound("cumulative compaction no suitable version error.");
    }

    RETURN_IF_ERROR(_check_version_continuity(*input_rowsets));

    LOG(INFO) << "pick tablet " << _tablet->tablet_id()
              << " cumulative compaction rowset version=" << input_rowsets->front()->start_version() << "-"
              << input_rowsets->back()->end_version() << " score=" << compaction_score;

    return Status::OK();
}

Status DefaultCumulativeBaseCompactionPolicy::_check_version_continuity_with_cumulative_point(
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

Status DefaultCumulativeBaseCompactionPolicy::_pick_rowsets_to_base_compact(std::vector<RowsetSharedPtr>* input_rowsets,
                                                                            double* score) {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    input_rowsets->clear();
    *score = 0;
    _tablet->pick_candicate_rowsets_to_base_compaction(&candidate_rowsets);
    if (candidate_rowsets.size() <= 1) {
        return Status::NotFound("base compaction no suitable version error.");
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    RETURN_IF_ERROR(_check_version_continuity(candidate_rowsets));
    RETURN_IF_ERROR(_check_rowset_overlapping(candidate_rowsets));

    if (candidate_rowsets.size() == 2 && candidate_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return Status::NotFound("base compaction no suitable version error.");
    }

    std::vector<RowsetSharedPtr> transient_rowsets;
    double compaction_score = 0;

    for (auto& rowset : candidate_rowsets) {
        if (compaction_score <= config::max_base_compaction_num_singleton_deltas) {
            transient_rowsets.push_back(rowset);
        }
        compaction_score += rowset->rowset_meta()->get_compaction_score();
    }

    if (!transient_rowsets.empty()) {
        *input_rowsets = transient_rowsets;
    }

    // TODO(meegoo): compaction policy still in optimize, need more test
    // 2. the ratio between base rowset and all input cumulative rowsets reachs the threshold
    int64_t base_size = 0;
    int64_t cumulative_total_size = 0;
    for (auto& rowset : *input_rowsets) {
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
        // expand compaction score when cumulative_base_ratio reach limit
        compaction_score = (1 + cumulative_base_ratio / base_cumulative_delta_ratio) * compaction_score;
    }

    // 3. the interval since last base compaction reachs the threshold
    int64_t base_creation_time = (*input_rowsets)[0]->creation_time();
    int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_base_compaction = time(nullptr) - base_creation_time;
    if (interval_since_last_base_compaction > interval_threshold) {
        // set compaction_score to tablet_max_versions means highest priority to compact
        compaction_score += config::tablet_max_versions;
    }

    if (compaction_score >= config::min_base_compaction_num_singleton_deltas) {
        *score = compaction_score;
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->tablet_id()
                  << ", cumulative_total_size=" << cumulative_total_size << ", base_size=" << base_size
                  << ", cumulative_base_ratio=" << cumulative_base_ratio
                  << ", policy_ratio=" << base_cumulative_delta_ratio
                  << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction
                  << ", interval_threshold=" << interval_threshold
                  << ", num_cumulative_rowsets=" << input_rowsets->size() - 1
                  << ", min_base_compaction_num_singleton_deltas=" << config::min_base_compaction_num_singleton_deltas
                  << ", score=" << compaction_score;
        return Status::OK();
    }

    VLOG(1) << "don't satisfy the base compaction policy. tablet=" << _tablet->tablet_id()
            << ", num_cumulative_rowsets=" << input_rowsets->size() - 1
            << ", cumulative_base_ratio=" << cumulative_base_ratio
            << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction
            << ", score=" << compaction_score;
    input_rowsets->clear();
    return Status::NotFound("base compaction no suitable version error.");
}

Status DefaultCumulativeBaseCompactionPolicy::_check_rowset_overlapping(const std::vector<RowsetSharedPtr>& rowsets) {
    for (auto& rs : rowsets) {
        if (rs->rowset_meta()->is_segments_overlapping()) {
            return Status::InternalError("base compaction segments overlapping error.");
        }
    }
    return Status::OK();
}

Status DefaultCumulativeBaseCompactionPolicy::_check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        const RowsetSharedPtr& rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "Tablet " << _tablet->tablet_id() << " has missed versions among rowsets. "
                         << "prev_rowset version=" << prev_rowset->start_version() << "-" << prev_rowset->end_version()
                         << ", rowset version=" << rowset->start_version() << "-" << rowset->end_version();
            return Status::InternalError("cumulative compaction miss version error.");
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

Status DefaultCumulativeBaseCompactionPolicy::_check_version_overlapping(const std::vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        const RowsetSharedPtr& rowset = rowsets[i];
        if (rowset->start_version() <= prev_rowset->end_version()) {
            LOG(WARNING) << "Tablet " << _tablet->tablet_id() << " has overlapping versions among rowsets. "
                         << "prev_rowset version=" << prev_rowset->start_version() << "-" << prev_rowset->end_version()
                         << ", rowset version=" << rowset->start_version() << "-" << rowset->end_version();
            return Status::InternalError("cumulative compaction overlapping version error.");
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

} // namespace starrocks

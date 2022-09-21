// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "storage/base_and_cumulative_compaction_policy.h"

#include <sstream>
#include <vector>

#include "common/config.h"
#include "storage/compaction_context.h"
#include "storage/compaction_task.h"
#include "storage/compaction_task_factory.h"
#include "storage/rowset/rowset.h"
#include "util/time.h"

namespace starrocks {

// should calculate the compaction score of each type
bool BaseAndCumulativeCompactionPolicy::need_compaction() {
    _compaction_context->cumulative_score = _get_cumulative_compaction_score();
    _compaction_context->base_score = _get_base_compaction_score();

    VLOG(2) << "need_compaction compaction context:" << _compaction_context->to_string();
    // for max_compaction_score is double type, use 0.999 instead of 1
    return _compaction_context->cumulative_score > COMPACTION_SCORE_THRESHOLD ||
           _compaction_context->base_score > COMPACTION_SCORE_THRESHOLD;
}

// // create CompactionTask for chosen_compaction_type in _compaction_context
std::shared_ptr<CompactionTask> BaseAndCumulativeCompactionPolicy::create_compaction() {
    // return nullptr if can not find enough rowsets
    VLOG(2) << "compaction context:" << _compaction_context->to_string();
    if (_compaction_context->chosen_compaction_type == CUMULATIVE_COMPACTION) {
        return _create_cumulative_compaction();
    } else if (_compaction_context->chosen_compaction_type == BASE_COMPACTION) {
        return _create_base_compaction();
    } else {
        LOG(WARNING) << "invalid compaction type:" << _compaction_context->chosen_compaction_type
                     << ", tablet:" << _compaction_context->tablet->tablet_id();
    }
    return nullptr;
}

// the first rowset in level-0 may be compacted already, and the creation_time may be larger than
// the rowsets behind. when pick level-0 rowsets, the first compacted rowset should be picked no matter
// whether the creation time is older enough.
bool BaseAndCumulativeCompactionPolicy::_is_rowset_creation_time_ordered(
        const std::set<Rowset*, RowsetComparator>& cumulative_rowsets) {
    if (cumulative_rowsets.size() <= 1) {
        return true;
    }
    // the compacted rowset can only be the first one, so just compare the first two rowsets
    auto iter = cumulative_rowsets.begin();
    Rowset* first_rowset = *iter;
    Rowset* second_rowset = *(++iter);
    return first_rowset->creation_time() <= second_rowset->creation_time();
}

void BaseAndCumulativeCompactionPolicy::_pick_cumulative_rowsets(bool* has_delete_version,
                                                                 size_t* rowsets_compaction_score,
                                                                 std::vector<RowsetSharedPtr>* rowsets) {
    if (_compaction_context->rowset_levels[0].size() == 0) {
        return;
    }
    int index = 0;
    for (auto rowset : _compaction_context->rowset_levels[0]) {
        if (_compaction_context->tablet->version_for_delete_predicate(rowset->version())) {
            *has_delete_version = true;
            break;
        }
<<<<<<< HEAD
        // For level-0, should consider the rowset creation time.
        // newly-created rowsets should be skipped.
        if ((is_creation_time_ordered || (!is_creation_time_ordered && index != 0)) &&
            rowset->creation_time() + config::cumulative_compaction_skip_window_seconds > now) {
            // rowset in rowset_levels is ordered
            LOG(INFO) << "rowset:" << rowset->rowset_id() << ", version:" << rowset->version()
                      << " is newly created. creation time:" << rowset->creation_time()
                      << ", threshold:" << config::cumulative_compaction_skip_window_seconds
                      << ", rowset overlapping:" << rowset->rowset_meta()->segments_overlap() << ", index:" << index
                      << ", is_creation_time_ordered:" << is_creation_time_ordered;
            break;
        }
=======
>>>>>>> 22af10574 ([Refactor] Remove useless config cumulative_compaction_skip_window_seconds (#11490))
        rowsets->emplace_back(rowset->shared_from_this());
        *rowsets_compaction_score += rowset->rowset_meta()->get_compaction_score();
        if (*rowsets_compaction_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            LOG(INFO) << "cumulative compaction rowsets_compaction_score:" << *rowsets_compaction_score
                      << " is larger than config:" << config::max_cumulative_compaction_num_singleton_deltas
                      << ", cumulative rowset size:" << _compaction_context->rowset_levels[0].size();
            break;
        }
        ++index;
    }
}

Status BaseAndCumulativeCompactionPolicy::_check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return Status::OK();
    }
    for (int i = 0; i < rowsets.size() - 1; ++i) {
        auto& current_rowset = rowsets[i];
        auto& next_rowset = rowsets[i + 1];
        if (next_rowset->start_version() != current_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                         << "current_rowset version=" << current_rowset->start_version() << "-"
                         << current_rowset->end_version() << ", rowset version=" << next_rowset->start_version() << "-"
                         << next_rowset->end_version();
            return Status::InternalError("level compaction miss versions error.");
        }
    }

    return Status::OK();
}

std::shared_ptr<CompactionTask> BaseAndCumulativeCompactionPolicy::_create_cumulative_compaction() {
    if (_compaction_context->rowset_levels[0].size() == 0) {
        LOG(WARNING) << "no cumulative rowsets to create compaction task.";
        return nullptr;
    }
    // decide which rowsets to compaction
    // TODO: 需要考虑加锁
    std::vector<RowsetSharedPtr> input_rowsets;
    bool has_delete_version = false;
    size_t rowsets_compaction_score = 0;
    _pick_cumulative_rowsets(&has_delete_version, &rowsets_compaction_score, &input_rowsets);
    if (!has_delete_version && rowsets_compaction_score < config::min_cumulative_compaction_num_singleton_deltas) {
        // There is no enough qualified rowsets, just skip compaction by return nullptr.
        // If has_delete_version is true, input_rowsets.size() will be large than 0, because
        // the leading rowsets can not be 'delete' rowset, which is guaranteed in Tablet::get_compaction_context
        LOG(INFO) << "rowsets_compaction_score:" << rowsets_compaction_score
                  << " is smaller than threshold:" << config::min_cumulative_compaction_num_singleton_deltas;
        return nullptr;
    }
    DCHECK(input_rowsets.size() > 0) << "input rowsets size can not be empty";

    if (input_rowsets.size() < 1) {
        LOG(INFO) << "no suitable rowsets for cumulative compaction";
        return nullptr;
    }

    auto st = _check_version_continuity(input_rowsets);
    if (!st.ok()) {
        return nullptr;
    }

    Version output_version;
    output_version.first = (*input_rowsets.begin())->start_version();
    output_version.second = (*input_rowsets.rbegin())->end_version();

    CompactionTaskFactory factory(output_version, _compaction_context->tablet, std::move(input_rowsets),
                                  _compaction_context->cumulative_score, CUMULATIVE_COMPACTION);
    std::shared_ptr<CompactionTask> compaction_task = factory.create_compaction_task();
    return compaction_task;
}

void BaseAndCumulativeCompactionPolicy::_pick_base_rowsets(std::vector<RowsetSharedPtr>* rowsets) {
    uint32_t input_rows_num = 0;
    size_t input_size = 0;
    size_t rowsets_compaction_score = 0;
    // add the base rowset to input_rowsets
    Rowset* base_rowset = *_compaction_context->rowset_levels[2].begin();
    if (base_rowset == nullptr) {
        return;
    }
    rowsets->push_back(base_rowset->shared_from_this());
    rowsets_compaction_score += base_rowset->rowset_meta()->get_compaction_score();
    input_rows_num += base_rowset->num_rows();
    input_size += base_rowset->data_disk_size();
    // add level-1 rowsets
    for (auto rowset : _compaction_context->rowset_levels[1]) {
        rowsets_compaction_score += rowset->rowset_meta()->get_compaction_score();
        if (rowsets_compaction_score >= config::max_base_compaction_num_singleton_deltas) {
            LOG(INFO) << "base compaction rowsets_compaction_score:" << rowsets_compaction_score
                      << " is larger than config:" << config::max_base_compaction_num_singleton_deltas
                      << ", base rowset size:"
                      << _compaction_context->rowset_levels[1].size() + _compaction_context->rowset_levels[2].size();
            break;
        }

        rowsets->push_back(rowset->shared_from_this());
        input_rows_num += rowset->num_rows();
        input_size += rowset->data_disk_size();
    }
}

std::shared_ptr<CompactionTask> BaseAndCumulativeCompactionPolicy::_create_base_compaction() {
    std::vector<RowsetSharedPtr> input_rowsets;
    _pick_base_rowsets(&input_rowsets);
    if (input_rowsets.size() <= 1) {
        LOG(INFO) << "no suitable version for compaction. tablet_id: :" << _compaction_context->tablet->tablet_id();
        return nullptr;
    }

    if (input_rowsets.size() == 2 && input_rowsets[0]->empty()) {
        // the tablet is with rowset: [0-x], [x+1-y], and [0-x] is empty,
        // no need to do base compaction.
        LOG(INFO) << "only two rowset. one is [0-x] and it is empty. do not compact. tablet:"
                  << _compaction_context->tablet->tablet_id();
        return nullptr;
    }
    DCHECK(input_rowsets.size() > 0) << "input rowsets size can not be empty";
    auto st = _check_version_continuity(input_rowsets);
    if (!st.ok()) {
        return nullptr;
    }

    Version output_version;
    output_version.first = (*input_rowsets.begin())->start_version();
    output_version.second = (*input_rowsets.rbegin())->end_version();

    CompactionTaskFactory factory(output_version, _compaction_context->tablet, std::move(input_rowsets),
                                  _compaction_context->base_score, BASE_COMPACTION);
    std::shared_ptr<CompactionTask> compaction_task = factory.create_compaction_task();
    return compaction_task;
}

double BaseAndCumulativeCompactionPolicy::_get_cumulative_compaction_score() {
    uint32_t segment_num_score = 0;
    size_t rowsets_size = 0;
    for (auto& rowset : _compaction_context->rowset_levels[0]) {
        segment_num_score += rowset->rowset_meta()->get_compaction_score();
        rowsets_size += rowset->rowset_meta()->total_disk_size();
    }

    double num_score = static_cast<double>(segment_num_score) / config::min_cumulative_compaction_num_singleton_deltas;
    double size_score = static_cast<double>(rowsets_size) / config::min_cumulative_compaction_size;
    double score = std::max(num_score, size_score);
    VLOG(2) << "tablet:" << _compaction_context->tablet->tablet_id() << ", cumulative compaction score:" << score
            << ", size_score:" << size_score << ", num_score:" << num_score << ", rowsets_size:" << rowsets_size;
    return score;
}

double BaseAndCumulativeCompactionPolicy::_get_base_compaction_score() {
    uint32_t segment_num_score = 0;
    size_t level_1_rowsets_size = 0;
    for (auto& rowset : _compaction_context->rowset_levels[1]) {
        segment_num_score += rowset->rowset_meta()->get_compaction_score();
        level_1_rowsets_size += rowset->data_disk_size();
    }

    double num_score = static_cast<double>(segment_num_score) / config::min_base_compaction_num_singleton_deltas;
    double size_score = static_cast<double>(level_1_rowsets_size) / config::min_base_compaction_size;

    double score = std::max(num_score, size_score);
    VLOG(2) << "tablet:" << _compaction_context->tablet->tablet_id() << ", base compaction score:" << score
            << ", size_score:" << size_score << ", num_score:" << num_score
            << ", segment_num_score:" << segment_num_score << ", level_1_rowsets_size:" << level_1_rowsets_size;
    if (score > COMPACTION_SCORE_THRESHOLD) {
        return score;
    }
    if (_compaction_context->rowset_levels[2].size() > 0) {
        DCHECK(_compaction_context->rowset_levels[2].size() == 1)
                << "invalid rowset size. " << _compaction_context->rowset_levels[2].size();
        Rowset* base_rowset = *_compaction_context->rowset_levels[2].begin();
        if (base_rowset->data_disk_size() > 0) {
            double size_ratio = static_cast<double>(level_1_rowsets_size) / base_rowset->data_disk_size();
            if (size_ratio >= config::base_cumulative_delta_ratio) {
                score = 1.0;
                LOG(INFO) << "satisfy the base compaction size ratio policy. tablet="
                          << _compaction_context->tablet->tablet_id()
                          << ", base disk size:" << base_rowset->data_disk_size()
                          << ", base rowsets num:" << _compaction_context->rowset_levels[1].size()
                          << ", base rowsets size:" << level_1_rowsets_size << ", size_ratio:" << size_ratio;
                return score;
            }
        }

        int64_t base_creation_time = base_rowset->creation_time();
        int64_t interval_since_last_base_compaction = time(nullptr) - base_creation_time;
        if (interval_since_last_base_compaction > config::base_compaction_interval_seconds_since_last_operation &&
            (_compaction_context->rowset_levels[1].size() > 1 ||
             (_compaction_context->rowset_levels[1].size() == 1 && !base_rowset->empty()))) {
            // the tablet is with rowsets: [0-x], [x+1-y], and [0-x] is empty.
            // in this situation, no need to do base compaction.
            LOG(INFO) << "satisfy the base compaction time policy. tablet=" << _compaction_context->tablet->tablet_id()
                      << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction
                      << ", interval_threshold=" << config::base_compaction_interval_seconds_since_last_operation
                      << ", base rowsets num:" << _compaction_context->rowset_levels[1].size()
                      << ", base rowsets size:" << level_1_rowsets_size;
            score = 1.0;
            return score;
        }
    }

    return score;
}

} // namespace starrocks

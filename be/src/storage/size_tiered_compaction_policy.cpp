// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/size_tiered_compaction_policy.h"

#include <cstdint>

#include "runtime/current_thread.h"
#include "storage/compaction_task_factory.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks::vectorized {

SizeTieredCompactionPolicy::SizeTieredCompactionPolicy(Tablet* tablet) : _tablet(tablet) {
    _compaction_type = INVALID_COMPACTION;
    if (_tablet->keys_type() == KeysType::DUP_KEYS) {
        _level_multiple = std::max(config::size_tiered_level_multiple_dupkey, config::size_tiered_level_multiple);
    } else {
        _level_multiple = config::size_tiered_level_multiple;
    }

    _max_level_size = config::size_tiered_min_level_size * pow(_level_multiple, config::size_tiered_level_num);
}

bool SizeTieredCompactionPolicy::need_compaction(double* score, CompactionType* type) {
    if (_tablet->tablet_state() != TABLET_RUNNING) {
        return false;
    }

    bool force_base_compaction = false;
    if (type && *type == BASE_COMPACTION) {
        force_base_compaction = true;
    }
    auto st = _pick_rowsets_to_size_tiered_compact(force_base_compaction, &_rowsets, &_score);

    if (st.ok()) {
        if (_rowsets[0]->start_version() == 0) {
            _compaction_type = BASE_COMPACTION;
        } else {
            _compaction_type = CUMULATIVE_COMPACTION;
        }
        *score = _score;
    } else {
        _compaction_type = INVALID_COMPACTION;
        *score = 0;
    }
    *type = _compaction_type;

    return _compaction_type != INVALID_COMPACTION;
}

std::shared_ptr<CompactionTask> SizeTieredCompactionPolicy::create_compaction(TabletSharedPtr tablet) {
    if (_compaction_type != INVALID_COMPACTION) {
        Version output_version;
        output_version.first = (*_rowsets.begin())->start_version();
        output_version.second = (*_rowsets.rbegin())->end_version();

        CompactionTaskFactory factory(output_version, tablet, std::move(_rowsets), _score, _compaction_type);
        std::shared_ptr<CompactionTask> compaction_task = factory.create_compaction_task();
        _compaction_type = INVALID_COMPACTION;
        return compaction_task;
    }
    return nullptr;
}

double SizeTieredCompactionPolicy::_cal_compaction_score(int64_t segment_num, int64_t level_size, int64_t total_size,
                                                         KeysType keys_type, bool reached_max_version) {
    // base score is segment num
    double score = segment_num;

    // data bonus
    if (keys_type == KeysType::DUP_KEYS) {
        // duplicate keys only has write amplification, so that we use more aggressive size-tiered strategy
        score += ((double)(total_size - level_size) / level_size) * 2;
    } else {
        // agg/unique key also has read amplification, segment num occupies a greater weight
        score += (segment_num - 1) * 2 + ((double)(total_size - level_size) / level_size);
    }
    // Normalized score, max data bouns limit to triple size_tiered_level_multiple
    score = std::min((double)config::size_tiered_level_multiple * 3 + segment_num, score);

    // level bonus: The lower the level means the smaller the data volume of the compaction, the higher the execution priority
    int64_t level_bonus = 0;
    for (int64_t v = level_size; v < _max_level_size && level_bonus <= 7; ++level_bonus) {
        v = v * _level_multiple;
    }
    score += level_bonus;

    // version limit bonus: The version num of the tablet is about to exceed the limit, we let it perform compaction faster and reduce the version num
    if (reached_max_version) {
        score *= 2;
    }

    return score;
}

Status SizeTieredCompactionPolicy::_pick_rowsets_to_size_tiered_compact(bool force_base_compaction,
                                                                        std::vector<RowsetSharedPtr>* input_rowsets,
                                                                        double* score) {
    input_rowsets->clear();
    *score = 0;
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_all_candicate_rowsets(&candidate_rowsets);

    if (candidate_rowsets.size() <= 1) {
        return Status::NotFound("compaction no suitable version error.");
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);

    if (!force_base_compaction && candidate_rowsets.size() == 2 && candidate_rowsets[0]->end_version() == 1 &&
        candidate_rowsets[1]->rowset_meta()->get_compaction_score() <= 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return Status::NotFound("compaction no suitable version error.");
    }

    if (time(nullptr) - candidate_rowsets[0]->creation_time() >
        config::base_compaction_interval_seconds_since_last_operation) {
        force_base_compaction = true;
    }

    // too many delete version will incur read overhead
    if (_tablet->delete_predicates().size() >= config::tablet_max_versions / 10) {
        force_base_compaction = true;
    }

    struct SizeTieredLevel {
        SizeTieredLevel(std::vector<RowsetSharedPtr> r, int64_t s, int64_t l, int64_t t, double sc)
                : rowsets(std::move(r)), segment_num(s), level_size(l), total_size(t), score(sc) {}

        std::vector<RowsetSharedPtr> rowsets;
        int64_t segment_num;
        int64_t level_size;
        int64_t total_size;
        double score;
    };

    struct LevelComparator {
        bool operator()(const SizeTieredLevel* left, const SizeTieredLevel* right) const {
            return left->score > right->score ||
                   (left->score == right->score &&
                    left->rowsets[0]->start_version() > right->rowsets[0]->start_version());
        }
    };

    std::vector<std::unique_ptr<SizeTieredLevel>> order_levels;
    std::set<SizeTieredLevel*, LevelComparator> priority_levels;
    std::vector<RowsetSharedPtr> transient_rowsets;
    size_t segment_num = 0;
    auto keys_type = _tablet->keys_type();
    auto min_compaction_segment_num = std::max(
<<<<<<< HEAD
            static_cast<int64_t>(2),
            std::min(config::min_cumulative_compaction_num_singleton_deltas, config::size_tiered_level_multiple));
=======
            static_cast<int64_t>(2), std::min(config::min_cumulative_compaction_num_singleton_deltas, _level_multiple));
>>>>>>> 2.5.18
    // make sure compact to one nonoverlapping segment
    if (force_base_compaction) {
        min_compaction_segment_num = 2;
    }

    bool reached_max_version = false;
    if (candidate_rowsets.size() > config::tablet_max_versions / 10 * 9) {
        reached_max_version = true;
    }

    int64_t level_size = -1;
    int64_t total_size = 0;
    int64_t prev_end_version = -1;
    bool skip_dup_large_base_rowset = true;
    for (auto rowset : candidate_rowsets) {
        // when duplicate key's base rowset larger than 0.8 * max_segment_file_size, we don't need compact it
        // if set force_base_compaction, we will compact it to make sure delete version can be compacted
        if (keys_type == KeysType::DUP_KEYS && skip_dup_large_base_rowset && !force_base_compaction &&
            !rowset->rowset_meta()->is_segments_overlapping() &&
            rowset->data_disk_size() > config::max_segment_file_size * 0.8) {
            continue;
        } else {
            skip_dup_large_base_rowset = false;
        }

        int64_t rowset_size = rowset->data_disk_size() > 0 ? rowset->data_disk_size() : 1;
        if (level_size == -1) {
            level_size = rowset_size < _max_level_size ? rowset_size : _max_level_size;
            total_size = 0;
        }

        // meet missed version
        if (rowset->start_version() != prev_end_version + 1) {
            if (!transient_rowsets.empty()) {
                auto level = std::make_unique<SizeTieredLevel>(
                        transient_rowsets, segment_num, level_size, total_size,
                        _cal_compaction_score(segment_num, level_size, total_size, keys_type, reached_max_version));
                priority_levels.emplace(level.get());
                order_levels.emplace_back(std::move(level));
            }
            level_size = rowset_size < _max_level_size ? rowset_size : _max_level_size;
            segment_num = 0;
            total_size = 0;
            transient_rowsets.clear();
        }

        if (_tablet->version_for_delete_predicate(rowset->version())) {
            // meet a delete version
            // base compaction can handle delete condition
            if (!transient_rowsets.empty() && transient_rowsets[0]->start_version() == 0) {
            } else {
                // while upper level segment num less min_compaction_segment_num, we can merge into one level
                int64_t upper_level = order_levels.size() - 1;
                while (upper_level >= 0) {
                    if ((order_levels[upper_level]->segment_num < min_compaction_segment_num ||
                         order_levels[upper_level]->rowsets.front()->start_version() == 0) &&
                        transient_rowsets.front()->start_version() ==
                                order_levels[upper_level]->rowsets.back()->end_version() + 1) {
                        transient_rowsets.insert(transient_rowsets.begin(), order_levels[upper_level]->rowsets.begin(),
                                                 order_levels[upper_level]->rowsets.end());
                        level_size = std::max(order_levels[upper_level]->level_size, level_size);
                        segment_num += order_levels[upper_level]->segment_num;
                        total_size += order_levels[upper_level]->total_size;
                        priority_levels.erase(order_levels[upper_level].get());
                        upper_level--;
                    } else {
                        break;
                    }
                }
                order_levels.resize(upper_level + 1);

                // after merge, check if we match base compaction condition
                if (!transient_rowsets.empty() && transient_rowsets[0]->start_version() != 0) {
                    auto level = std::make_unique<SizeTieredLevel>(
                            transient_rowsets, segment_num, level_size, total_size,
                            _cal_compaction_score(segment_num, level_size, total_size, keys_type, reached_max_version));
                    priority_levels.emplace(level.get());
                    order_levels.emplace_back(std::move(level));
                }

                if (transient_rowsets.empty() ||
                    (!transient_rowsets.empty() && transient_rowsets[0]->start_version() != 0)) {
                    segment_num = 0;
                    transient_rowsets.clear();
                    level_size = -1;
                    continue;
                }
            }
        } else if ((!force_base_compaction ||
                    (!transient_rowsets.empty() && transient_rowsets[0]->start_version() != 0)) &&
                   level_size > config::size_tiered_min_level_size && rowset_size < level_size &&
<<<<<<< HEAD
                   level_size / rowset_size > (level_multiple - 1)) {
=======
                   level_size / rowset_size > (_level_multiple - 1)) {
>>>>>>> 2.5.18
            if (!transient_rowsets.empty()) {
                auto level = std::make_unique<SizeTieredLevel>(
                        transient_rowsets, segment_num, level_size, total_size,
                        _cal_compaction_score(segment_num, level_size, total_size, keys_type, reached_max_version));
                priority_levels.emplace(level.get());
                order_levels.emplace_back(std::move(level));
            }
            segment_num = 0;
            transient_rowsets.clear();
            level_size = rowset_size < _max_level_size ? rowset_size : _max_level_size;
            total_size = 0;
        }

        segment_num += rowset->rowset_meta()->get_compaction_score();
        total_size += rowset_size;
        transient_rowsets.emplace_back(rowset);
        prev_end_version = rowset->end_version();
    }

    if (!transient_rowsets.empty()) {
        auto level = std::make_unique<SizeTieredLevel>(
                transient_rowsets, segment_num, level_size, total_size,
                _cal_compaction_score(segment_num, level_size, total_size, keys_type, reached_max_version));
        priority_levels.emplace(level.get());
        order_levels.emplace_back(std::move(level));
    }

    for (auto& level : order_levels) {
        *score += level->score;
    }

    SizeTieredLevel* selected_level = nullptr;
    if (!priority_levels.empty()) {
        // We need a minimum number of segments that trigger compaction to
        // avoid triggering compaction too frequently compared to the old version
        // But in the old version of compaction, the user may set a large min_cumulative_compaction_num_singleton_deltas
        // to avoid TOO_MANY_VERSION errors, it is unnecessary in size tiered compaction
        selected_level = *priority_levels.begin();
        if (selected_level->segment_num >= min_compaction_segment_num) {
            *input_rowsets = selected_level->rowsets;
        }
    }

    // Cumulative compaction will process with at least 1 rowset.
    // So when there is no rowset being chosen, we should return Status::NotFound("cumulative compaction no suitable version error.");
    if (input_rowsets->empty()) {
        return Status::NotFound("cumulative compaction no suitable version error.");
    }

    RETURN_IF_ERROR(_check_version_continuity(*input_rowsets));

    LOG(INFO) << "pick tablet " << _tablet->tablet_id()
              << " for size-tiered compaction rowset version=" << input_rowsets->front()->start_version() << "-"
              << input_rowsets->back()->end_version() << " score=" << selected_level->score
              << " level_size=" << selected_level->level_size << " total_size=" << selected_level->total_size
              << " segment_num=" << selected_level->segment_num << " force_base_compaction=" << force_base_compaction
              << " reached_max_versions=" << reached_max_version;

    return Status::OK();
}

Status SizeTieredCompactionPolicy::_check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        const RowsetSharedPtr& rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                         << "prev_rowset version=" << prev_rowset->start_version() << "-" << prev_rowset->end_version()
                         << ", rowset version=" << rowset->start_version() << "-" << rowset->end_version();
            return Status::InternalError("cumulative compaction miss version error.");
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

} // namespace starrocks::vectorized

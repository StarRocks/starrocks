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

#include "storage/size_tiered_compaction_policy.h"

#include "runtime/current_thread.h"
#include "storage/compaction_task_factory.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

SizeTieredCompactionPolicy::SizeTieredCompactionPolicy(Tablet* tablet) : _tablet(tablet) {
    _max_level_size =
            config::size_tiered_min_level_size * pow(config::size_tiered_level_multiple, config::size_tiered_level_num);
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
        score = ((double)(total_size - level_size) / level_size) * 2;
    } else {
        // agg/unique key also has read amplification, segment num occupies a greater weight
        score = (segment_num - 1) * 2 + ((double)(total_size - level_size) / level_size);
    }
    // Normalized score, max data bouns limit to triple size_tiered_level_multiple
    score = std::min((double)config::size_tiered_level_multiple * 3 + segment_num, score);

    // level bonus: The lower the level means the smaller the data volume of the compaction, the higher the execution priority
    int64_t level_bonus = 0;
    for (int64_t v = level_size; v < _max_level_size && level_bonus <= 7; ++level_bonus) {
        v = v * config::size_tiered_level_multiple;
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
    int64_t level_multiple = config::size_tiered_level_multiple;
    auto keys_type = _tablet->keys_type();

    bool reached_max_version = false;
    if (candidate_rowsets.size() > config::tablet_max_versions / 10 * 9) {
        reached_max_version = true;
    }

    int64_t level_size = -1;
    int64_t total_size = 0;
    int64_t prev_end_version = -1;
    for (auto rowset : candidate_rowsets) {
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
                // if upper level only has one rowset, we can merge into one level
                int64_t i = order_levels.size() - 1;
                while (i >= 0) {
                    if (order_levels[i]->rowsets.size() == 1 &&
                        transient_rowsets[0]->start_version() == order_levels[i]->rowsets[0]->end_version() + 1 &&
                        !_tablet->version_for_delete_predicate(order_levels[i]->rowsets[0]->version())) {
                        transient_rowsets.insert(transient_rowsets.begin(), order_levels[i]->rowsets[0]);
                        auto rs = order_levels[i]->rowsets[0]->data_disk_size() > 0
                                          ? order_levels[i]->rowsets[0]->data_disk_size()
                                          : 1;
                        level_size = rs < _max_level_size ? rs : _max_level_size;
                        segment_num += order_levels[i]->segment_num;
                        total_size += level_size;
                        priority_levels.erase(order_levels[i].get());
                        i--;
                    } else {
                        break;
                    }
                }
                order_levels.resize(i + 1);

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
        } else if (!force_base_compaction && level_size > config::size_tiered_min_level_size &&
                   rowset_size < level_size && level_size / rowset_size > (level_multiple - 1)) {
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
        selected_level = *priority_levels.begin();
        if (selected_level->rowsets.size() > 1) {
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

} // namespace starrocks

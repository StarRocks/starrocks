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

#include "storage/lake/compaction_policy.h"

#include "common/config.h"
#include "gutil/strings/join.h"
#include "runtime/exec_env.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class BaseAndCumulativeCompactionPolicy : public CompactionPolicy {
public:
    explicit BaseAndCumulativeCompactionPolicy(TabletManager* tablet_mgr,
                                               std::shared_ptr<const TabletMetadataPB> tablet_metadata)
            : CompactionPolicy(tablet_mgr, std::move(tablet_metadata)) {}

    ~BaseAndCumulativeCompactionPolicy() override = default;

    StatusOr<std::vector<RowsetPtr>> pick_rowsets() override;

private:
    StatusOr<std::vector<RowsetPtr>> pick_cumulative_rowsets();
    StatusOr<std::vector<RowsetPtr>> pick_base_rowsets();
    void debug_rowsets(CompactionType type, const std::vector<RowsetPtr>& input_rowset_ids);
};

struct SizeTieredLevel {
    SizeTieredLevel(std::vector<int> r, int64_t s, int64_t l, int64_t t, double sc)
            : rowsets(std::move(r)), segment_num(s), level_size(l), total_size(t), score(sc) {}

    std::vector<int> rowsets;
    int64_t segment_num;
    int64_t level_size;
    int64_t total_size;
    double score;
};

class SizeTieredCompactionPolicy : public CompactionPolicy {
public:
    explicit SizeTieredCompactionPolicy(TabletManager* tablet_mgr,
                                        std::shared_ptr<const TabletMetadataPB> tablet_metadata)
            : CompactionPolicy(tablet_mgr, std::move(tablet_metadata)),
              _max_level_size(config::size_tiered_min_level_size *
                              pow(config::size_tiered_level_multiple, config::size_tiered_level_num)) {}

    ~SizeTieredCompactionPolicy() override = default;

    StatusOr<std::vector<RowsetPtr>> pick_rowsets() override;

    static StatusOr<std::unique_ptr<SizeTieredLevel>> pick_max_level(const TabletMetadataPB& metadata);

private:
    static double cal_compaction_score(int64_t segment_num, int64_t level_size, int64_t total_size,
                                       int64_t max_level_size, KeysType keys_type, bool reached_max_version);

    struct LevelReverseOrderComparator {
        bool operator()(const SizeTieredLevel* left, const SizeTieredLevel* right) const {
            return left->score > right->score || (left->score == right->score && left->rowsets[0] > right->rowsets[0]);
        }
    };

    int64_t _max_level_size;
};

struct RowsetStat {
    size_t num_rows = 0;
    size_t num_dels = 0;
    size_t bytes = 0;
};

class RowsetCandidate {
public:
    RowsetCandidate(const RowsetMetadataPB* rp, const RowsetStat& rs, int index)
            : rowset_meta_ptr(rp), stat(rs), rowset_index(index) {
        calculate_score();
    }
    // The goal of lake primary table compaction is to reduce the overhead of reading data.
    // So the first thing we need to do is quantify the overhead of reading the data.
    // In object storage, we can use this to define overhead:
    //
    // OverHead (score) = IO count / Read bytes
    //
    // Same bytes, if we use more io to fetch it, that means more overhead.
    // And in one rowset, the IO count is equal overlapped segment count plus their delvec files.
    double io_count() const {
        // rowset_meta_ptr->segments_size() could be zero here, so make sure this >= 1 using max.
        double cnt = rowset_meta_ptr->overlapped() ? std::max(rowset_meta_ptr->segments_size(), 1) : 1;
        if (stat.num_dels > 0) {
            // if delvec file exist, that means we need to read segment files and delvec files both
            // And update_compaction_delvec_file_io_ratio control the io amp ratio of delvec files, default is 2.
            // Bigger update_compaction_delvec_file_io_amp_ratio means high priority about merge rowset with delvec files.
            cnt *= config::update_compaction_delvec_file_io_amp_ratio;
        }
        return cnt;
    }
    double delete_bytes() const {
        if (stat.num_rows == 0) return 0.0;
        if (stat.num_dels >= stat.num_rows) return (double)stat.bytes;
        return (double)stat.bytes * ((double)stat.num_dels / (double)stat.num_rows);
    }
    double read_bytes() const { return (double)stat.bytes - delete_bytes() + 1; }
    void calculate_score() { score = (io_count() * 1024 * 1024) / read_bytes(); }
    bool operator<(const RowsetCandidate& other) const { return score < other.score; }

    const RowsetMetadataPB* rowset_meta_ptr;
    RowsetStat stat;
    int rowset_index;
    double score;
};

class PrimaryCompactionPolicy : public CompactionPolicy {
public:
    explicit PrimaryCompactionPolicy(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> tablet_metadata)
            : CompactionPolicy(tablet_mgr, std::move(tablet_metadata)) {}

    ~PrimaryCompactionPolicy() override = default;

    StatusOr<std::vector<RowsetPtr>> pick_rowsets() override;
    StatusOr<std::vector<RowsetPtr>> pick_rowsets(const std::shared_ptr<const TabletMetadataPB>& tablet_metadata,
                                                  std::vector<bool>* has_dels);

private:
    int64_t _get_data_size(const std::shared_ptr<const TabletMetadataPB>& tablet_metadata) {
        int size = 0;
        for (const auto& rowset : tablet_metadata->rowsets()) {
            size += rowset.data_size();
        }
        return size;
    }
};

StatusOr<std::vector<RowsetPtr>> PrimaryCompactionPolicy::pick_rowsets() {
    return pick_rowsets(_tablet_metadata, nullptr);
}

StatusOr<std::vector<RowsetPtr>> PrimaryCompactionPolicy::pick_rowsets(
        const std::shared_ptr<const TabletMetadataPB>& tablet_metadata, std::vector<bool>* has_dels) {
    std::vector<RowsetPtr> input_rowsets;
    UpdateManager* mgr = _tablet_mgr->update_mgr();
    std::priority_queue<RowsetCandidate> rowset_queue;
    const auto tablet_id = tablet_metadata->id();
    const auto tablet_version = tablet_metadata->version();
    const int64_t compaction_data_size_threshold =
            static_cast<int64_t>((double)_get_data_size(tablet_metadata) * config::update_compaction_ratio_threshold);
    for (int i = 0, sz = tablet_metadata->rowsets_size(); i < sz; i++) {
        const RowsetMetadataPB& rowset_pb = tablet_metadata->rowsets(i);
        RowsetStat stat;
        stat.num_rows = rowset_pb.num_rows();
        stat.bytes = rowset_pb.data_size();
        if (rowset_pb.has_num_dels()) {
            stat.num_dels = rowset_pb.num_dels();
        } else {
            stat.num_dels = mgr->get_rowset_num_deletes(tablet_id, tablet_version, rowset_pb);
        }
        rowset_queue.emplace(&rowset_pb, stat, i);
    }
    size_t cur_compaction_result_bytes = 0;

    std::stringstream input_infos;
    while (!rowset_queue.empty()) {
        const auto& rowset_candidate = rowset_queue.top();
        cur_compaction_result_bytes += rowset_candidate.read_bytes();
        input_rowsets.emplace_back(
                std::make_shared<Rowset>(_tablet_mgr, tablet_metadata, rowset_candidate.rowset_index));
        if (has_dels != nullptr) {
            has_dels->push_back(rowset_candidate.delete_bytes() > 0);
        }
        input_infos << input_rowsets.back()->id() << "|";

        if (cur_compaction_result_bytes >
                    std::max(config::update_compaction_result_bytes, compaction_data_size_threshold) ||
            input_rowsets.size() >= config::max_update_compaction_num_singleton_deltas) {
            break;
        }
        rowset_queue.pop();
    }
    VLOG(2) << strings::Substitute("lake PrimaryCompactionPolicy pick_rowsets tabletid:$0 version:$1 inputs:$2",
                                   tablet_id, tablet_metadata->version(), input_infos.str());

    return input_rowsets;
}

StatusOr<uint32_t> primary_compaction_score_by_policy(TabletManager* tablet_mgr,
                                                      const std::shared_ptr<const TabletMetadataPB>& metadata) {
    PrimaryCompactionPolicy policy(tablet_mgr, metadata);
    std::vector<bool> has_dels;
    ASSIGN_OR_RETURN(auto pick_rowsets, policy.pick_rowsets(metadata, &has_dels));
    uint32_t segment_num_score = 0;
    for (int i = 0; i < pick_rowsets.size(); i++) {
        const auto& pick_rowset = pick_rowsets[i];
        const bool has_del = has_dels[i];
        auto current_score = pick_rowset->is_overlapped() ? pick_rowset->num_segments() : 1;
        if (has_del) {
            // if delvec file exist, expand score by config.
            current_score *= config::update_compaction_delvec_file_io_amp_ratio;
        }
        segment_num_score += current_score;
    }
    return segment_num_score;
}

double primary_compaction_score(TabletManager* tablet_mgr, const std::shared_ptr<const TabletMetadataPB>& metadata) {
    // calc compaction score by picked rowsets
    auto score_st = primary_compaction_score_by_policy(tablet_mgr, metadata);
    if (!score_st.ok()) {
        // should not happen, return score zero if error
        LOG(ERROR) << "primary_compaction_score by policy fail, tablet_id: " << metadata->id()
                   << ", st: " << score_st.status();
        return 0;
    } else {
        return *score_st;
    }
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_cumulative_rowsets() {
    std::vector<RowsetPtr> input_rowsets;
    uint32_t cumulative_point = _tablet_metadata->cumulative_point();
    uint32_t segment_num_score = 0;
    for (uint32_t i = cumulative_point, size = _tablet_metadata->rowsets_size(); i < size; ++i) {
        const auto& rowset = _tablet_metadata->rowsets(i);
        if (rowset.has_delete_predicate()) {
            if (!input_rowsets.empty()) {
                break;
            } else {
                DCHECK(segment_num_score == 0);
                continue;
            }
        }

        input_rowsets.emplace_back(std::make_shared<Rowset>(_tablet_mgr, _tablet_metadata, i));

        segment_num_score += rowset.overlapped() ? rowset.segments_size() : 1;
        if (segment_num_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            break;
        }
    }
    // TODO: need check min_cumulative_compaction_num_singleton_deltas?

    debug_rowsets(CUMULATIVE_COMPACTION, input_rowsets);

    return input_rowsets;
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_base_rowsets() {
    std::vector<RowsetPtr> input_rowsets;
    uint32_t cumulative_point = _tablet_metadata->cumulative_point();
    uint32_t segment_num_score = 0;
    for (uint32_t i = 0; i < cumulative_point; ++i) {
        input_rowsets.emplace_back(std::make_shared<Rowset>(_tablet_mgr, _tablet_metadata, i));
        if (++segment_num_score >= config::max_base_compaction_num_singleton_deltas) {
            break;
        }
    }

    debug_rowsets(BASE_COMPACTION, input_rowsets);

    return input_rowsets;
}

void BaseAndCumulativeCompactionPolicy::debug_rowsets(CompactionType type,
                                                      const std::vector<RowsetPtr>& input_rowsets) {
    static const int verboselevel = 3;

    if (!VLOG_IS_ON(verboselevel)) {
        return;
    }
    std::vector<uint32_t> rowset_ids;
    std::vector<uint32_t> delete_rowset_ids;
    std::vector<uint32_t> input_rowset_ids;
    for (const auto& rowset : _tablet_metadata->rowsets()) {
        rowset_ids.emplace_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            delete_rowset_ids.emplace_back(rowset.id());
        }
    }
    input_rowset_ids.reserve(input_rowsets.size());
    for (const auto& input_rowset : input_rowsets) {
        input_rowset_ids.emplace_back(input_rowset->id());
    }
    VLOG(verboselevel) << "Pick compaction input rowsets. tablet: " << _tablet_metadata->id()
                       << ", type: " << to_string(type) << ", version: " << _tablet_metadata->version()
                       << ", cumulative point: " << _tablet_metadata->cumulative_point()
                       << ", input rowsets size: " << input_rowset_ids.size() << ", input rowsets: ["
                       << JoinInts(input_rowset_ids, ",") << "]"
                       << ", rowsets: [" << JoinInts(rowset_ids, ",") << "]"
                       << ", delete rowsets: [" << JoinInts(delete_rowset_ids, ",") + "]";
}

double cumulative_compaction_score(const std::shared_ptr<const TabletMetadataPB>& metadata) {
    if (metadata->rowsets_size() == 0) {
        return 0;
    }

    uint32_t segment_num_score = 0;
    for (uint32_t i = metadata->cumulative_point(), size = metadata->rowsets_size(); i < size; ++i) {
        const auto& rowset = metadata->rowsets(i);
        segment_num_score += rowset.overlapped() ? rowset.segments_size() : 1;
    }
    VLOG(2) << "Tablet: " << metadata->id() << ", cumulative compaction score: " << segment_num_score;
    return segment_num_score;
}

double base_compaction_score(const std::shared_ptr<const TabletMetadataPB>& metadata) {
    return metadata->cumulative_point();
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_rowsets() {
    DCHECK(_tablet_metadata != nullptr) << "_tablet_metadata is null";
    double cumulative_score = cumulative_compaction_score(_tablet_metadata);
    double base_score = base_compaction_score(_tablet_metadata);
    if (base_score > cumulative_score) {
        return pick_base_rowsets();
    } else {
        return pick_cumulative_rowsets();
    }
}

double SizeTieredCompactionPolicy::cal_compaction_score(int64_t segment_num, int64_t level_size, int64_t total_size,
                                                        int64_t max_level_size, KeysType keys_type,
                                                        bool reached_max_version) {
    // base score is segment num
    double score = segment_num;

    // data bonus
    double data_bonus = 0;
    if (keys_type == KeysType::DUP_KEYS) {
        // duplicate keys only has write amplification, so that we use more aggressive size-tiered strategy
        data_bonus = ((double)(total_size - level_size) / level_size) * 2;
    } else {
        // agg/unique key also has read amplification, segment num occupies a greater weight
        data_bonus = (segment_num - 1) * 2 + ((double)(total_size - level_size) / level_size);
    }
    // Normalized score, max data bonus limit to triple size_tiered_level_multiple
    data_bonus = std::min((double)config::size_tiered_level_multiple * 3, data_bonus);
    score += data_bonus;

    // level bonus: The lower the level means the smaller the data volume of the compaction,
    // the higher the execution priority
    int64_t level_bonus = 0;
    for (int64_t v = level_size; v < max_level_size && level_bonus <= 7; ++level_bonus) {
        v = v * config::size_tiered_level_multiple;
    }
    score += level_bonus;

    // version limit bonus: The version num of the tablet is about to exceed the limit,
    // we let it perform compaction faster and reduce the version num
    if (reached_max_version) {
        score *= 2;
    }

    return score;
}

StatusOr<std::unique_ptr<SizeTieredLevel>> SizeTieredCompactionPolicy::pick_max_level(
        const TabletMetadataPB& metadata) {
    int64_t max_level_size =
            config::size_tiered_min_level_size * pow(config::size_tiered_level_multiple, config::size_tiered_level_num);
    const auto& rowsets = metadata.rowsets();

    if (rowsets.empty() || (rowsets.size() == 1 && !rowsets[0].overlapped())) {
        return nullptr;
    }

    // too many delete version will incur read overhead
    size_t num_delete_rowsets = 0;
    for (auto& rowset : rowsets) {
        if (rowset.has_delete_predicate()) {
            ++num_delete_rowsets;
        }
    }
    bool force_base_compaction = (num_delete_rowsets >= config::tablet_max_versions / 10);

    // check reach max version
    bool reached_max_version = (rowsets.size() > config::tablet_max_versions / 10 * 9);
    VLOG(3) << "Pick compaction max level. force base compaction: " << force_base_compaction
            << ", reached max version: " << reached_max_version;

    std::vector<std::unique_ptr<SizeTieredLevel>> order_levels;
    std::set<SizeTieredLevel*, LevelReverseOrderComparator> priority_levels;
    // rowset index
    std::vector<int> transient_rowsets;
    size_t segment_num = 0;
    int64_t level_multiple = config::size_tiered_level_multiple;
    auto keys_type = metadata.schema().keys_type();
    auto min_compaction_segment_num =
            std::max<int64_t>(2, std::min(config::min_cumulative_compaction_num_singleton_deltas, level_multiple));
    int64_t level_size = -1;
    int64_t total_size = 0;
    for (int i = 0, size = rowsets.size(); i < size; ++i) {
        const auto& rowset = rowsets[i];
        int64_t rowset_size = rowset.data_size() > 0 ? rowset.data_size() : 1;
        if (level_size == -1) {
            level_size = rowset_size < max_level_size ? rowset_size : max_level_size;
            total_size = 0;
        }

        if (rowset.has_delete_predicate()) {
            // meet a delete version
            // base compaction can handle delete condition
            // 1. the first level that has some data rowsets
            // 2. the first rowset is delete rowset
            if ((!transient_rowsets.empty() && transient_rowsets[0] == 0) || i == 0) {
                // do nothing
            } else {
                // while upper level segment num less min_compaction_segment_num, we can merge into one level
                int64_t upper_level = order_levels.size() - 1;
                while (upper_level >= 0) {
                    if ((order_levels[upper_level]->segment_num < min_compaction_segment_num ||
                         order_levels[upper_level]->rowsets.front() == 0) &&
                        transient_rowsets.front() == order_levels[upper_level]->rowsets.back() + 1) {
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
                if (!transient_rowsets.empty() && transient_rowsets[0] != 0) {
                    auto level = std::make_unique<SizeTieredLevel>(
                            transient_rowsets, segment_num, level_size, total_size,
                            cal_compaction_score(segment_num, level_size, total_size, max_level_size, keys_type,
                                                 reached_max_version));
                    priority_levels.emplace(level.get());
                    order_levels.emplace_back(std::move(level));
                }

                if (transient_rowsets.empty() || transient_rowsets[0] != 0) {
                    segment_num = 0;
                    transient_rowsets.clear();
                    level_size = -1;
                    continue;
                }
            }
        } else if ((!force_base_compaction || (!transient_rowsets.empty() && transient_rowsets[0] != 0)) &&
                   level_size > config::size_tiered_min_level_size && rowset_size < level_size &&
                   level_size / rowset_size > (level_multiple - 1)) {
            if (!transient_rowsets.empty()) {
                auto level = std::make_unique<SizeTieredLevel>(
                        transient_rowsets, segment_num, level_size, total_size,
                        cal_compaction_score(segment_num, level_size, total_size, max_level_size, keys_type,
                                             reached_max_version));
                priority_levels.emplace(level.get());
                order_levels.emplace_back(std::move(level));
            }
            segment_num = 0;
            transient_rowsets.clear();
            level_size = rowset_size < max_level_size ? rowset_size : max_level_size;
            total_size = 0;
        }

        segment_num += rowset.overlapped() ? rowset.segments_size() : 1;
        total_size += rowset_size;
        transient_rowsets.emplace_back(i);
    }

    if (!transient_rowsets.empty()) {
        auto level =
                std::make_unique<SizeTieredLevel>(transient_rowsets, segment_num, level_size, total_size,
                                                  cal_compaction_score(segment_num, level_size, total_size,
                                                                       max_level_size, keys_type, reached_max_version));
        priority_levels.emplace(level.get());
        order_levels.emplace_back(std::move(level));
    }

    if (priority_levels.empty()) {
        return nullptr;
    }

    auto* selected_level = *priority_levels.begin();
    return std::make_unique<SizeTieredLevel>(selected_level->rowsets, selected_level->segment_num,
                                             selected_level->level_size, selected_level->total_size,
                                             selected_level->score);
}

StatusOr<std::vector<RowsetPtr>> SizeTieredCompactionPolicy::pick_rowsets() {
    ASSIGN_OR_RETURN(auto selected_level, pick_max_level(*_tablet_metadata));
    std::vector<RowsetPtr> input_rowsets;
    if (selected_level == nullptr) {
        return input_rowsets;
    }
    int64_t level_multiple = config::size_tiered_level_multiple;
    auto min_compaction_segment_num =
            std::max<int64_t>(2, std::min(config::min_cumulative_compaction_num_singleton_deltas, level_multiple));

    // We need a minimum number of segments that trigger compaction to
    // avoid triggering compaction too frequently compared to the old version
    // But in the old version of compaction, the user may set a large min_cumulative_compaction_num_singleton_deltas
    // to avoid TOO_MANY_VERSION errors, it is unnecessary in size tiered compaction
    if (selected_level->segment_num >= min_compaction_segment_num) {
        int64_t max_segments = config::max_cumulative_compaction_num_singleton_deltas;
        for (auto i : selected_level->rowsets) {
            DCHECK_LT(i, _tablet_metadata->rowsets_size());
            auto rowset = std::make_shared<Rowset>(_tablet_mgr, _tablet_metadata, i);
            max_segments -= rowset->metadata().overlapped() ? rowset->metadata().segments_size() : 1;
            input_rowsets.emplace_back(std::move(rowset));
            if (max_segments <= 0) {
                break;
            }
        }
    }

    const int log_level = 3;
    // debug
    if (!VLOG_IS_ON(log_level)) {
        return input_rowsets;
    }

    std::vector<uint32_t> input_rowset_ids;
    input_rowset_ids.reserve(input_rowsets.size());
    for (const auto& r : input_rowsets) {
        input_rowset_ids.emplace_back(r->id());
    }
    const auto& level_rowsets = selected_level->rowsets;
    auto type = !level_rowsets.empty() && level_rowsets[0] == 0 ? BASE_COMPACTION : CUMULATIVE_COMPACTION;
    VLOG(log_level) << "Pick compaction input rowsets. tablet: " << _tablet_metadata->id()
                    << ", type: " << to_string(type) << ", input rowsets: [" << JoinInts(input_rowset_ids, ",") << "]"
                    << ", input rowsets size: " << input_rowset_ids.size()
                    << ", level rowsets size: " << level_rowsets.size()
                    << ", level segment num: " << selected_level->segment_num
                    << ", level size: " << selected_level->level_size
                    << ", level total size: " << selected_level->total_size
                    << ", level score: " << selected_level->score;
    return input_rowsets;
}

double size_tiered_compaction_score(const std::shared_ptr<const TabletMetadataPB>& metadata) {
    auto selected_level_or = SizeTieredCompactionPolicy::pick_max_level(*metadata);
    if (!selected_level_or.ok()) {
        return 0;
    }
    auto selected_level = std::move(selected_level_or).value();
    if (selected_level == nullptr) {
        return 0;
    }
    return selected_level->segment_num;
}

CompactionPolicy::~CompactionPolicy() = default;

StatusOr<CompactionAlgorithm> CompactionPolicy::choose_compaction_algorithm(const std::vector<RowsetPtr>& rowsets) {
    // TODO: support row source mask buffer based on starlet fs
    // The current row source mask buffer is based on posix tmp file,
    // if there is no storage root path, use horizontal compaction.
    if (ExecEnv::GetInstance()->store_paths().empty()) {
        return HORIZONTAL_COMPACTION;
    }

    size_t total_iterator_num = 0;
    for (auto& rowset : rowsets) {
        ASSIGN_OR_RETURN(auto rowset_iterator_num, rowset->get_read_iterator_num());
        total_iterator_num += rowset_iterator_num;
    }
    size_t num_columns = _tablet_metadata->schema().column_size();
    return CompactionUtils::choose_compaction_algorithm(num_columns, config::vertical_compaction_max_columns_per_group,
                                                        total_iterator_num);
}

StatusOr<CompactionPolicyPtr> CompactionPolicy::create(TabletManager* tablet_mgr,
                                                       std::shared_ptr<const TabletMetadataPB> tablet_metadata) {
    if (tablet_metadata->schema().keys_type() == PRIMARY_KEYS) {
        return std::make_shared<PrimaryCompactionPolicy>(tablet_mgr, std::move(tablet_metadata));
    } else if (config::enable_size_tiered_compaction_strategy) {
        return std::make_shared<SizeTieredCompactionPolicy>(tablet_mgr, std::move(tablet_metadata));
    } else {
        return std::make_shared<BaseAndCumulativeCompactionPolicy>(tablet_mgr, std::move(tablet_metadata));
    }
}

double compaction_score(TabletManager* tablet_mgr, const std::shared_ptr<const TabletMetadataPB>& metadata) {
    if (is_primary_key(*metadata)) {
        return primary_compaction_score(tablet_mgr, metadata);
    }
    if (config::enable_size_tiered_compaction_strategy) {
        return size_tiered_compaction_score(metadata);
    }
    return std::max(base_compaction_score(metadata), cumulative_compaction_score(metadata));
}

} // namespace starrocks::lake

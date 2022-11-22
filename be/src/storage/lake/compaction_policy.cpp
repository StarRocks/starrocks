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
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class BaseAndCumulativeCompactionPolicy : public CompactionPolicy {
public:
    explicit BaseAndCumulativeCompactionPolicy(TabletPtr tablet) : _tablet(std::move(tablet)) {}
    ~BaseAndCumulativeCompactionPolicy() override = default;

    StatusOr<std::vector<RowsetPtr>> pick_rowsets(int64_t version) override;

private:
    StatusOr<std::vector<RowsetPtr>> pick_cumulative_rowsets();
    StatusOr<std::vector<RowsetPtr>> pick_base_rowsets();
    void debug_rowsets(CompactionType type, const std::vector<uint32_t>& input_rowset_ids);

    TabletPtr _tablet;
    TabletMetadataPtr _tablet_metadata;
};

struct RowsetStat {
    double score_per_row = 0.0;
    size_t num_rows = 0;
    size_t num_dels = 0;
    size_t bytes = 0;
};

class PrimaryCompactionPolicy : public CompactionPolicy {
public:
    using RowsetCandidate = std::pair<RowsetMetadataPtr, RowsetStat>;
    explicit PrimaryCompactionPolicy(TabletPtr tablet) : _tablet(std::move(tablet)) {}
    ~PrimaryCompactionPolicy() override = default;

    StatusOr<std::vector<RowsetPtr>> pick_rowsets(int64_t version) override;

private:
    double calc_compaction_score(const RowsetStat& stats);
    static const size_t compaction_result_bytes_threashold = 1000000000;
    static const size_t compaction_result_rows_threashold = 10000000;
    static const int64_t compaction_cost_seek = 32 * 1024 * 1024;

private:
    TabletPtr _tablet;
};

double PrimaryCompactionPolicy::calc_compaction_score(const RowsetStat& stat) {
    if (stat.num_rows < 10) {
        return compaction_cost_seek;
    }
    const int64_t cost_record_write = 1;
    const int64_t cost_record_read = 4;
    // use double to prevent overflow
    auto delete_bytes = (int64_t)(stat.bytes * (double)stat.num_dels / stat.num_rows);
    return compaction_cost_seek + (cost_record_read + cost_record_write) * delete_bytes -
           cost_record_write * stat.bytes;
}

StatusOr<std::vector<RowsetPtr>> PrimaryCompactionPolicy::pick_rowsets(int64_t version) {
    ASSIGN_OR_RETURN(auto tablet_metadata, _tablet->get_metadata(version));

    std::vector<RowsetPtr> input_rowsets;
    // pick high score rowset
    struct RowsetCompare {
        bool operator()(const RowsetCandidate& a, const RowsetCandidate& b) const {
            return a.second.score_per_row < b.second.score_per_row;
        }
    };
    UpdateManager* mgr = _tablet->update_mgr();
    std::priority_queue<RowsetCandidate, std::vector<RowsetCandidate>, RowsetCompare> rowset_queue;
    for (const auto& rowset_pb : tablet_metadata->rowsets()) {
        RowsetStat stat;
        stat.num_rows = rowset_pb.num_rows();
        stat.bytes = rowset_pb.data_size();
        stat.num_dels = mgr->get_rowset_num_deletes(_tablet->id(), version, rowset_pb);
        // calc rowset compaction score, and get per row score
        stat.score_per_row = calc_compaction_score(stat) / (stat.num_rows - stat.num_dels);
        rowset_queue.push(std::make_pair(std::make_shared<RowsetMetadata>(rowset_pb), stat));
    }
    size_t cur_compaction_result_bytes = 0;
    size_t cur_compaction_result_rows = 0;
    int i = 0;
    std::stringstream input_infos;
    while (!rowset_queue.empty()) {
        const auto& rowset_candidate = rowset_queue.top();
        cur_compaction_result_bytes += rowset_candidate.second.bytes;
        cur_compaction_result_rows += rowset_candidate.second.num_rows;
        if (input_rowsets.size() > 0 && ((cur_compaction_result_bytes > compaction_result_bytes_threashold * 3 / 2) ||
                                         (cur_compaction_result_rows > compaction_result_rows_threashold * 3 / 2))) {
            break;
        }
        input_rowsets.emplace_back(std::make_shared<Rowset>(_tablet.get(), std::move(rowset_candidate.first), i++));
        input_infos << input_rowsets.back()->id() << "|";

        if (cur_compaction_result_bytes > compaction_result_bytes_threashold ||
            cur_compaction_result_rows > compaction_result_rows_threashold ||
            input_rowsets.size() >= config::max_update_compaction_num_singleton_deltas) {
            break;
        }
        rowset_queue.pop();
    }
    LOG(INFO) << strings::Substitute("lake PrimaryCompactionPolicy pick_rowsets tabletid:$0 version:$1 inputs:$2",
                                     _tablet->id(), version, input_infos.str());

    return input_rowsets;
}

double cumulative_compaction_score(const TabletMetadataPB& metadata) {
    if (metadata.rowsets_size() == 0) {
        return 0;
    }

    uint32_t segment_num_score = 0;
    for (uint32_t i = metadata.cumulative_point(), size = metadata.rowsets_size(); i < size; ++i) {
        const auto& rowset = metadata.rowsets(i);
        segment_num_score += rowset.overlapped() ? rowset.segments_size() : 1;
    }
    VLOG(2) << "tablet: " << metadata.id() << ", cumulative compaction score: " << segment_num_score;
    return segment_num_score;
}

double base_compaction_score(const TabletMetadataPB& metadata) {
    uint32_t cumulative_point = metadata.cumulative_point();
    if (cumulative_point == 0 || metadata.rowsets_size() == 0) {
        return 0;
    }
    return cumulative_point - 1;
}

double primary_compaction_score(const TabletMetadataPB& metadata) {
    uint32_t segment_num_score = 0;
    for (uint32_t i = 0; i < metadata.rowsets_size(); i++) {
        const auto& rowset = metadata.rowsets(i);
        segment_num_score += rowset.overlapped() ? rowset.segments_size() : 1;
    }
    LOG(INFO) << fmt::format("tablet: {}, primary compaction score: {}", metadata.id(), segment_num_score);
    return segment_num_score;
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_cumulative_rowsets() {
    std::vector<RowsetPtr> input_rowsets;
    std::vector<uint32_t> input_rowset_ids;
    uint32_t cumulative_point = _tablet_metadata->cumulative_point();
    uint32_t segment_num_score = 0;
    for (uint32_t i = cumulative_point, size = _tablet_metadata->rowsets_size(); i < size; ++i) {
        const auto& rowset = _tablet_metadata->rowsets(i);
        if (rowset.has_delete_predicate()) {
            if (!input_rowsets.empty()) {
                break;
            } else {
                DCHECK(input_rowset_ids.empty());
                DCHECK(segment_num_score == 0);
                continue;
            }
        }

        input_rowset_ids.emplace_back(rowset.id());
        auto metadata_ptr = std::make_shared<RowsetMetadata>(rowset);
        input_rowsets.emplace_back(std::make_shared<Rowset>(_tablet.get(), std::move(metadata_ptr), i));

        segment_num_score += rowset.overlapped() ? rowset.segments_size() : 1;
        if (segment_num_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            break;
        }
    }
    // TODO: need check min_cumulative_compaction_num_singleton_deltas?

    debug_rowsets(CUMULATIVE_COMPACTION, input_rowset_ids);

    return input_rowsets;
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_base_rowsets() {
    std::vector<RowsetPtr> input_rowsets;
    std::vector<uint32_t> input_rowset_ids;
    uint32_t cumulative_point = _tablet_metadata->cumulative_point();
    uint32_t segment_num_score = 0;
    for (uint32_t i = 0; i < cumulative_point; ++i) {
        const auto& rowset = _tablet_metadata->rowsets(i);
        DCHECK(!rowset.overlapped());
        input_rowset_ids.emplace_back(rowset.id());
        auto metadata_ptr = std::make_shared<RowsetMetadata>(rowset);
        input_rowsets.emplace_back(std::make_shared<Rowset>(_tablet.get(), std::move(metadata_ptr), i));

        if (++segment_num_score >= config::max_base_compaction_num_singleton_deltas) {
            break;
        }
    }

    debug_rowsets(BASE_COMPACTION, input_rowset_ids);

    return input_rowsets;
}

void BaseAndCumulativeCompactionPolicy::debug_rowsets(CompactionType type,
                                                      const std::vector<uint32_t>& input_rowset_ids) {
    static const int verboselevel = 3;

    if (!VLOG_IS_ON(verboselevel)) {
        return;
    }
    std::vector<uint32_t> rowset_ids;
    std::vector<uint32_t> delete_rowset_ids;
    for (const auto& rowset : _tablet_metadata->rowsets()) {
        rowset_ids.emplace_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            delete_rowset_ids.emplace_back(rowset.id());
        }
    }
    VLOG(verboselevel) << "pick compaction input rowsets. tablet: " << _tablet->id() << ", type: " << to_string(type)
                       << ", version: " << _tablet_metadata->version()
                       << ", cumulative point: " << _tablet_metadata->cumulative_point()
                       << ", input rowsets size: " << input_rowset_ids.size() << ", input rowsets: ["
                       << JoinInts(input_rowset_ids, ",") + "]"
                       << ", rowsets: [" << JoinInts(rowset_ids, ",") << "]"
                       << ", delete rowsets: [" << JoinInts(delete_rowset_ids, ",") + "]";
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_rowsets(int64_t version) {
    ASSIGN_OR_RETURN(_tablet_metadata, _tablet->get_metadata(version));

    double cumulative_score = cumulative_compaction_score(*_tablet_metadata);
    double base_score = base_compaction_score(*_tablet_metadata);
    if (base_score > cumulative_score) {
        return pick_base_rowsets();
    } else {
        return pick_cumulative_rowsets();
    }
}

StatusOr<CompactionPolicyPtr> CompactionPolicy::create_compaction_policy(TabletPtr tablet) {
    ASSIGN_OR_RETURN(auto tablet_schema, tablet->get_schema());
    if (tablet_schema->keys_type() == PRIMARY_KEYS) {
        return std::make_shared<PrimaryCompactionPolicy>(std::move(tablet));
    }
    return std::make_shared<BaseAndCumulativeCompactionPolicy>(std::move(tablet));
}

} // namespace starrocks::lake

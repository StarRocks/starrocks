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

#include <queue>
#include <vector>

#include "common/config.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/compaction_policy.h"

namespace starrocks::lake {

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
    //
    // Special case: For non-overlapped rowsets that are already large enough
    // (>= lake_compaction_max_rowset_size), they are already well-compacted
    // and should have zero compaction priority. This prevents them from being
    // selected for compaction when they don't need it.
    double io_count() const {
        int64_t large_rowset_threshold = config::lake_compaction_max_rowset_size;

        // For non-overlapped rowsets that are already large enough, return 0
        // to indicate they don't need compaction. The only exception is if they have deletes,
        // in which case we still want to consider compacting them to reclaim space.
        if (!rowset_meta_ptr->overlapped() && stat.num_dels == 0) {
            int64_t rowset_size = static_cast<int64_t>(rowset_meta_ptr->data_size());
            if (rowset_size >= large_rowset_threshold) {
                // Already a large, well-compacted rowset with no deletes - zero priority
                return 0;
            }
        }

        double cnt = 1;
        if (rowset_meta_ptr->overlapped()) {
            int segments_size = rowset_meta_ptr->segments_size();
            if (segments_size == 0) {
                cnt = 1;
            } else if (rowset_meta_ptr->segment_size_size() == 0) {
                // No segment_size info, fall back to counting all segments
                cnt = segments_size;
            } else {
                // Count only segments smaller than the large segment threshold
                int effective_count = 0;
                for (int i = 0; i < rowset_meta_ptr->segment_size_size(); i++) {
                    if (static_cast<int64_t>(rowset_meta_ptr->segment_size(i)) < large_rowset_threshold) {
                        effective_count++;
                    }
                }
                cnt = std::max(1, effective_count);
            }
        }
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
    // Rowset has multi segments and these segments are overlapped
    bool multi_segment_with_overlapped() const {
        return rowset_meta_ptr->overlapped() && rowset_meta_ptr->segments_size() > 1;
    }
    bool operator<(const RowsetCandidate& other) const { return score < other.score; }

    const RowsetMetadataPB* rowset_meta_ptr;
    RowsetStat stat;
    int rowset_index;
    double score;
};

struct PKSizeTieredLevel {
    PKSizeTieredLevel(const std::vector<RowsetCandidate>& rs, int64_t compact_level)
            : rowsets(rs.begin(), rs.end()), compact_level(compact_level) {
        calc_compaction_score(rs);
    }
    PKSizeTieredLevel(const PKSizeTieredLevel& level)
            : rowsets(level.rowsets), score(level.score), compact_level(level.compact_level) {}

    // caculate the score of this level.
    void calc_compaction_score(const std::vector<RowsetCandidate>& rs) {
        std::stringstream debug_ss;
        for (const auto& rowset : rs) {
            score += rowset.score;
            debug_ss << "[Rowset: " << rowset.rowset_meta_ptr->id() << " Size: " << rowset.rowset_meta_ptr->data_size()
                     << " Rows: " << rowset.rowset_meta_ptr->num_rows()
                     << " Dels: " << rowset.rowset_meta_ptr->num_dels() << " Score: " << rowset.score << "] ";
        }
        VLOG(2) << "PKSizeTieredLevel " << debug_ss.str();
    }

    // Merge another level's rowset
    void merge_level(PKSizeTieredLevel& other) {
        while (!other.rowsets.empty()) {
            const auto& top_rowset = other.rowsets.top();
            rowsets.push(top_rowset);
            score += top_rowset.score;
            other.rowsets.pop();
        }
    }

    // Add other level's rowsets.
    void add_other_level_rowsets(PKSizeTieredLevel& other) {
        while (!other.rowsets.empty()) {
            const auto& top_rowset = other.rowsets.top();
            other_level_rowsets.push_back(top_rowset);
            other.rowsets.pop();
        }
    }

    int64_t get_compact_level() { return compact_level; }

    bool operator<(const PKSizeTieredLevel& other) const { return score < other.score; }

    std::priority_queue<RowsetCandidate> rowsets;
    std::vector<RowsetCandidate> other_level_rowsets;
    double score = 0.0;
    int64_t compact_level = 0;
};

class PrimaryCompactionPolicy : public CompactionPolicy {
public:
    explicit PrimaryCompactionPolicy(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> tablet_metadata,
                                     bool force_base_compaction)
            : CompactionPolicy(tablet_mgr, std::move(tablet_metadata), force_base_compaction) {}

    ~PrimaryCompactionPolicy() override = default;

    StatusOr<std::vector<RowsetPtr>> pick_rowsets() override;
    StatusOr<std::vector<RowsetPtr>> pick_rowsets(const std::shared_ptr<const TabletMetadataPB>& tablet_metadata,
                                                  std::vector<bool>* has_dels);

    // Common function to return the picked rowset indexes.
    // For compaction score, only picked rowset indexes are needed.
    // For compaction, picked rowsets can be constructed by picked rowset indexes.
    StatusOr<std::vector<int64_t>> pick_rowset_indexes(const std::shared_ptr<const TabletMetadataPB>& tablet_metadata,
                                                       std::vector<bool>* has_dels);

    // When using Sized-tiered compaction policy, we need this function to pick highest score level.
    static StatusOr<std::unique_ptr<PKSizeTieredLevel>> pick_max_level(std::vector<RowsetCandidate>& rowsets);

private:
    int64_t _get_data_size(const std::shared_ptr<const TabletMetadataPB>& tablet_metadata);
};

} // namespace starrocks::lake
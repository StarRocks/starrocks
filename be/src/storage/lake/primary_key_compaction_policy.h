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

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/compaction_policy.h"

namespace starrocks::lake {

// SDCG background convergence trigger.
//
// A column-mode partial update appends a sparse `.spcols` overlay layer (or an inline patch) on top
// of the base segment it touches; reading a column then walks the whole overlay chain. To keep that
// chain bounded WITHOUT a synchronous in-place rewrite on the publish critical path (the p95 spike we
// removed in the phase-1 commit), we let normal background lake PK compaction converge it: compaction
// reads each input rowset THROUGH the delta-column-group overlay (see Rowset::dcg_loader), emits a
// fresh dense segment, and apply_opcompaction drops the old DCG entries + orphans the `.spcols` files.
// All that is missing is a SCORE/selection signal so compaction actually fires on a deep chain -- this
// constant set provides it.
//
// A chain of depth D adds ~D extra file reads per row access, i.e. read amplification comparable to D
// extra overlapped segments, so we model each layer as SCORE_PER_LAYER units of compaction score. With
// SCORE_PER_LAYER == 1.0 a depth-TRIGGER chain contributes exactly TRIGGER to the tablet compaction
// score; TRIGGER == 10 is chosen to coincide with the FE default lake_compaction_score_selector_min_score
// (10.0) so a single over-deep chain crosses the FE scheduling threshold on its own. This is far below
// the synchronous safety valve (config::sdcg_promotion_hard_count, 256), so in steady state chains
// converge in the background around depth ~10-15 and the safety valve effectively never fires.
inline constexpr int64_t SDCG_COMPACTION_TRIGGER_DEPTH = 10;
inline constexpr double SDCG_COMPACTION_SCORE_PER_LAYER = 1.0;
// A sparse chain whose max per-layer value bytes/row is at least this is treated as WIDE and folded at
// the (much lower) config::sdcg_compaction_trigger_depth_wide instead of SDCG_COMPACTION_TRIGGER_DEPTH:
// a wide value column is re-decoded per layer per read (~30ms/layer, measured), so its read-amp justifies
// folding to ~depth-1, whereas a narrow chain is cheap to read deep and must not churn compaction.
inline constexpr double SDCG_WIDE_BYTES_PER_ROW = 512.0;

// Compaction-score contribution of a sparse overlay chain of |chain_depth| with effective fold
// |trigger_depth| (narrow = SDCG_COMPACTION_TRIGGER_DEPTH; wide = the lower wide trigger). Returns 0
// below the trigger (shallow chains are cheap and must not perturb scheduling). At/above it the score is
// RENORMALIZED by (SDCG_COMPACTION_TRIGGER_DEPTH / trigger_depth) so a chain exactly AT a lowered (wide)
// trigger contributes the same baseline (== SDCG_COMPACTION_TRIGGER_DEPTH units) a narrow chain does at
// the default trigger -- i.e. a single over-deep WIDE chain still crosses the FE min_score on its own.
inline double sdcg_chain_score_contribution(int64_t chain_depth, int64_t trigger_depth) {
    if (trigger_depth < 1) {
        trigger_depth = 1;
    }
    if (chain_depth < trigger_depth) {
        return 0.0;
    }
    const double per_layer = SDCG_COMPACTION_SCORE_PER_LAYER *
                             (static_cast<double>(SDCG_COMPACTION_TRIGGER_DEPTH) / static_cast<double>(trigger_depth));
    return static_cast<double>(chain_depth) * per_layer;
}

// Measure the deepest SDCG sparse-overlay chain across all segments of |rowset|, using the
// delta-column-group metadata |dcg_meta| from the same tablet metadata. The depth of one segment's
// chain is the number of SPARSE_PERCOL `.spcols` files plus inline patches recorded for that segment's
// rssid; DENSE_COLS entries do not count (a dense layer is row-complete and supersedes older sparse
// layers of its columns). Unlike inspect_existing_sparse_chain() on the write path -- which is scoped
// to a single column batch -- this is column-agnostic: compaction rewrites the whole segment, so the
// total layer count is the read-amplification measure that matters. Returns 0 for rowsets with no DCG.
int64_t max_sparse_chain_depth_for_rowset(const RowsetMetadataPB& rowset, const DeltaColumnGroupMetadataPB& dcg_meta);

// Effective fold trigger depth for |rowset|'s sparse overlay chain: config::sdcg_compaction_trigger_depth_wide
// when any SPARSE_PERCOL layer is WIDE (max per-layer value bytes/row >= SDCG_WIDE_BYTES_PER_ROW),
// else SDCG_COMPACTION_TRIGGER_DEPTH. Perf-only (drives background fold selection/score, never data).
int64_t sdcg_effective_trigger_depth_for_rowset(const RowsetMetadataPB& rowset,
                                                const DeltaColumnGroupMetadataPB& dcg_meta);

struct RowsetStat {
    size_t num_rows = 0;
    size_t num_dels = 0;
    size_t bytes = 0;
};

class RowsetCandidate {
public:
    RowsetCandidate(const RowsetMetadataPB* rp, const RowsetStat& rs, int index, int64_t chain_depth = 0,
                    int64_t chain_trigger = SDCG_COMPACTION_TRIGGER_DEPTH)
            : rowset_meta_ptr(rp),
              stat(rs),
              rowset_index(index),
              sparse_chain_depth(chain_depth),
              sparse_chain_trigger(chain_trigger) {
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
    double io_count() const;
    double delete_bytes() const {
        if (stat.num_rows == 0) return 0.0;
        if (stat.num_dels >= stat.num_rows) return (double)stat.bytes;
        return (double)stat.bytes * ((double)stat.num_dels / (double)stat.num_rows);
    }
    double read_bytes() const { return (double)stat.bytes - delete_bytes() + 1; }
    void calculate_score() { score = (io_count() * 1024 * 1024) / read_bytes(); }
    // Rowset has multi segments and these segments are overlapped
    bool multi_segment_with_overlapped() const {
        return rowset_meta_ptr->overlapped() && rowset_meta_ptr->segment_metas_size() > 1;
    }
    bool operator<(const RowsetCandidate& other) const { return score < other.score; }

    const RowsetMetadataPB* rowset_meta_ptr;
    RowsetStat stat;
    int rowset_index;
    // Deepest SDCG sparse-overlay chain across this rowset's segments (0 if none / non-SDCG tablet).
    // Folded into io_count() so an over-deep chain raises the rowset's compaction priority.
    int64_t sparse_chain_depth = 0;
    // Effective fold trigger for this rowset's chain (wide-aware); paired with sparse_chain_depth in
    // io_count()'s score so a wide chain reaches FE min_score at its lower trigger.
    int64_t sparse_chain_trigger = SDCG_COMPACTION_TRIGGER_DEPTH;
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

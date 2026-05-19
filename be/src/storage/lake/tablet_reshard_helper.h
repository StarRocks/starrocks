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

#include <cstdint>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {
class TabletManager;
} // namespace starrocks::lake

namespace starrocks::lake::tablet_reshard_helper {

void set_all_data_files_shared(RowsetMetadataPB* rowset_metadata);
void set_all_data_files_shared(TxnLogPB* txn_log);
void set_all_data_files_shared(TabletMetadataPB* tablet_metadata, bool skip_delvecs = false);

StatusOr<TabletRangePB> intersect_range(const TabletRangePB& lhs_pb, const TabletRangePB& rhs_pb);
StatusOr<TabletRangePB> union_range(const TabletRangePB& lhs_pb, const TabletRangePB& rhs_pb);
Status update_rowset_range(RowsetMetadataPB* rowset, const TabletRangePB& range);
Status update_rowset_ranges(TxnLogPB* txn_log, const TabletRangePB& range);

// True iff |lhs ∪ rhs| has no gap. Touching at a boundary counts as
// contiguous iff at least one side is included at the touching point.
// Same-direction unbounded sides always extend without forming a gap.
// Empty input ranges return false.
bool ranges_are_contiguous(const TabletRangePB& lhs_pb, const TabletRangePB& rhs_pb);

// Sort+merge a list of ranges into a disjoint, ascending-by-lower list. Adjacent
// (touching with compatible included flags) and overlapping intervals are
// merged. Empty inputs are dropped.
StatusOr<std::vector<TabletRangePB>> sort_and_merge_adjacent_ranges(std::vector<TabletRangePB> ranges);

// Complement of |sorted_disjoint_children| within |bound|. The children list
// must already be sorted ascending by lower bound and pairwise disjoint
// (callers can use sort_and_merge_adjacent_ranges first). Children outside
// |bound| are clipped. Returns empty list iff children fully cover |bound|.
StatusOr<std::vector<TabletRangePB>> compute_disjoint_gaps_within(
        const TabletRangePB& bound, const std::vector<TabletRangePB>& sorted_disjoint_children);

// Complement of |sorted_disjoint_children| over an unbounded (-∞, +∞) universe.
// Equivalent to compute_disjoint_gaps_within with an unbounded bound. Used when
// callers need to mask everything not covered by contributors, including keys
// outside any specific bounded range (e.g., PK-index sstable indexes from a
// pre-split tablet that extend beyond the merged tablet range).
StatusOr<std::vector<TabletRangePB>> compute_non_contributed_ranges(
        const std::vector<TabletRangePB>& sorted_disjoint_children);

// Mirrors Rowset::get_seek_range() fallback chain:
//   rowset.range if has_range, else ctx_metadata.range if has_range,
//   else an unbounded singleton owned by this function.
const TabletRangePB& effective_child_local_range(const RowsetMetadataPB& rowset, const TabletMetadataPB& ctx_metadata);

void update_rowset_data_stats(RowsetMetadataPB* rowset, int32_t split_count, int32_t split_index);
void update_txn_log_data_stats(TxnLogPB* txn_log, int32_t split_count, int32_t split_index);

// Collect full storage paths of output-side files produced by compaction in
// |txn_log|, resolved under |txn_log.tablet_id()| (the tablet where the
// compaction writer emitted them).
//
// Used by MERGING cross-publish: a compaction txn committed on a source tablet
// may be published after the merge, and its output files were written under
// the source tablet's directory. Dropping the compaction leaves those files
// unreferenced; the caller should pass the returned paths to delete_files_async.
//
// Only output-side files are collected. Input rowsets/sstables are deliberately
// excluded — they have been absorbed into the merged tablet by the preceding
// merge publish and remain live; deleting them would corrupt data.
//
// No dedup is performed — lake file names are UUID-based per tablet/txn, so
// collisions are not expected in practice. Matches the no-dedup style of
// transactions.cpp::collect_files_in_log.
std::vector<std::string> collect_compaction_output_file_paths(const TxnLogPB& txn_log, TabletManager* tablet_manager);

// Allocate `total` across `out->size()` buckets in proportion to
// weights[i] / Σweights using the largest-remainder (Hare-Niemeyer) method,
// preserving Σ result == total exactly.
//
// Tie-break for equal fractional remainders is deterministic:
// (remainder desc, index asc) — reproducible across machines and runs.
//
// Preconditions (DCHECK in debug):
//   - out != nullptr
//   - total >= 0
//   - weights.size() == out->size() and >= 1
//   - all weights[i] >= 0
//   - `weights` and `*out` must NOT alias
//
// Postcondition (DCHECK in debug):
//   - Σ (*out)[i] == total
//
// Degenerate inputs:
//   - total == 0: all zeros.
//   - total > 0 and Σweights == 0: deterministic uniform fallback —
//     floor(total/N) to each, +1 to the lowest indices to absorb the
//     remainder. Preserves Σ exactly.
//
// Used by per-rowset stat anchoring during tablet split (see
// `tablet_splitter.cpp`'s `apply_rowset_anchor`). The future
// cross-publish (P2) refactor of `update_txn_log_data_stats` is expected
// to reuse this helper for sibling-wide range-aware allocation.
void allocate_proportionally(int64_t total, const std::vector<int64_t>& weights, std::vector<int64_t>* out);

// Given per-bucket row counts and a pre-allocated per-bucket num_dels vector,
// cap each (*dels)[i] to rows[i] and redistribute the overflow to buckets
// with headroom (rows[i] - dels[i] > 0) using (headroom desc, index asc) as
// the deterministic redistribution order.
//
// Contract:
//   If Σ dels (pre-cap) ≤ Σ rows, post-call Σ dels == Σ dels (pre-cap)
//   exactly and (*dels)[i] ≤ rows[i] for every i.
//   If Σ dels (pre-cap) > Σ rows (caller failed to clamp), post-call
//   Σ dels == Σ rows (the maximum feasible) and per-bucket cap still holds.
//
// Preconditions (DCHECK in debug):
//   - dels != nullptr
//   - rows.size() == dels->size()
//   - rows[i] >= 0 for all i
//   - (*dels)[i] >= 0 for all i
void cap_and_redistribute_dels(const std::vector<int64_t>& rows, std::vector<int64_t>* dels);

} // namespace starrocks::lake::tablet_reshard_helper

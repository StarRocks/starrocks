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

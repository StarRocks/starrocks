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

} // namespace starrocks::lake::tablet_reshard_helper

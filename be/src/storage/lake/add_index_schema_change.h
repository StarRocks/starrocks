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

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment.pb.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "storage/delta_column_group.h"
#include "storage/lake/versioned_tablet.h"

namespace starrocks {
class Segment;
class TabletColumn;
class TabletIndex;
class TabletSchema;
class ThreadPool;
class WritableFile;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;
class IndexFileWriter;

// Orchestrates the ADD INDEX fast-path for a single lake tablet alter job.
//
// Given a base tablet + a set of new indexes, this class walks every
// rowset/segment of the base tablet's visible version, builds one .idx file
// per segment containing all new index blobs, and fills an OpAddIndex TxnLog
// whose SegmentEntries reference the .idx files by relative path.
//
// Per-segment work is submitted to SegmentTaskRunner (which runs on the
// dedicated lake_schema_change pool). Errors propagate through the runner's
// fail-fast mechanism; on failure the half-written .idx files become orphans
// reachable through the txn abort path.
//
// Currently supported index types: BITMAP. Other types (NGRAMBF / GIN) return
// Status::NotSupported and should be added incrementally; the framework is
// type-agnostic and only build_bitmap_for_column is bitmap-specific today.
class AddIndexSchemaChange {
public:
    AddIndexSchemaChange(TabletManager* tablet_mgr, int64_t txn_id, VersionedTablet base_tablet,
                         VersionedTablet new_tablet, std::vector<TabletIndexPB> indexes_to_build, int64_t alter_version,
                         ThreadPool* lake_schema_change_pool = nullptr);

    ~AddIndexSchemaChange();

    AddIndexSchemaChange(const AddIndexSchemaChange&) = delete;
    AddIndexSchemaChange& operator=(const AddIndexSchemaChange&) = delete;

    // Walk rowsets x segments of base_tablet, build a .idx per segment in
    // parallel, and populate `op_add_index`. Thread-safe; returns the first
    // error collected.
    Status run(TxnLogPB_OpAddIndex* op_add_index);

private:
    // Build one .idx for (rowset, seg_idx_in_rowset). Opens the Segment via
    // TabletManager::load_segment, opens one column iterator per index to
    // build, dispatches to the per-index-type builder, finalizes the
    // IndexFileWriter, and fills the caller-supplied IDG entry.
    //
    // `indexes` is the subset of _indexes_to_build whose column is served
    // from the BASE segment on this rssid. Columns overlaid by a Delta
    // Column Group are excluded and handled by rewrite_dcg_for_segment()
    // instead — a `.idx` keyed by the base segment describes base values,
    // which is not what a query reads for an overlaid column.
    Status build_idg_for_segment(const RowsetMetadataPB& rowset_meta, uint32_t seg_idx_in_rowset, uint32_t rssid,
                                 const std::vector<TabletIndexPB>& indexes, IndexDeltaGroupEntryPB* out_entry);

    // Rewrite the DCG-overlaid columns of one segment into a NEW `.cols`
    // that carries the just-added index inlined in its footer, and describe
    // it in `out_entry` so publish can append it as a newer DCG layer.
    //
    // Values are read from the currently effective overlay, so the rewrite is
    // value-preserving: only the physical file changes, gaining an index.
    // This is what makes ADD INDEX complete for a table that took a
    // column-mode partial update BEFORE the alter — the pre-existing `.cols`
    // was written when the column had no index and carries none, and the
    // base `.idx` is (correctly) ignored for an overlaid column.
    Status rewrite_dcg_for_segment(const RowsetMetadataPB& rowset_meta, uint32_t seg_idx_in_rowset, uint32_t rssid,
                                   const std::vector<TabletIndexPB>& indexes, TxnLogPB_OpAddIndex_DcgEntry* out_entry);

    // Split _indexes_to_build for one segment into the columns served from
    // the base segment and the columns overlaid by a DCG at _alter_version.
    // `base_out` / `dcg_out` are cleared first. A tablet with no DCG at all
    // short-circuits to "everything is base".
    Status classify_indexes_for_segment(uint32_t rssid, std::vector<TabletIndexPB>* base_out,
                                        std::vector<TabletIndexPB>* dcg_out);

    // Build the write schema for the rewritten `.cols`: the overlaid columns
    // only, with has_bitmap_index / is_bf_column set and the new TabletIndexPB
    // present in table_indices, so SegmentWriter inlines the index. The flags
    // cannot be read off the tablet schema here: apply_add_index() sets them
    // at publish time, which is strictly after this code runs.
    StatusOr<std::shared_ptr<TabletSchema>> build_dcg_write_schema(const std::vector<TabletIndexPB>& indexes);

    // Build a BITMAP index for `column` of an already-opened Segment and
    // write its blob into `target_wfile`. The resulting ColumnIndexMetaPB is
    // returned via `out_meta`; the caller is expected to register it with the
    // surrounding IndexFileWriter.
    Status build_bitmap_for_column(Segment* segment, const TabletColumn& column, WritableFile* target_wfile,
                                   ColumnIndexMetaPB* out_meta);

    // Build a BLOOM_FILTER / NGRAMBF index for `column`. The two share the
    // same builder class (BloomFilterIndexWriter) differing only in the
    // BloomFilterOptions.use_ngram / gram_num flags. `index_type` must be
    // either BITMAP is rejected and handled by build_bitmap_for_column;
    // this path handles the bloom family.
    Status build_bloom_for_column(Segment* segment, const TabletColumn& column, IndexType index_type,
                                  const TabletIndexPB& ix, WritableFile* target_wfile, ColumnIndexMetaPB* out_meta);

    // Best-effort remove every .idx / rewritten .cols file whose path we
    // recorded in `_written_paths`. Called from `run()` when the overall build fails so
    // we don't leak objects on S3 when the ADD INDEX fast path aborts and
    // the caller falls back to the legacy rewrite path. Errors here are
    // swallowed (logged): the cleanup is a courtesy, vacuum still reclaims
    // these files later as orphans.
    void cleanup_written_idx_files();

    TabletManager* _tablet_mgr;
    ThreadPool* _lake_schema_change_pool;
    const int64_t _txn_id;
    VersionedTablet _base_tablet;
    VersionedTablet _new_tablet;
    std::vector<TabletIndexPB> _indexes_to_build;
    const int64_t _alter_version;
    std::mutex _op_mtx;            // protects concurrent writes to op_add_index.segment_entries / dcg_entries
    std::mutex _written_paths_mtx; // protects _written_paths
    // absolute paths of files created by this alter (.idx from
    // build_idg_for_segment, rewritten .cols from rewrite_dcg_for_segment)
    std::vector<std::string> _written_paths;
    // DCG list per rssid at _alter_version, empty when the tablet has no DCG.
    // Populated once in run() before any task is submitted, then read-only.
    std::unordered_map<uint32_t, DeltaColumnGroupList> _dcgs_by_rssid;
};

} // namespace starrocks::lake

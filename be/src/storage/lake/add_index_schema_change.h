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

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment.pb.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "storage/lake/types_fwd.h"
#include "storage/lake/versioned_tablet.h"

namespace starrocks {
class Segment;
class TabletColumn;
class TabletIndex;
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
    // `authoritative_schema` is the schema that resolves index column unique ids
    // and opens every source segment. The caller must pass the schema FE attached
    // to the alter request when there is one — see resolve_authoritative_schema()
    // in schema_change.cpp. Passing the tablet metadata schema instead is unsafe
    // under fast schema evolution v2, where it can be missing the very column
    // being indexed, and would additionally seed the segment metacache with a
    // subset schema (Segment::_create_column_readers walks the schema, not the
    // footer, so a column absent from it gets no reader for every later reader
    // that reuses the cached Segment).
    AddIndexSchemaChange(TabletManager* tablet_mgr, int64_t txn_id, VersionedTablet base_tablet,
                         VersionedTablet new_tablet, std::vector<TabletIndexPB> indexes_to_build, int64_t alter_version,
                         TabletSchemaPtr authoritative_schema, ThreadPool* lake_schema_change_pool = nullptr);

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
    // Leaves `out_entry` empty (no keys, no index_file) and writes no .idx when
    // none of the indexes can be built on this segment because their columns are
    // physically absent from it — see classify_index_for_segment(). The caller
    // drops such empty entries.
    Status build_idg_for_segment(const RowsetMetadataPB& rowset_meta, uint32_t seg_idx_in_rowset, uint32_t rssid,
                                 IndexDeltaGroupEntryPB* out_entry);

    // What to do with one index on one segment.
    enum class IndexDisposition {
        kBuild, // the column is physically present in this segment
        kSkip,  // the column is legitimately absent; its rows read as default/null
    };

    // Decide whether `ix` can be built on `segment`, or must be skipped because
    // the column was added by a metadata-only ALTER after this segment was
    // written and so has no bytes in it.
    //
    // Returns an error rather than a disposition for the two cases that are not
    // legitimate absences: a column missing from the authoritative schema
    // altogether (FE/BE disagree about the column set), and a column that is
    // absent from the segment yet has neither a default value nor nullability to
    // synthesize from (metadata and data genuinely disagree). Neither is softened
    // into a skip: doing so would let real corruption publish as a successful
    // alter with quietly incomplete index coverage.
    StatusOr<IndexDisposition> classify_index_for_segment(Segment* segment, const TabletIndexPB& ix,
                                                          const TabletColumn** out_column) const;

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

    // Best-effort remove every .idx file whose path we recorded in
    // `_written_paths`. Called from `run()` when the overall build fails so
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
    TabletSchemaPtr _authoritative_schema;
    // (segment, index) pairs skipped for a physically absent column, summed across
    // the per-segment pool tasks so run() can log one aggregate line per tablet.
    std::atomic<int64_t> _skipped_pairs{0};
    std::mutex _op_mtx;                      // protects concurrent writes to op_add_index.segment_entries
    std::mutex _written_paths_mtx;           // protects _written_paths
    std::vector<std::string> _written_paths; // absolute paths of .idx files created by build_idg_for_segment
};

} // namespace starrocks::lake

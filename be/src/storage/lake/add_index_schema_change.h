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
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "storage/lake/versioned_tablet.h"

namespace starrocks {
class Segment;
class TabletColumn;
class WritableFile;
class TabletIndex;
}

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
                         VersionedTablet new_tablet, std::vector<TabletIndexPB> indexes_to_build, int64_t alter_version);

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
    Status build_idg_for_segment(const RowsetMetadataPB& rowset_meta, uint32_t seg_idx_in_rowset, uint32_t rssid,
                                 IndexDeltaGroupEntryPB* out_entry);

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

    TabletManager* _tablet_mgr;
    const int64_t _txn_id;
    VersionedTablet _base_tablet;
    VersionedTablet _new_tablet;
    std::vector<TabletIndexPB> _indexes_to_build;
    const int64_t _alter_version;
    std::mutex _op_mtx; // protects concurrent writes to op_add_index.segment_entries
};

} // namespace starrocks::lake

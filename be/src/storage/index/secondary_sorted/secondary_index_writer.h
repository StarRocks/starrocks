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
#include <string>
#include <vector>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class Segment;
} // namespace starrocks

namespace starrocks::lake {
class TabletManager;
} // namespace starrocks::lake

namespace starrocks::secondary_sorted {

// SecondaryIndexWriter scans the segments produced for one rowset, sorts the
// (idx_cols..., __sidx_pos__) records by the index column prefix, and writes
// a single segment-format file -- the "secondary index file" -- next to the
// data segments in the Lake fileset.
//
// PoC design choices (documented in the design doc):
//   * One file per (rowset, index). Path: <data_root>/sidx_<txn>_<index>.idx.
//   * Synthetic TabletSchema = the index columns cloned from the source
//     schema + one trailing BIGINT column named "__sidx_pos__" encoding
//     (segment_id, rowid_in_segment) via `encode_position()`.
//   * In-memory sort only. Memory budget is bounded by
//     `config::secondary_index_build_mem_limit_mb`; PoC fails the build (and
//     therefore the rowset commit) if exceeded. Production will spill.
class SecondaryIndexWriter {
public:
    struct BuildInput {
        // Source segments to scan. May come from a load (DeltaWriter) or a
        // compaction (Horizontal/Vertical). Caller has already finalized them
        // and recorded their basenames in the rowset metadata.
        std::vector<std::shared_ptr<Segment>> segments;
        // Source schema (the table's TabletSchema). Used to look up index
        // column ids and to clone column definitions into the synthetic
        // index-file schema.
        TabletSchemaCSPtr source_schema;
        // Logical index name and ordered index column names.
        std::string index_name;
        std::vector<std::string> index_col_names;
        // Lake plumbing for resolving the output file path and opening the
        // WritableFile in the same fileset as the data segments.
        lake::TabletManager* tablet_mgr = nullptr;
        std::shared_ptr<FileSystem> fs;
        int64_t tablet_id = 0;
        int64_t txn_id = 0;
    };

    // Build the index file. On success, returns a populated FilePB ready to
    // be appended to RowsetMetadataPB.secondary_indexes. On failure, no file
    // is persisted (or the partially-written file is left for vacuum).
    static StatusOr<SecondaryIndexFilePB> build(const BuildInput& input);

private:
    // Look up index column ids in the source schema; emits NotFound on any
    // unresolved name.
    static StatusOr<std::vector<uint32_t>> resolve_index_col_ids(const TabletSchema& source_schema,
                                                                 const std::vector<std::string>& col_names);

    // Construct the synthetic TabletSchema: clone the index columns out of
    // `source_schema` and append a BIGINT column named "__sidx_pos__".
    static TabletSchemaSPtr build_index_schema(const TabletSchema& source_schema,
                                               const std::vector<uint32_t>& index_col_ids);
};

} // namespace starrocks::secondary_sorted

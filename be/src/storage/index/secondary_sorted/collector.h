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

#include "column/column.h"
#include "column/fixed_length_column.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/index/secondary_sorted/index_registry.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class Chunk;
} // namespace starrocks

namespace starrocks::lake {
class TabletManager;
} // namespace starrocks::lake

namespace starrocks::secondary_sorted {

// SecondaryIndexCollector accumulates the index-column values + encoded
// (seg_id, rowid_in_segment) positions from chunks that flow through a
// TabletWriter, then writes one segment-format index file per registered
// index at finalize time.
//
// Why in-line: in shared-data mode the data segments themselves are buffered
// by a BundleWritableFileContext and only become OSS-visible after the LAST
// writer in the bundle's partition closes. Reading segments back inside
// DeltaWriter::finish_with_txnlog therefore races with the bundle uploader.
// Extracting index data straight from the chunk avoids that race entirely.
//
// Lifecycle:
//   * SecondaryIndexCollector::create() -- per tablet, lazily, before the
//     first write. No-op (returns nullptr) when no index is registered.
//   * add_chunk(chunk, seg_id, base_rowid_in_segment) -- called from the
//     TabletWriter right after a chunk has been appended to its segment.
//     base_rowid_in_segment is the segment's row count BEFORE this chunk.
//   * finalize(...) -- called from the TabletWriter on commit. Sorts each
//     index, writes its segment file, returns the PB entries for the
//     caller (DeltaWriter / compaction task) to attach to the new rowset.
class SecondaryIndexCollector {
public:
    static StatusOr<std::unique_ptr<SecondaryIndexCollector>> create(int64_t tablet_id, int64_t txn_id,
                                                                     const std::vector<SecondaryIndexDef>& defs,
                                                                     const TabletSchemaCSPtr& source_schema);

    // Empty collector: behaves as a no-op everywhere. Caller can skip the
    // add_chunk/finalize calls but checking empty() is cheaper.
    bool empty() const { return _indexes.empty(); }

    Status add_chunk(const Chunk& chunk, uint32_t seg_id, uint32_t base_rowid);

    // Writes index files to OSS and returns one PB entry per index that
    // produced output. Caller is responsible for attaching the entries to
    // the rowset metadata.
    StatusOr<std::vector<SecondaryIndexFilePB>> finalize(std::shared_ptr<FileSystem> fs,
                                                         lake::TabletManager* tablet_mgr);

private:
    struct PerIndexState {
        std::string name;
        std::vector<std::string> col_names;
        std::vector<uint32_t> source_col_ids; // positions in source_schema
        MutableColumns idx_cols;              // lazily initialised from first chunk
        Int64Column::MutablePtr pos_col;
    };

    SecondaryIndexCollector(int64_t tablet_id, int64_t txn_id, TabletSchemaCSPtr source_schema);

    Status _add_chunk_to_index(PerIndexState& st, const Chunk& chunk, uint32_t seg_id, uint32_t base_rowid);

    StatusOr<SecondaryIndexFilePB> _write_one_index(PerIndexState& st, std::shared_ptr<FileSystem> fs,
                                                    lake::TabletManager* tablet_mgr);

    int64_t _tablet_id = 0;
    int64_t _txn_id = 0;
    TabletSchemaCSPtr _source_schema;
    std::vector<PerIndexState> _indexes;
};

} // namespace starrocks::secondary_sorted

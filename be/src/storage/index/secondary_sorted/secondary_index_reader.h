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
#include <unordered_map>
#include <vector>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/tablet_schema.h"

#include "roaring/roaring.hh"

namespace starrocks {
class Segment;
class ColumnPredicate;
} // namespace starrocks

namespace starrocks::lake {
class TabletManager;
} // namespace starrocks::lake

namespace starrocks::secondary_sorted {

// Per-(rowset segment) candidate rowid bitmap produced by an index lookup.
// Key is the segment's ordinal in the rowset; value contains the candidate
// row ids that match the index predicate. DelVec filtering happens later
// in the existing segment-scan pipeline.
using PerSegmentRowidBitmap = std::unordered_map<uint32_t, roaring::Roaring>;

// SecondaryIndexReader opens a secondary index file (which is itself a
// segment with schema [idx_cols..., __sidx_pos__ BIGINT]) and answers
// predicate queries by scanning that segment, applying predicates on the
// index columns, and decoding the position column.
//
// PoC layout choices:
//   * One index reader per rowset per index (caller decides which to open).
//   * Predicates passed in must be expressed against the *source* tablet
//     schema's index columns; the reader translates names internally.
//   * For PoC v1 we do a simple sequential scan with predicate pushdown.
//     v2 will exploit the short-key index for prefix narrowing -- segment
//     v2 already builds one, so this is mostly hook-up work later.
class SecondaryIndexReader {
public:
    struct OpenInput {
        std::shared_ptr<FileSystem> fs;
        lake::TabletManager* tablet_mgr = nullptr;
        int64_t tablet_id = 0;
        // PB record copied out of RowsetMetadataPB.secondary_indexes
        SecondaryIndexFilePB file_pb;
        // Source tablet schema -- used to resolve column types/IDs for the
        // synthetic read schema.
        TabletSchemaCSPtr source_schema;
    };

    static StatusOr<std::shared_ptr<SecondaryIndexReader>> open(const OpenInput& input);

    // Lookup: scan the index file with the given predicates restricted to
    // index columns. Returns one Roaring per source segment that actually
    // had matches; segments with no matches are absent from the map.
    //
    // Predicates that reference non-index columns must have been stripped
    // by the caller. The reader does not validate this -- it will simply
    // ignore predicates on columns absent from its read schema.
    StatusOr<PerSegmentRowidBitmap> lookup(const std::vector<const ColumnPredicate*>& index_col_predicates);

    const SecondaryIndexFilePB& file_pb() const { return _file_pb; }

private:
    SecondaryIndexReader(std::shared_ptr<FileSystem> fs, lake::TabletManager* tablet_mgr, int64_t tablet_id,
                         SecondaryIndexFilePB file_pb, TabletSchemaCSPtr source_schema);

    Status _init();

    std::shared_ptr<FileSystem> _fs;
    lake::TabletManager* _tablet_mgr;
    int64_t _tablet_id;
    SecondaryIndexFilePB _file_pb;
    TabletSchemaCSPtr _source_schema;

    // Synthetic index-file schema: [idx_cols..., __sidx_pos__ BIGINT].
    TabletSchemaCSPtr _index_schema;
    // Index of the encoded-position column in _index_schema.
    uint32_t _encoded_pos_col_idx = 0;
    // Index column IDs in the *source* schema; used by the caller to
    // recognize whether a query predicate is index-eligible.
    std::vector<uint32_t> _source_index_col_ids;

    std::shared_ptr<Segment> _segment;
};

} // namespace starrocks::secondary_sorted

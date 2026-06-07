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
#include "roaring/roaring.hh"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/rowset/rowid_range_option.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class Segment;
class ColumnPredicate;
class ObjectPool;
class PredicateTree;
class DelvecLoader;
class ChunkIterator;
class Schema;
struct OlapReaderStatistics;
using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;
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
// Layout choices:
//   * One index reader per rowset per index (caller decides which to open).
//   * Predicates passed in are expressed against the *source* tablet
//     schema's column ids; the reader remaps them into the index file's
//     synthetic schema before pushing them into the inner segment scan.
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

    // Like open(), but consults a process-wide LRU cache keyed by
    // (tablet_id, file_name). Subsequent queries against the same .idx file
    // skip the OSS GET + footer parse + column reader init -- ~100 ms saved
    // on a 355 MB index file at the 100 M-row/1-tablet scale.
    // |stats| (optional) records open time + cache hit/miss for the profile.
    static StatusOr<std::shared_ptr<SecondaryIndexReader>> open_cached(const OpenInput& input,
                                                                       OlapReaderStatistics* stats = nullptr);

    // Lookup: scan the index file applying the predicates from
    // |source_pred_tree| that touch index columns. The reader internally
    // remaps each predicate's column id from the source tablet schema's
    // space into the index file's synthetic schema (0..K-1) before
    // pushing it through the inner SegmentIterator's pred_tree pipeline.
    // Returns one Roaring per source segment that actually had matches;
    // segments with no matches are absent from the map.
    //
    // |obj_pool| owns the lifetime of all cloned predicates used during
    // this lookup and must outlive the returned bitmap consumption.
    StatusOr<PerSegmentRowidBitmap> lookup(const PredicateTree& source_pred_tree, ObjectPool* obj_pool,
                                           OlapReaderStatistics* stats = nullptr);

    // Like lookup(), but memoizes the per-segment candidate bitmap in a
    // process-wide cache keyed by |cache_key|. A single tablet's scan is
    // split into many morsels (tablet-internal parallelism) and each morsel
    // builds its own TabletReader; without this, the identical .idx scan runs
    // once per morsel (e.g. 72x for a 100M-row tablet). The first caller
    // computes via lookup(); the rest share the result via std::call_once.
    // The returned bitmap is shared+immutable -- callers must copy if they
    // need to mutate (e.g. AND-merge across multiple indexes).
    // |cache_key| must uniquely identify (.idx file, predicate); the caller
    // builds it from file_name + a predicate signature.
    StatusOr<std::shared_ptr<const PerSegmentRowidBitmap>> lookup_cached(const std::string& cache_key,
                                                                         const PredicateTree& source_pred_tree,
                                                                         ObjectPool* obj_pool,
                                                                         OlapReaderStatistics* stats = nullptr);

    const SecondaryIndexFilePB& file_pb() const { return _file_pb; }

    // Covering-index fast path: when the query's predicate columns AND its
    // output columns are all contained in this index, the query can be
    // answered ENTIRELY from the .idx file -- no base-table readback. Returns
    // a ChunkIterator that scans the .idx with the (remapped) predicate
    // pushed down, decodes __sidx_pos__ -> (seg_id, rowid), drops rows marked
    // deleted by DelVec (via |delvec_loader|, keyed by rowset_id_base+seg_id),
    // and emits the requested |output_schema| columns straight from the index.
    //
    // |output_schema| fields must all be index columns (matched by name).
    // |rowset_id_base| is the rowset's id; the global DelVec segment id is
    // rowset_id_base + (seg_id decoded from the position), matching the base
    // scan's `_opts.rowset_id + segment_id()`.
    // |idx_rowid_range| (optional): restricts the inner .idx segment scan to a
    // sub-range of the index's OWN row positions. This is how the covering scan
    // is parallelized across morsels: the .idx is ordered by index key (not base
    // rowid) so it cannot be split by the morsel's base-rowid range, but since
    // the .idx holds exactly one entry per base row (N_idx == N_base), each
    // morsel's base-rowid coverage maps numerically onto a disjoint slice of
    // [0, N_idx). Every morsel scans its own .idx slice and emits all rows in
    // it; the union covers the whole matching range exactly once -- parallel,
    // no redundant rescans, no per-row morsel filter. Null => scan all (single
    // morsel / no split).
    StatusOr<ChunkIteratorPtr> make_covering_iterator(
            const Schema& output_schema, const PredicateTree& source_pred_tree, ObjectPool* obj_pool,
            int64_t rowset_id_base, int64_t version, std::shared_ptr<DelvecLoader> delvec_loader, int chunk_size,
            SparseRangePtr idx_rowid_range = nullptr, OlapReaderStatistics* stats = nullptr);

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

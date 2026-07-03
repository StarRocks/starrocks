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
#include <optional>
#include <roaring/roaring.hh>
#include <vector>

#include "common/statusor.h"
#include "storage/olap_common.h"
#include "storage/primitive/chunk_iterator.h"
#include "storage/primitive/range.h"

namespace starrocks {
class Segment;
class SeekRange;

class ColumnIterator;
class ColumnPredicate;
class PredicateTree;
class Schema;
class SegmentReadOptions;
struct LakeIOOptions;

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const Schema& schema,
                                      const SegmentReadOptions& options);

// Resolve a key-space SeekRange to the corresponding rowid range within |segment|.
// Wraps the lookup machinery of SegmentIterator so callers outside the
// iterator scan path can convert a TabletRangePB-derived SeekRange into a
// contiguous [lower, upper) rowid window.
// Returns std::nullopt when the range is empty on this segment.
StatusOr<std::optional<Range<rowid_t>>> segment_seek_range_to_rowid_range(const std::shared_ptr<Segment>& segment,
                                                                          const SeekRange& range,
                                                                          const LakeIOOptions& lake_io_opts);

// Picks the raw embedding column for brute-force vector distance computation. The column may
// live in the output `chunk` (FE kept it eager) or in `dict_chunk` (FE pruned v from the
// output schema; the BE re-added it to its read schema in _setup_brute_force_fallback so the
// column is read internally but never propagated up). Returns InternalError if it appears in
// neither — without this guard, Chunk::get_column_by_id would default-insert into the cid
// index and return _columns[0], which the brute-force kernel would then downcast to
// ArrayColumn and corrupt memory.
StatusOr<ColumnPtr> resolve_brute_force_vector_column(const Chunk* chunk, const Chunk* dict_chunk, ColumnId vec_col_id);

// Evaluate the WHOLE `pred_tree` (AND / OR / compound / expr) over the rows in `candidate` by
// reading the predicate columns, returning the matching subset as an exact row bitmap. Declared
// here so it can be unit-tested directly, like resolve_brute_force_vector_column above.
//
//   schema                   supplies the Field (field->id() == ColumnId) per predicate column.
//   column_iterators_by_cid  positioned readers indexed by ColumnId; non-predicate entries may be
//                            null. A predicate column missing from either is an InternalError --
//                            silently skipping it would widen the bitmap.
//   fallback_rowids          optional; filled per batch with the batch's segment rowids, for
//                            predicates that resolve chunk rows back to rowids (the inverted-index
//                            fallback for a MATCH inside an OR).
//   dict_code_candidates_by_cid
//                            optional; flags columns whose predicates the ColumnPredicateRewriter
//                            rewrote into dict codes. A flagged, all-page-dict-encoded column is
//                            read as codes (DictCodeColumnIterator + code-typed field) so the
//                            rewritten predicates evaluate against matching values.
//
// Reads in <= 4096-row batches and seeks every batch, so prior iterator positions do not matter.
StatusOr<roaring::Roaring> evaluate_pred_tree_to_bitmap(
        const PredicateTree& pred_tree, const Schema& schema,
        const std::vector<std::unique_ptr<ColumnIterator>>& column_iterators_by_cid,
        std::vector<rowid_t>* fallback_rowids, const roaring::Roaring& candidate,
        const std::vector<uint8_t>* dict_code_candidates_by_cid = nullptr);

} // namespace starrocks

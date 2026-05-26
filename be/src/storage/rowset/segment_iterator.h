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
#include <vector>

#include "common/statusor.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/range.h"

namespace starrocks {
class Segment;
class SeekRange;

class ColumnPredicate;
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

} // namespace starrocks

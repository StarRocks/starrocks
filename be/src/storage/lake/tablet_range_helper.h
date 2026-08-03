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

#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"
#include "storage/lake/sst_seek_range.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {
class TabletRangeHelper {
public:
    /**
     * @brief Create a SeekRange from TabletRangePB.
     *
     * @note IMPORTANT:
     *  - If `mem_pool` is nullptr, the returned SeekRange may contain Slices that point
     *    directly to the string data within `tablet_range_pb` for string-like columns.
     *    The caller MUST ensure that `tablet_range_pb` outlives the returned SeekRange
     *    to avoid dangling pointers.
     *  - If `mem_pool` is non-null, the string data will be copied into `mem_pool`, and
     *    the caller MUST ensure that `mem_pool` outlives the returned SeekRange.
     *
     * @param tablet_range_pb The protobuf source of the range.
     * @param tablet_schema The schema used to parse types.
     * @param mem_pool Optional memory pool used to allocate string data for Slices.
     * @return StatusOr<SeekRange> A SeekRange referencing data owned either by
     *         `tablet_range_pb` or `mem_pool`, depending on `mem_pool`.
     */
    // Decode a persisted tablet range into a SeekRange under `tablet_schema` -- the schema the target
    // segment is read with (for an old rowset, its archived schema). A range whose bound arity exceeds
    // `tablet_schema`'s sort-key arity can occur when a newer build persisted the range after a
    // metadata-only trailing sort-key add and this (possibly downgraded) build reads a rowset that still
    // carries a narrower archived schema. Such a bound is projected onto the leading sort-key columns:
    // the dropped trailing columns are compared against their read-time defaults (from `current_schema`,
    // which must contain them) to set each bound's inclusivity, so the seek stays exact. `current_schema`
    // is required only when a bound is wider than `tablet_schema`'s sort key; a same-arity range decodes
    // directly and ignores it.
    static StatusOr<SeekRange> create_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                      const TabletSchemaCSPtr& tablet_schema, MemPool* mem_pool,
                                                      const TabletSchemaCSPtr& current_schema = nullptr);

    static StatusOr<SstSeekRange> create_sst_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                             const TabletSchemaCSPtr& tablet_schema);

    static StatusOr<TabletRangePB> convert_t_range_to_pb_range(const TTabletRange& t_range);

    // Check that a single tablet range is well-formed: closed-open semantics
    // (lower_bound inclusive when set, upper_bound exclusive when set), with
    // the corresponding `*_bound_included` flag explicitly present.
    // Either or both bounds may be absent (unbounded).
    static Status validate_tablet_range(const TabletRangePB& tablet_range_pb);

    // Check that a list of new-tablet ranges tiles the old tablet range
    // exactly: old range itself is well-formed; each new range is
    // well-formed and non-zero-width (lower != upper by byte equality);
    // adjacent ranges meet exactly with no gaps or overlaps; first.lower
    // matches old.lower; last.upper matches old.upper. Used by the external boundaries
    // pre-split path to validate FE-supplied ranges before BE commits to
    // writing K new tablets.
    //
    // Note: strict semantic ordering (lower < upper, ranges monotonically
    // increasing) requires a schema for type-aware comparison and is the
    // caller's responsibility. This helper only does schema-free structural
    // checks.
    static Status validate_new_tablet_ranges(
            const TabletRangePB& old_tablet_range,
            const google::protobuf::RepeatedPtrField<TabletRangePB>& new_tablet_ranges);
};

} // namespace starrocks::lake

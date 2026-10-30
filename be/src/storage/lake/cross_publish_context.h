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

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/primary_key_encoding_types.h"

namespace starrocks::lake {

// Decides, row by row, which rows of a cross-published rowset this tablet actually owns.
//
// A SPLIT cross publish hands every child the parent's whole op_write payload and leaves each side to
// work out which rows are its own. The tablet range already answers that whenever it can:
// SegmentIterator::_apply_tablet_range turns it into a rowid interval, and _lookup_ordinal resolves
// the bounds exactly -- the short key index only brackets the search, which then binary-searches the
// actual rows and compares per column. So a child reading a shared segment, on the read path or
// through get_each_segment_iterator on the publish path, sees precisely its own rows.
//
// That holds only while the range and the physical order live in the same key space. A primary-key
// tablet ordered by a separate sort key has its rows scattered through the segment, no rowid interval
// describes them, and Rowset::set_segment_tablet_range withholds the range outright -- at which point
// the iterator emits every row and a consumer that assumes otherwise silently takes a sibling's.
//
// This selector is the answer for exactly that case, and the two mechanisms are complementary rather
// than redundant: where the range narrows, the mask it produces is all ones and costs nothing; where
// the range is withheld, the mask is the only thing that knows. No such tablet exists yet --
// CreateTableAnalyzer requires a range-distributed primary-key table to declare ORDER BY equal to its
// key columns, and OPTIMIZE, the only clause that rewrites a sort key, is rejected on range-distributed
// tables -- so this is built ahead of that relaxation rather than as a fix for a live defect. #77653
// stages the rest of the consumers.
//
// What the shape of the answer buys, and why it is a mask rather than a filtered chunk: the chunk
// keeps its rows and their positions, so SegmentPKChunkRef's "chunk[i] is physical rowid
// physical_rowid_offset + i" still holds for every i, owned or not. A consumer that must name a row
// in the segment file -- a delvec entry, an index location -- reads it straight off the index it is
// already looping over. Compacting the chunk instead would renumber the survivors and every rowid
// derived from them would be wrong.
//
// Consuming the selection (index lookup, upsert, delvec, prebuilt SSTs, condition/partial update) is
// deliberately NOT part of this: it lands with the stages that own those paths.
class CrossPublishRowSelector {
public:
    // Builds a selector iff |rowset| is actually being cross-published to a ranged primary-key
    // tablet, i.e. it carries at least one segment the split marked `shared` and |metadata| has a
    // range. Returns nullptr otherwise, which is the ordinary publish path and must stay free: a
    // rowset written by this tablet's own sink is in range by construction.
    //
    // Derived from committed metadata rather than passed down from PublishTabletInfo on purpose --
    // the same reasoning the design applies to the split's own cutover. A retry, a leader failover
    // or a replayed publish reaches the same verdict without carrying a bit for it.
    static StatusOr<std::unique_ptr<CrossPublishRowSelector>> create_if_needed(const TabletMetadataPB& metadata,
                                                                               const TabletSchemaCSPtr& tablet_schema,
                                                                               const RowsetMetadataPB& rowset);

    // One byte per row of |chunk|: 1 if this tablet owns the row, 0 if a sibling does. Empty for an
    // empty chunk. |chunk| holds the primary-key columns, in key order.
    //
    // const, and it holds no scratch of its own: RowsetUpdateState::load_segment is documented
    // thread-safe across segment ids and every one of a rowset's iterators shares this selector, so
    // an encode buffer kept as a member would be written by several segments at once. Allocating it
    // per call makes sharing safe by construction rather than by discipline; the encoded bytes were
    // allocated per chunk either way, so only the column header is re-made.
    StatusOr<Filter> select(const Chunk& chunk) const;

    // Same verdict from keys that are already V2-encoded -- the publish path encodes the primary key
    // for its index lookup anyway, so a consumer that has that column should hand it over rather
    // than pay a second encode.
    StatusOr<Filter> select_encoded(const Column& encoded_keys) const;

private:
    // Every member is set once by create_if_needed and read-only afterwards, which is what lets one
    // selector serve a rowset's segments concurrently.
    SstSeekRange _seek_range;
    Schema _pkey_schema;
    PrimaryKeyEncodingType _encoding_type = PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2;
};

using CrossPublishRowSelectorPtr = std::unique_ptr<CrossPublishRowSelector>;

} // namespace starrocks::lake

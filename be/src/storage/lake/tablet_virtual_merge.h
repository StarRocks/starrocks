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

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

class TabletManager;

// Builds the query-side parent alias that a split of a range-distributed PRIMARY KEY table whose ORDER BY
// differs from the primary key keeps alive until its UNSHARE completes. Reads stay pinned to the parent for
// that window, so every ordinary publish rebuilds this metadata for the parent's id from the children's.
//
// "Virtual" because nothing here owns data: no segment is written, no primary-index sstable is rebuilt, no
// rowset id is allocated for anyone else to use. The output is a read-only view, thrown away and rebuilt at
// the next version -- so a read is all it has to serve, and a read needs rowsets and delete vectors.
//
// That makes it a two-line job. Rowsets are keyed by `uid`, the global id of a rowset's data that a split
// copies verbatim into every child: one the children inherited appears once, one a child wrote after the
// split appears in that child alone, and every one of them is emitted under a fresh sequential id (the
// children allocate ids independently, so their post-split rowsets collide). Delete vectors are then
// unioned per segment onto the emitted ids -- each row of a shared segment belongs to exactly one child and
// only its owner marks it deleted, so the union is the parent's view of that segment.
//
// It deliberately does NOT reuse merge_tablet. A real MERGE has to decide which source still owns each row
// of a shared segment, which needs gap-delvec synthesis -- and that turns a key range into a rowid window,
// which this shape cannot do at all (its range is in primary-key space, its segments are in sort-key
// order). Everything the real merge carries for that decision, and for owning its output, is absent here.
StatusOr<MutableTabletMetadataPtr> virtual_merge_for_read(TabletManager* tablet_manager,
                                                          const std::vector<TabletMetadataPtr>& child_metadatas,
                                                          const MergingTabletInfoPB& merging_tablet,
                                                          int64_t new_version, const TxnInfoPB& txn_info);

} // namespace starrocks::lake

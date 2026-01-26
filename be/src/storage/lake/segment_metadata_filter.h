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

#include "gen_cpp/lake_types.pb.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

// Segment metadata filter for Lake tables.
// Uses sort_key_min/max from SegmentMetadataPB to filter segments
// that cannot possibly contain data matching the query predicates.
class SegmentMetadataFilter {
public:
    // Check if a segment may contain data matching the predicates.
    // Returns true if the segment may contain matching data (need to read).
    // Returns false if the segment definitely does not contain matching data (can skip).
    static bool may_contain(const SegmentMetadataPB& segment_meta, const PredicateTree& pred_tree_for_zone_map,
                            const TabletSchema& tablet_schema);
};

} // namespace starrocks::lake

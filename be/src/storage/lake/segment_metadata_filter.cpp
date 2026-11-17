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

#include "storage/lake/segment_metadata_filter.h"

#include "storage/datum_variant.h"
#include "storage/sort_key_bounds.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/zone_map_detail.h"

namespace starrocks::lake {

namespace {

// Predicate tree visitor that determines if a segment can be pruned.
struct MetadataPruner {
    // Handle single column predicate.
    bool operator()(const PredicateColumnNode& node) const {
        const auto* col_pred = node.col_pred();
        if (col_pred == nullptr) {
            return false;
        }

        // IS NULL / IS NOT NULL predicates are not supported.
        if (col_pred->type() == PredicateType::kIsNull || col_pred->type() == PredicateType::kNotNull) {
            return false;
        }

        const ColumnId column_id = col_pred->column_id();

        // Only the leading sort key column has correct per-column min/max from the
        // composite sort key bounds. Non-leading columns' bound values are NOT
        // actual per-column min/max, so we restrict pruning to the leading sort key only.
        if (sort_key_idxes.empty() || sort_key_idxes[0] != column_id) {
            return false;
        }

        // Decode the leading bounds on first use, once per may_contain call. Absent or malformed
        // bounds leave this leaf unable to prune, so the segment is conservatively kept.
        if (!decode_attempted) {
            decode_attempted = true;
            decode_status = decode_segment_leading_sort_key_bounds(*segment_meta, &leading_min, &leading_max);
        }
        if (!decode_status.ok()) {
            return false;
        }

        // DatumVariant owns the underlying Slice data via CopiedDatum. ZoneMapDetail only
        // holds shallow Datum copies, so leading_min/leading_max (members of this pruner)
        // must stay alive while zone_map_filter accesses the data.
        ZoneMapDetail detail(leading_min.value(), leading_max.value());
        return !col_pred->zone_map_filter(detail);
    }

    // AND node: if any child can prune, the whole AND can prune.
    bool operator()(const PredicateAndNode& node) const {
        return std::any_of(node.children().begin(), node.children().end(),
                           [this](const auto& child) { return child.visit(*this); });
    }

    // OR node: all children must be able to prune for the whole OR to prune.
    bool operator()(const PredicateOrNode& node) const {
        return !node.empty() && std::all_of(node.children().begin(), node.children().end(),
                                            [this](const auto& child) { return child.visit(*this); });
    }

    const SegmentMetadataPB* segment_meta;
    const std::vector<ColumnId>& sort_key_idxes;

    // Column 0 of the segment's minimum and maximum sort key, decoded lazily by the first
    // leading-sort-key predicate that needs them.
    mutable bool decode_attempted = false;
    mutable Status decode_status;
    mutable DatumVariant leading_min;
    mutable DatumVariant leading_max;
};

} // namespace

bool SegmentMetadataFilter::may_contain(const SegmentMetadataPB& segment_meta,
                                        const PredicateTree& pred_tree_for_zone_map,
                                        const TabletSchema& tablet_schema) {
    // Get sort key column indices. The bound values are stored in this order.
    const auto& sort_key_idxes = tablet_schema.sort_key_idxes();
    if (sort_key_idxes.empty()) {
        return true;
    }
    // Without bounds nothing can be pruned, so skip the predicate tree walk.
    if (!has_segment_sort_key_bounds(segment_meta)) {
        return true;
    }

    MetadataPruner pruner{&segment_meta, sort_key_idxes};
    bool can_prune = pred_tree_for_zone_map.visit(pruner);

    return !can_prune;
}

} // namespace starrocks::lake

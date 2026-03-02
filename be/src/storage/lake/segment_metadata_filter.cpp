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

#include "storage/column_predicate.h"
#include "storage/datum_variant.h"
#include "storage/zone_map_detail.h"

namespace starrocks::lake {

namespace {

// Build ZoneMapDetail from TuplePB for a specific column.
// Returns error if column_idx is out of range.
StatusOr<ZoneMapDetail> build_zone_map_detail(const TuplePB& min_tuple, const TuplePB& max_tuple, int column_idx) {
    if (column_idx >= min_tuple.values_size() || column_idx >= max_tuple.values_size()) {
        return Status::InvalidArgument(fmt::format("column_idx {} out of range, min size: {}, max size: {}", column_idx,
                                                   min_tuple.values_size(), max_tuple.values_size()));
    }

    DatumVariant min_variant;
    RETURN_IF_ERROR(min_variant.from_proto(min_tuple.values(column_idx)));

    DatumVariant max_variant;
    RETURN_IF_ERROR(max_variant.from_proto(max_tuple.values(column_idx)));

    // Data is sorted by sort key, nulls are placed first.
    // If min is null, the segment contains null values (has_null = true).
    // If min is not null, the segment does not contain null values.
    // ZoneMapDetail(min_or_null_value, max_value) constructor handles this automatically.
    ZoneMapDetail detail(min_variant.value(), max_variant.value());
    return detail;
}

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
        // composite sort_key_min/max tuples. Non-leading columns' tuple values are NOT
        // actual per-column min/max, so we restrict pruning to the leading sort key only.
        if (sort_key_idxes.empty() || sort_key_idxes[0] != column_id) {
            return false;
        }

        // Build ZoneMapDetail using position 0 (the leading sort key) in the tuple.
        auto detail_or = build_zone_map_detail(min_tuple, max_tuple, /*column_idx=*/0);
        if (!detail_or.ok()) {
            return false;
        }

        // Reuse ColumnPredicate::zone_map_filter.
        // zone_map_filter returns false if segment definitely does not contain matching data.
        return !col_pred->zone_map_filter(detail_or.value());
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

    const TuplePB& min_tuple;
    const TuplePB& max_tuple;
    const std::vector<ColumnId>& sort_key_idxes;
};

} // namespace

bool SegmentMetadataFilter::may_contain(const SegmentMetadataPB& segment_meta,
                                        const PredicateTree& pred_tree_for_zone_map,
                                        const TabletSchema& tablet_schema) {
    // No metadata available, conservatively return true.
    if (!segment_meta.has_sort_key_min() || !segment_meta.has_sort_key_max()) {
        return true;
    }

    const auto& min_tuple = segment_meta.sort_key_min();
    const auto& max_tuple = segment_meta.sort_key_max();

    if (min_tuple.values_size() == 0 || max_tuple.values_size() == 0) {
        return true;
    }

    // Get sort key column indices. The tuple values are stored in this order.
    const auto& sort_key_idxes = tablet_schema.sort_key_idxes();
    if (sort_key_idxes.empty()) {
        return true;
    }

    MetadataPruner pruner{min_tuple, max_tuple, sort_key_idxes};
    bool can_prune = pred_tree_for_zone_map.visit(pruner);

    return !can_prune;
}

} // namespace starrocks::lake

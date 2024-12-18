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

#include "formats/parquet/group_reader.h"
#include "storage/predicate_tree/predicate_tree.h"

namespace starrocks::parquet {

enum class FilterLevel { ROW_GROUP = 0, PAGE_INDEX };

class ZoneMapEvaluatorUtils {
public:
    static bool is_satisfy(const std::vector<const ColumnPredicate*>& predicates, const ZoneMapDetail& detail,
                           const CompoundNodeType pred_relation) {
        if (pred_relation == CompoundNodeType::AND) {
            return std::ranges::all_of(predicates, [&](const auto* pred) { return pred->zone_map_filter(detail); });
        } else {
            return predicates.empty() ||
                   std::ranges::any_of(predicates, [&](const auto* pred) { return pred->zone_map_filter(detail); });
        }
    }

    template <CompoundNodeType Type>
    static void merge_row_ranges(std::optional<SparseRange<uint64_t>>& dest, SparseRange<uint64_t>& source) {
        if (!dest.has_value()) {
            dest = std::move(source);
        } else {
            if constexpr (Type == CompoundNodeType::AND) {
                dest.value() &= source;
            } else {
                dest.value() |= source;
            }
        }
    }
};

template <FilterLevel level>
struct ZoneMapEvaluator {
    template <CompoundNodeType Type>
    StatusOr<std::optional<SparseRange<uint64_t>>> operator()(const PredicateCompoundNode<Type>& node) {
        std::optional<SparseRange<uint64_t>> row_ranges = std::nullopt;
        const uint64_t rg_first_row = group_reader->get_row_group_first_row();
        const uint64_t rg_num_rows = group_reader->get_row_group_metadata()->num_rows;

        const auto& ctx = pred_tree.compound_node_context(node.id());
        const auto& cid_to_col_preds = ctx.cid_to_col_preds(node);

        for (const auto& [cid, col_preds] : cid_to_col_preds) {
            SparseRange<uint64_t> cur_row_ranges;

            auto* column_reader = group_reader->get_column_reader(cid);

            if (column_reader == nullptr) {
                // ColumnReader not found, select all by default
                cur_row_ranges.add({rg_first_row, rg_first_row + rg_num_rows});
            } else if (level == FilterLevel::ROW_GROUP) {
                ASSIGN_OR_RETURN(bool is_selected,
                                 column_reader->row_group_zone_map_filter(col_preds, Type, rg_first_row, rg_num_rows));
                if (is_selected) {
                    cur_row_ranges.add({rg_first_row, rg_first_row + rg_num_rows});
                }
            } else {
                ASSIGN_OR_RETURN(bool has_filtered,
                                 column_reader->page_index_zone_map_filter(
                                         col_preds, &cur_row_ranges, Type, group_reader->get_row_group_first_row(),
                                         group_reader->get_row_group_metadata()->num_rows));
                if (!has_filtered) {
                    // no filter happened, select the whole row group by default
                    cur_row_ranges.add({rg_first_row, rg_first_row + rg_num_rows});
                }
            }

            ZoneMapEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges);
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(auto cur_row_ranges_opt, child.visit(*this));
            if (cur_row_ranges_opt.has_value()) {
                ZoneMapEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges_opt.value());
            }
        }
        return row_ranges;
    }

    const PredicateTree& pred_tree;
    GroupReader* group_reader;
};

} // namespace starrocks::parquet
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

class PredicateFilterEvaluatorUtils {
public:
    static bool zonemap_satisfy(const std::vector<const ColumnPredicate*>& predicates, const ZoneMapDetail& detail,
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

struct PredicateFilterEvaluator {
    enum Evaluator {
        NONE = 0,
        ROWGROUP_ZONEMAP = 1,
        PAGE_INDEX_ZONEMAP = 2,
        BLOOM_FILTER = 4,
        ALL = ROWGROUP_ZONEMAP | PAGE_INDEX_ZONEMAP | BLOOM_FILTER,
    };

    template <CompoundNodeType Type>
    StatusOr<std::optional<SparseRange<uint64_t>>> visit_for_rowgroup_zonemap(const PredicateCompoundNode<Type>& node) {
        DCHECK(group_reader != nullptr);
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
            } else {
                bool row_group_filtered = false;
                auto ret = column_reader->row_group_zone_map_filter(col_preds, Type, rg_first_row, rg_num_rows);
                row_group_filtered = ret.value_or(false);
                counter.statistics_tried_counter++;
                counter.statistics_success_counter += row_group_filtered;
                if (row_group_filtered) {
                    cur_row_ranges.clear();
                } else {
                    cur_row_ranges.add({rg_first_row, rg_first_row + rg_num_rows});
                }
            }

            PredicateFilterEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges);
            if (Type == CompoundNodeType::AND && row_ranges->span_size() == 0) {
                return row_ranges;
            }
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(auto cur_row_ranges_opt, child.visit(*this, Evaluator::ROWGROUP_ZONEMAP));
            if (cur_row_ranges_opt.has_value()) {
                PredicateFilterEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges_opt.value());
            }
        }
        return row_ranges;
    }

    template <CompoundNodeType Type>
    StatusOr<std::optional<SparseRange<uint64_t>>> visit_for_page_index(const PredicateCompoundNode<Type>& node) {
        DCHECK(group_reader != nullptr);
        std::optional<SparseRange<uint64_t>> row_ranges;
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
            } else {
                auto ret = column_reader->page_index_zone_map_filter(col_preds, &cur_row_ranges, Type,
                                                                     group_reader->get_row_group_first_row(),
                                                                     group_reader->get_row_group_metadata()->num_rows);
                bool page_filtered = ret.value_or(false);
                counter.page_index_tried_counter++;
                counter.page_index_success_counter += page_filtered;
                if (page_filtered) {
                    bool row_group_filtered = cur_row_ranges.span_size() == 0;
                    counter.page_index_filter_group_counter += row_group_filtered;
                } else {
                    cur_row_ranges.add({rg_first_row, rg_first_row + rg_num_rows});
                }
            }

            PredicateFilterEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges);
            if (Type == CompoundNodeType::AND && row_ranges->span_size() == 0) {
                return row_ranges;
            }
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(auto cur_row_ranges_opt, child.visit(*this, Evaluator::PAGE_INDEX_ZONEMAP));
            if (cur_row_ranges_opt.has_value()) {
                PredicateFilterEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges_opt.value());
            }
        }
        return row_ranges;
    }

    template <CompoundNodeType Type>
    StatusOr<std::optional<SparseRange<uint64_t>>> visit_for_bloom_filter(const PredicateCompoundNode<Type>& node) {
        DCHECK(group_reader != nullptr);
        std::optional<SparseRange<uint64_t>> row_ranges;
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
            } else {
                bool row_group_filtered = false;
                auto ret = column_reader->adaptive_judge_if_apply_bloom_filter(
                        row_ranges_before_bf.has_value() ? row_ranges_before_bf->span_size() : rg_num_rows);
                if (ret.value_or(true)) {
                    ret = column_reader->row_group_bloom_filter(col_preds, Type, rg_first_row, rg_num_rows);
                    row_group_filtered = ret.value_or(false);
                    counter.bloom_filter_tried_counter++;
                    counter.bloom_filter_success_counter += row_group_filtered;
                }

                if (row_group_filtered) {
                    cur_row_ranges.clear();
                } else {
                    cur_row_ranges.add({rg_first_row, rg_first_row + rg_num_rows});
                }
            }

            PredicateFilterEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges);
            if (Type == CompoundNodeType::AND && row_ranges->span_size() == 0) {
                return row_ranges;
            }
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(auto cur_row_ranges_opt, child.visit(*this, Evaluator::BLOOM_FILTER));
            if (cur_row_ranges_opt.has_value()) {
                PredicateFilterEvaluatorUtils::merge_row_ranges<Type>(row_ranges, cur_row_ranges_opt.value());
            }
        }
        return row_ranges;
    }

    template <CompoundNodeType Type>
    StatusOr<std::optional<SparseRange<uint64_t>>> operator()(const PredicateCompoundNode<Type>& node) {
        return this->operator()(node, Evaluator::ALL);
    }

    template <CompoundNodeType Type>
    StatusOr<std::optional<SparseRange<uint64_t>>> operator()(const PredicateCompoundNode<Type>& node, Evaluator mode) {
        DCHECK(group_reader != nullptr);
        bool row_group_filtered = false;
        std::optional<SparseRange<uint64_t>> row_ranges;
        if (mode & Evaluator::ROWGROUP_ZONEMAP) {
            auto ret = visit_for_rowgroup_zonemap(node);
            if (ret.ok() && ret.value().has_value()) {
                row_group_filtered = ret.value()->span_size() == 0;
                row_ranges = std::move(ret.value());
            }
        }
        if (mode & Evaluator::PAGE_INDEX_ZONEMAP) {
            if (enable_page_index && !row_group_filtered) {
                auto ret = visit_for_page_index(node);
                if (ret.ok() && ret.value().has_value()) {
                    row_group_filtered = ret.value()->span_size() == 0;
                    row_ranges = std::move(ret.value());
                }
            }
        }
        if (mode & Evaluator::BLOOM_FILTER) {
            if (enable_bloom_filter && !row_group_filtered) {
                if (mode != Evaluator::BLOOM_FILTER) {
                    row_ranges_before_bf = row_ranges;
                }
                auto ret = visit_for_bloom_filter(node);
                if (ret.ok() && ret.value().has_value()) {
                    row_group_filtered = ret.value()->span_size() == 0;
                    if (row_group_filtered) {
                        row_ranges = std::move(ret.value());
                    }
                }
            }
        }
        return row_ranges;
    }

    const PredicateTree& pred_tree;
    GroupReader* group_reader;
    bool enable_page_index;
    bool enable_bloom_filter;
    OptimizationCounter counter;
    std::optional<SparseRange<uint64_t>> row_ranges_before_bf = std::nullopt;
};

} // namespace starrocks::parquet
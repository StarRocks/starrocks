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

#include "storage/rowset/bitmap_index_evaluator.h"

#include "storage/chunk_helper.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/roaring2range.h"

namespace starrocks {

/// Initialize bitmap index iterators for the columns related to a PredicateTree.
/// `parent->_ctx.is_node_support_bitmap` will be updated to indicate whether a predicate node can be applied bitmap to.
/// Return true if the PredicateTree can be applied bitmap to.
struct BitmapIndexInitializer {
    StatusOr<bool> operator()(const PredicateColumnNode& node, BitmapContext::CompoundNodeContext* parent_node_ctx,
                              CompoundNodeType parent_type) const {
        DCHECK(parent_node_ctx != nullptr);

        const auto* col_pred = node.col_pred();
        const auto cid = col_pred->column_id();

        if (!col_pred->support_bitmap_filter()) {
            return false;
        }

        auto* bitmap_iter = parent->_bitmap_index_iterators[cid];
        if (bitmap_iter == nullptr) {
            ASSIGN_OR_RETURN(bitmap_iter, supplier(cid));
            parent->_bitmap_index_iterators[cid] = bitmap_iter;
        }

        if (bitmap_iter == nullptr) {
            return false;
        }

        parent->_ctx.is_node_support_bitmap[&node] = true;
        auto& col_ctx = _get_or_create_column_context(bitmap_iter, cid, parent_node_ctx, parent_type);
        col_ctx.nodes.emplace_back(&node);
        return true;
    }

    template <CompoundNodeType Type>
    StatusOr<bool> operator()(const PredicateCompoundNode<Type>& node, BitmapContext::CompoundNodeContext*,
                              CompoundNodeType) const {
        auto& node_ctx = parent->_ctx.compound_node_to_context.emplace(&node, BitmapContext::CompoundNodeContext{})
                                 .first->second;
        bool has_bitmap_index = Type == CompoundNodeType::AND ? false : true;
        for (const auto& child : node.children()) {
            ASSIGN_OR_RETURN(const auto child_has_bitmap_index, child.visit(*this, &node_ctx, Type));
            if constexpr (Type == CompoundNodeType::AND) {
                has_bitmap_index |= child_has_bitmap_index;
            } else {
                if (!child_has_bitmap_index) {
                    has_bitmap_index = false;
                    break;
                }
            }
        }
        parent->_ctx.is_node_support_bitmap[&node] = has_bitmap_index;
        return has_bitmap_index;
    }

    static BitmapContext::ColumnContext& _get_or_create_column_context(
            const BitmapIndexIterator* bitmap_iter, ColumnId cid, BitmapContext::CompoundNodeContext* parent_node_ctx,
            CompoundNodeType parent_type) {
        auto it = parent_node_ctx->col_contexts.find(cid);
        if (it == parent_node_ctx->col_contexts.end()) {
            const auto cardinality = bitmap_iter->bitmap_nums();
            if (parent_type == CompoundNodeType::AND) {
                it = parent_node_ctx->col_contexts
                             .emplace(cid, BitmapContext::ColumnContext{SparseRange<>{0, cardinality}, cardinality})
                             .first;
            } else {
                it = parent_node_ctx->col_contexts
                             .emplace(cid, BitmapContext::ColumnContext{SparseRange<>{}, cardinality})
                             .first;
            }
        }
        return it->second;
    }

    BitmapIndexEvaluator* parent;
    const BitmapIndexEvaluator::BitmapIndexIteratorSupplier& supplier;
};

struct BitmapIndexSeeker {
    enum class ResultType : uint8_t { ALWAYS_FALSE, ALWAYS_TRUE, NOT_USED, OK };

    StatusOr<ResultType> operator()(const PredicateAndNode& node) {
        if (!parent->_ctx.is_node_support_bitmap[&node]) {
            return ResultType::NOT_USED;
        }

        DCHECK(parent->_ctx.compound_node_to_context.find(&node) != parent->_ctx.compound_node_to_context.end());
        auto& node_ctx = parent->_ctx.compound_node_to_context[&node];

        std::vector<const PredicateBaseNode*> children_to_erase;
        size_t num_always_true_child = 0;
        size_t num_not_used_children = 0;

        size_t mul_selected = 1;
        size_t mul_cardinality = 1;
        std::vector<ColumnId> cid_to_erase;
        bool need_estimate_selectivity = false;

        for (const auto& [cid, col_ctx] : node_ctx.col_contexts) {
            for (const auto* col_node : col_ctx.nodes) {
                ASSIGN_OR_RETURN(const auto used, _seek_column_node<CompoundNodeType::AND>(*col_node, node_ctx));
                if (used) {
                    children_to_erase.emplace_back(col_node);
                } else { // NOT_USED
                    num_not_used_children++;
                }
            }

            // ALWAYS_FALSE
            if (col_ctx.bitmap_ranges.empty()) {
                parent->_ctx.nodes_to_erase.emplace(&node);
                return ResultType::ALWAYS_FALSE;
            }

            // ALWAYS_TRUE
            if (col_ctx.bitmap_ranges.span_size() >= col_ctx.cardinality) {
                cid_to_erase.emplace_back(cid);
                num_always_true_child += col_ctx.nodes.size();
            } else {
                // OK
                need_estimate_selectivity = true;
                mul_selected *= col_ctx.bitmap_ranges.span_size();
                mul_cardinality *= col_ctx.cardinality;
            }
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(const auto res_type, child.visit(*this));
            switch (res_type) {
            case ResultType::ALWAYS_FALSE:
                parent->_ctx.nodes_to_erase.emplace(&node);
                return ResultType::ALWAYS_FALSE;
            case ResultType::ALWAYS_TRUE:
                children_to_erase.emplace_back(&child);
                num_always_true_child++;
                break;
            case ResultType::NOT_USED:
                num_not_used_children++;
                break;
            case ResultType::OK:
                // Do nothing.
                break;
            }
        }

        const size_t num_children = node.num_children();
        if (num_not_used_children == num_children) {
            return ResultType::NOT_USED;
        }

        if (num_always_true_child == num_children) {
            parent->_ctx.nodes_to_erase.emplace(&node);
            return ResultType::ALWAYS_TRUE;
        }

        for (const auto& cid : cid_to_erase) {
            node_ctx.col_contexts.erase(cid);
        }

        // ---------------------------------------------------------
        // Estimate the selectivity of the bitmap index.
        // ---------------------------------------------------------
        if (num_always_true_child + num_not_used_children >= num_children ||
            (need_estimate_selectivity && mul_selected * 1000 > mul_cardinality * config::bitmap_max_filter_ratio)) {
            return ResultType::NOT_USED;
        }

        parent->_ctx.nodes_to_erase.insert(children_to_erase.begin(), children_to_erase.end());

        node_ctx.used = true;
        return ResultType::OK;
    }

    StatusOr<ResultType> operator()(const PredicateOrNode& node) {
        if (!parent->_ctx.is_node_support_bitmap[&node]) {
            return ResultType::NOT_USED;
        }

        DCHECK(parent->_ctx.compound_node_to_context.find(&node) != parent->_ctx.compound_node_to_context.end());
        auto& node_ctx = parent->_ctx.compound_node_to_context[&node];

        std::vector<const PredicateBaseNode*> children_to_erase;
        size_t num_always_false_child = 0;
        bool has_not_used_child = false;

        size_t mul_selected = 1;
        size_t mul_cardinality = 1;
        std::vector<ColumnId> cid_to_erase;
        bool need_estimate_selectivity = false;

        for (const auto& [cid, col_ctx] : node_ctx.col_contexts) {
            for (const auto* col_node : col_ctx.nodes) {
                ASSIGN_OR_RETURN(const auto used, _seek_column_node<CompoundNodeType::OR>(*col_node, node_ctx));
                if (used) {
                    children_to_erase.emplace_back(col_node);
                } else { // NOT_USED
                    has_not_used_child = true;
                }
            }

            // ALWAYS_FALSE
            if (col_ctx.bitmap_ranges.empty()) {
                cid_to_erase.emplace_back(cid);
                num_always_false_child += col_ctx.nodes.size();
                continue;
            }

            // ALWAYS_TRUE
            if (col_ctx.bitmap_ranges.span_size() >= col_ctx.cardinality) {
                parent->_ctx.nodes_to_erase.emplace(&node);
                return ResultType::ALWAYS_TRUE;
            } else {
                // OK
                need_estimate_selectivity = true;
                mul_selected *= col_ctx.bitmap_ranges.span_size();
                mul_cardinality *= col_ctx.cardinality;
            }
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(const auto res_type, child.visit(*this));
            switch (res_type) {
            case ResultType::ALWAYS_FALSE:
                children_to_erase.emplace_back(&child);
                num_always_false_child++;
                break;
            case ResultType::ALWAYS_TRUE:
                parent->_ctx.nodes_to_erase.emplace(&node);
                return ResultType::ALWAYS_TRUE;
            case ResultType::NOT_USED:
                has_not_used_child = true;
                break;
            case ResultType::OK:
                // Do nothing.
                break;
            }
        }

        if (has_not_used_child) {
            return ResultType::NOT_USED;
        }

        const size_t num_children = node.num_children();
        if (num_always_false_child == num_children) {
            parent->_ctx.nodes_to_erase.emplace(&node);
            return ResultType::ALWAYS_FALSE;
        }

        for (const auto& cid : cid_to_erase) {
            node_ctx.col_contexts.erase(cid);
        }

        // ---------------------------------------------------------
        // Estimate the selectivity of the bitmap index.
        // ---------------------------------------------------------
        if (num_always_false_child >= num_children ||
            (need_estimate_selectivity && mul_selected * 1000 > mul_cardinality * config::bitmap_max_filter_ratio)) {
            return ResultType::NOT_USED;
        }

        parent->_ctx.nodes_to_erase.insert(children_to_erase.begin(), children_to_erase.end());

        node_ctx.used = true;
        return ResultType::OK;
    }

    template <CompoundNodeType Type>
    StatusOr<bool> _seek_column_node(const PredicateColumnNode& node,
                                     BitmapContext::CompoundNodeContext& parent_node_ctx) const {
        if (!parent->_ctx.is_node_support_bitmap[&node]) {
            return false;
        }

        const auto* col_pred = node.col_pred();
        const auto cid = col_pred->column_id();
        auto* bitmap_iter = parent->_bitmap_index_iterators[cid];
        if (bitmap_iter == nullptr) {
            return false;
        }

        DCHECK(parent_node_ctx.col_contexts.find(cid) != parent_node_ctx.col_contexts.end());
        auto& col_ctx = parent_node_ctx.col_contexts.find(cid)->second;

        SparseRange<> r;
        const Status st = col_pred->seek_bitmap_dictionary(bitmap_iter, &r);
        if (st.ok()) {
            if constexpr (Type == CompoundNodeType::AND) {
                col_ctx.bitmap_ranges &= r;
            } else {
                col_ctx.bitmap_ranges |= r;
            }
            col_ctx.has_is_null_pred |= (col_pred->type() == PredicateType::kIsNull);
        } else if (st.is_cancelled()) {
            return false;
        } else {
            return st;
        }

        return true;
    }

    BitmapIndexEvaluator* parent;
};

struct BitmapIndexRetriver {
    template <CompoundNodeType Type>
    StatusOr<std::optional<Roaring>> operator()(const PredicateCompoundNode<Type>& node) const {
        std::optional<Roaring> result_roaring;

        auto it = parent->_ctx.compound_node_to_context.find(&node);
        if (it == parent->_ctx.compound_node_to_context.end() || !it->second.used) {
            return result_roaring;
        }
        const auto& node_ctx = it->second;

        for (const auto& [cid, col_ctx] : node_ctx.col_contexts) {
            auto* bitmap_iter = parent->_bitmap_index_iterators[cid];

            Roaring roaring;
            RETURN_IF_ERROR(bitmap_iter->read_union_bitmap(col_ctx.bitmap_ranges, &roaring));
            if (bitmap_iter->has_null_bitmap() && !col_ctx.has_is_null_pred) {
                Roaring null_bitmap;
                RETURN_IF_ERROR(bitmap_iter->read_null_bitmap(&null_bitmap));
                roaring -= null_bitmap;
            }

            _merge_roaring<Type>(result_roaring, roaring);
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(auto roaring, child.visit(*this));
            if (!roaring.has_value()) {
                continue;
            }
            _merge_roaring<Type>(result_roaring, roaring.value());
        }

        return result_roaring;
    }

    template <CompoundNodeType Type>
    void _merge_roaring(std::optional<Roaring>& result_roaring, Roaring& roaring) const {
        if (!result_roaring.has_value()) {
            result_roaring = std::move(roaring);
        } else {
            if constexpr (Type == CompoundNodeType::AND) {
                result_roaring.value() &= roaring;
            } else {
                result_roaring.value() |= roaring;
            }
        }
    }

    BitmapIndexEvaluator* parent;
};

struct BitmapIndexPredicateEraser {
    template <CompoundNodeType ParentType>
    void operator()(const PredicateColumnNode& node, PredicateCompoundNode<ParentType>& parent_node) const {
        if (!parent->_ctx.nodes_to_erase.contains(&node)) {
            parent_node.add_child(node);
        }
    }

    template <CompoundNodeType Type, CompoundNodeType ParentType>
    void operator()(const PredicateCompoundNode<Type>& node, PredicateCompoundNode<ParentType>& parent_node) const {
        if (parent->_ctx.nodes_to_erase.contains(&node)) {
            return;
        }

        PredicateCompoundNode<Type> new_node;
        for (const auto& child : node.children()) {
            child.visit(*this, new_node);
        }

        if (!new_node.empty()) {
            parent_node.add_child(new_node);
        }
    }

    BitmapIndexEvaluator* parent;
};

void BitmapIndexEvaluator::close() {
    if (_closed) {
        return;
    }
    _closed = true;

    for (const auto* bitmap_iter : _bitmap_index_iterators) {
        delete bitmap_iter;
    }
    _bitmap_index_iterators.clear();
}

Status BitmapIndexEvaluator::init(BitmapIndexIteratorSupplier&& supplier) {
    _bitmap_index_iterators.resize(ChunkHelper::max_column_id(_schema) + 1, nullptr);
    ASSIGN_OR_RETURN(_has_bitmap_index,
                     _pred_tree.visit(BitmapIndexInitializer{this, supplier}, nullptr, CompoundNodeType::AND));
    return Status::OK();
}

Status BitmapIndexEvaluator::evaluate(SparseRange<>& dst_scan_range, PredicateTree& dst_pred_tree) {
    // ---------------------------------------------------------
    // Seek bitmap index.
    //  - Seek to the position of predicate's operand within
    //    bitmap index dictionary.
    // ---------------------------------------------------------
    ASSIGN_OR_RETURN(const auto seek_res, _pred_tree.visit(BitmapIndexSeeker{this}));
    switch (seek_res) {
    case BitmapIndexSeeker::ResultType::ALWAYS_FALSE:
        dst_scan_range.clear();
        return Status::OK();
    case BitmapIndexSeeker::ResultType::ALWAYS_TRUE:
        dst_pred_tree = PredicateTree{};
        return Status::OK();
    case BitmapIndexSeeker::ResultType::NOT_USED:
        return Status::OK();
    case BitmapIndexSeeker::ResultType::OK:
        // Do nothing.
        break;
    }

    // ---------------------------------------------------------
    // Retrieve the bitmap of each field.
    // ---------------------------------------------------------
    Roaring row_bitmap = range2roaring(dst_scan_range);
    const size_t input_rows = row_bitmap.cardinality();
    DCHECK_EQ(input_rows, dst_scan_range.span_size());

    ASSIGN_OR_RETURN(auto roaring, _pred_tree.visit(BitmapIndexRetriver{this}));
    if (!roaring.has_value()) {
        return Status::OK();
    }
    row_bitmap &= roaring.value();

    DCHECK_LE(row_bitmap.cardinality(), dst_scan_range.span_size());
    if (row_bitmap.cardinality() < dst_scan_range.span_size()) {
        dst_scan_range = roaring2range(row_bitmap);
    }

    // ---------------------------------------------------------
    // Erase predicates that hit bitmap index.
    // ---------------------------------------------------------
    PredicateAndNode new_pred_root;
    _pred_tree.visit(BitmapIndexPredicateEraser{this}, new_pred_root);
    dst_pred_tree = PredicateTree::create(std::move(new_pred_root));

    return Status::OK();
}

} // namespace starrocks

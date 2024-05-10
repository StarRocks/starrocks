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

#include "gutil/strings/substitute.h"
#include "simd/simd.h"
#include "storage/predicate_tree/predicate_tree.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// PredicateCompoundNode
// ------------------------------------------------------------------------------------

template <CompoundNodeType Type>
ConstPredicateNodeIterator<PredicateCompoundNode<Type>::DualType> PredicateCompoundNode<Type>::children() const {
    return {_col_children_map.begin(), _col_children_map.end(), _compound_children.begin(), _compound_children.end()};
}
template <CompoundNodeType Type>
MutablePredicateNodeIterator<PredicateCompoundNode<Type>::DualType> PredicateCompoundNode<Type>::children() {
    return {_col_children_map.begin(), _col_children_map.end(), _compound_children.begin(), _compound_children.end()};
}

template <CompoundNodeType Type>
std::string PredicateCompoundNode<Type>::debug_string() const {
    std::stringstream ss;
    if constexpr (Type == CompoundNodeType::AND) {
        ss << "{\"and\":[";
    } else {
        ss << "{\"or\":[";
    }

    const auto num_total_children = num_children();
    size_t count_children = 0;
    for (const auto& child : children()) {
        ss << child.visit([](const auto& node) { return node.debug_string(); });
        if (++count_children < num_total_children) {
            ss << ",";
        }
    }

    ss << "]}";
    return ss.str();
}

template <CompoundNodeType Type>
template <CompoundNodeType ChildType>
void PredicateCompoundNode<Type>::add_child(PredicateCompoundNode<ChildType>&& child) {
    if constexpr (Type == ChildType) {
        for (auto grand_child_var : child.children()) {
            grand_child_var.visit([&](auto& grand_child) { add_child(std::move(grand_child)); });
        }
    } else {
        _compound_children.emplace_back(std::move(child));
    }
}

template <CompoundNodeType Type>
template <CompoundNodeType ChildType>
void PredicateCompoundNode<Type>::add_child(const PredicateCompoundNode<ChildType>& child) {
    if constexpr (Type == ChildType) {
        for (const auto grand_child_var : child.children()) {
            grand_child_var.visit([&](const auto& grand_child) { add_child(grand_child); });
        }
    } else {
        _compound_children.emplace_back(child);
    }
}

template <CompoundNodeType Type>
void PredicateCompoundNode<Type>::add_child(PredicateColumnNode&& child) {
    _col_children_map[child.col_pred()->column_id()].emplace_back(std::move(child));
}

template <CompoundNodeType Type>
void PredicateCompoundNode<Type>::add_child(const PredicateColumnNode& child) {
    _col_children_map[child.col_pred()->column_id()].emplace_back(child);
}

template <CompoundNodeType Type>
size_t PredicateCompoundNode<Type>::num_children() const {
    auto cnt = _compound_children.size();
    for (const auto& [_, col_children] : _col_children_map) {
        cnt += col_children.size();
    }
    return cnt;
}
template <CompoundNodeType Type>
bool PredicateCompoundNode<Type>::empty() const {
    return _col_children_map.empty() && _compound_children.empty();
}

template <CompoundNodeType Type>
template <typename Vistor>
void PredicateCompoundNode<Type>::partition_copy(Vistor&& cond, PredicateAndNode* true_pred_tree,
                                                 PredicateAndNode* false_pred_tree) const {
    for (const auto& child_var : children()) {
        if (cond(child_var)) {
            child_var.visit([&](const auto& child) { true_pred_tree->add_child(child); });
        } else {
            child_var.visit([&](const auto& child) { false_pred_tree->add_child(child); });
        }
    }
}

template <CompoundNodeType Type>
template <typename Vistor>
void PredicateCompoundNode<Type>::partition_move(Vistor&& cond, PredicateAndNode* true_pred_tree,
                                                 PredicateAndNode* false_pred_tree) {
    for (auto child_var : children()) {
        if (cond(child_var)) {
            child_var.visit([&](auto& child) { true_pred_tree->add_child(std::move(child)); });
        } else {
            child_var.visit([&](auto& child) { false_pred_tree->add_child(std::move(child)); });
        }
    }
}

template <>
inline Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                     uint8_t* selection, uint16_t from,
                                                                     uint16_t to) const;
template <>
inline Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_and(CompoundNodeContexts& contexts,
                                                                         const Chunk* chunk, uint8_t* selection,
                                                                         uint16_t from, uint16_t to) const;
template <>
inline Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_or(CompoundNodeContexts& contexts,
                                                                        const Chunk* chunk, uint8_t* selection,
                                                                        uint16_t from, uint16_t to) const;
template <>
inline Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                    uint8_t* selection, uint16_t from,
                                                                    uint16_t to) const;
template <>
inline Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_and(CompoundNodeContexts& contexts,
                                                                        const Chunk* chunk, uint8_t* selection,
                                                                        uint16_t from, uint16_t to) const;
template <>
inline Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_or(CompoundNodeContexts& contexts,
                                                                       const Chunk* chunk, uint8_t* selection,
                                                                       uint16_t from, uint16_t to) const;

// ------------------------------------------------------------------------------------
// PredicateAndNode
// ------------------------------------------------------------------------------------

template <>
inline Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                     uint8_t* selection, uint16_t from,
                                                                     uint16_t to) const {
    const auto num_rows = to - from;
    if (empty()) {
        memset(selection + from, 1, num_rows);
        return Status::OK();
    }

    auto& node_ctx = contexts[id()];

    if (!node_ctx.and_context.has_value()) {
        auto& ctx = node_ctx.and_context.emplace();
        const auto num_total_children = num_children();
        ctx.non_vec_children.reserve(num_total_children);
        ctx.vec_children.reserve(num_total_children);

        for (const auto& [_, col_children] : _col_children_map) {
            for (const auto& child : col_children) {
                if (!child.col_pred()->can_vectorized()) {
                    ctx.non_vec_children.emplace_back(&child);
                } else {
                    ctx.vec_children.emplace_back(&child);
                }
            }
        }
        for (const auto& child : _compound_children) {
            ctx.vec_children.emplace_back(&child);
        }
    }
    auto& ctx = node_ctx.and_context.value();

    // Evaluate vectorized predicates first.
    bool first = true;
    bool contains_true = true;
    for (const auto& child : ctx.vec_children) {
        if (first) {
            first = false;
            RETURN_IF_ERROR(
                    child.visit([&](const auto& pred) { return pred.evaluate(contexts, chunk, selection, from, to); }));
        } else {
            RETURN_IF_ERROR(child.visit(
                    [&](const auto& pred) { return pred.evaluate_and(contexts, chunk, selection, from, to); }));
        }

        contains_true = SIMD::count_nonzero(selection + from, num_rows);
        if (!contains_true) {
            break;
        }
    }

    // Evaluate non-vectorized predicates using evaluate_branchless.
    if (contains_true && !ctx.non_vec_children.empty()) {
        auto& selected_idx_buffer = node_ctx.selected_idx_buffer;
        if (UNLIKELY(selected_idx_buffer.size() < to)) {
            selected_idx_buffer.resize(to);
        }
        auto* selected_idx = selected_idx_buffer.data();

        uint16_t selected_size = 0;
        if (first) {
            // When there is no any vectorized predicate, should initialize selected_idx in a vectorized way.
            selected_size = to - from;
            for (uint16_t i = from, j = 0; i < to; ++i, ++j) {
                selected_idx[j] = i;
            }
        } else {
            for (uint16_t i = from; i < to; ++i) {
                selected_idx[selected_size] = i;
                selected_size += selection[i];
            }
        }

        for (const auto& col_pred : ctx.non_vec_children) {
            ASSIGN_OR_RETURN(selected_size, col_pred->evaluate_branchless(chunk, selected_idx, selected_size));
            if (selected_size == 0) {
                break;
            }
        }

        memset(&selection[from], 0, to - from);
        for (uint16_t i = 0; i < selected_size; ++i) {
            selection[selected_idx[i]] = 1;
        }
    }

    return Status::OK();
}

template <>
inline Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_and(CompoundNodeContexts& contexts,
                                                                         const Chunk* chunk, uint8_t* selection,
                                                                         uint16_t from, uint16_t to) const {
    for (const auto& child : children()) {
        RETURN_IF_ERROR(
                child.visit([&](const auto& pred) { return pred.evaluate_and(contexts, chunk, selection, from, to); }));
    }
    return Status::OK();
}

template <>
inline Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_or(CompoundNodeContexts& contexts,
                                                                        const Chunk* chunk, uint8_t* selection,
                                                                        uint16_t from, uint16_t to) const {
    if (empty()) {
        return Status::OK();
    }

    auto& node_ctx = contexts[id()];
    auto& selection_buffer = node_ctx.selection_buffer;

    if (UNLIKELY(selection_buffer.size() < to)) {
        selection_buffer.resize(to);
    }
    auto* or_selection = selection_buffer.data();

    RETURN_IF_ERROR(evaluate(contexts, chunk, or_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] |= or_selection[i];
    }

    return Status::OK();
}

// ------------------------------------------------------------------------------------
// PredicateOrNode
// ------------------------------------------------------------------------------------

template <>
inline Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                    uint8_t* selection, uint16_t from,
                                                                    uint16_t to) const {
    const auto num_rows = to - from;
    if (empty()) {
        memset(selection + from, 1, num_rows);
        return Status::OK();
    }

    bool first = true;
    for (const auto& child : children()) {
        if (first) {
            first = false;
            RETURN_IF_ERROR(
                    child.visit([&](const auto& pred) { return pred.evaluate(contexts, chunk, selection, from, to); }));
        } else {
            RETURN_IF_ERROR(child.visit(
                    [&](const auto& pred) { return pred.evaluate_or(contexts, chunk, selection, from, to); }));
        }

        const auto num_falses = SIMD::count_zero(selection + from, num_rows);
        if (!num_falses) {
            break;
        }
    }

    return Status::OK();
}

template <>
inline Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_and(CompoundNodeContexts& contexts,
                                                                        const Chunk* chunk, uint8_t* selection,
                                                                        uint16_t from, uint16_t to) const {
    if (empty()) {
        return Status::OK();
    }

    auto& node_ctx = contexts[id()];
    auto& selection_buffer = node_ctx.selection_buffer;

    if (UNLIKELY(selection_buffer.size() < to)) {
        selection_buffer.resize(to);
    }
    auto* and_selection = selection_buffer.data();

    RETURN_IF_ERROR(evaluate(contexts, chunk, and_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] &= and_selection[i];
    }

    return Status::OK();
}

template <>
inline Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_or(CompoundNodeContexts& contexts,
                                                                       const Chunk* chunk, uint8_t* selection,
                                                                       uint16_t from, uint16_t to) const {
    for (const auto& child : children()) {
        RETURN_IF_ERROR(
                child.visit([&](const auto& pred) { return pred.evaluate_or(contexts, chunk, selection, from, to); }));
    }
    return Status::OK();
}

// ------------------------------------------------------------------------------------
// CompoundNodeContext
// ------------------------------------------------------------------------------------

template <CompoundNodeType Type>
const ColumnPredicateMap& CompoundNodeContext::cid_to_col_preds(const PredicateCompoundNode<Type>& node) const {
    if (_cached_cid_to_col_preds.has_value()) {
        return _cached_cid_to_col_preds.value();
    }

    auto& cid_to_col_preds = _cached_cid_to_col_preds.emplace();
    for (const auto& [cid, col_children] : node.col_children_map()) {
        for (const auto& child : col_children) {
            cid_to_col_preds[cid].emplace_back(child.col_pred());
        }
    }
    return cid_to_col_preds;
}

} // namespace starrocks

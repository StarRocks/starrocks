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

} // namespace starrocks

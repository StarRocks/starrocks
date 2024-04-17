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

#include "storage/predicate_tree/predicate_tree.hpp"

namespace starrocks {

// ------------------------------------------------------------------------------------
// PredicateColumnNode
// ------------------------------------------------------------------------------------

std::string PredicateColumnNode::debug_string() const {
    return strings::Substitute(R"({"pred":"$0"})", _col_pred->debug_string());
}

// ------------------------------------------------------------------------------------
// PredicateTree
// ------------------------------------------------------------------------------------

template <CompoundNodeType Type>
static void assign_compound_node_id(PredicateCompoundNode<Type>& root, PredicateNodeId& next_id) {
    root.set_id(next_id++);
    for (auto& child : root.compound_children()) {
        assign_compound_node_id(child, next_id);
    }
}

template <CompoundNodeType Type>
static void assign_column_node_id(PredicateCompoundNode<Type>& root, PredicateNodeId& next_id) {
    for (auto& [_, col_children] : root.col_children_map()) {
        for (auto& child : col_children) {
            child.set_id(next_id++);
        }
    }
    for (auto& child : root.compound_children()) {
        assign_column_node_id(child, next_id);
    }
}

PredicateTree PredicateTree::create(PredicateAndNode&& root) {
    // The id of all the compound nodes is placed before the id of all the column nodes (leaf nodes).
    PredicateNodeId next_id = 0;

    assign_compound_node_id(root, next_id);
    const auto num_compound_nodes = next_id;

    assign_column_node_id(root, next_id);

    return PredicateTree(std::move(root), num_compound_nodes);
}

PredicateTree::PredicateTree(PredicateAndNode&& root, uint32_t num_compound_nodes)
        : _root(std::move(root)), _compound_node_contexts(num_compound_nodes) {}

template <CompoundNodeType Type>
static void collect_column_ids(const PredicateCompoundNode<Type>& node, std::unordered_set<ColumnId>& column_ids) {
    for (const auto& [cid, _] : node.col_children_map()) {
        column_ids.emplace(cid);
    }
    for (const auto& child : node.compound_children()) {
        collect_column_ids(child, column_ids);
    }
}

const std::unordered_set<ColumnId>& PredicateTree::column_ids() const {
    if (_cached_column_ids.has_value()) {
        return _cached_column_ids.value();
    }

    auto& all_column_ids = _cached_column_ids.emplace();
    collect_column_ids(_root, all_column_ids);
    return all_column_ids;
}

bool PredicateTree::contains_column(ColumnId cid) const {
    return column_ids().contains(cid);
}
size_t PredicateTree::num_columns() const {
    return column_ids().size();
}

template <CompoundNodeType Type>
static size_t get_size(const PredicateCompoundNode<Type>& node) {
    size_t total_size = 0;
    for (const auto& [_, col_children] : node.col_children_map()) {
        total_size += col_children.size();
    }
    for (const auto& child : node.compound_children()) {
        total_size += get_size(child);
    }
    return total_size;
}

size_t PredicateTree::size() const {
    return get_size(_root);
}

template <CompoundNodeType Type>
static size_t is_empty(const PredicateCompoundNode<Type>& node) {
    for (const auto& [_, col_children] : node.col_children_map()) {
        if (!col_children.empty()) {
            return false;
        }
    }
    for (const auto& child : node.compound_children()) {
        if (!is_empty(child)) {
            return false;
        }
    }
    return true;
}

bool PredicateTree::empty() const {
    return is_empty(_root);
}

PredicateAndNode PredicateTree::release_root() {
    return std::move(_root);
}

ColumnPredicateMap PredicateTree::get_immediate_column_predicate_map() const {
    ColumnPredicateMap col_pred_map;
    for (const auto& [cid, col_children] : _root.col_children_map()) {
        for (const auto& col_child : col_children) {
            col_pred_map[cid].emplace_back(col_child.col_pred());
        }
    }
    return col_pred_map;
}

} // namespace starrocks

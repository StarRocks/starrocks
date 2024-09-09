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

#include "gutil/strings/substitute.h"
#include "simd/simd.h"

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

// Ensure that these template class methods are specialized before they are instantiated.
// PredicateCompoundNode<CompoundNodeType::AND>::evaluate_xxx and
// PredicateCompoundNode<CompoundNodeType::OR>::evaluate_xxx will call each other, causing other template class methods
// to be instantiated when specializing a template class method, making it impossible to specialize these methods later.
// Therefore here, before specializing all evaluate_xxx methods, specialize and declare these methods first.

template <>
Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                              uint8_t* selection, uint16_t from, uint16_t to) const;
template <>
Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                  uint8_t* selection, uint16_t from, uint16_t to) const;
template <>
Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                 uint8_t* selection, uint16_t from, uint16_t to) const;
template <>
Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                             uint8_t* selection, uint16_t from, uint16_t to) const;
template <>
Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                 uint8_t* selection, uint16_t from, uint16_t to) const;
template <>
Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                uint8_t* selection, uint16_t from, uint16_t to) const;

// ------------------------------------------------------------------------------------
// PredicateAndNode
// ------------------------------------------------------------------------------------

template <>
Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                              uint8_t* selection, uint16_t from, uint16_t to) const {
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
Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                  uint8_t* selection, uint16_t from,
                                                                  uint16_t to) const {
    for (const auto& child : children()) {
        RETURN_IF_ERROR(
                child.visit([&](const auto& pred) { return pred.evaluate_and(contexts, chunk, selection, from, to); }));
    }
    return Status::OK();
}

template <>
Status PredicateCompoundNode<CompoundNodeType::AND>::evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                 uint8_t* selection, uint16_t from, uint16_t to) const {
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
Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                             uint8_t* selection, uint16_t from, uint16_t to) const {
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
Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                 uint8_t* selection, uint16_t from, uint16_t to) const {
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
Status PredicateCompoundNode<CompoundNodeType::OR>::evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk,
                                                                uint8_t* selection, uint16_t from, uint16_t to) const {
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

// ------------------------------------------------------------------------------------
// PredicateColumnNode
// ------------------------------------------------------------------------------------

Status PredicateColumnNode::evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection,
                                     uint16_t from, uint16_t to) const {
    return _col_pred->evaluate(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}
Status PredicateColumnNode::evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection,
                                         uint16_t from, uint16_t to) const {
    return _col_pred->evaluate_and(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}
Status PredicateColumnNode::evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection,
                                        uint16_t from, uint16_t to) const {
    return _col_pred->evaluate_or(chunk->get_column_by_id(_col_pred->column_id()).get(), selection, from, to);
}

StatusOr<uint16_t> PredicateColumnNode::evaluate_branchless(const Chunk* chunk, uint16_t* sel,
                                                            uint16_t sel_size) const {
    return _col_pred->evaluate_branchless(chunk->get_column_by_id(_col_pred->column_id()).get(), sel, sel_size);
}

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

Status PredicateTree::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _root.evaluate(_compound_node_contexts, chunk, selection, from, to);
}
Status PredicateTree::evaluate(const Chunk* chunk, uint8_t* selection) const {
    return evaluate(chunk, selection, 0, chunk->num_rows());
}

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

const ColumnPredicateMap& PredicateTree::get_immediate_column_predicate_map() const {
    return _compound_node_contexts[0].cid_to_col_preds(_root);
}

// ------------------------------------------------------------------------------------
// PredicateNodeIterator
// ------------------------------------------------------------------------------------

template <CompoundNodeType Type, bool Constant>
PredicateNodeIterator<Type, Constant>::PredicateNodeIterator(ColumnNodeMapIterator col_map_it,
                                                             ColumnNodeMapIterator col_map_end_it,
                                                             CompoundNodeIterator compound_it,
                                                             CompoundNodeIterator compound_end_it)
        : _col_map_it(col_map_it),
          _col_map_end_it(col_map_end_it),
          _col_it(_col_map_it != _col_map_end_it ? _col_map_it->second.begin() : ColumnNodeIterator{}),
          _col_end_it(_col_map_it != _col_map_end_it ? _col_map_it->second.end() : ColumnNodeIterator{}),
          _compound_it(compound_it),
          _compound_end_it(compound_end_it) {
    skip_empty_col_nodes();
}

template <CompoundNodeType Type, bool Constant>
auto PredicateNodeIterator<Type, Constant>::operator*() -> NodePtr {
    if (is_in_col_nodes()) {
        return NodePtr(&(*_col_it));
    } else {
        return NodePtr(&(*_compound_it));
    }
}

template <CompoundNodeType Type, bool Constant>
auto PredicateNodeIterator<Type, Constant>::operator++() -> PredicateNodeIterator& {
    if (!is_in_col_nodes()) {
        ++_compound_it;
    } else {
        if (_col_it != _col_end_it) {
            ++_col_it;
        }
        skip_empty_col_nodes();
    }
    return *this;
}

template <CompoundNodeType Type, bool Constant>
void PredicateNodeIterator<Type, Constant>::skip_empty_col_nodes() {
    if (!is_in_col_nodes()) {
        return;
    }

    while (_col_it == _col_end_it) {
        if (++_col_map_it == _col_map_end_it) {
            _col_it = ColumnNodeIterator{};
            break;
        }
        _col_it = _col_map_it->second.begin();
        _col_end_it = _col_map_it->second.end();
    }
}

// ------------------------------------------------------------------------------------
// Instantiate the templates
// ------------------------------------------------------------------------------------

#define InstantiateCompoundNode(Type)                                                                                 \
    template class PredicateCompoundNode<Type>;                                                                       \
                                                                                                                      \
    template void PredicateCompoundNode<Type>::add_child(PredicateCompoundNode<CompoundNodeType::AND>&& child);       \
    template void PredicateCompoundNode<Type>::add_child(PredicateCompoundNode<CompoundNodeType::OR>&& child);        \
    template void PredicateCompoundNode<Type>::add_child(const PredicateCompoundNode<CompoundNodeType::AND>& child);  \
    template void PredicateCompoundNode<Type>::add_child(const PredicateCompoundNode<CompoundNodeType::OR>& child);   \
                                                                                                                      \
    template class PredicateNodeIterator<Type, false>;                                                                \
    template class PredicateNodeIterator<Type, true>;                                                                 \
                                                                                                                      \
    template const ColumnPredicateMap& CompoundNodeContext::cid_to_col_preds(const PredicateCompoundNode<Type>& node) \
            const;

InstantiateCompoundNode(CompoundNodeType::AND);
InstantiateCompoundNode(CompoundNodeType::OR);

#undef InstantiateCompoundNode

} // namespace starrocks

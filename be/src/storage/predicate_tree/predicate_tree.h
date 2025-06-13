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

#include <memory>
#include <set>
#include <vector>

#include "common/overloaded.h"
#include "storage/column_predicate.h"
#include "storage/predicate_tree/predicate_tree_fwd.h"

namespace starrocks {

class ColumnPredicate;

// ------------------------------------------------------------------------------------
// Utils
// ------------------------------------------------------------------------------------

using PredicateColumnNodes = std::vector<PredicateColumnNode>;
using PredicateColumnNodeMap = std::unordered_map<ColumnId, PredicateColumnNodes>;

using PredicateNodeId = uint32_t;
using ColumnPredicates = std::vector<const ColumnPredicate*>;
using ColumnPredicateMap = std::unordered_map<ColumnId, ColumnPredicates>;

template <typename T>
concept PredicateNodeType = std::is_same_v<T, PredicateColumnNode> || std::is_same_v<T, PredicateAndNode> ||
                            std::is_same_v<T, PredicateOrNode>;

// ------------------------------------------------------------------------------------
// PredicateTreeNode Nodes
// ------------------------------------------------------------------------------------

class PredicateBaseNode {
public:
    PredicateNodeId id() const { return _id; }
    void set_id(PredicateNodeId id) { _id = id; }

protected:
    /// The id of any compound node is placed before the id of any column node.
    /// The id of each node is set after the PredicateTree is constructed.
    PredicateNodeId _id = 0;
};

template <typename Derived>
class PredicateNodeFactory : public PredicateBaseNode {
public:
    Status evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate(contexts, chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate_and(contexts, chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection) const {
        return derived()->evaluate_or(contexts, chunk, selection, 0, chunk->num_rows());
    }

    template <typename Vistor, typename... Args>
    auto visit(Vistor&& visitor, Args&&... args) const {
        return visitor(*derived(), std::forward<Args>(args)...);
    }

    template <typename Vistor, typename... Args>
    auto visit(Vistor&& visitor, Args&&... args) {
        return visitor(*derived(), std::forward<Args>(args)...);
    }

private:
    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }
};

class PredicateColumnNode final : public PredicateNodeFactory<PredicateColumnNode> {
public:
    explicit PredicateColumnNode(const ColumnPredicate* col_pred) : _col_pred(DCHECK_NOTNULL(col_pred)) {}

    Status evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                    uint16_t to) const;
    Status evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                        uint16_t to) const;
    Status evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                       uint16_t to) const;
    StatusOr<uint16_t> evaluate_branchless(const Chunk* chunk, uint16_t* sel, uint16_t sel_size) const;

    const ColumnPredicate* col_pred() const { return _col_pred; }
    void set_col_pred(const ColumnPredicate* col_pred) { _col_pred = col_pred; }

    std::string debug_string() const;

    using PredicateNodeFactory<PredicateColumnNode>::evaluate;

private:
    const ColumnPredicate* _col_pred;
};

template <CompoundNodeType Type>
struct CompoundNodeTraits;

template <>
struct CompoundNodeTraits<CompoundNodeType::AND> {
    static constexpr auto DualType = CompoundNodeType::OR;
};

template <>
struct CompoundNodeTraits<CompoundNodeType::OR> {
    static constexpr auto DualType = CompoundNodeType::AND;
};

template <CompoundNodeType Type>
class PredicateCompoundNode final : public PredicateNodeFactory<PredicateCompoundNode<Type>> {
public:
    static constexpr auto DualType = CompoundNodeTraits<Type>::DualType;

    Status evaluate(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                    uint16_t to) const;
    Status evaluate_and(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                        uint16_t to) const;
    Status evaluate_or(CompoundNodeContexts& contexts, const Chunk* chunk, uint8_t* selection, uint16_t from,
                       uint16_t to) const;

    template <CompoundNodeType ChildType>
    void add_child(PredicateCompoundNode<ChildType>&& child);
    template <CompoundNodeType ChildType>
    void add_child(const PredicateCompoundNode<ChildType>& child);

    void add_child(PredicateColumnNode&& child);
    void add_child(const PredicateColumnNode& child);

    size_t num_children() const;
    bool empty() const;

    /// Returns an iterator to traverse each PredicateColumnNode and PredicateCompoundNode child in turn.
    /// `*iterator` returns PredicateNodePtr or ConstPredicateNodePtr, which encapsulates the variant pointers
    /// of PredicateColumnNode and PredicateCompoundNode.
    ConstPredicateNodeIterator<DualType> children() const;
    MutablePredicateNodeIterator<DualType> children();

    const PredicateColumnNodeMap& col_children_map() const { return _col_children_map; }
    PredicateColumnNodeMap& col_children_map() { return _col_children_map; }
    const std::vector<PredicateCompoundNode<DualType>>& compound_children() const { return _compound_children; }
    std::vector<PredicateCompoundNode<DualType>>& compound_children() { return _compound_children; }

    std::string debug_string() const;

    template <typename Vistor>
    void partition_copy(Vistor&& cond, PredicateAndNode* true_pred_tree, PredicateAndNode* false_pred_tree) const;
    template <typename Vistor>
    void partition_move(Vistor&& cond, PredicateAndNode* true_pred_tree, PredicateAndNode* false_pred_tree);

    using PredicateNodeFactory<PredicateCompoundNode>::evaluate;

private:
    PredicateColumnNodeMap _col_children_map;
    std::vector<PredicateCompoundNode<DualType>> _compound_children;
};

// ------------------------------------------------------------------------------------
// PredicateNodeIterator
// ------------------------------------------------------------------------------------

using PredicateNodePtrVar = std::variant<PredicateColumnNode*, PredicateAndNode*, PredicateOrNode*>;
using ConstPredicateNodePtrVar =
        std::variant<const PredicateColumnNode*, const PredicateAndNode*, const PredicateOrNode*>;

/// Encapsulates the variant pointers of PredicateColumnNode and PredicateCompoundNode.
struct PredicateNodePtr {
    template <PredicateNodeType NodeType>
    explicit PredicateNodePtr(NodeType* node) : node_ptr_var(node) {}

    template <typename Vistor, typename... Args>
    auto visit(Vistor&& visitor, Args&&... args) const {
        return std::visit([&](const auto* node) { return visitor(*node, std::forward<Args>(args)...); }, node_ptr_var);
    }
    template <typename Vistor, typename... Args>
    auto visit(Vistor&& visitor, Args&&... args) {
        return std::visit([&](auto* node) { return visitor(*node, std::forward<Args>(args)...); }, node_ptr_var);
    }

    PredicateNodePtrVar node_ptr_var;
};

/// Encapsulates the variant pointers of const PredicateColumnNode and PredicateCompoundNode.
struct ConstPredicateNodePtr {
    template <PredicateNodeType NodeType>
    explicit ConstPredicateNodePtr(const NodeType* node) : node_ptr_var(node) {}

    template <typename Vistor, typename... Args>
    auto visit(Vistor&& visitor, Args&&... args) const {
        return std::visit([&](const auto* node) { return visitor(*node, std::forward<Args>(args)...); }, node_ptr_var);
    }

    ConstPredicateNodePtrVar node_ptr_var;
};

template <CompoundNodeType Type, bool Constant>
struct PredicateNodeIteratorTraits;

template <CompoundNodeType Type>
struct PredicateNodeIteratorTraits<Type, true> {
    using ColumnNodeMapIterator = PredicateColumnNodeMap::const_iterator;
    using ColumnNodeIterator = PredicateColumnNodes::const_iterator;
    using CompoundNodeIterator = typename std::vector<PredicateCompoundNode<Type>>::const_iterator;
    using NodePtr = ConstPredicateNodePtr;
};

template <CompoundNodeType Type>
struct PredicateNodeIteratorTraits<Type, false> {
    using ColumnNodeMapIterator = PredicateColumnNodeMap::iterator;
    using ColumnNodeIterator = PredicateColumnNodes::iterator;
    using CompoundNodeIterator = typename std::vector<PredicateCompoundNode<Type>>::iterator;
    using NodePtr = PredicateNodePtr;
};

/// An iterator to traverse each PredicateColumnNode and PredicateCompoundNode child in turn.
/// PredicateColumnNodes are stored in a map<ColumnId, PredicateColumnNodes>, and PredicateCompoundNodes are stored in a vector.
template <CompoundNodeType Type, bool Constant>
class PredicateNodeIterator {
public:
    using ColumnNodeMapIterator = typename PredicateNodeIteratorTraits<Type, Constant>::ColumnNodeMapIterator;
    using ColumnNodeIterator = typename PredicateNodeIteratorTraits<Type, Constant>::ColumnNodeIterator;
    using CompoundNodeIterator = typename PredicateNodeIteratorTraits<Type, Constant>::CompoundNodeIterator;
    using NodePtr = typename PredicateNodeIteratorTraits<Type, Constant>::NodePtr;

    // For supporting std::iterator_traits.
    using iterator_category = std::forward_iterator_tag;
    using value_type = NodePtr;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    PredicateNodeIterator(ColumnNodeMapIterator col_map_it, ColumnNodeMapIterator col_map_end_it,
                          CompoundNodeIterator compound_it, CompoundNodeIterator compound_end_it);

    NodePtr operator*();

    PredicateNodeIterator& operator++();

    bool operator!=(const PredicateNodeIterator& other) const { return !this->operator==(other); }

    bool operator==(const PredicateNodeIterator& other) const {
        return _col_map_it == other._col_map_it && _col_it == other._col_it && _compound_it == other._compound_it;
    }

    PredicateNodeIterator begin() { return *this; }

    PredicateNodeIterator end() { return {_col_map_end_it, _col_map_end_it, _compound_end_it, _compound_end_it}; }

private:
    /// The PredicateColumnNodes of `map<ColumnId, PredicateColumnNodes>` may be empty.
    /// This function is used to skip the empty PredicateColumnNodes.
    void skip_empty_col_nodes();
    bool is_in_col_nodes() const { return _col_map_it != _col_map_end_it; }

    ColumnNodeMapIterator _col_map_it;
    const ColumnNodeMapIterator _col_map_end_it;

    ColumnNodeIterator _col_it;
    ColumnNodeIterator _col_end_it;

    CompoundNodeIterator _compound_it;
    const CompoundNodeIterator _compound_end_it;
};

// ------------------------------------------------------------------------------------
// PredicateTree
// ------------------------------------------------------------------------------------

struct CompoundNodeContext {
    template <CompoundNodeType Type>
    const ColumnPredicateMap& cid_to_col_preds(const PredicateCompoundNode<Type>& node) const;

    std::vector<uint8_t> selection_buffer;
    std::vector<uint16_t> selected_idx_buffer;

    struct CompoundAndContext {
        std::vector<const PredicateColumnNode*> non_vec_children;
        std::vector<ConstPredicateNodePtr> vec_children;
    };
    std::optional<CompoundAndContext> and_context;

    mutable std::optional<ColumnPredicateMap> _cached_cid_to_col_preds;
};

/// Stores multiple ColumnPredicates in a tree. Internal nodes are AND or OR relations, and leaf nodes are ColumnPredicates.
/// The root node is guaranteed to be an AND relation. A specific example is as follows:
///
///                     AND
///                /   |   |      \
///               /    |   |       \
///             c3>3 c3<30 c4=1     OR
///                             /        \
///                           AND         AND
///                          /    \      /   \
///                        c1>1 c1<10   c2>2 c2<20
///
/// Provides methods and context for each node of the tree.
///
/// The PredicateTree owns all the nodes, and each node cannot be modified unless `release_root()` is called,
/// which also means that it has given up ownership of all the nodes.
///
/// All the methods are not thread-safe.
class PredicateTree {
public:
    PredicateTree() : PredicateTree(create(PredicateAndNode{})) {}
    static PredicateTree create(PredicateAndNode&& root);

    Status evaluate(const Chunk* chunk, uint8_t* selection) const;
    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    const std::unordered_set<ColumnId>& column_ids() const;
    bool contains_column(ColumnId cid) const;
    size_t num_columns() const;

    /// The number of ColumnPredicates, that is the leaf nodes, in the tree.
    size_t size() const;
    /// Whether there is no ColumnPredicates in the tree.
    bool empty() const;

    const PredicateAndNode& root() const { return _root; }
    /// Release the ownership of all the nodes.
    PredicateAndNode release_root();

    template <typename Vistor, typename... Args>
    auto visit(Vistor&& visitor, Args&&... args) const {
        return _root.visit(std::forward<Vistor>(visitor), std::forward<Args>(args)...);
    }
    template <typename Vistor, typename... Args>
    auto visit(Vistor&& visitor, Args&&... args) {
        return _root.visit(std::forward<Vistor>(visitor), std::forward<Args>(args)...);
    }

    const CompoundNodeContext& compound_node_context(size_t idx) const { return _compound_node_contexts[idx]; }
    CompoundNodeContext& compound_node_context(size_t idx) { return _compound_node_contexts[idx]; }

    /// The immediate children of the root contain ColumnPredicates and OR relations.
    /// This method get all the ColumnPredicates in the root immediate children.
    /// In this way, we can use the ColumnPredicates part where OR predicates are not supported.
    const ColumnPredicateMap& get_immediate_column_predicate_map() const;

private:
    explicit PredicateTree(PredicateAndNode&& root, uint32_t num_compound_nodes);

    PredicateAndNode _root;

    mutable std::optional<std::unordered_set<ColumnId>> _cached_column_ids;

    mutable CompoundNodeContexts _compound_node_contexts;
};

} // namespace starrocks

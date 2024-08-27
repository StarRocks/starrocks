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

#include <cstdint>

#include "column/schema.h"
#include "common/object_pool.h"
#include "runtime/global_dict/types.h"
#include "storage/column_predicate.h"
#include "storage/conjunctive_predicates.h"
#include "storage/olap_common.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/rowset/column_decoder.h"
#include "storage/rowset/column_reader.h"

namespace starrocks {

class ColumnExprPredicate;
class ColumnPredicate;
using ColumnPredicatePtr = std::unique_ptr<ColumnPredicate>;

// For dictionary columns, predicates can be rewriten
// Int columns.
// ColumnPredicateRewriter was a helper class, won't acquire any resource

// column predicate rewrite in SegmentIter->init-> rewrite stage
class ColumnPredicateRewriter {
public:
    using ColumnIterators = std::vector<std::unique_ptr<ColumnIterator>>;
    using ColumnPredicateMap = std::unordered_map<ColumnId, PredicateList>;

    ColumnPredicateRewriter(const ColumnIterators& column_iterators, const Schema& schema,
                            std::vector<uint8_t>& need_rewrite, int column_size, SparseRange<>& scan_range)
            : _column_iterators(column_iterators),
              _schema(schema),
              _need_rewrite(need_rewrite),
              _column_size(column_size),
              _scan_range(scan_range) {}

    Status rewrite_predicate(ObjectPool* pool, PredicateTree& pred_tree);

private:
    friend struct RewritePredicateTreeVisitor;

    // TODO: use pair<slice,int>
    using SortedDicts = std::vector<std::pair<std::string, int>>;
    using DictAndCodes = std::pair<ColumnPtr, ColumnPtr>;

    enum class RewriteStatus : uint8_t { ALWAYS_TRUE, ALWAYS_FALSE, UNCHANGED, CHANGED };

    /// Rewrite a column predicate.
    /// If returned status is CHANGED, a new ColumnPredicate will be created and assigned to *dest_pred.
    StatusOr<RewriteStatus> _rewrite_predicate(ObjectPool* pool, const FieldPtr& field, const ColumnPredicate* src_pred,
                                               ColumnPredicate** dest_pred);
    StatusOr<RewriteStatus> _rewrite_expr_predicate(ObjectPool* pool, const ColumnPtr& dict_column,
                                                    const ColumnPtr& code_column, bool field_nullable,
                                                    const ColumnPredicate* src_pred, ColumnPredicate** dest_pred);

    StatusOr<const SortedDicts*> _get_or_load_segment_dict(ColumnId cid);
    Status _load_segment_dict(std::vector<std::pair<std::string, int>>* dicts, ColumnIterator* iter);

    StatusOr<const DictAndCodes*> _get_or_load_segment_dict_vec(ColumnId cid, const FieldPtr& field);
    Status _load_segment_dict_vec(ColumnIterator* iter, ColumnPtr* dict_column, ColumnPtr* code_column,
                                  bool field_nullable);

    const ColumnIterators& _column_iterators;
    const Schema& _schema;
    std::vector<uint8_t>& _need_rewrite;
    const int _column_size;
    SparseRange<>& _scan_range;

    std::unordered_map<ColumnId, SortedDicts> _cid_to_sorted_dicts;
    std::unordered_map<ColumnId, DictAndCodes> _cid_to_vec_sorted_dicts;
};

// For global dictionary columns, predicates can be rewriten.
// GlobalDictPredicatesRewriter is a helper class, won't acquire any resource.
// GlobalDictPredicatesRewriter will rewrite ConjunctivePredicates in TabletScanner.
//
// TODO: refactor GlobalDictPredicatesRewriter and ColumnPredicateRewriter
class GlobalDictPredicatesRewriter {
public:
    GlobalDictPredicatesRewriter(const ColumnIdToGlobalDictMap& dict_maps)
            : GlobalDictPredicatesRewriter(dict_maps, nullptr) {}
    GlobalDictPredicatesRewriter(const ColumnIdToGlobalDictMap& dict_maps, std::vector<uint8_t>* disable_rewrite)
            : _dict_maps(dict_maps), _disable_dict_rewrite(disable_rewrite) {}

    Status rewrite_predicate(ObjectPool* pool, ConjunctivePredicates& predicates);

    Status rewrite_predicate(ObjectPool* pool, PredicateTree& pred_tree);

private:
    friend struct GlobalDictPredicateTreeVisitor;

    /// Rewrites a single predicate.
    /// Returns the rewritten predicate or an error status.
    /// If the pred needn't be rewritten, return nullptr.
    StatusOr<ColumnPredicatePtr> _rewrite_predicate(const ColumnPredicate* pred, std::vector<uint8_t>& selection);

    bool _column_need_rewrite(ColumnId cid) {
        if (_disable_dict_rewrite && (*_disable_dict_rewrite)[cid]) return false;
        return _dict_maps.count(cid);
    }

    const ColumnIdToGlobalDictMap& _dict_maps;
    std::vector<uint8_t>* _disable_dict_rewrite;
};

// For zone map index, some predicates can not be used directly,
// but they can be used by rewriting them into equivalent monotonic functions.
// ZonemapPredicatesRewriter is to implement the rewriting strategy.
// It will only be used before performing segment-level and page-level zone map filters
class ZonemapPredicatesRewriter {
public:
    using ColumnPredicates = std::vector<const ColumnPredicate*>;
    using ColumnPredicateMap = std::unordered_map<ColumnId, ColumnPredicates>;

    static Status rewrite_predicate_tree(ObjectPool* pool, const PredicateTree& src_pred_tree,
                                         PredicateTree& dst_pred_tree);

private:
    friend struct ZonemapPredicatesRewriterVisitor;

    template <CompoundNodeType ParentType>
    static Status _rewrite_predicate(ObjectPool* pool, const ColumnPredicate* src_pred,
                                     PredicateCompoundNode<ParentType>& dst_node);
    static Status _rewrite_column_expr_predicate(ObjectPool* pool, const ColumnPredicate* src_pred,
                                                 std::vector<const ColumnExprPredicate*>& dst_preds);
};

} // namespace starrocks

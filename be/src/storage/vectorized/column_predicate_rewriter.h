// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstdint>

#include "column/schema.h"
#include "common/object_pool.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_decoder.h"
#include "storage/rowset/column_reader.h"
#include "storage/vectorized/column_predicate.h"
#include "storage/vectorized/conjunctive_predicates.h"

namespace starrocks::vectorized {
class ColumnExprPredicate;

// For dictionary columns, predicates can be rewriten
// Int columns.
// ColumnPredicateRewriter was a helper class, won't acquire any resource

// column predicate rewrite in SegmentIter->init-> rewrite stage
class ColumnPredicateRewriter {
public:
    using ColumnIterators = std::vector<ColumnIterator*>;
    using PushDownPredicates = std::unordered_map<ColumnId, PredicateList>;

    ColumnPredicateRewriter(ColumnIterators& column_iterators, PushDownPredicates& pushdown_predicates,
                            const Schema& schema, std::vector<uint8_t>& need_rewrite, int column_size,
                            SparseRange& scan_range)
            : _column_iterators(column_iterators),
              _predicates(pushdown_predicates),
              _schema(schema),
              _need_rewrite(need_rewrite),
              _column_size(column_size),
              _scan_range(scan_range) {}

    void rewrite_predicate(ObjectPool* pool);

private:
    bool _rewrite_predicate(ObjectPool* pool, const FieldPtr& field);
    bool _rewrite_expr_predicate(ObjectPool* pool, const ColumnPredicate*, const ColumnPtr& dict_column,
                                 const ColumnPtr& code_column, bool field_nullable, ColumnPredicate** ptr);
    void _get_segment_dict(std::vector<std::pair<std::string, int>>* dicts, ColumnIterator* iter);
    void _get_segment_dict_vec(ColumnIterator* iter, ColumnPtr* dict_column, ColumnPtr* code_column,
                               bool field_nullable);

    ColumnIterators& _column_iterators;
    PushDownPredicates& _predicates;
    const Schema& _schema;
    std::vector<uint8_t>& _need_rewrite;
    const int _column_size;
    SparseRange& _scan_range;
};

// For global dictionary columns, predicates can be rewriten
// ConjunctivePredicatesRewriter was a helper class, won't acquire any resource
// ConjunctivePredicatesRewriter will rewrite ConjunctivePredicates in TabletScanner
//
// TODO: refactor ConjunctivePredicatesRewriter and ColumnPredicateRewriter
class ConjunctivePredicatesRewriter {
public:
    ConjunctivePredicatesRewriter(ConjunctivePredicates& predicates, const ColumnIdToGlobalDictMap& dict_maps)
            : ConjunctivePredicatesRewriter(predicates, dict_maps, nullptr) {}
    ConjunctivePredicatesRewriter(ConjunctivePredicates& predicates, const ColumnIdToGlobalDictMap& dict_maps,
                                  std::vector<uint8_t>* disable_rewrite)
            : _predicates(predicates), _dict_maps(dict_maps), _disable_dict_rewrite(disable_rewrite) {}

    void rewrite_predicate(ObjectPool* pool);

    bool column_need_rewrite(ColumnId cid) {
        if (_disable_dict_rewrite && (*_disable_dict_rewrite)[cid]) return false;
        return _dict_maps.count(cid);
    }

private:
    ConjunctivePredicates& _predicates;
    const ColumnIdToGlobalDictMap& _dict_maps;
    std::vector<uint8_t>* _disable_dict_rewrite;
};

// For zone map index, some predicates can not be used directly,
// but they can be used by rewriting them into equivalent monotonic functions.
// ZonemapPredicatesRewriter is to implement the rewriting strategy.
// It will only be used before performing segment-level and page-level zone map filters
class ZonemapPredicatesRewriter {
public:
    using PredicateList = std::vector<const ColumnPredicate*>;

    static Status rewrite_predicate_map(ObjectPool* pool, const std::unordered_map<ColumnId, PredicateList>& src,
                                        std::unordered_map<ColumnId, PredicateList>* dst);

    static Status rewrite_predicate_list(ObjectPool* pool, const PredicateList& src, PredicateList* dst);

private:
    static Status _rewrite_column_expr_predicates(ObjectPool* pool, const ColumnPredicate* pred,
                                                  std::vector<const ColumnExprPredicate*>* new_preds);
};

} // namespace starrocks::vectorized

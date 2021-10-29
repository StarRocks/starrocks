// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstdint>

#include "column/schema.h"
#include "common/object_pool.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_v2/column_reader.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks::vectorized {
// For dictionary columns, predicates can be rewriten
// Int columns.
// ColumnPredicateRewriter was a helper class, won't acquire any resource

// column predicate rewrite in SegmentIter->init-> rewrite stage
class ColumnPredicateRewriter {
public:
    using ColumnIterators = std::vector<segment_v2::ColumnIterator*>;
    using PushDownPredicates = std::unordered_map<ColumnId, PredicateList>;

    ColumnPredicateRewriter(ColumnIterators& column_iterators, PushDownPredicates& pushdown_predicates, Schema& schema,
                            std::vector<uint8_t>& need_rewrite, int predicate_column_size, SparseRange& scan_range)
            : _column_iterators(column_iterators),
              _predicates(pushdown_predicates),
              _schema(schema),
              _predicate_need_rewrite(need_rewrite),
              _predicate_column_size(predicate_column_size),
              _scan_range(scan_range) {}

    void rewrite_predicate(ObjectPool* pool);

private:
    bool _rewrite_predicate(ObjectPool* pool, const FieldPtr& field);
    void get_segment_dict(std::vector<std::pair<std::string, int>>* dicts, segment_v2::ColumnIterator* iter);

    ColumnIterators& _column_iterators;
    PushDownPredicates& _predicates;
    Schema& _schema;
    std::vector<uint8_t>& _predicate_need_rewrite;
    int _predicate_column_size;
    SparseRange& _scan_range;
};
} // namespace starrocks::vectorized
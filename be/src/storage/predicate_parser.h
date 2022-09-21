// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

namespace starrocks {

class TabletSchema;
class TCondition;
class ExprContext;
class SlotDescriptor;
class RuntimeState;

namespace vectorized {

class ColumnPredicate;

class PredicateParser {
public:
    explicit PredicateParser(const TabletSchema& schema) : _schema(schema) {}

    bool can_pushdown(const ColumnPredicate* predicate) const;

    bool can_pushdown(const SlotDescriptor* slot_desc) const;

    // Parse |condition| into a predicate that can be pushed down.
    // return nullptr if parse failed.
    ColumnPredicate* parse_thrift_cond(const TCondition& condition) const;

    ColumnPredicate* parse_expr_ctx(const SlotDescriptor& slot_desc, RuntimeState*, ExprContext* expr_ctx) const;

private:
    const TabletSchema& _schema;
};

} // namespace vectorized
} // namespace starrocks
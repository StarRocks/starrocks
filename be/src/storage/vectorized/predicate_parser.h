// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <string>

namespace starrocks {

class TabletSchema;
class TCondition;

namespace vectorized {

class ColumnPredicate;

class PredicateParser {
public:
    explicit PredicateParser(const TabletSchema& schema) : _schema(schema) {}

    bool can_pushdown(const ColumnPredicate* predicate) const;

    // Parse |condition| into a predicate that can be pushed down.
    // return nullptr if parse failed.
    ColumnPredicate* parse(const TCondition& condition) const;

private:
    const TabletSchema& _schema;
};

} // namespace vectorized
} // namespace starrocks
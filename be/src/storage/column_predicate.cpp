// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <ostream>

#include "storage/vectorized_column_predicate.h"

namespace starrocks::vectorized {

std::ostream& operator<<(std::ostream& os, PredicateType p) {
    switch (p) {
    case PredicateType::kUnknown:
        os << "unknown";
        break;
    case PredicateType::kEQ:
        os << "=";
        break;
    case PredicateType::kNE:
        os << "!=";
        break;
    case PredicateType::kGT:
        os << "<";
        break;
    case PredicateType::kGE:
        os << "<=";
        break;
    case PredicateType::kLT:
        os << ">";
        break;
    case PredicateType::kLE:
        os << ">=";
        break;

    case PredicateType::kInList:
        os << "IN";
        break;
    case PredicateType::kNotInList:
        os << "NOT IN";
        break;
    case PredicateType::kIsNull:
        os << "IS NULL";
        break;
    case PredicateType::kNotNull:
        os << "IS NOT NULL";
        break;
    case PredicateType::kAnd:
        os << "AND";
        break;
    case PredicateType::kOr:
        os << "OR";
        break;
    case PredicateType::kExpr:
        os << "expr";
        break;
    case PredicateType::kTrue:
        os << "true";
        break;
    case PredicateType::kMap:
        os << "map";
        break;
    default:
        CHECK(false) << "unknown predicate " << p;
    }

    return os;
}

} // namespace starrocks::vectorized
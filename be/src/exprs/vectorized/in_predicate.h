// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exprs/predicate.h"

namespace starrocks {
class Expr;
class TExprNode;

namespace vectorized {

class VectorizedInPredicateFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace vectorized
} // namespace starrocks

// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/predicate.h"

namespace starrocks::vectorized {

class VectorizedIsNullPredicateFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace starrocks::vectorized

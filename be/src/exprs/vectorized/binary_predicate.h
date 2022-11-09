// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/object_pool.h"
#include "exprs/predicate.h"

namespace starrocks {
namespace vectorized {

class VectorizedBinaryPredicateFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace vectorized
} // namespace starrocks

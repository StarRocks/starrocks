// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/expr.h"

namespace starrocks {
namespace vectorized {

class VectorizedArithmeticExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace vectorized
} // namespace starrocks

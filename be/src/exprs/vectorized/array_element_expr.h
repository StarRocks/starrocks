// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exprs/expr.h"

namespace starrocks {
class Expr;
class TExprNode;
}  // namespace starrocks

namespace starrocks::vectorized {

class ArrayElementExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace starrocks::vectorized
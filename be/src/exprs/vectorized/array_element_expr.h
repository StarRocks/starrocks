// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exprs/expr.h"

namespace starrocks::vectorized {

class ArrayElementExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace starrocks::vectorized
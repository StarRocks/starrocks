// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Inc.

#pragma once

#include "exprs/expr.h"

namespace starrocks::vectorized {

class SubfieldExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
};

} // namespace starrocks::vectorized
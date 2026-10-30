// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "exprs/expr.h"
#include "types/logical_type.h"

namespace starrocks {

// Result-type-partitioned factory helpers (defined in case_expr_g{1,2,3}.cpp). Each returns
// nullptr for result types it does not handle.
Expr* case_from_thrift_group1(const TExprNode& node, LogicalType resultType, LogicalType whenType);
Expr* case_from_thrift_group2(const TExprNode& node, LogicalType resultType, LogicalType whenType);
Expr* case_from_thrift_group3(const TExprNode& node, LogicalType resultType, LogicalType whenType);

class VectorizedCaseExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node);
    // for tests
    static Expr* from_thrift(const starrocks::TExprNode& node, LogicalType resultType, LogicalType whenType);
};
} // namespace starrocks

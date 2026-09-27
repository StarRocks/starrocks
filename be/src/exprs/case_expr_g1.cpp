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

#include "exprs/case_expr_tpl.hpp"

namespace starrocks {

// One third of the VectorizedCaseExpr<When, Result> result-type cartesian, split out so the
// ~550 template instantiations compile across parallel TUs instead of one 536s file.
Expr* case_from_thrift_group1(const starrocks::TExprNode& node, LogicalType resultType, LogicalType whenType) {
    switch (resultType) {
        CASE_RESULT_TYPE(TYPE_TINYINT)
        CASE_RESULT_TYPE(TYPE_SMALLINT)
        CASE_RESULT_TYPE(TYPE_INT)
        CASE_RESULT_TYPE(TYPE_BIGINT)
        CASE_RESULT_TYPE(TYPE_LARGEINT)
        CASE_RESULT_TYPE(TYPE_FLOAT)
        CASE_RESULT_TYPE(TYPE_DOUBLE)
        CASE_RESULT_TYPE(TYPE_DECIMAL32)
        CASE_RESULT_TYPE(TYPE_DECIMAL64)
    default:
        return nullptr;
    }
}

} // namespace starrocks

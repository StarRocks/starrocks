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

#include "exprs/cast_expr_tpl.hpp"

namespace starrocks {

// Primitive to_type cast dispatch, group 1. Split out of create_primitive_cast to
// distribute the VectorizedCastExpr<From,To> instantiation cost across TUs.
Expr* create_primitive_cast_group1(const TExprNode& node, LogicalType from_type, LogicalType to_type,
                                   bool allow_throw_exception) {
    switch (to_type) {
        CASE_TO_TYPE(TYPE_BOOLEAN, allow_throw_exception);
        CASE_TO_TYPE(TYPE_TINYINT, allow_throw_exception);
        CASE_TO_TYPE(TYPE_SMALLINT, allow_throw_exception);
        CASE_TO_TYPE(TYPE_INT, allow_throw_exception);
        CASE_TO_TYPE(TYPE_BIGINT, allow_throw_exception);
        CASE_TO_TYPE(TYPE_LARGEINT, allow_throw_exception);
    default:
        return nullptr;
    }
    return nullptr;
}

} // namespace starrocks

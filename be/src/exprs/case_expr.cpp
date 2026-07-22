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

#include "exprs/case_expr.h"

#include <cstdint>

#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/runtime_type_traits.h"
#include "column/simd_mulselector.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "types/logical_type_infra.h"
#include "types/percentile_value.h"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/expr_jit_codegen.h"
#include "exprs/jit/ir_helper.h"
#endif

namespace starrocks {

// The VectorizedCaseExpr<When, Result> template class and the per-result-type instantiations
// live in case_expr_tpl.hpp / case_expr_g{1,2,3}.cpp. This factory only routes by result type,
// so it instantiates none of them.

Expr* VectorizedCaseExprFactory::from_thrift(const starrocks::TExprNode& node) {
    LogicalType resultType = TypeDescriptor::from_thrift(node.type).type;
    LogicalType whenType = thrift_to_type(node.child_type);
    if (resultType == TYPE_NULL) {
        resultType = TYPE_BOOLEAN;
    }
    Expr* expr = case_from_thrift_group1(node, resultType, whenType);
    if (expr == nullptr) expr = case_from_thrift_group2(node, resultType, whenType);
    if (expr == nullptr) expr = case_from_thrift_group3(node, resultType, whenType);
    if (expr == nullptr) {
        LOG(WARNING) << "vectorized engine case expr no support result type: " << resultType;
    }
    return expr;
}

Expr* VectorizedCaseExprFactory::from_thrift(const starrocks::TExprNode& node, LogicalType resultType,
                                             LogicalType whenType) {
    if (resultType == TYPE_NULL) {
        resultType = TYPE_BOOLEAN;
    }
    Expr* expr = case_from_thrift_group1(node, resultType, whenType);
    if (expr == nullptr) expr = case_from_thrift_group2(node, resultType, whenType);
    if (expr == nullptr) expr = case_from_thrift_group3(node, resultType, whenType);
    if (expr == nullptr) {
        LOG(WARNING) << "vectorized engine case expr no support result type: " << resultType;
    }
    return expr;
}

} // namespace starrocks

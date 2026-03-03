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

#include "common/object_pool.h"
#include "exprs/array_map_expr.h"
#include "exprs/array_sort_lambda_expr.h"
#include "exprs/arrow_function_call.h"
#include "exprs/dict_query_expr.h"
#include "exprs/dictionary_get_expr.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_factory_internal.h"
#include "exprs/function_call_expr.h"
#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/expr_jit_pass.h"
#endif
#include "exprs/java_function_call_expr.h"
#include "exprs/map_apply_expr.h"

namespace starrocks {

namespace {

Status create_non_core_vectorized_expr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                       RuntimeState* /*state*/) {
    switch (texpr_node.node_type) {
    case TExprNodeType::COMPUTE_FUNCTION_CALL:
    case TExprNodeType::FUNCTION_CALL: {
        if (texpr_node.fn.binary_type == TFunctionBinaryType::SRJAR) {
            *expr = pool->add(new JavaFunctionCallExpr(texpr_node));
        } else if (texpr_node.fn.binary_type == TFunctionBinaryType::PYTHON) {
            *expr = pool->add(new ArrowFunctionCallExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "array_map") {
            *expr = pool->add(new ArrayMapExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "array_sort_lambda") {
            *expr = pool->add(new ArraySortLambdaExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "map_apply") {
            *expr = pool->add(new MapApplyExpr(texpr_node));
        } else {
            *expr = pool->add(new VectorizedFunctionCallExpr(texpr_node));
        }
        return Status::OK();
    }
    case TExprNodeType::DICT_EXPR:
        *expr = pool->add(new DictMappingExpr(texpr_node));
        return Status::OK();
    case TExprNodeType::DICT_QUERY_EXPR:
        *expr = pool->add(new DictQueryExpr(texpr_node));
        return Status::OK();
    case TExprNodeType::DICTIONARY_GET_EXPR:
        *expr = pool->add(new DictionaryGetExpr(texpr_node));
        return Status::OK();
    default:
        break;
    }
    return Status::NotSupported("unsupported node in ExprFactory non-core creator");
}

Status rewrite_root_with_jit_hook(Expr** root_expr, ObjectPool* pool, RuntimeState* state) {
#ifdef STARROCKS_JIT_ENABLE
    return ExprJITPass::rewrite_root(root_expr, pool, state);
#else
    return Status::OK();
#endif
}

struct ExprFactoryExtRegistrar {
    ExprFactoryExtRegistrar() {
        ExprFactoryInternal::register_non_core_expr_creator(create_non_core_vectorized_expr);
        ExprFactoryInternal::register_jit_rewrite_hook(rewrite_root_with_jit_hook);
    }
};

ExprFactoryExtRegistrar g_expr_factory_ext_registrar;

} // namespace

} // namespace starrocks

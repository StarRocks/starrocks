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
#include "exprs/expr_factory.h"
#include "exprs/function_call_expr.h"
#include "exprs/java_function_call_expr.h"
#include "exprs/map_apply_expr.h"
#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/expr_jit_pass.h"
#endif

namespace starrocks {

namespace {

Status expr_factory_non_core_create_pre_hook(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                             RuntimeState* state) {
    (void)state;
    if (*expr != nullptr) {
        return Status::OK();
    }
    if (texpr_node.node_type != TExprNodeType::FUNCTION_CALL &&
        texpr_node.node_type != TExprNodeType::COMPUTE_FUNCTION_CALL) {
        return Status::OK();
    }

    if (texpr_node.fn.binary_type == TFunctionBinaryType::SRJAR) {
        *expr = pool->add(new JavaFunctionCallExpr(texpr_node));
    } else if (texpr_node.fn.binary_type == TFunctionBinaryType::PYTHON) {
        *expr = pool->add(new ArrowFunctionCallExpr(texpr_node));
    }
    return Status::OK();
}

Status expr_factory_non_core_create_post_hook(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                              RuntimeState* state) {
    (void)state;
    if (*expr != nullptr) {
        return Status::OK();
    }

    switch (texpr_node.node_type) {
    case TExprNodeType::FUNCTION_CALL:
    case TExprNodeType::COMPUTE_FUNCTION_CALL:
        if (texpr_node.fn.name.function_name == "array_map") {
            *expr = pool->add(new ArrayMapExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "array_sort_lambda") {
            *expr = pool->add(new ArraySortLambdaExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "map_apply") {
            *expr = pool->add(new MapApplyExpr(texpr_node));
        } else {
            *expr = pool->add(new VectorizedFunctionCallExpr(texpr_node));
        }
        break;
    case TExprNodeType::DICT_EXPR:
        *expr = pool->add(new DictMappingExpr(texpr_node));
        break;
    case TExprNodeType::DICT_QUERY_EXPR:
        *expr = pool->add(new DictQueryExpr(texpr_node));
        break;
    case TExprNodeType::DICTIONARY_GET_EXPR:
        *expr = pool->add(new DictionaryGetExpr(texpr_node));
        break;
    default:
        break;
    }
    return Status::OK();
}

Status expr_factory_jit_rewrite_hook(Expr** root_expr, ObjectPool* pool, RuntimeState* state) {
#ifdef STARROCKS_JIT_ENABLE
    return ExprJITPass::rewrite_root(root_expr, pool, state);
#else
    (void)root_expr;
    (void)pool;
    (void)state;
    return Status::OK();
#endif
}

struct ExprFactoryExprsExtensionRegistrar {
    ExprFactoryExprsExtensionRegistrar() {
        ExprFactory::set_non_core_create_pre_hook(expr_factory_non_core_create_pre_hook);
        ExprFactory::set_non_core_create_post_hook(expr_factory_non_core_create_post_hook);
        ExprFactory::set_jit_rewrite_hook(expr_factory_jit_rewrite_hook);
    }
};

ExprFactoryExprsExtensionRegistrar k_expr_factory_exprs_extension_registrar;

} // namespace

} // namespace starrocks

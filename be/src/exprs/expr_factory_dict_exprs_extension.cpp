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
#include "exprs/dict_query_expr.h"
#include "exprs/dictionary_get_expr.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr_factory.h"

namespace starrocks {

namespace {

Status expr_factory_dict_create_post_hook(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                          RuntimeState* state) {
    (void)state;
    if (*expr != nullptr) {
        return Status::OK();
    }

    switch (texpr_node.node_type) {
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

struct ExprFactoryDictExprsExtensionRegistrar {
    ExprFactoryDictExprsExtensionRegistrar() {
        ExprFactory::set_non_core_create_post_hook(expr_factory_dict_create_post_hook);
    }
};

ExprFactoryDictExprsExtensionRegistrar k_expr_factory_dict_exprs_extension_registrar;

} // namespace

} // namespace starrocks

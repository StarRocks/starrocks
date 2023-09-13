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

#include "exprs/in_predicate.h"

#include "common/object_pool.h"
#include "exprs/in_const_predicate.hpp"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

struct InConstPredicateBuilder {
    template <LogicalType ltype>
    Expr* operator()(const TExprNode& node) {
        if constexpr (lt_is_collection<ltype> || ltype == TYPE_JSON) {
            return new VectorizedInConstPredicateGeneric(node);
        } else {
            return new VectorizedInConstPredicate<ltype>(node);
        }
    }
};

Expr* VectorizedInPredicateFactory::from_thrift(const TExprNode& node) {
    // children type
    LogicalType child_type = thrift_to_type(node.child_type);
    if (node.__isset.child_type_desc) {
        child_type = TypeDescriptor::from_thrift(node.child_type_desc).type;
    } else {
        child_type = thrift_to_type(node.child_type);
    }

    if (child_type == TYPE_CHAR) {
        child_type = TYPE_VARCHAR;
    }

    switch (node.opcode) {
    case TExprOpcode::FILTER_IN:
    case TExprOpcode::FILTER_NOT_IN:
        return type_dispatch_basic_and_complex_types(child_type, InConstPredicateBuilder(), node);
    case TExprOpcode::FILTER_NEW_IN:
    case TExprOpcode::FILTER_NEW_NOT_IN:
        // NOTE: These two opcode are deprecated
    default:
        LOG(WARNING) << "vectorized engine in predicate not support: " << node.opcode;
        return nullptr;
    }

    return nullptr;
}

} // namespace starrocks

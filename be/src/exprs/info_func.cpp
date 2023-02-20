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

#include "exprs/info_func.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"

namespace starrocks {

VectorizedInfoFunc::VectorizedInfoFunc(const TExprNode& node) : Expr(node) {
    switch (_type.type) {
    case TYPE_BIGINT: {
        _value = ColumnHelper::create_const_column<TYPE_BIGINT>(node.info_func.int_value, 1);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        // @IMPORTANT: build slice though get_data, else maybe will case multi-thread crash in scanner
        _value = ColumnHelper::create_const_column<TYPE_VARCHAR>(node.info_func.str_value, 1);
        break;
    }
    default:
        DCHECK(false) << "Vectorized engine not implement type: " << _type.type;
        break;
    }
}

StatusOr<ColumnPtr> VectorizedInfoFunc::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ColumnPtr column = _value->clone_empty();
    column->append(*_value, 0, 1);
    if (ptr != nullptr) {
        column->resize(ptr->num_rows());
    }
    return column;
}

std::string VectorizedInfoFunc::debug_string() const {
    std::stringstream out;
    out << "VectorizedInfoFunc("
        << "type=" << this->type().debug_string() << " )";
    return out.str();
}

VectorizedInfoFunc::~VectorizedInfoFunc() = default;

} // namespace starrocks

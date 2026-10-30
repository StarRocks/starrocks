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

#include "exprs/column_access_path_resolver.h"

#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "runtime/runtime_state.h"

namespace starrocks {

StatusOr<std::string> resolve_column_access_path(const TColumnAccessPath& column_path, RuntimeState* state,
                                                 ObjectPool* pool) {
    ExprContext* expr_ctx = nullptr;
    // Todo: may support late materialization? to compute path by other column predicate.
    RETURN_IF_ERROR(ExprFactory::create_expr_tree(pool, column_path.path, &expr_ctx, state));
    if (!expr_ctx->root()->is_constant()) {
        return Status::InternalError("error column access constant path.");
    }

    RETURN_IF_ERROR(expr_ctx->prepare(state));
    RETURN_IF_ERROR(expr_ctx->open(state));
    ASSIGN_OR_RETURN(ColumnPtr column, expr_ctx->evaluate(nullptr));

    if (column->is_null(0)) {
        return Status::InternalError("error column access null path.");
    }

    const Column* data = ColumnHelper::get_data_column(column.get());
    if (!data->is_binary()) {
        return Status::InternalError("error column access string path.");
    }

    return ColumnHelper::as_raw_column<BinaryColumn>(data)->get_slice(0).to_string();
}

ColumnAccessPath::PathResolver make_column_access_path_resolver(RuntimeState* state, ObjectPool* pool) {
    return [state, pool](const TColumnAccessPath& column_path) {
        return resolve_column_access_path(column_path, state, pool);
    };
}

} // namespace starrocks

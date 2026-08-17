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

#include "compute_env/sorting/data_segment.h"

#include "column/column_helper.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"

namespace starrocks {

void DataSegment::init(const std::vector<ExprContext*>* sort_exprs, const ChunkPtr& cnk) {
    chunk = cnk;
    order_by_columns.reserve(sort_exprs->size());
    for (ExprContext* expr_ctx : (*sort_exprs)) {
        order_by_columns.push_back(EVALUATE_NULL_IF_ERROR(expr_ctx, expr_ctx->root(), chunk.get()));
    }
}

} // namespace starrocks

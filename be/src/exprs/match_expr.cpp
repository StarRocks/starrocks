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

#include "exprs/match_expr.h"

namespace starrocks {

StatusOr<ColumnPtr> MatchExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    return Status::InternalError("Match can only used as a pushdown predicate on column with GIN in a single query.");
}

Status MatchExpr::prepare(RuntimeState* state, ExprContext* context) {
    return Status::OK();
}

} // namespace starrocks

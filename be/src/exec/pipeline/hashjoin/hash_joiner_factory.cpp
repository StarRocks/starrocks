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

#include "exec/pipeline/hashjoin/hash_joiner_factory.h"

namespace starrocks::pipeline {

Status HashJoinerFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._other_join_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._other_join_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._conjunct_ctxs, state));
    return Status::OK();
}

void HashJoinerFactory::close(RuntimeState* state) {
    Expr::close(_param._conjunct_ctxs, state);
    Expr::close(_param._other_join_conjunct_ctxs, state);
    Expr::close(_param._probe_expr_ctxs, state);
    Expr::close(_param._build_expr_ctxs, state);
}

} // namespace starrocks::pipeline

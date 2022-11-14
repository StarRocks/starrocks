// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

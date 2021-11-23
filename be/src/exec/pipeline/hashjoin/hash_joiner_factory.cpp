// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/hashjoin/hash_joiner_factory.h"

namespace starrocks {
namespace pipeline {
Status HashJoinerFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_build_expr_ctxs, state, _build_row_descriptor));
    RETURN_IF_ERROR(Expr::prepare(_probe_expr_ctxs, state, _probe_row_descriptor));
    RETURN_IF_ERROR(Expr::prepare(_other_join_conjunct_ctxs, state, _row_descriptor));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, _row_descriptor));
    RETURN_IF_ERROR(Expr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_other_join_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

void HashJoinerFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    Expr::close(_other_join_conjunct_ctxs, state);
    Expr::close(_probe_expr_ctxs, state);
    Expr::close(_build_expr_ctxs, state);
}

} // namespace pipeline
} // namespace starrocks
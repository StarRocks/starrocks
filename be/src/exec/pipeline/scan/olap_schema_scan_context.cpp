// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/olap_schema_scan_context.h"

#include <boost/algorithm/string.hpp>

#include "exec/vectorized/schema_scanner.h"
#include "exprs/expr.h"

namespace starrocks::pipeline {

Status OlapSchemaScanContext::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::create_expr_trees(&_obj_pool, _tnode.conjuncts, &_conjunct_ctxs));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    RETURN_IF_ERROR(_prepare_params(state));
    return Status::OK();
}

Status OlapSchemaScanContext::_prepare_params(RuntimeState* state) {
    _param = std::make_shared<vectorized::SchemaScannerParam>();
    if (_tnode.schema_scan_node.__isset.db) {
        _param->db = _obj_pool.add(new std::string(_tnode.schema_scan_node.db));
    }

    if (_tnode.schema_scan_node.__isset.table) {
        _param->table = _obj_pool.add(new std::string(_tnode.schema_scan_node.table));
    }

    if (_tnode.schema_scan_node.__isset.wild) {
        _param->wild = _obj_pool.add(new std::string(_tnode.schema_scan_node.wild));
    }

    if (_tnode.schema_scan_node.__isset.current_user_ident) {
        _param->current_user_ident = _obj_pool.add(new TUserIdentity(_tnode.schema_scan_node.current_user_ident));
    } else {
        if (_tnode.schema_scan_node.__isset.user) {
            _param->user = _obj_pool.add(new std::string(_tnode.schema_scan_node.user));
        }
        if (_tnode.schema_scan_node.__isset.user_ip) {
            _param->user_ip = _obj_pool.add(new std::string(_tnode.schema_scan_node.user_ip));
        }
    }

    if (_tnode.schema_scan_node.__isset.ip) {
        _param->ip = _obj_pool.add(new std::string(_tnode.schema_scan_node.ip));
    }
    if (_tnode.schema_scan_node.__isset.port) {
        _param->port = _tnode.schema_scan_node.port;
    }

    if (_tnode.schema_scan_node.__isset.thread_id) {
        _param->thread_id = _tnode.schema_scan_node.thread_id;
    }

    // only for no predicate and limit parameter is set
    if (_tnode.conjuncts.empty() && _tnode.limit > 0) {
        _param->without_db_table = true;
        _param->limit = _tnode.limit;
    }
    return Status::OK();
}

} // namespace starrocks::pipeline

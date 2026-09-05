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

#include "exec/schema_scanner.h"

#include <boost/algorithm/string.hpp>

#include "column/runtime_type_traits.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/literal.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

StarRocksServer* SchemaScanner::_s_starrocks_server;

SchemaScanner::SchemaScanner(ColumnDesc* columns, int column_num) : _columns(columns), _column_num(column_num) {}

SchemaScanner::~SchemaScanner() = default;

Status SchemaScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("call Start before Init.");
    }

    _runtime_state = state;

    return Status::OK();
}

Status SchemaScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    return Status::OK();
}

Status SchemaScanner::init_schema_scanner_state(RuntimeState* state) {
    if (nullptr == _param || nullptr == _param->ip || 0 == _param->port) {
        return Status::InternalError("IP or port doesn't exists");
    }
    _ss_state.ip = *(_param->ip);
    _ss_state.port = _param->port;
    _ss_state.timeout_ms = state->query_options().query_timeout * 1000;
    VLOG(2) << "ip=" << _ss_state.ip << ", port=" << _ss_state.port << ", timeout=" << _ss_state.timeout_ms;
    _ss_state.param = _param;
    return Status::OK();
}

Status SchemaScanner::init(SchemaScannerParam* param, ObjectPool* pool) {
    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == param || nullptr == pool || (nullptr == _columns && 0 != _column_num)) {
        return Status::InternalError("invalid parameter");
    }

    RETURN_IF_ERROR(_create_slot_descs(pool));

    _param = param;
    _is_init = true;

    return Status::OK();
}

Status SchemaScanner::_create_slot_descs(ObjectPool* pool) {
    int null_column = 0;

    for (int i = 0; i < _column_num; ++i) {
        if (_columns[i].is_null) {
            null_column++;
        }
    }

    int offset = (null_column + 7) / 8;
    for (int i = 0; i < _column_num; ++i) {
        TSlotDescriptor t_slot_desc;
        const TypeDescriptor& type_desc = _columns[i].type;
        t_slot_desc.__set_id(i + 1);
        t_slot_desc.__set_slotType(type_desc.to_thrift());
        t_slot_desc.__set_colName(_columns[i].name);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);

        t_slot_desc.__set_isNullable(_columns[i].is_null);

        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);

        SlotDescriptor* slot = pool->add(new (std::nothrow) SlotDescriptor(t_slot_desc));

        if (nullptr == slot) {
            return Status::InternalError("no memory for _slot_descs.");
        }

        _slot_descs.push_back(slot);
        offset += _columns[i].size;
    }

    return Status::OK();
}

TAuthInfo SchemaScanner::build_auth_info() {
    TAuthInfo auth_info;
    if (nullptr != _param->catalog) {
        auth_info.__set_catalog_name(*(_param->catalog));
    }
    if (nullptr != _param->db) {
        auth_info.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        auth_info.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            auth_info.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            auth_info.__set_user_ip(*(_param->user_ip));
        }
    }
    return auth_info;
}

bool SchemaScanner::_parse_expr_predicate(const std::string& col_name, std::string& result) {
    if (_param->expr_contexts == nullptr) {
        return false;
    }
    for (auto* expr_context : *(_param->expr_contexts)) {
        Expr* conjunct = expr_context->root();
        if (_parse_expr_predicate(conjunct, col_name, result)) {
            return true;
        }
    }
    return false;
}

bool SchemaScanner::_parse_expr_predicate(Expr* conjunct, const std::string& col_name, std::string& result) {
    const TExprNodeType::type& node_type = conjunct->node_type();
    const TExprOpcode::type& op_type = conjunct->op();
    // only support equal binary predicate, eg: task_name='xxx'.
    if (node_type != TExprNodeType::BINARY_PRED || op_type != TExprOpcode::EQ) {
        return false;
    }
    Expr* child0 = conjunct->get_child(0);
    Expr* child1 = conjunct->get_child(1);

    SlotId slot_id;
    int result_child_idx = 0;
    if (child0->node_type() == TExprNodeType::type::SLOT_REF && child1->node_type() == TExprNodeType::STRING_LITERAL) {
        slot_id = down_cast<ColumnRef*>(child0)->slot_id();
        result_child_idx = 1;
    } else if (child1->node_type() == TExprNodeType::type::SLOT_REF &&
               child0->node_type() == TExprNodeType::STRING_LITERAL) {
        slot_id = down_cast<ColumnRef*>(child1)->slot_id();
        result_child_idx = 0;
    } else {
        return false;
    }

    auto& slot_id_mapping = _param->slot_id_mapping;
    if (slot_id_mapping.find(slot_id) == slot_id_mapping.end()) {
        return false;
    }
    auto slot_name = slot_id_mapping.at(slot_id)->col_name();
    if (!boost::iequals(slot_name, col_name)) {
        return false;
    }

    Expr* string_literal_expr = (result_child_idx == 0) ? child0 : child1;
    auto* eq_target = dynamic_cast<VectorizedLiteral*>(string_literal_expr);
    DCHECK(eq_target != nullptr);
    auto literal_col_status = eq_target->evaluate_checked(nullptr, nullptr);
    if (!literal_col_status.ok()) {
        return false;
    }
    auto literal_col = literal_col_status.value();
    Slice padded_value(literal_col->get(0).get_slice());
    result = padded_value.to_string();
    VLOG(2) << "schema scaner parse expr value:" << result << ", col_name:" << col_name << ", slot_id=" << slot_id
            << ", result_child_idx=" << result_child_idx;
    return true;
}

} // namespace starrocks

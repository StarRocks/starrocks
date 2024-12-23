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

#include "formats/parquet/parquet_ut_base.h"

#include <gtest/gtest.h>

#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "storage/predicate_parser.h"
#include "testutil/assert.h"
#include "testutil/exprs_test_helper.h"
#include "types/logical_type.h"

namespace starrocks::parquet {

void ParquetUTBase::create_conjunct_ctxs(ObjectPool* pool, RuntimeState* runtime_state, std::vector<TExpr>* tExprs,
                                         std::vector<ExprContext*>* conjunct_ctxs) {
    ASSERT_OK(Expr::create_expr_trees(pool, *tExprs, conjunct_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(*conjunct_ctxs, runtime_state));
    ASSERT_OK(Expr::open(*conjunct_ctxs, runtime_state));
}

void ParquetUTBase::append_decimal_conjunct(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                            std::vector<TExpr>* tExprs) {
    TTypeDesc decimal_type = ExprsTestHelper::create_decimal_type_desc(TPrimitiveType::DECIMAL128, 27, 9);

    TExprNode binary_pred = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::DECIMAL128, opcode);
    TExprNode decimal_col_ref = ExprsTestHelper::create_slot_expr_node(0, slot_id, decimal_type, true);
    TExprNode decimal_literal = ExprsTestHelper::create_decimal_literal(value, decimal_type, false);

    TExpr t_expr;
    t_expr.nodes.emplace_back(binary_pred);
    t_expr.nodes.emplace_back(decimal_col_ref);
    t_expr.nodes.emplace_back(decimal_literal);

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::append_smallint_conjunct(TExprOpcode::type opcode, SlotId slot_id, int value,
                                             std::vector<TExpr>* tExprs) {
    TTypeDesc smallint_type = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::SMALLINT);

    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::SMALLINT, opcode);
    TExprNode smallint_col_ref = ExprsTestHelper::create_slot_expr_node(0, slot_id, smallint_type, true);
    TExprNode smallint_literal = ExprsTestHelper::create_int_literal<int32_t>(value, smallint_type, false);

    TExpr t_expr;
    t_expr.nodes.emplace_back(pred_node);
    t_expr.nodes.emplace_back(smallint_col_ref);
    t_expr.nodes.emplace_back(smallint_literal);

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::append_int_conjunct(TExprOpcode::type opcode, SlotId slot_id, int value,
                                        std::vector<TExpr>* tExprs) {
    TTypeDesc int_type = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::INT);

    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::INT, opcode);
    TExprNode int_col_ref = ExprsTestHelper::create_slot_expr_node(0, slot_id, int_type, true);
    TExprNode int_literal = ExprsTestHelper::create_int_literal<int32_t>(value, int_type, false);

    TExpr t_expr;
    t_expr.nodes.emplace_back(pred_node);
    t_expr.nodes.emplace_back(int_col_ref);
    t_expr.nodes.emplace_back(int_literal);

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::append_bigint_conjunct(TExprOpcode::type opcode, SlotId slot_id, int64_t value,
                                           std::vector<TExpr>* tExprs) {
    TTypeDesc int_type = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::BIGINT);

    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::BIGINT, opcode);
    TExprNode int_col_ref = ExprsTestHelper::create_slot_expr_node(0, slot_id, int_type, true);
    TExprNode int_literal = ExprsTestHelper::create_int_literal<int64_t>(value, int_type, false);

    TExpr t_expr;
    t_expr.nodes.emplace_back(pred_node);
    t_expr.nodes.emplace_back(int_col_ref);
    t_expr.nodes.emplace_back(int_literal);

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::append_datetime_conjunct(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                             std::vector<TExpr>* tExprs) {
    TTypeDesc datetime_type = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::DATETIME);

    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::DATETIME, opcode);
    TExprNode datetime_col_ref = ExprsTestHelper::create_slot_expr_node(0, slot_id, datetime_type, true);
    TExprNode datetime_literal = ExprsTestHelper::create_date_literal(value, datetime_type, false);

    TExpr t_expr;
    t_expr.nodes.emplace_back(pred_node);
    t_expr.nodes.emplace_back(datetime_col_ref);
    t_expr.nodes.emplace_back(datetime_literal);

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::append_string_conjunct(TExprOpcode::type opcode, starrocks::SlotId slot_id, std::string value,
                                           std::vector<TExpr>* tExprs) {
    TTypeDesc varchar_type = ExprsTestHelper::create_varchar_type_desc(10);

    TExprNode pre_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, opcode);
    TExprNode varchar_col_ref = ExprsTestHelper::create_slot_expr_node(0, slot_id, varchar_type, true);
    TExprNode varchar_literal = ExprsTestHelper::create_varchar_literal(value, varchar_type, false);

    TExpr t_expr;
    t_expr.nodes.emplace_back(pre_node);
    t_expr.nodes.emplace_back(varchar_col_ref);
    t_expr.nodes.emplace_back(varchar_literal);

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::is_null_pred(starrocks::SlotId slot_id, bool null, std::vector<TExpr>* tExprs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    node0.node_type = TExprNodeType::FUNCTION_CALL;
    node0.num_children = 1;
    node0.fn.name.function_name = null ? "is_null_pred" : "is_not_null_pred";
    node0.__isset.fn = true;
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExpr t_expr;
    t_expr.nodes = nodes;

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::create_in_predicate_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                          std::set<int32_t>& values, std::vector<TExpr>* tExprs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::IN_PRED;
    node0.opcode = opcode;
    node0.child_type = TPrimitiveType::INT;
    node0.num_children = values.size() + 1;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    for (int32_t value : values) {
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        TIntLiteral int_literal;
        int_literal.value = value;
        node.__set_int_literal(int_literal);
        node.is_nullable = false;
        nodes.emplace_back(node);
    }

    TExpr t_expr;
    t_expr.nodes = nodes;

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::create_in_predicate_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                             std::set<std::string>& values,
                                                             std::vector<TExpr>* tExprs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::IN_PRED;
    node0.opcode = opcode;
    node0.child_type = TPrimitiveType::VARCHAR;
    node0.num_children = values.size() + 1;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    for (std::string value : values) {
        TExprNode node;
        node.node_type = TExprNodeType::STRING_LITERAL;
        node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        node.num_children = 0;
        TStringLiteral string_literal;
        string_literal.value = value;
        node.__set_string_literal(string_literal);
        node.is_nullable = false;
        nodes.emplace_back(node);
    }

    TExpr t_expr;
    t_expr.nodes = nodes;

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::create_in_predicate_date_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                           TPrimitiveType::type type, std::set<std::string>& values,
                                                           std::vector<TExpr>* tExprs) {
    std::vector<TExprNode> nodes;

    TExprNode node0;
    node0.node_type = TExprNodeType::IN_PRED;
    node0.opcode = opcode;
    node0.child_type = type;
    node0.num_children = values.size() + 1;
    node0.__isset.opcode = true;
    node0.__isset.child_type = true;
    node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(type);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    for (std::string value : values) {
        TExprNode node;
        node.node_type = TExprNodeType::DATE_LITERAL;
        node.type = gen_type_desc(type);
        node.num_children = 0;
        TDateLiteral date_literal;
        date_literal.value = value;
        node.__set_date_literal(date_literal);
        node.is_nullable = false;
        nodes.emplace_back(node);
    }

    TExpr t_expr;
    t_expr.nodes = nodes;

    tExprs->emplace_back(t_expr);
}

void ParquetUTBase::setup_conjuncts_manager(std::vector<ExprContext*>& conjuncts, TupleDescriptor* tuple_desc,
                                            RuntimeState* runtime_state, HdfsScannerContext* params) {
    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &conjuncts;
    opts.tuple_desc = tuple_desc;
    opts.obj_pool = runtime_state->obj_pool();
    opts.runtime_filters = runtime_state->obj_pool()->add(new RuntimeFilterProbeCollector());
    opts.runtime_state = runtime_state;
    opts.enable_column_expr_predicate = true;
    opts.is_olap_scan = false;
    opts.pred_tree_params = {true, true};
    params->conjuncts_manager = std::make_unique<ScanConjunctsManager>(std::move(opts));
    ASSERT_TRUE(params->conjuncts_manager->parse_conjuncts().ok());
    ConnectorPredicateParser predicate_parser{&params->slot_descs};
    auto st = params->conjuncts_manager->get_predicate_tree(&predicate_parser, params->predicate_free_pool);
    ASSERT_TRUE(st.ok());
    params->predicate_tree = st.value();
}

} // namespace starrocks::parquet

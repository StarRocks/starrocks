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

#pragma once

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/array_expr.h"
#include "exprs/map_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class ExprsTestHelper {
public:
    template <LogicalType Type>
    static TExpr create_column_ref_t_expr(SlotId slot_id, bool is_nullable) {
        TExpr expr;
        expr.nodes.emplace_back(TExprNode());
        expr.nodes[0].__set_type(TypeDescriptor(Type).to_thrift());
        expr.nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        expr.nodes[0].__set_is_nullable(is_nullable);
        expr.nodes[0].__set_slot_ref(TSlotRef());
        expr.nodes[0].slot_ref.__set_slot_id(slot_id);
        return expr;
    }

    static TTypeDesc create_decimal_type_desc(const TPrimitiveType::type type, int32_t precision, int32_t scale) {
        TScalarType scalar_type;
        scalar_type.__set_type(type);
        scalar_type.__set_precision(precision);
        scalar_type.__set_scale(scale);

        TTypeNode type_node;
        type_node.__set_type(TTypeNodeType::SCALAR);
        type_node.__set_scalar_type(scalar_type);

        TTypeDesc type_desc;
        type_desc.types.push_back(type_node);
        return type_desc;
    }

    static TTypeDesc create_varchar_type_desc(int32_t length) {
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.len = length;

        TTypeNode type_node;
        type_node.__set_type(TTypeNodeType::SCALAR);
        type_node.__set_scalar_type(scalar_type);

        TTypeDesc type_desc;
        type_desc.types.push_back(type_node);

        return type_desc;
    }

    static TTypeDesc create_scalar_type_desc(const TPrimitiveType::type t_type) {
        TScalarType scalar_type;
        scalar_type.__set_type(t_type);

        TTypeNode type_node;
        type_node.__set_type(TTypeNodeType::SCALAR);
        type_node.__set_scalar_type(scalar_type);

        TTypeDesc type_desc;
        type_desc.types.push_back(type_node);

        return type_desc;
    }

    static const TTypeDesc SmallIntTTypeDesc;
    static const TTypeDesc IntTTypeDesc;
    static const TTypeDesc BigIntTTypeDesc;
    static const TTypeDesc DateTTypeDesc;
    static const TTypeDesc DateTimeTTypeDesc;
    // precision=27, scale=9
    static const TTypeDesc Decimal128TTypeDesc;
    static const TTypeDesc VarcharTTypeDesc;

    static TSlotDescriptor create_slot_desc(const TTypeDesc& type, TupleId tuple_id, SlotId slot_id,
                                            const std::string& col_name) {
        //k1_int
        TSlotDescriptor slot_desc;
        slot_desc.id = slot_id;
        slot_desc.parent = tuple_id;
        slot_desc.slotType = type;
        slot_desc.columnPos = -1;
        slot_desc.byteOffset = 4;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 1;
        slot_desc.colName = col_name;
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;
        return slot_desc;
    }

    static TDescriptorTable create_table_desc(std::vector<TTupleDescriptor> tuple_descs,
                                              std::vector<TSlotDescriptor> slot_descs) {
        TDescriptorTable t_desc_table;
        for (auto& slot_desc : slot_descs) {
            t_desc_table.slotDescriptors.push_back(slot_desc);
        }
        t_desc_table.__isset.slotDescriptors = true;
        for (auto& tuple_desc : tuple_descs) {
            t_desc_table.tupleDescriptors.push_back(tuple_desc);
        }
        return t_desc_table;
    }

    static TTupleDescriptor create_tuple_desc(int tuple_id) {
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = tuple_id;
        t_tuple_desc.byteSize = 32;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.numNullSlots = 4;
        return t_tuple_desc;
    }

    static std::unique_ptr<Expr> create_array_expr(const TTypeDesc& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::ARRAY_EXPR);
        node.__set_is_nullable(true);
        node.__set_type(type);
        node.__set_num_children(0);

        auto* expr = ArrayExprFactory::from_thrift(node);
        return std::unique_ptr<Expr>(expr);
    }

    static std::unique_ptr<Expr> create_array_expr(const TypeDescriptor& type) {
        return create_array_expr(type.to_thrift());
    }

    static std::unique_ptr<Expr> create_map_expr(const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::MAP_EXPR);
        node.__set_is_nullable(true);
        node.__set_type(type.to_thrift());
        node.__set_num_children(0);

        auto* expr = MapExprFactory::from_thrift(node);
        return std::unique_ptr<Expr>(expr);
    }

    static TExprNode create_binary_pred_node(TPrimitiveType::type type, TExprOpcode::type opcode) {
        TExprNode node;
        node.node_type = TExprNodeType::BINARY_PRED;
        node.num_children = 2;
        node.__set_opcode(opcode);
        node.__set_child_type(type);
        node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        return node;
    }

    template <LogicalType LType>
    static TTypeDesc get_ttype_desc() {
        if constexpr (LType == TYPE_SMALLINT) {
            return SmallIntTTypeDesc;
        } else if constexpr (LType == TYPE_INT) {
            return IntTTypeDesc;
        } else if constexpr (LType == TYPE_BIGINT) {
            return BigIntTTypeDesc;
        } else if constexpr (LType == TYPE_DATE) {
            return DateTTypeDesc;
        } else if constexpr (LType == TYPE_DATETIME) {
            return DateTimeTTypeDesc;
        } else if constexpr (LType == TYPE_VARCHAR) {
            return VarcharTTypeDesc;
        } else if constexpr (LType == TYPE_DECIMAL128) {
            return Decimal128TTypeDesc;
        } else {
            // not implement
            CHECK(false);
        }
    }

    template <LogicalType LType>
    static TExprNode create_in_pred_node(size_t size) {
        TExprNode node;

        node.node_type = TExprNodeType::IN_PRED;
        node.num_children = size;
        node.type = get_ttype_desc<LType>();

        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_child_type(to_thrift(LType));

        return node;
    }

    template <LogicalType LType>
    static TExprNode create_not_in_pred_node(size_t size) {
        TExprNode node;
        TInPredicate in_pred;
        in_pred.__set_is_not_in(true);

        node.node_type = TExprNodeType::IN_PRED;
        node.num_children = size;
        node.type = get_ttype_desc<LType>();

        node.__set_in_predicate(in_pred);
        node.__set_opcode(TExprOpcode::FILTER_NOT_IN);
        node.__set_child_type(to_thrift(LType));

        return node;
    }

    template <LogicalType LType, typename ValueType>
    static TExpr create_in_pred_texpr(SlotId slot_id, const std::vector<ValueType>& values, bool has_null) {
        std::vector<TExprNode> nodes;

        nodes.emplace_back(create_in_pred_node<LType>(values.size() + 1 + has_null));
        nodes.emplace_back(create_slot_expr_node_t<LType>(0, slot_id, true));
        for (auto value : values) {
            nodes.emplace_back(create_literal<LType, ValueType>(value, false));
        }
        if (has_null) {
            nodes.emplace_back(create_null_literal<LType>());
        }

        TExpr t_expr;
        t_expr.nodes = nodes;
        return t_expr;
    }

    template <LogicalType LType, typename ValueType>
    static TExpr create_not_in_pred_texpr(SlotId slot_id, const std::vector<ValueType>& values, bool has_null) {
        std::vector<TExprNode> nodes;

        nodes.emplace_back(create_not_in_pred_node<LType>(values.size() + 1 + has_null));
        nodes.emplace_back(create_slot_expr_node_t<LType>(0, slot_id, true));
        for (auto value : values) {
            nodes.emplace_back(create_literal<LType, ValueType>(value, false));
        }
        if (has_null) {
            nodes.emplace_back(create_null_literal<LType>());
        }

        TExpr t_expr;
        t_expr.nodes = nodes;
        return t_expr;
    }

    template <LogicalType LType, typename ValueType>
    static TExpr create_binary_pred_texpr(SlotId slot_id, ValueType value) {
        TExprNode pred_node = create_binary_pred_node(to_thrift(LType), TExprOpcode::GT);
        TExprNode col_ref = create_slot_expr_node_t<LType>(0, slot_id, true);
        TExprNode literal = create_literal<LType, ValueType>(value, false);

        TExpr texpr;
        texpr.nodes.emplace_back(pred_node);
        texpr.nodes.emplace_back(col_ref);
        texpr.nodes.emplace_back(literal);

        return texpr;
    }

    static Status create_and_open_conjunct_ctxs(ObjectPool* pool, RuntimeState* runtime_state,
                                                std::vector<TExpr>* tExprs, std::vector<ExprContext*>* conjunct_ctxs) {
        RETURN_IF_ERROR(Expr::create_expr_trees(pool, *tExprs, conjunct_ctxs, nullptr));
        RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, runtime_state));
        return Expr::open(*conjunct_ctxs, runtime_state);
    }

    static TExprNode create_slot_expr_node(TupleId tuple_id, SlotId slot_id, TTypeDesc ttype, bool is_nullable) {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = ttype;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = slot_id;
        slot_ref.slot_ref.tuple_id = tuple_id;
        slot_ref.__set_is_nullable(is_nullable);
        return slot_ref;
    }

    template <LogicalType LType>
    static TExprNode create_slot_expr_node_t(TupleId tuple_id, SlotId slot_id, bool is_nullable) {
        return create_slot_expr_node(tuple_id, slot_id, get_ttype_desc<LType>(), is_nullable);
    }

    template <LogicalType LType, typename ValueType>
    static TExprNode create_literal(ValueType value, bool is_nullable) {
        TExprNode node;
        node.num_children = 0;
        node.is_nullable = is_nullable;

        if constexpr (LType == TYPE_SMALLINT) {
            node.type = SmallIntTTypeDesc;
            TIntLiteral int_literal;
            int_literal.value = value;
            node.__set_int_literal(int_literal);
            node.node_type = TExprNodeType::INT_LITERAL;
        } else if constexpr (LType == TYPE_INT) {
            node.type = IntTTypeDesc;
            TIntLiteral int_literal;
            int_literal.value = value;
            node.__set_int_literal(int_literal);
            node.node_type = TExprNodeType::INT_LITERAL;
        } else if constexpr (LType == TYPE_BIGINT) {
            node.type = BigIntTTypeDesc;
            TIntLiteral int_literal;
            int_literal.value = value;
            node.__set_int_literal(int_literal);
            node.node_type = TExprNodeType::INT_LITERAL;
        } else if constexpr (LType == TYPE_DATE) {
            node.type = DateTTypeDesc;
            TDateLiteral date_literal;
            date_literal.value = value;
            node.__set_date_literal(date_literal);
            node.node_type = TExprNodeType::DATE_LITERAL;
        } else if constexpr (LType == TYPE_DATETIME) {
            node.type = DateTimeTTypeDesc;
            TDateLiteral date_literal;
            date_literal.value = value;
            node.__set_date_literal(date_literal);
            node.node_type = TExprNodeType::DATE_LITERAL;
        } else if constexpr (LType == TYPE_VARCHAR) {
            node.type = VarcharTTypeDesc;
            TStringLiteral string_literal;
            string_literal.value = value;
            node.__set_string_literal(string_literal);
            node.node_type = TExprNodeType::STRING_LITERAL;
        } else if constexpr (LType == TYPE_DECIMAL128) {
            node.type = Decimal128TTypeDesc;
            TDecimalLiteral decimal_literal;
            decimal_literal.value = value;
            node.__set_decimal_literal(decimal_literal);
            node.node_type = TExprNodeType::DECIMAL_LITERAL;
        } else {
            // not implement
            CHECK(false);
        }

        return node;
    }

    template <LogicalType LType>
    static TExprNode create_null_literal() {
        TExprNode node;
        node.node_type = TExprNodeType::NULL_LITERAL;
        node.type = get_ttype_desc<LType>();
        return node;
    }

    static TExpr create_slot_expr(TExprNode slot_ref) {
        TExpr expr;
        expr.nodes.push_back(slot_ref);
        return expr;
    }

    static TFunction create_builtin_function(const std::string func_name, const std::vector<TTypeDesc>& arg_types,
                                             const TTypeDesc& intermediate_type, const TTypeDesc& ret_type) {
        TFunction fn;
        {
            TFunctionName fn_name;
            fn_name.function_name = func_name;
            fn.name = fn_name;
            fn.binary_type = TFunctionBinaryType::BUILTIN;
            fn.arg_types = arg_types;
            fn.ret_type = ret_type;
            fn.aggregate_fn.intermediate_type = intermediate_type;
            fn.has_var_args = false;
        }
        return fn;
    }

    static TExpr create_aggregate_expr(TFunction fn, const std::vector<TExprNode>& children) {
        TExpr expr;

        TExprNode node;
        node.node_type = TExprNodeType::AGG_EXPR;
        node.has_nullable_child = false;

        TAggregateExpr agg_expr;
        agg_expr.is_merge_agg = false;

        node.agg_expr = agg_expr;
        node.fn = fn;
        node.num_children = children.size();

        expr.nodes.push_back(node);
        for (auto& child : children) {
            expr.nodes.push_back(child);
        }
        return expr;
    }
};

class TExprBuilder {
public:
    TExprBuilder& operator<<(const LogicalType& slot_type) {
        TExpr expr;
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        TTypeDesc tdesc;
        TTypeNode ttpe;
        TScalarType scalar_tp;
        scalar_tp.type = to_thrift(slot_type);
        scalar_tp.__set_len(200);
        scalar_tp.__set_precision(27);
        scalar_tp.__set_scale(9);
        ttpe.__set_scalar_type(scalar_tp);
        ttpe.type = TTypeNodeType::SCALAR;
        tdesc.types.push_back(std::move(ttpe));
        node.__set_type(tdesc);
        TSlotRef slot_ref;
        slot_ref.__set_tuple_id(tuple_id);
        slot_ref.__set_slot_id(column_id++);
        node.__set_slot_ref(slot_ref);
        expr.nodes.push_back(std::move(node));
        res.push_back(expr);
        return *this;
    }
    std::vector<TExpr> get_res() { return res; }

private:
    const int tuple_id = 0;
    int column_id = 0;
    std::vector<TExpr> res;
};
} // namespace starrocks

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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/es/es_predicate.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/es/es_predicate.h"

#include <gutil/strings/substitute.h>

#include <map>
#include <sstream>
#include <utility>

#include "column/column.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/es/es_query_builder.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/in_const_predicate.hpp"
#include "runtime/large_int_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"

namespace starrocks {

using namespace std;

#define RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(expr)                                          \
    do {                                                                                   \
        const Expr* expr_without_cast = Expr::expr_without_cast(expr);                     \
        if (expr_without_cast->node_type() != TExprNodeType::SLOT_REF) {                   \
            return Status::InternalError("build disjuncts failed: child is not slot ref"); \
        }                                                                                  \
    } while (false)

template <typename T, typename... Ts>
static constexpr bool is_type_in() {
    return (std::is_same_v<T, Ts> || ...);
}

VExtLiteral::VExtLiteral(LogicalType type, ColumnPtr column, const std::string& timezone) {
    DCHECK(!column->empty());
    // We need to convert the predicate column into the corresponding string.
    // Some types require special handling, because the default behavior of Datum may not match the behavior of ES.
    if (type == TYPE_DATE) {
        ColumnViewer<TYPE_DATE> viewer(column);
        DCHECK(!viewer.is_null(0));
        _value = viewer.value(0).to_string();
    } else if (type == TYPE_DATETIME) {
        ColumnViewer<TYPE_DATETIME> viewer(column);
        DCHECK(!viewer.is_null(0));
        TimestampValue datetime_value = viewer.value(0);
        // Use timezone variable from FE
        cctz::time_zone timezone_obj;
        if (!TimezoneUtils::find_cctz_time_zone(timezone, timezone_obj)) {
            // Use default +8 timezone instead.
            TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, timezone_obj);
        }
        int64_t offsets = TimezoneUtils::to_utc_offset(timezone_obj);
        _value = std::to_string((datetime_value.to_unix_second() - offsets) * 1000);
    } else if (type == TYPE_BOOLEAN) {
        ColumnViewer<TYPE_BOOLEAN> viewer(column);
        if (viewer.value(0)) {
            _value = "true";
        } else {
            _value = "false";
        }
    } else {
        _value = _value_to_string(column);
    }
}

std::string VExtLiteral::_value_to_string(ColumnPtr& column) {
    auto v = column->get(0);
    std::string res;
    v.visit([&](auto& variant) {
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (is_type_in<T, std::monostate, int96_t, decimal12_t, DecimalV2Value, DatumArray,
                                             DatumMap, HyperLogLog*, BitmapValue*, PercentileValue*, JsonValue*>()) {
                        // ignore these types
                    } else if constexpr (std::is_same_v<T, Slice>) {
                        res = std::string(arg.data, arg.size);
                    } else if constexpr (std::is_same_v<T, __int128_t>) {
                        res = LargeIntValue::to_string(arg);
                    } else {
                        res = std::to_string(arg);
                    }
                },
                variant);
    });
    return res;
}

EsPredicate::EsPredicate(ExprContext* context, const TupleDescriptor* tuple_desc, std::string timezone,
                         ObjectPool* pool)
        : _context(context),
          _tuple_desc(tuple_desc),
          _es_query_status(Status::OK()),
          _timezone(std::move(timezone)),
          _pool(pool) {}

EsPredicate::~EsPredicate() {
    for (auto& _disjunct : _disjuncts) {
        delete _disjunct;
    }
    _disjuncts.clear();
}

Status EsPredicate::build_disjuncts_list() {
    return _vec_build_disjuncts_list(_context->root());
}

// make sure to build by build_disjuncts_list
const std::vector<ExtPredicate*>& EsPredicate::get_predicate_list() {
    return _disjuncts;
}

static bool ignore_cast(const SlotDescriptor* slot, const Expr* expr) {
    if (slot->type().is_date_type() && expr->type().is_date_type()) {
        return true;
    }
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    return false;
}

static bool is_literal_node(const Expr* expr) {
    switch (expr->node_type()) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DATE_LITERAL:
        return true;
    default:
        return false;
    }
}

Status EsPredicate::_vec_build_disjuncts_list(const Expr* conjunct) {
    bool handled = false;
    RETURN_IF_ERROR(_build_binary_predicate(conjunct, &handled));
    RETURN_IF_ERROR(_build_functioncall_predicate(conjunct, &handled));
    RETURN_IF_ERROR(_build_in_predicate(conjunct, &handled));
    RETURN_IF_ERROR(_build_compound_predicate(conjunct, &handled));
    if (handled) {
        return Status::OK();
    } else {
        return Status::InternalError(
                fmt::format("build disjuncts failed: node type {} is not supported", conjunct->node_type()));
    }
}

Status EsPredicate::_build_binary_predicate(const Expr* conjunct, bool* handled) {
    using ColumnRef = ColumnRef;
    if (TExprNodeType::BINARY_PRED == conjunct->node_type()) {
        DCHECK_EQ(conjunct->children().size(), 2);
        *handled = true;

        ColumnRef* column_ref = nullptr;
        Expr* expr = nullptr;
        TExprOpcode::type op;
        // k1 = 2  k1 is float (marked for processing later),
        // doris on es should ignore this doris native cast transformation, we push down this `cast` to elasticsearch
        // conjunct->get_child(0)->node_type() return CAST_EXPR
        // conjunct->get_child(1)->node_type() return FLOAT_LITERAL
        // the left child is literal and right child is SlotRef maybe not happend, but here we just process
        // this situation regardless of the rewrite logic from the FE's Query Engine
        if (TExprNodeType::SLOT_REF == conjunct->get_child(0)->node_type() ||
            TExprNodeType::CAST_EXPR == conjunct->get_child(0)->node_type()) {
            expr = conjunct->get_child(1);
            // process such as sub-query: select * from (select split_part(k, "_", 1) as new_field from table) t where t.new_field > 1;
            RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(conjunct->get_child(0));
            // process cast expr, such as:
            // k (float) > 2.0, k(int) > 3.2
            column_ref = const_cast<ColumnRef*>(
                    down_cast<const ColumnRef*>(Expr::expr_without_cast(conjunct->get_child(0))));
            op = conjunct->op();
        } else if (TExprNodeType::SLOT_REF == conjunct->get_child(1)->node_type() ||
                   TExprNodeType::CAST_EXPR == conjunct->get_child(1)->node_type()) {
            expr = conjunct->get_child(0);
            RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(conjunct->get_child(1));
            column_ref = const_cast<ColumnRef*>(
                    down_cast<const ColumnRef*>(Expr::expr_without_cast(conjunct->get_child(1))));
            op = conjunct->op();
        } else {
            return Status::InternalError("build disjuncts failed: no SLOT_REF child");
        }

        const SlotDescriptor* slot_desc = get_slot_desc(column_ref->slot_id());
        if (column_ref == nullptr) {
            return Status::InternalError("build disjuncts failed: slot_desc is null");
        }

        if (!is_literal_node(expr)) {
            return Status::InternalError("build disjuncts failed: expr is not literal type");
        }

        // how to process literal
        ASSIGN_OR_RETURN(auto expr_value, _context->evaluate(expr, nullptr));
        auto literal = _pool->add(new VExtLiteral(expr->type().type, std::move(expr_value), _timezone));
        std::string col = slot_desc->col_name();

        // ES does not support non-bool literal pushdown for bool type
        if (column_ref->type().type == TYPE_BOOLEAN && expr->type().type != TYPE_BOOLEAN) {
            return Status::InternalError("ES does not support non-bool literal pushdown");
        }

        if (_field_context.find(col) != _field_context.end()) {
            col = _field_context[col];
        }
        ExtPredicate* predicate =
                new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, col, slot_desc->type(), op, literal);

        _disjuncts.push_back(predicate);
    }
    return Status::OK();
}

Status EsPredicate::_build_functioncall_predicate(const Expr* conjunct, bool* handled) {
    using ColumnRef = ColumnRef;

    if (TExprNodeType::FUNCTION_CALL == conjunct->node_type()) {
        *handled = true;

        std::string fname = conjunct->fn().name.function_name;
        if (fname == "esquery") {
            if (conjunct->children().size() != 2) {
                return Status::InternalError("build disjuncts failed: number of childs is not 2");
            }
            Expr* expr = conjunct->get_child(1);
            ASSIGN_OR_RETURN(auto expr_value, _context->evaluate(expr, nullptr));
            auto literal = _pool->add(new VExtLiteral(expr->type().type, std::move(expr_value), _timezone));
            std::vector<ExtLiteral*> query_conditions;
            query_conditions.emplace_back(literal);
            std::vector<ExtColumnDesc> cols;
            ExtPredicate* predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, "esquery", cols, query_conditions);
            if (_es_query_status.ok()) {
                _es_query_status = BooleanQueryBuilder::check_es_query(*(ExtFunction*)predicate);
                if (!_es_query_status.ok()) {
                    delete predicate;
                    return _es_query_status;
                }
            }
            _disjuncts.push_back(predicate);
        } else if (fname == "is_null_pred" || fname == "is_not_null_pred") {
            if (conjunct->children().size() != 1) {
                return Status::InternalError("build disjuncts failed: number of childs is not 1");
            }
            // such as sub-query: select * from (select split_part(k, "_", 1) as new_field from table) t where t.new_field > 1;
            // conjunct->get_child(0)->node_type() == TExprNodeType::FUNCTION_CALL, at present doris on es can not support push down function
            RETURN_ERROR_IF_EXPR_IS_NOT_SLOTREF(conjunct->get_child(0));

            auto* column_ref = const_cast<ColumnRef*>(
                    down_cast<const ColumnRef*>(Expr::expr_without_cast(conjunct->get_child(0))));

            const SlotDescriptor* slot_desc = get_slot_desc(column_ref->slot_id());

            if (slot_desc == nullptr) {
                return Status::InternalError("build disjuncts failed: no SLOT_REF child");
            }
            bool is_not_null = fname == "is_not_null_pred" ? true : false;
            std::string col = slot_desc->col_name();
            if (_field_context.find(col) != _field_context.end()) {
                col = _field_context[col];
            }
            // use TExprNodeType::IS_NULL_PRED for BooleanQueryBuilder translate
            auto* predicate = new ExtIsNullPredicate(TExprNodeType::IS_NULL_PRED, col, slot_desc->type(), is_not_null);
            _disjuncts.push_back(predicate);
        } else if (fname == "like") {
            if (conjunct->children().size() != 2) {
                return Status::InternalError("build disjuncts failed: number of childs is not 2");
            }
            ColumnRef* column_ref = nullptr;
            Expr* expr = nullptr;
            if (TExprNodeType::SLOT_REF == conjunct->get_child(0)->node_type()) {
                expr = conjunct->get_child(1);
                column_ref = const_cast<ColumnRef*>(down_cast<const ColumnRef*>(conjunct->get_child(0)));
            } else if (TExprNodeType::SLOT_REF == conjunct->get_child(1)->node_type()) {
                expr = conjunct->get_child(0);
                column_ref = const_cast<ColumnRef*>(down_cast<const ColumnRef*>(conjunct->get_child(1)));
            } else {
                return Status::InternalError("build disjuncts failed: no SLOT_REF child");
            }
            const SlotDescriptor* slot_desc = get_slot_desc(column_ref->slot_id());
            if (slot_desc == nullptr) {
                return Status::InternalError("build disjuncts failed: slot_desc is null");
            }

            LogicalType type = expr->type().type;
            if (type != TYPE_VARCHAR && type != TYPE_CHAR) {
                return Status::InternalError("build disjuncts failed: like value is not a string");
            }
            std::string col = slot_desc->col_name();
            if (_field_context.find(col) != _field_context.end()) {
                col = _field_context[col];
            }

            ASSIGN_OR_RETURN(auto expr_col, _context->evaluate(expr, nullptr));
            auto literal = _pool->add(new VExtLiteral(type, std::move(expr_col), _timezone));
            ExtPredicate* predicate = new ExtLikePredicate(TExprNodeType::LIKE_PRED, col, slot_desc->type(), literal);

            _disjuncts.push_back(predicate);
        } else {
            std::stringstream ss;
            ss << "can not process function predicate[ " << fname << " ]";
            return Status::InternalError(ss.str());
        }
        return Status::OK();
    }

    return Status::OK();
}

template <LogicalType type, typename Func>
Status build_inpred_values(const Predicate* pred, bool& is_not_in, Func&& func) {
    const auto* vpred = down_cast<const VectorizedInConstPredicate<type>*>(pred);
    const auto& hash_set = vpred->hash_set();
    bool has_null = vpred->null_in_set();
    is_not_in = vpred->is_not_in();
    if (has_null) {
        return Status::InternalError("build disjuncts failed: hash set has a null value");
    }
    for (const auto& v : hash_set) {
        func(v);
    }
    return Status::OK();
}

#define BUILD_INPRED_VALUES(TYPE)                                                                        \
    case TYPE: {                                                                                         \
        RETURN_IF_ERROR(build_inpred_values<TYPE>(pred, is_not_in, [&](auto& v) {                        \
            in_pred_values.emplace_back(_pool->add(new VExtLiteral(                                      \
                    slot_desc->type().type, ColumnHelper::create_const_column<TYPE>(v, 1), _timezone))); \
        }));                                                                                             \
        break;                                                                                           \
    }

Status EsPredicate::_build_in_predicate(const Expr* conjunct, bool* handled) {
    using ColumnRef = ColumnRef;

    if (TExprNodeType::IN_PRED == conjunct->node_type()) {
        *handled = true;
        // the op code maybe FILTER_NEW_IN, it means there is function in list
        // like col_a in (abs(1))
        if (TExprOpcode::FILTER_IN != conjunct->op() && TExprOpcode::FILTER_NOT_IN != conjunct->op()) {
            return Status::InternalError(
                    "build disjuncts failed: "
                    "opcode in IN_PRED is neither FILTER_IN nor FILTER_NOT_IN");
        }

        std::vector<ExtLiteral*> in_pred_values;
        const auto* pred = static_cast<const Predicate*>(conjunct);

        const Expr* expr = Expr::expr_without_cast(pred->get_child(0));
        if (expr->node_type() != TExprNodeType::SLOT_REF) {
            return Status::InternalError("build disjuncts failed: node type is not slot ref");
        }

        const SlotDescriptor* slot_desc = get_slot_desc(static_cast<const ColumnRef*>(expr)->slot_id());
        if (slot_desc == nullptr) {
            return Status::InternalError("build disjuncts failed: slot_desc is null");
        }

        if (pred->get_child(0)->type().type != slot_desc->type().type) {
            if (!ignore_cast(slot_desc, pred->get_child(0))) {
                return Status::InternalError("build disjuncts failed");
            }
        }

        bool is_not_in = false;
        // insert in list to ExtLiteral
        switch (expr->type().type) {
            BUILD_INPRED_VALUES(TYPE_BOOLEAN)
            BUILD_INPRED_VALUES(TYPE_INT)
            BUILD_INPRED_VALUES(TYPE_TINYINT)
            BUILD_INPRED_VALUES(TYPE_SMALLINT)
            BUILD_INPRED_VALUES(TYPE_BIGINT)
            BUILD_INPRED_VALUES(TYPE_LARGEINT)
            BUILD_INPRED_VALUES(TYPE_FLOAT)
            BUILD_INPRED_VALUES(TYPE_DOUBLE)
            BUILD_INPRED_VALUES(TYPE_DATE)
            BUILD_INPRED_VALUES(TYPE_DATETIME)
            BUILD_INPRED_VALUES(TYPE_CHAR)
            BUILD_INPRED_VALUES(TYPE_VARCHAR)
        default:
            // We don't support pushdown json type's in_predicates to ES now.
            // If failed here, we will fallback in_predicates executed on the BE side.
            return Status::InternalError("unsupported type to push down to ES");
        }

        std::string col = slot_desc->col_name();
        if (_field_context.find(col) != _field_context.end()) {
            col = _field_context[col];
        }
        ExtPredicate* predicate = new ExtInPredicate(TExprNodeType::IN_PRED, is_not_in, col, slot_desc->type(),
                                                     std::move(in_pred_values));
        _disjuncts.push_back(predicate);

        return Status::OK();
    }
    return Status::OK();
}

Status EsPredicate::_build_compound_predicate(const Expr* conjunct, bool* handled) {
    using ColumnRef = ColumnRef;

    if (TExprNodeType::COMPOUND_PRED == conjunct->node_type()) {
        *handled = true;
        // processe COMPOUND_AND, such as:
        // k = 1 or (k1 = 7 and (k2 in (6,7) or k3 = 12))
        // k1 = 7 and (k2 in (6,7) or k3 = 12) is compound pred, we should rebuild this sub tree
        if (conjunct->op() == TExprOpcode::COMPOUND_AND) {
            std::vector<EsPredicate*> conjuncts;
            for (int i = 0; i < conjunct->get_num_children(); ++i) {
                EsPredicate* predicate = _pool->add(new EsPredicate(_context, _tuple_desc, _timezone, _pool));
                predicate->set_field_context(_field_context);
                Status status = predicate->_vec_build_disjuncts_list(conjunct->children()[i]);
                if (status.ok()) {
                    conjuncts.push_back(predicate);
                } else {
                    return Status::InternalError("build COMPOUND_AND conjuncts failed");
                }
            }
            auto* compound_predicate = new ExtCompPredicates(TExprOpcode::COMPOUND_AND, conjuncts);
            _disjuncts.push_back(compound_predicate);
            return Status::OK();
        } else if (conjunct->op() == TExprOpcode::COMPOUND_NOT) {
            // reserved for processing COMPOUND_NOT
            return Status::InternalError("currently do not support COMPOUND_NOT push-down");
        }
        DCHECK(conjunct->op() == TExprOpcode::COMPOUND_OR);
        RETURN_IF_ERROR(_vec_build_disjuncts_list(conjunct->get_child(0)));
        RETURN_IF_ERROR(_vec_build_disjuncts_list(conjunct->get_child(1)));
    }
    return Status::OK();
}

const SlotDescriptor* EsPredicate::get_slot_desc(SlotId slot_id) {
    const SlotDescriptor* slot_desc = nullptr;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        if (slot->id() == slot_id) {
            slot_desc = slot;
            break;
        }
    }
    return slot_desc;
}

} // namespace starrocks

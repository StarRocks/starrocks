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

#include "paimon_evaluator.h"

#include "column/column.h"
#include "column/datum.h"
#include "exprs/column_ref.h"
#include "exprs/literal.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"

namespace starrocks {

PaimonEvaluator::PaimonEvaluator(const std::vector<SlotDescriptor*>& slots) : slots(slots) {}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate(const std::vector<Expr*>* conjuncts) {
    if (VLOG_ROW_IS_ON) {
        VLOG(10) << "PaimonEvaluator evaluating " << conjuncts->size() << " conjuncts";
        for (size_t i = 0; i < conjuncts->size(); ++i) {
            VLOG(10) << "Conjunct " << i << ": " << (*conjuncts)[i]->debug_string();
        }
    }
    auto result = evaluate_compound(TExprOpcode::type::COMPOUND_AND, conjuncts, false);

    if (VLOG_ROW_IS_ON) {
        VLOG(10) << "PaimonEvaluator result: " << (result ? "predicate created" : "null predicate");
    }
    return result;
}

std::shared_ptr<::paimon::Predicate> PaimonEvaluator::evaluate(starrocks::Expr* conjunct, bool neg) {
    const TExprNodeType::type& node_type = conjunct->node_type();
    if (node_type == TExprNodeType::type::RUNTIME_FILTER_MIN_MAX_EXPR) {
        return nullptr;
    }

    const TExprOpcode::type& op_type = conjunct->op();

    if (node_type == TExprNodeType::type::COMPOUND_PRED) {
        if (op_type == TExprOpcode::COMPOUND_AND || op_type == TExprOpcode::COMPOUND_OR) {
            return evaluate_compound(op_type, &conjunct->children(), neg);
        }
        if (op_type == TExprOpcode::COMPOUND_NOT) {
            DCHECK(conjunct->children().size() == 1);
            return evaluate(conjunct->get_child(0), !neg);
        }
    }

    // slot ref, like SELECT * FROM tbl where col;
    if (node_type == TExprNodeType::type::SLOT_REF) {
        auto* ref = down_cast<const ColumnRef*>(conjunct);
        DCHECK(conjunct->type().type == LogicalType::TYPE_BOOLEAN);
        for (size_t i = 0; i < slots.size(); ++i) {
            SlotDescriptor* slot = slots[i];
            if (slot->id() == ref->slot_id()) {
                if (!_ok_to_paimon_type(slot->type())) {
                    break;
                }
                return evaluate_equal(i, std::string(slot->col_name()), translate_to_paimon_type(slot->type()),
                                      ::paimon::Literal(true), neg);
            }
        }
    }

    // literal processing
    if (node_type == TExprNodeType::BOOL_LITERAL || node_type == TExprNodeType::NULL_LITERAL) {
        return nullptr;
    }

    Expr* left = conjunct->get_child(0);
    DCHECK(left->is_slotref());
    auto* ref = down_cast<const ColumnRef*>(left);
    for (size_t i = 0; i < slots.size(); ++i) {
        SlotDescriptor* slot = slots[i];
        if (slot->id() == ref->slot_id()) {
            if (!_ok_to_paimon_type(slot->type())) {
                break;
            }
            ::paimon::FieldType fieldType = translate_to_paimon_type(slot->type());
            auto fieldName = std::string(slot->col_name());
            if (node_type == TExprNodeType::IS_NULL_PRED || node_type == TExprNodeType::FUNCTION_CALL) {
                std::string null_function_name;
                if (conjunct->is_null_scalar_function(null_function_name)) {
                    if (null_function_name == "null") {
                        VLOG(10) << "evaluate IS_NULL " << fieldName;
                        return evaluate_null(i, fieldName, fieldType, neg);
                    } else if (null_function_name == "not null") {
                        VLOG(10) << "evaluate IS_NOT_NULL " << fieldName;
                        return evaluate_null(i, fieldName, fieldType, !neg);
                    }
                }
            }
            if (node_type == TExprNodeType::type::BINARY_PRED) {
                Expr* lit = conjunct->get_child(1);
                if (_ok_to_paimon_literal(lit)) {
                    ::paimon::Literal&& literal = translate_to_paimon_literal(conjunct->get_child(1));
                    switch (op_type) {
                    case TExprOpcode::EQ:
                    case TExprOpcode::EQ_FOR_NULL:
                        return evaluate_equal(i, fieldName, fieldType, literal, neg);
                    case TExprOpcode::NE:
                        return evaluate_equal(i, fieldName, fieldType, literal, !neg);
                    case TExprOpcode::LE:
                        return evaluate_le(i, fieldName, fieldType, literal, neg);
                    case TExprOpcode::LT:
                        return evaluate_lt(i, fieldName, fieldType, literal, neg);
                    case TExprOpcode::GE:
                        return evaluate_ge(i, fieldName, fieldType, literal, neg);
                    case TExprOpcode::GT:
                        return evaluate_gt(i, fieldName, fieldType, literal, neg);
                    default:
                        break;
                    }
                }
            }
            if (node_type == TExprNodeType::IN_PRED && conjunct->get_num_children() > 1) {
                std::vector<::paimon::Literal> literals;
                translate_to_paimon_in_list_literals(conjunct, literals);
                return evaluate_in(i, fieldName, fieldType, literals, (op_type == TExprOpcode::FILTER_NOT_IN) ^ neg);
            }
        }
    }

    return nullptr;
}

std::shared_ptr<::paimon::Predicate> PaimonEvaluator::evaluate_compound(TExprOpcode::type op_type,
                                                                        const std::vector<Expr*>* children, bool neg) {
    std::vector<std::shared_ptr<::paimon::Predicate>> predicates;
    predicates.reserve(children->size());
    for (const auto& item : *children) {
        auto predicate = evaluate(item, neg);
        if (neg ^ (op_type == TExprOpcode::type::COMPOUND_AND)) {
            if (predicate != nullptr) {
                predicates.push_back(predicate);
            }
        } else {
            if (predicate == nullptr) {
                return nullptr;
            }
            predicates.push_back(predicate);
        }
    }

    if (predicates.size() == 0) {
        return nullptr;
    }

    if (neg ^ (op_type == TExprOpcode::type::COMPOUND_AND)) {
        const ::paimon::Result<std::shared_ptr<::paimon::Predicate>> ret = ::paimon::PredicateBuilder::And(predicates);
        return ret.value();
    }
    const ::paimon::Result<std::shared_ptr<::paimon::Predicate>> ret = ::paimon::PredicateBuilder::Or(predicates);
    return ret.value();
}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate_null(int32_t field_index, const std::string& field_name,
                                                                  const paimon::FieldType& fieldType, bool neg) {
    return neg ? ::paimon::PredicateBuilder::IsNotNull(field_index, field_name, fieldType)
               : ::paimon::PredicateBuilder::IsNull(field_index, field_name, fieldType);
}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate_equal(int32_t field_index, const std::string& field_name,
                                                                   const paimon::FieldType& fieldType,
                                                                   const paimon::Literal& literal, bool neg) {
    return neg ? ::paimon::PredicateBuilder::NotEqual(field_index, field_name, fieldType, literal)
               : ::paimon::PredicateBuilder::Equal(field_index, field_name, fieldType, literal);
}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate_le(int32_t field_index, const std::string& field_name,
                                                                const paimon::FieldType& fieldType,
                                                                const paimon::Literal& literal, bool neg) {
    return neg ? ::paimon::PredicateBuilder::GreaterThan(field_index, field_name, fieldType, literal)
               : ::paimon::PredicateBuilder::LessOrEqual(field_index, field_name, fieldType, literal);
}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate_lt(int32_t field_index, const std::string& field_name,
                                                                const paimon::FieldType& fieldType,
                                                                const paimon::Literal& literal, bool neg) {
    return neg ? ::paimon::PredicateBuilder::GreaterOrEqual(field_index, field_name, fieldType, literal)
               : ::paimon::PredicateBuilder::LessThan(field_index, field_name, fieldType, literal);
}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate_ge(int32_t field_index, const std::string& field_name,
                                                                const paimon::FieldType& fieldType,
                                                                const paimon::Literal& literal, bool neg) {
    return neg ? ::paimon::PredicateBuilder::LessThan(field_index, field_name, fieldType, literal)
               : ::paimon::PredicateBuilder::GreaterOrEqual(field_index, field_name, fieldType, literal);
}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate_gt(int32_t field_index, const std::string& field_name,
                                                                const paimon::FieldType& fieldType,
                                                                const paimon::Literal& literal, bool neg) {
    return neg ? ::paimon::PredicateBuilder::LessOrEqual(field_index, field_name, fieldType, literal)
               : ::paimon::PredicateBuilder::GreaterThan(field_index, field_name, fieldType, literal);
}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate_in(int32_t field_index, const std::string& field_name,
                                                                const ::paimon::FieldType& fieldType,
                                                                const std::vector<::paimon::Literal>& literals,
                                                                bool neg) {
    return neg ? ::paimon::PredicateBuilder::NotIn(field_index, field_name, fieldType, literals)
               : ::paimon::PredicateBuilder::In(field_index, field_name, fieldType, literals);
}

bool PaimonEvaluator::_ok_to_paimon_literal(starrocks::Expr* lit) {
    TExprNodeType::type node_type = lit->node_type();
    LogicalType ltype = lit->type().type;
    if (node_type == TExprNodeType::type::NULL_LITERAL) {
        return true;
    }
    switch (ltype) {
    case LogicalType::TYPE_BOOLEAN:
    case LogicalType::TYPE_TINYINT:
    case LogicalType::TYPE_SMALLINT:
    case LogicalType::TYPE_INT:
    case LogicalType::TYPE_BIGINT:
    case LogicalType::TYPE_FLOAT:
    case LogicalType::TYPE_DOUBLE:
    case LogicalType::TYPE_VARCHAR:
    case LogicalType::TYPE_CHAR:
        return true;
    default:
        return false;
    }
}

// Literal support BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, STRING, BINARY,
// TIMESTAMP, DECIMAL, DATE
paimon::Literal PaimonEvaluator::translate_to_paimon_literal(starrocks::Expr* lit) {
    TExprNodeType::type node_type = lit->node_type();
    if (node_type == TExprNodeType::type::NULL_LITERAL) {
        return ::paimon::Literal(paimon::FieldType::BOOLEAN);
    }

    auto* vlit = down_cast<VectorizedLiteral*>(lit);
    auto ptr = vlit->evaluate_checked(nullptr, nullptr).value();
    if (ptr->only_null()) {
        return ::paimon::Literal(paimon::FieldType::BOOLEAN);
    }

    paimon::FieldType paimon_data_type = translate_to_paimon_type(lit->type());
    const Datum& datum = ptr->get(0);

    switch (paimon_data_type) {
    case ::paimon::FieldType::BOOLEAN:
    case ::paimon::FieldType::TINYINT:
        return ::paimon::Literal(datum.get_int8());
    case ::paimon::FieldType::SMALLINT:
        return ::paimon::Literal(datum.get_int16());
    case ::paimon::FieldType::INT:
        return ::paimon::Literal(datum.get_int32());
    case ::paimon::FieldType::BIGINT:
        return ::paimon::Literal(datum.get_int64());
    case ::paimon::FieldType::FLOAT:
        return ::paimon::Literal(datum.get_float());
    case ::paimon::FieldType::DOUBLE:
        return ::paimon::Literal(datum.get_double());
    case ::paimon::FieldType::STRING: {
        const Slice& slice = datum.get_slice();
        return ::paimon::Literal(::paimon::FieldType::STRING, slice.data, slice.size);
    }
    default:
        throw std::runtime_error("unknown data type error");
    }
}

bool PaimonEvaluator::_ok_to_paimon_type(const starrocks::TypeDescriptor& type) {
    LogicalType ltype = type.type;
    switch (ltype) {
    case LogicalType::TYPE_BOOLEAN:
    case LogicalType::TYPE_TINYINT:
    case LogicalType::TYPE_SMALLINT:
    case LogicalType::TYPE_INT:
    case LogicalType::TYPE_BIGINT:
    case LogicalType::TYPE_FLOAT:
    case LogicalType::TYPE_DOUBLE:
    case LogicalType::TYPE_CHAR:
    case LogicalType::TYPE_VARCHAR:
        return true;
    default:
        return false;
    }
}

::paimon::FieldType PaimonEvaluator::translate_to_paimon_type(const starrocks::TypeDescriptor& type) {
    LogicalType ltype = type.type;
    switch (ltype) {
    case LogicalType::TYPE_BOOLEAN:
        return ::paimon::FieldType::BOOLEAN;
    case LogicalType::TYPE_TINYINT:
        return ::paimon::FieldType::TINYINT;
    case LogicalType::TYPE_SMALLINT:
        return ::paimon::FieldType::SMALLINT;
    case LogicalType::TYPE_INT:
        return ::paimon::FieldType::INT;
    case LogicalType::TYPE_BIGINT:
        return ::paimon::FieldType::BIGINT;
    case LogicalType::TYPE_FLOAT:
        return ::paimon::FieldType::FLOAT;
    case LogicalType::TYPE_DOUBLE:
        return ::paimon::FieldType::DOUBLE;
    case LogicalType::TYPE_CHAR:
    case LogicalType::TYPE_VARCHAR:
        return ::paimon::FieldType::STRING;
    case LogicalType::TYPE_BINARY:
        return ::paimon::FieldType::BINARY;
    default:
        throw std::runtime_error("unknown data type error");
    }
}

void PaimonEvaluator::translate_to_paimon_in_list_literals(starrocks::Expr* in_list_expr,
                                                           std::vector<::paimon::Literal>& ret) {
    for (int i = 1; i < in_list_expr->get_num_children(); i++) {
        ret.emplace_back(translate_to_paimon_literal(in_list_expr->get_child(i)));
    }
}
//
} // namespace starrocks

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

#include "connector/hive/paimon/paimon_evaluator.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "column/column.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/literal.h"
#include "gutil/casts.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "runtime/descriptors.h"
#include "types/datum.h"

namespace starrocks {
namespace {

using PredicatePtr = std::shared_ptr<paimon::Predicate>;

struct ReadField {
    int32_t index;
    std::string name;
    paimon::FieldType type;
};

std::optional<paimon::FieldType> to_paimon_type(const TypeDescriptor& type) {
    switch (type.type) {
    case LogicalType::TYPE_BOOLEAN:
        return paimon::FieldType::BOOLEAN;
    case LogicalType::TYPE_TINYINT:
        return paimon::FieldType::TINYINT;
    case LogicalType::TYPE_SMALLINT:
        return paimon::FieldType::SMALLINT;
    case LogicalType::TYPE_INT:
        return paimon::FieldType::INT;
    case LogicalType::TYPE_BIGINT:
        return paimon::FieldType::BIGINT;
    case LogicalType::TYPE_FLOAT:
        return paimon::FieldType::FLOAT;
    case LogicalType::TYPE_DOUBLE:
        return paimon::FieldType::DOUBLE;
    case LogicalType::TYPE_CHAR:
    case LogicalType::TYPE_VARCHAR:
        return paimon::FieldType::STRING;
    case LogicalType::TYPE_BINARY:
    case LogicalType::TYPE_VARBINARY:
        return paimon::FieldType::BINARY;
    default:
        return std::nullopt;
    }
}

std::optional<ReadField> find_read_field(const std::vector<SlotDescriptor*>& read_slots, SlotId slot_id) {
    for (size_t i = 0; i < read_slots.size(); ++i) {
        const SlotDescriptor* slot = read_slots[i];
        if (slot == nullptr || slot->id() != slot_id || i > std::numeric_limits<int32_t>::max()) {
            continue;
        }

        auto field_type = to_paimon_type(slot->type());
        if (!field_type.has_value()) {
            return std::nullopt;
        }
        return ReadField{static_cast<int32_t>(i), std::string(slot->col_name()), *field_type};
    }
    return std::nullopt;
}

std::optional<paimon::Literal> to_paimon_literal(const Expr* expr, paimon::FieldType expected_type) {
    if (expr == nullptr) {
        return std::nullopt;
    }
    if (expr->node_type() == TExprNodeType::NULL_LITERAL) {
        return paimon::Literal(expected_type);
    }
    if (!expr->is_literal()) {
        return std::nullopt;
    }

    auto literal_type = to_paimon_type(expr->type());
    if (!literal_type.has_value() || *literal_type != expected_type) {
        return std::nullopt;
    }

    const auto* literal = down_cast<const VectorizedLiteral*>(expr);
    const ColumnPtr value = literal->value();
    if (value == nullptr || value->empty()) {
        return std::nullopt;
    }
    if (value->only_null()) {
        return paimon::Literal(expected_type);
    }

    const Datum datum = value->get(0);
    if (datum.is_null()) {
        return paimon::Literal(expected_type);
    }

    switch (expected_type) {
    case paimon::FieldType::BOOLEAN:
        return paimon::Literal(datum.get_uint8() != 0);
    case paimon::FieldType::TINYINT:
        return paimon::Literal(datum.get_int8());
    case paimon::FieldType::SMALLINT:
        return paimon::Literal(datum.get_int16());
    case paimon::FieldType::INT:
        return paimon::Literal(datum.get_int32());
    case paimon::FieldType::BIGINT:
        return paimon::Literal(datum.get_int64());
    case paimon::FieldType::FLOAT:
        return paimon::Literal(datum.get_float());
    case paimon::FieldType::DOUBLE:
        return paimon::Literal(datum.get_double());
    case paimon::FieldType::STRING:
    case paimon::FieldType::BINARY: {
        const Slice& slice = datum.get_slice();
        return paimon::Literal(expected_type, slice.data, slice.size);
    }
    default:
        return std::nullopt;
    }
}

PredicatePtr combine_predicates(bool use_and, const std::vector<PredicatePtr>& predicates) {
    auto result = use_and ? paimon::PredicateBuilder::And(predicates) : paimon::PredicateBuilder::Or(predicates);
    if (!result.ok()) {
        return nullptr;
    }
    return result.value();
}

PredicatePtr make_null_predicate(const ReadField& field, bool negated) {
    return negated ? paimon::PredicateBuilder::IsNotNull(field.index, field.name, field.type)
                   : paimon::PredicateBuilder::IsNull(field.index, field.name, field.type);
}

PredicatePtr make_equal_predicate(const ReadField& field, const paimon::Literal& literal, bool negated) {
    return negated ? paimon::PredicateBuilder::NotEqual(field.index, field.name, field.type, literal)
                   : paimon::PredicateBuilder::Equal(field.index, field.name, field.type, literal);
}

PredicatePtr make_binary_predicate(const ReadField& field, TExprOpcode::type opcode, const paimon::Literal& literal,
                                   bool negated) {
    switch (opcode) {
    case TExprOpcode::EQ:
        return make_equal_predicate(field, literal, negated);
    case TExprOpcode::EQ_FOR_NULL:
        if (literal.IsNull()) {
            return make_null_predicate(field, negated);
        }
        if (!negated) {
            return make_equal_predicate(field, literal, false);
        }
        // NOT (field <=> non_null_literal) is true for NULL fields as well as unequal fields.
        return combine_predicates(false,
                                  {paimon::PredicateBuilder::IsNull(field.index, field.name, field.type),
                                   paimon::PredicateBuilder::NotEqual(field.index, field.name, field.type, literal)});
    case TExprOpcode::NE:
        return make_equal_predicate(field, literal, !negated);
    case TExprOpcode::LE:
        return negated ? paimon::PredicateBuilder::GreaterThan(field.index, field.name, field.type, literal)
                       : paimon::PredicateBuilder::LessOrEqual(field.index, field.name, field.type, literal);
    case TExprOpcode::LT:
        return negated ? paimon::PredicateBuilder::GreaterOrEqual(field.index, field.name, field.type, literal)
                       : paimon::PredicateBuilder::LessThan(field.index, field.name, field.type, literal);
    case TExprOpcode::GE:
        return negated ? paimon::PredicateBuilder::LessThan(field.index, field.name, field.type, literal)
                       : paimon::PredicateBuilder::GreaterOrEqual(field.index, field.name, field.type, literal);
    case TExprOpcode::GT:
        return negated ? paimon::PredicateBuilder::LessOrEqual(field.index, field.name, field.type, literal)
                       : paimon::PredicateBuilder::GreaterThan(field.index, field.name, field.type, literal);
    default:
        return nullptr;
    }
}

PredicatePtr evaluate_expr(const Expr* expr, bool negated, const std::vector<SlotDescriptor*>& read_slots);

PredicatePtr evaluate_compound(TExprOpcode::type opcode, const std::vector<Expr*>& children, bool negated,
                               const std::vector<SlotDescriptor*>& read_slots) {
    if (opcode != TExprOpcode::COMPOUND_AND && opcode != TExprOpcode::COMPOUND_OR) {
        return nullptr;
    }

    // De Morgan's law turns a negated AND into OR and a negated OR into AND. Unsupported children
    // may be omitted only from an AND: each remaining child is still a necessary condition. An OR
    // must be abandoned if any child is unsupported, otherwise valid rows could be filtered out.
    const bool use_and = negated ^ (opcode == TExprOpcode::COMPOUND_AND);
    std::vector<PredicatePtr> predicates;
    predicates.reserve(children.size());
    for (const Expr* child : children) {
        PredicatePtr predicate = evaluate_expr(child, negated, read_slots);
        if (predicate != nullptr) {
            predicates.emplace_back(std::move(predicate));
        } else if (!use_and) {
            return nullptr;
        }
    }

    if (predicates.empty()) {
        return nullptr;
    }
    return combine_predicates(use_and, predicates);
}

PredicatePtr evaluate_expr(const Expr* expr, bool negated, const std::vector<SlotDescriptor*>& read_slots) {
    if (expr == nullptr || expr->node_type() == TExprNodeType::RUNTIME_FILTER_MIN_MAX_EXPR) {
        return nullptr;
    }

    const TExprNodeType::type node_type = expr->node_type();
    const TExprOpcode::type opcode = expr->op();
    if (node_type == TExprNodeType::COMPOUND_PRED) {
        if (opcode == TExprOpcode::COMPOUND_NOT) {
            if (expr->get_num_children() != 1) {
                return nullptr;
            }
            return evaluate_expr(expr->get_child(0), !negated, read_slots);
        }
        return evaluate_compound(opcode, expr->children(), negated, read_slots);
    }

    // A boolean slot reference is a complete predicate, for example: WHERE is_active.
    if (node_type == TExprNodeType::SLOT_REF) {
        if (expr->type().type != LogicalType::TYPE_BOOLEAN) {
            return nullptr;
        }
        const auto* column_ref = down_cast<const ColumnRef*>(expr);
        auto field = find_read_field(read_slots, column_ref->slot_id());
        if (!field.has_value() || field->type != paimon::FieldType::BOOLEAN) {
            return nullptr;
        }
        return make_equal_predicate(*field, paimon::Literal(true), negated);
    }

    if (node_type == TExprNodeType::BOOL_LITERAL || node_type == TExprNodeType::NULL_LITERAL) {
        return nullptr;
    }

    // Validate the node shape before reading children. This keeps newly introduced or malformed
    // expression kinds on the residual-evaluation path rather than relying on DCHECKs.
    switch (node_type) {
    case TExprNodeType::IS_NULL_PRED:
    case TExprNodeType::FUNCTION_CALL:
    case TExprNodeType::BINARY_PRED:
    case TExprNodeType::IN_PRED:
        break;
    default:
        return nullptr;
    }
    if (expr->get_num_children() == 0 || !expr->get_child(0)->is_slotref()) {
        return nullptr;
    }

    const auto* column_ref = down_cast<const ColumnRef*>(expr->get_child(0));
    auto field = find_read_field(read_slots, column_ref->slot_id());
    if (!field.has_value()) {
        return nullptr;
    }

    if (node_type == TExprNodeType::IS_NULL_PRED || node_type == TExprNodeType::FUNCTION_CALL) {
        if (expr->get_num_children() != 1) {
            return nullptr;
        }
        std::string null_function_name;
        if (!expr->is_null_scalar_function(null_function_name)) {
            return nullptr;
        }
        if (null_function_name == "null") {
            return make_null_predicate(*field, negated);
        }
        if (null_function_name == "not null") {
            return make_null_predicate(*field, !negated);
        }
        return nullptr;
    }

    if (node_type == TExprNodeType::BINARY_PRED) {
        if (expr->get_num_children() != 2) {
            return nullptr;
        }
        auto literal = to_paimon_literal(expr->get_child(1), field->type);
        if (!literal.has_value()) {
            return nullptr;
        }
        // Paimon rejects NULL literals in leaf predicates. EQ_FOR_NULL has an
        // exact IS NULL translation; ordinary SQL comparisons keep their
        // NULL semantics on the residual-evaluation path.
        if (literal->IsNull() && opcode != TExprOpcode::EQ_FOR_NULL) {
            return nullptr;
        }
        return make_binary_predicate(*field, opcode, *literal, negated);
    }

    if (node_type != TExprNodeType::IN_PRED || expr->get_num_children() <= 1 ||
        (opcode != TExprOpcode::FILTER_IN && opcode != TExprOpcode::FILTER_NOT_IN)) {
        return nullptr;
    }

    std::vector<paimon::Literal> literals;
    literals.reserve(expr->get_num_children() - 1);
    for (int i = 1; i < expr->get_num_children(); ++i) {
        auto literal = to_paimon_literal(expr->get_child(i), field->type);
        if (!literal.has_value() || literal->IsNull()) {
            return nullptr;
        }
        literals.emplace_back(std::move(*literal));
    }

    const bool use_not_in = (opcode == TExprOpcode::FILTER_NOT_IN) ^ negated;
    return use_not_in ? paimon::PredicateBuilder::NotIn(field->index, field->name, field->type, literals)
                      : paimon::PredicateBuilder::In(field->index, field->name, field->type, literals);
}

} // namespace

PaimonEvaluator::PaimonEvaluator(const std::vector<SlotDescriptor*>& read_slots) : _read_slots(read_slots) {}

std::shared_ptr<paimon::Predicate> PaimonEvaluator::evaluate(const std::vector<Expr*>* conjuncts) const {
    if (conjuncts == nullptr || conjuncts->empty()) {
        return nullptr;
    }
    return evaluate_compound(TExprOpcode::COMPOUND_AND, *conjuncts, false, _read_slots);
}

} // namespace starrocks

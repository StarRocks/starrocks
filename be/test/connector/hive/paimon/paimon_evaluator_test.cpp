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

#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "exprs/binary_predicate.h"
#include "exprs/column_ref.h"
#include "exprs/compound_predicate.h"
#include "exprs/in_predicate.h"
#include "exprs/literal.h"
#include "paimon/predicate/leaf_predicate.h"
#include "runtime/descriptors.h"

namespace starrocks {
namespace {

class PaimonEvaluatorTest : public ::testing::Test {
protected:
    SlotDescriptor* add_slot(SlotId id, std::string name, LogicalType type = TYPE_INT) {
        return _pool.add(new SlotDescriptor(id, std::move(name), TypeDescriptor(type)));
    }

    ColumnRef* add_column_ref(const SlotDescriptor* slot) { return _pool.add(new ColumnRef(slot)); }

    VectorizedLiteral* add_int_literal(int32_t value) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_type(TypeDescriptor(TYPE_INT).to_thrift());
        node.__set_num_children(0);
        TIntLiteral literal;
        literal.__set_value(value);
        node.__set_int_literal(literal);
        return _pool.add(new VectorizedLiteral(node));
    }

    VectorizedLiteral* add_null_int_literal() {
        TExprNode node;
        node.__set_node_type(TExprNodeType::NULL_LITERAL);
        node.__set_type(TypeDescriptor(TYPE_INT).to_thrift());
        node.__set_num_children(0);
        node.__set_is_nullable(true);
        return _pool.add(new VectorizedLiteral(node));
    }

    Expr* add_binary_predicate(TExprOpcode::type opcode, Expr* lhs, Expr* rhs) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::BINARY_PRED);
        node.__set_opcode(opcode);
        node.__set_child_type(TPrimitiveType::INT);
        node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        node.__set_num_children(2);

        Expr* predicate = _pool.add(VectorizedBinaryPredicateFactory::from_thrift(node));
        predicate->add_child(lhs);
        predicate->add_child(rhs);
        return predicate;
    }

    Expr* add_in_predicate(const SlotDescriptor* slot, std::initializer_list<Expr*> literals) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::IN_PRED);
        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_child_type(TPrimitiveType::INT);
        node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        node.__set_num_children(literals.size() + 1);

        Expr* predicate = _pool.add(VectorizedInPredicateFactory::from_thrift(node));
        predicate->add_child(add_column_ref(slot));
        for (Expr* literal : literals) {
            predicate->add_child(literal);
        }
        return predicate;
    }

    Expr* add_compound_predicate(TExprOpcode::type opcode, std::initializer_list<Expr*> children) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::COMPOUND_PRED);
        node.__set_opcode(opcode);
        node.__set_child_type(TPrimitiveType::BOOLEAN);
        node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        node.__set_num_children(children.size());

        Expr* predicate = _pool.add(VectorizedCompoundPredicateFactory::from_thrift(node));
        for (Expr* child : children) {
            predicate->add_child(child);
        }
        return predicate;
    }

    static std::shared_ptr<paimon::Predicate> evaluate(const PaimonEvaluator& evaluator,
                                                       std::initializer_list<Expr*> conjuncts) {
        std::vector<Expr*> conjunct_vector(conjuncts);
        return evaluator.evaluate(&conjunct_vector);
    }

    ObjectPool _pool;
};

TEST_F(PaimonEvaluatorTest, UsesReadProjectionOrderForFieldIndex) {
    SlotDescriptor* projected_first = add_slot(10, "projected_first");
    SlotDescriptor* projected_second = add_slot(20, "projected_second");
    PaimonEvaluator evaluator({projected_first, projected_second});

    Expr* comparison = add_binary_predicate(TExprOpcode::GT, add_column_ref(projected_second), add_int_literal(7));
    auto predicate = evaluate(evaluator, {comparison});

    ASSERT_NE(nullptr, predicate);
    EXPECT_EQ("GreaterThan(projected_second, 7)", predicate->ToString());
    auto leaf = std::dynamic_pointer_cast<paimon::LeafPredicate>(predicate);
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ(1, leaf->FieldIndex());
}

TEST_F(PaimonEvaluatorTest, ConvertsBooleanSlotReference) {
    SlotDescriptor* active = add_slot(30, "is_active", TYPE_BOOLEAN);
    PaimonEvaluator evaluator({active});

    auto predicate = evaluate(evaluator, {add_column_ref(active)});

    ASSERT_NE(nullptr, predicate);
    EXPECT_EQ("Equal(is_active, true)", predicate->ToString());
    auto leaf = std::dynamic_pointer_cast<paimon::LeafPredicate>(predicate);
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ(0, leaf->FieldIndex());
}

TEST_F(PaimonEvaluatorTest, RejectsNullInOrdinaryComparisonAndInList) {
    SlotDescriptor* value = add_slot(40, "value");
    PaimonEvaluator evaluator({value});

    Expr* null_safe_comparison =
            add_binary_predicate(TExprOpcode::EQ_FOR_NULL, add_column_ref(value), add_null_int_literal());
    auto null_safe_predicate = evaluate(evaluator, {null_safe_comparison});
    ASSERT_NE(nullptr, null_safe_predicate);
    EXPECT_EQ("IsNull(value)", null_safe_predicate->ToString());

    Expr* null_comparison = add_binary_predicate(TExprOpcode::EQ, add_column_ref(value), add_null_int_literal());
    EXPECT_EQ(nullptr, evaluate(evaluator, {null_comparison}));

    Expr* in_with_null = add_in_predicate(value, {add_int_literal(1), add_null_int_literal()});
    EXPECT_EQ(nullptr, evaluate(evaluator, {in_with_null}));
}

TEST_F(PaimonEvaluatorTest, RejectsUnsupportedAndNonSlotExpressions) {
    SlotDescriptor* value = add_slot(50, "value");
    PaimonEvaluator evaluator({value});

    EXPECT_EQ(nullptr, evaluate(evaluator, {add_int_literal(1)}));

    Expr* non_slot_comparison = add_binary_predicate(TExprOpcode::EQ, add_int_literal(1), add_int_literal(2));
    EXPECT_EQ(nullptr, evaluate(evaluator, {non_slot_comparison}));
}

TEST_F(PaimonEvaluatorTest, PartiallyPushesAndButAbandonsOr) {
    SlotDescriptor* value = add_slot(60, "value");
    PaimonEvaluator evaluator({value});
    Expr* supported = add_binary_predicate(TExprOpcode::GE, add_column_ref(value), add_int_literal(5));
    Expr* unsupported = add_int_literal(1);

    Expr* and_predicate = add_compound_predicate(TExprOpcode::COMPOUND_AND, {supported, unsupported});
    auto pushed_and = evaluate(evaluator, {and_predicate});
    ASSERT_NE(nullptr, pushed_and);
    EXPECT_EQ("GreaterOrEqual(value, 5)", pushed_and->ToString());

    Expr* or_predicate = add_compound_predicate(TExprOpcode::COMPOUND_OR, {supported, unsupported});
    EXPECT_EQ(nullptr, evaluate(evaluator, {or_predicate}));
}

} // namespace
} // namespace starrocks

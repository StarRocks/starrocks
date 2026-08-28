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

#include "connector/hive/paimon/paimon_predicate_converter.h"

#include <gtest/gtest.h>
#include <paimon/predicate/leaf_predicate.h>

#include <memory>

#include "column/vectorized_fwd.h"
#include "exprs/column_ref.h"
#include "exprs/literal.h"
#include "runtime/descriptors.h"
#include "types/logical_type.h"

namespace starrocks {
namespace {

class TestExpr final : public Expr {
public:
    explicit TestExpr(const TExprNode& node) : Expr(node) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new TestExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        return Status::NotSupported("test expression is not evaluable");
    }
};

TExprNode create_binary_predicate_node(TPrimitiveType::type child_type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_opcode(TExprOpcode::EQ);
    node.__set_child_type(child_type);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    node.__set_num_children(2);
    return node;
}

TExprNode create_function_node(TPrimitiveType::type return_type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(gen_type_desc(return_type));
    node.__set_num_children(0);
    return node;
}

TExprNode create_in_predicate_node(TPrimitiveType::type child_type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__set_child_type(child_type);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    node.__set_num_children(2);
    return node;
}

TExprNode create_boolean_literal_node(bool value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::BOOL_LITERAL);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    node.__set_num_children(0);
    TBoolLiteral literal;
    literal.__set_value(value);
    node.__set_bool_literal(literal);
    return node;
}

TExprNode create_tinyint_literal_node(int8_t value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_type(gen_type_desc(TPrimitiveType::TINYINT));
    node.__set_num_children(0);
    TIntLiteral literal;
    literal.__set_value(value);
    node.__set_int_literal(literal);
    return node;
}

std::shared_ptr<paimon::LeafPredicate> as_leaf_predicate(const std::shared_ptr<paimon::Predicate>& predicate) {
    return std::dynamic_pointer_cast<paimon::LeafPredicate>(predicate);
}

} // namespace

TEST(PaimonPredicateConverterTest, SkipsPredicateWithNonSlotRefLeftOperand) {
    SlotDescriptor worldcode_id(1, "worldcode_id", TypeDescriptor(TYPE_VARCHAR));
    SlotDescriptor isolation(7, "isolation", TypeDescriptor(TYPE_BOOLEAN));
    SlotDescriptor score(10, "_INDEX_SCORE", TypeDescriptor(TYPE_FLOAT));
    PaimonPredicateConverter converter({&worldcode_id, &isolation, &score});

    TestExpr predicate(create_binary_predicate_node(TPrimitiveType::BOOLEAN));
    TestExpr coalesce(create_function_node(TPrimitiveType::BOOLEAN));
    VectorizedLiteral false_literal(create_boolean_literal_node(false));
    predicate.add_child(&coalesce);
    predicate.add_child(&false_literal);

    std::vector<Expr*> conjuncts{&predicate};
    EXPECT_EQ(nullptr, converter.convert(&conjuncts));
}

TEST(PaimonPredicateConverterTest, ConvertsBooleanLiteralToPaimonBoolean) {
    SlotDescriptor isolation(7, "isolation", TypeDescriptor(TYPE_BOOLEAN));
    PaimonPredicateConverter converter({&isolation});

    TestExpr predicate(create_binary_predicate_node(TPrimitiveType::BOOLEAN));
    ColumnRef isolation_ref(TypeDescriptor(TYPE_BOOLEAN), isolation.id());
    VectorizedLiteral false_literal(create_boolean_literal_node(false));
    predicate.add_child(&isolation_ref);
    predicate.add_child(&false_literal);

    std::vector<Expr*> conjuncts{&predicate};
    auto leaf = as_leaf_predicate(converter.convert(&conjuncts));
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ("isolation", leaf->FieldName());
    EXPECT_EQ(paimon::FieldType::BOOLEAN, leaf->GetFieldType());
    ASSERT_EQ(1, leaf->Literals().size());
    EXPECT_EQ(paimon::FieldType::BOOLEAN, leaf->Literals()[0].GetType());
    EXPECT_FALSE(leaf->Literals()[0].GetValue<bool>());
}

TEST(PaimonPredicateConverterTest, KeepsTinyintLiteralAsPaimonTinyint) {
    SlotDescriptor tinyint_slot(8, "tinyint_col", TypeDescriptor(TYPE_TINYINT));
    PaimonPredicateConverter converter({&tinyint_slot});

    TestExpr predicate(create_binary_predicate_node(TPrimitiveType::TINYINT));
    ColumnRef tinyint_ref(TypeDescriptor(TYPE_TINYINT), tinyint_slot.id());
    VectorizedLiteral tinyint_literal(create_tinyint_literal_node(-1));
    predicate.add_child(&tinyint_ref);
    predicate.add_child(&tinyint_literal);

    std::vector<Expr*> conjuncts{&predicate};
    auto leaf = as_leaf_predicate(converter.convert(&conjuncts));
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ(paimon::FieldType::TINYINT, leaf->GetFieldType());
    ASSERT_EQ(1, leaf->Literals().size());
    EXPECT_EQ(paimon::FieldType::TINYINT, leaf->Literals()[0].GetType());
    EXPECT_EQ(-1, leaf->Literals()[0].GetValue<int8_t>());
}

TEST(PaimonPredicateConverterTest, SkipsPredicateWithNonLiteralRightOperand) {
    SlotDescriptor isolation(7, "isolation", TypeDescriptor(TYPE_BOOLEAN));
    PaimonPredicateConverter converter({&isolation});

    TestExpr predicate(create_binary_predicate_node(TPrimitiveType::BOOLEAN));
    ColumnRef isolation_ref(TypeDescriptor(TYPE_BOOLEAN), isolation.id());
    TestExpr function_result(create_function_node(TPrimitiveType::BOOLEAN));
    predicate.add_child(&isolation_ref);
    predicate.add_child(&function_result);

    std::vector<Expr*> conjuncts{&predicate};
    EXPECT_EQ(nullptr, converter.convert(&conjuncts));
}

TEST(PaimonPredicateConverterTest, SkipsInPredicateWithNonLiteralOperand) {
    SlotDescriptor isolation(7, "isolation", TypeDescriptor(TYPE_BOOLEAN));
    PaimonPredicateConverter converter({&isolation});

    TestExpr predicate(create_in_predicate_node(TPrimitiveType::BOOLEAN));
    ColumnRef isolation_ref(TypeDescriptor(TYPE_BOOLEAN), isolation.id());
    TestExpr function_result(create_function_node(TPrimitiveType::BOOLEAN));
    predicate.add_child(&isolation_ref);
    predicate.add_child(&function_result);

    std::vector<Expr*> conjuncts{&predicate};
    EXPECT_EQ(nullptr, converter.convert(&conjuncts));
}

} // namespace starrocks

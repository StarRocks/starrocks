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
#include <paimon/data/decimal.h>
#include <paimon/predicate/leaf_predicate.h>

#include <memory>

#include "column/vectorized_fwd.h"
#include "exprs/column_ref.h"
#include "exprs/literal.h"
#include "runtime/descriptors.h"
#include "types/date_value.h"
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

TExprNode create_null_literal_node() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::NULL_LITERAL);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    node.__set_num_children(0);
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

TTypeDesc gen_decimal_type_desc(TPrimitiveType::type type, int precision, int scale) {
    TTypeDesc desc = gen_type_desc(type);
    desc.types[0].scalar_type.__set_precision(precision);
    desc.types[0].scalar_type.__set_scale(scale);
    return desc;
}

TExprNode create_date_literal_node(const std::string& value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::DATE_LITERAL);
    node.__set_type(gen_type_desc(TPrimitiveType::DATE));
    node.__set_num_children(0);
    TDateLiteral literal;
    literal.__set_value(value);
    node.__set_date_literal(literal);
    return node;
}

TExprNode create_decimal_literal_node(TPrimitiveType::type type, int precision, int scale, const std::string& value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::DECIMAL_LITERAL);
    node.__set_type(gen_decimal_type_desc(type, precision, scale));
    node.__set_num_children(0);
    TDecimalLiteral literal;
    literal.__set_value(value);
    node.__set_decimal_literal(literal);
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

TEST(PaimonPredicateConverterTest, ConvertsNullSafeEqualNullToIsNull) {
    SlotDescriptor isolation(7, "isolation", TypeDescriptor(TYPE_BOOLEAN));
    PaimonPredicateConverter converter({&isolation});

    TExprNode predicate_node = create_binary_predicate_node(TPrimitiveType::BOOLEAN);
    predicate_node.__set_opcode(TExprOpcode::EQ_FOR_NULL);
    TestExpr predicate(predicate_node);
    ColumnRef isolation_ref(TypeDescriptor(TYPE_BOOLEAN), isolation.id());
    VectorizedLiteral null_literal(create_null_literal_node());
    predicate.add_child(&isolation_ref);
    predicate.add_child(&null_literal);

    std::vector<Expr*> conjuncts{&predicate};
    auto leaf = as_leaf_predicate(converter.convert(&conjuncts));
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ("isolation", leaf->FieldName());
    EXPECT_EQ(paimon::Function::Type::IS_NULL, leaf->GetFunction().GetType());
    EXPECT_TRUE(leaf->Literals().empty());
}

TEST(PaimonPredicateConverterTest, SkipsInPredicateWithNullLiteral) {
    SlotDescriptor tinyint_slot(8, "tinyint_col", TypeDescriptor(TYPE_TINYINT));
    PaimonPredicateConverter converter({&tinyint_slot});

    // paimon-cpp rejects null literals in predicates, so `col IN (1, NULL)` must skip
    // pushdown instead of producing a predicate that fails reader creation.
    TestExpr predicate(create_in_predicate_node(TPrimitiveType::TINYINT));
    ColumnRef tinyint_ref(TypeDescriptor(TYPE_TINYINT), tinyint_slot.id());
    VectorizedLiteral tinyint_literal(create_tinyint_literal_node(1));
    VectorizedLiteral null_literal(create_null_literal_node());
    predicate.add_child(&tinyint_ref);
    predicate.add_child(&tinyint_literal);
    predicate.add_child(&null_literal);

    std::vector<Expr*> conjuncts{&predicate};
    EXPECT_EQ(nullptr, converter.convert(&conjuncts));
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

TEST(PaimonPredicateConverterTest, ConvertsDateLiteralToDaysSinceEpoch) {
    SlotDescriptor ship_date(16, "l_shipdate", TypeDescriptor(TYPE_DATE));
    PaimonPredicateConverter converter({&ship_date});
    TExprNode predicate_node = create_binary_predicate_node(TPrimitiveType::DATE);
    predicate_node.__set_opcode(TExprOpcode::GE);
    TestExpr predicate(predicate_node);
    ColumnRef date_ref(TypeDescriptor(TYPE_DATE), ship_date.id());
    VectorizedLiteral date_literal(create_date_literal_node("1995-01-01"));
    predicate.add_child(&date_ref);
    predicate.add_child(&date_literal);
    std::vector<Expr*> conjuncts{&predicate};

    auto leaf = as_leaf_predicate(converter.convert(&conjuncts));
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ("l_shipdate", leaf->FieldName());
    EXPECT_EQ(paimon::FieldType::DATE, leaf->GetFieldType());
    EXPECT_EQ(paimon::Function::Type::GREATER_OR_EQUAL, leaf->GetFunction().GetType());
    ASSERT_EQ(1, leaf->Literals().size());
    EXPECT_EQ(paimon::FieldType::DATE, leaf->Literals()[0].GetType());
    // paimon stores DATE as days since 1970-01-01, the same encoding parquet uses for its stats
    EXPECT_EQ(9131, leaf->Literals()[0].GetValue<int32_t>());
}

TEST(PaimonPredicateConverterTest, ConvertsDateInListToDateLiterals) {
    SlotDescriptor ship_date(16, "l_shipdate", TypeDescriptor(TYPE_DATE));
    PaimonPredicateConverter converter({&ship_date});
    TestExpr predicate(create_in_predicate_node(TPrimitiveType::DATE));
    ColumnRef date_ref(TypeDescriptor(TYPE_DATE), ship_date.id());
    VectorizedLiteral first(create_date_literal_node("1970-01-01"));
    VectorizedLiteral second(create_date_literal_node("1970-01-03"));
    predicate.add_child(&date_ref);
    predicate.add_child(&first);
    predicate.add_child(&second);
    std::vector<Expr*> conjuncts{&predicate};

    auto leaf = as_leaf_predicate(converter.convert(&conjuncts));
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ(paimon::Function::Type::IN, leaf->GetFunction().GetType());
    ASSERT_EQ(2, leaf->Literals().size());
    EXPECT_EQ(0, leaf->Literals()[0].GetValue<int32_t>());
    EXPECT_EQ(2, leaf->Literals()[1].GetValue<int32_t>());
}

TEST(PaimonPredicateConverterTest, ConvertsDecimal64LiteralKeepingPrecisionAndScale) {
    SlotDescriptor discount(7, "l_discount", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 15, 2));
    PaimonPredicateConverter converter({&discount});
    TExprNode predicate_node = create_binary_predicate_node(TPrimitiveType::DECIMAL64);
    predicate_node.__set_opcode(TExprOpcode::GT);
    TestExpr predicate(predicate_node);
    ColumnRef discount_ref(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 15, 2), discount.id());
    VectorizedLiteral decimal_literal(create_decimal_literal_node(TPrimitiveType::DECIMAL64, 15, 2, "0.05"));
    predicate.add_child(&discount_ref);
    predicate.add_child(&decimal_literal);
    std::vector<Expr*> conjuncts{&predicate};

    auto leaf = as_leaf_predicate(converter.convert(&conjuncts));
    ASSERT_NE(nullptr, leaf);
    EXPECT_EQ(paimon::FieldType::DECIMAL, leaf->GetFieldType());
    EXPECT_EQ(paimon::Function::Type::GREATER_THAN, leaf->GetFunction().GetType());
    ASSERT_EQ(1, leaf->Literals().size());
    EXPECT_EQ(paimon::FieldType::DECIMAL, leaf->Literals()[0].GetType());
    auto decimal = leaf->Literals()[0].GetValue<paimon::Decimal>();
    EXPECT_EQ(15, decimal.Precision());
    EXPECT_EQ(2, decimal.Scale());
    EXPECT_EQ(5, static_cast<int64_t>(decimal.Value()));
}

TEST(PaimonPredicateConverterTest, ConvertsDecimal128LiteralAsUnscaledInt128) {
    SlotDescriptor amount(3, "amount", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 9));
    PaimonPredicateConverter converter({&amount});
    TestExpr predicate(create_binary_predicate_node(TPrimitiveType::DECIMAL128));
    ColumnRef amount_ref(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 9), amount.id());
    VectorizedLiteral decimal_literal(
            create_decimal_literal_node(TPrimitiveType::DECIMAL128, 38, 9, "12345678901234567890.5"));
    predicate.add_child(&amount_ref);
    predicate.add_child(&decimal_literal);
    std::vector<Expr*> conjuncts{&predicate};

    auto leaf = as_leaf_predicate(converter.convert(&conjuncts));
    ASSERT_NE(nullptr, leaf);
    auto decimal = leaf->Literals()[0].GetValue<paimon::Decimal>();
    EXPECT_EQ(38, decimal.Precision());
    EXPECT_EQ(9, decimal.Scale());
    __int128_t expected = static_cast<__int128_t>(12345678901234567890ULL) * 1000000000 + 500000000;
    EXPECT_TRUE(expected == decimal.Value());
}

TEST(PaimonPredicateConverterTest, SkipsDatetimePredicate) {
    // paimon-cpp 0.3.0 cannot push TIMESTAMP predicates into parquet, so they stay with StarRocks
    SlotDescriptor ts(5, "ts", TypeDescriptor(TYPE_DATETIME));
    PaimonPredicateConverter converter({&ts});
    TExprNode predicate_node = create_binary_predicate_node(TPrimitiveType::DATETIME);
    predicate_node.__set_opcode(TExprOpcode::GE);
    TestExpr predicate(predicate_node);
    ColumnRef ts_ref(TypeDescriptor(TYPE_DATETIME), ts.id());
    TExprNode literal_node = create_date_literal_node("1995-01-01 00:00:00");
    literal_node.__set_type(gen_type_desc(TPrimitiveType::DATETIME));
    VectorizedLiteral ts_literal(literal_node);
    predicate.add_child(&ts_ref);
    predicate.add_child(&ts_literal);
    std::vector<Expr*> conjuncts{&predicate};
    EXPECT_EQ(nullptr, converter.convert(&conjuncts));
}

} // namespace starrocks

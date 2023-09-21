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

#include "storage/conjunctive_predicates.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <unordered_map>
#include <vector>

#include "exec/olap_scan_prepare.h"
#include "exprs/binary_predicate.h"
#include "exprs/column_ref.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/runtime_filter_bank.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/predicate_parser.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "types/logical_type.h"

namespace starrocks {

inline TExprOpcode::type convert_predicate_type_to_thrift(PredicateType p) {
    static std::unordered_map<PredicateType, TExprOpcode::type> mapping = {
            {PredicateType::kEQ, TExprOpcode::EQ},
            {PredicateType::kNE, TExprOpcode::NE},
            {PredicateType::kGT, TExprOpcode::GT},
            {PredicateType::kGE, TExprOpcode::GE},
            {PredicateType::kLT, TExprOpcode::LT},
            {PredicateType::kLE, TExprOpcode::LE},
            {PredicateType::kInList, TExprOpcode::FILTER_IN},
            {PredicateType::kNotInList, TExprOpcode::FILTER_NOT_IN},
            {PredicateType::kAnd, TExprOpcode::COMPOUND_AND},
            {PredicateType::kOr, TExprOpcode::COMPOUND_OR},
    };
    return mapping[p];
}

// to string of boolean values, e.g,
//   [1,2,3] => "1,1,1"
//   [1,0,3] => "1,0,1"
static inline std::string to_string(const std::vector<uint8_t>& v) {
    std::stringstream ss;
    for (uint8_t n : v) {
        ss << (n != 0) << ",";
    }
    std::string s = ss.str();
    s.pop_back();
    return s;
}

using PredicatePtr = std::unique_ptr<ColumnPredicate>;

// NOLINTNEXTLINE
TEST(ConjunctivePredicatesTest, test_evaluate) {
    SchemaPtr schema(new Schema());
    auto c0_field = std::make_shared<Field>(0, "c0", TYPE_INT, true);
    auto c1_field = std::make_shared<Field>(1, "c1", TYPE_CHAR, true);
    auto c2_field = std::make_shared<Field>(2, "c2", TYPE_DATE, true);
    auto c3_field = std::make_shared<Field>(3, "c3", TYPE_DATETIME, true);
    auto c4_field = std::make_shared<Field>(4, "c4", TYPE_DECIMALV2, true);

    schema->append(c0_field);
    schema->append(c1_field);
    schema->append(c2_field);
    schema->append(c3_field);
    schema->append(c4_field);

    auto c0 = ChunkHelper::column_from_field(*c0_field);
    auto c1 = ChunkHelper::column_from_field(*c1_field);
    auto c2 = ChunkHelper::column_from_field(*c2_field);
    auto c3 = ChunkHelper::column_from_field(*c3_field);
    auto c4 = ChunkHelper::column_from_field(*c4_field);

    // +------+-------+------------+----------------------+----------+
    // | c0   | c1    | c2         | c3                   | c4       |
    // +------+-------+------------+----------------------+----------+
    // | NULL | 'aba' | 1990-01-01 | 1990-01-01 00:00:00  | NULL     |
    // |    1 | 'abb' | 1991-01-01 | NULL                 | 0.000001 |
    // |    2 | 'abc' | NULL       | 1992-01-01 00:00:00  | 0.000002 |
    // |    3 | NULL  | 1992-01-01 | 1993-01-01 00:00:00  | 0.000003 |
    // +------+------+-------------+----------------------+----------+
    c0->append_datum(Datum());
    c0->append_datum(Datum(1));
    c0->append_datum(Datum(2));
    c0->append_datum(Datum(3));

    c1->append_datum(Datum("aba"));
    c1->append_datum(Datum("abb"));
    c1->append_datum(Datum("abc"));
    c1->append_datum(Datum());

    c2->append_datum(Datum(DateValue::create(1990, 1, 1)));
    c2->append_datum(Datum(DateValue::create(1991, 1, 1)));
    c2->append_datum(Datum());
    c2->append_datum(Datum(DateValue::create(1992, 1, 1)));

    c3->append_datum(Datum(TimestampValue::create(1990, 1, 1, 0, 0, 0)));
    c3->append_datum(Datum());
    c3->append_datum(Datum(TimestampValue::create(1992, 1, 1, 0, 0, 0)));
    c3->append_datum(Datum(TimestampValue::create(1993, 1, 1, 0, 0, 0)));

    c4->append_datum(Datum());
    c4->append_datum(Datum(DecimalV2Value("0.000001")));
    c4->append_datum(Datum(DecimalV2Value("0.000002")));
    c4->append_datum(Datum(DecimalV2Value("0.000003")));

    ChunkPtr chunk = std::make_shared<Chunk>(Columns{c0, c1, c2, c3, c4}, schema);

    std::vector<uint8_t> selection(chunk->num_rows(), 0);

    // c3 > '1991-01-01 00:00:00'
    {
        PredicatePtr p0(new_column_gt_predicate(get_type_info(TYPE_DATETIME), 3, "1991-01-01 00:00:00"));

        ConjunctivePredicates conjuncts({p0.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,0,1,1", to_string(selection));
    }
    // c3 > '1991-01-01 00:00:00' and c3 <= '1992-02-10 00:00:00'
    {
        PredicatePtr p0(new_column_gt_predicate(get_type_info(TYPE_DATETIME), 3, "1991-01-01 00:00:00"));
        PredicatePtr p1(new_column_le_predicate(get_type_info(TYPE_DATETIME), 3, "1992-02-10 00:00:00"));

        ConjunctivePredicates conjuncts({p0.get(), p1.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,0,1,0", to_string(selection));
    }
    // c2 < '2020-01-01' and c0 is not null
    {
        PredicatePtr p0(new_column_lt_predicate(get_type_info(TYPE_DATE), 2, "2020-01-01"));
        PredicatePtr p1(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));

        ConjunctivePredicates conjuncts({p0.get(), p1.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,1,0,1", to_string(selection));
    }
    // c0 is not null and c1 >= 'aaa' and c4 in ('0.000001', '0.000003')
    {
        PredicatePtr p0(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));
        PredicatePtr p1(new_column_ge_predicate(get_type_info(TYPE_CHAR), 1, "aaa"));
        PredicatePtr p2(new_column_in_predicate(get_type_info(TYPE_DECIMALV2), 4, {"0.000001", "0.000003"}));

        ConjunctivePredicates conjuncts({p0.get(), p1.get(), p2.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,1,0,0", to_string(selection));
    }
}

// NOLINTNEXTLINE
TEST(ConjunctivePredicatesTest, test_evaluate_and) {
    SchemaPtr schema(new Schema());
    schema->append(std::make_shared<Field>(0, "c0", TYPE_INT, true));

    auto c0 = ChunkHelper::column_from_field_type(TYPE_INT, true);

    // +------+
    // | c0   |
    // +------+
    // | NULL |
    // |    1 |
    // |    2 |
    // |    3 |
    // +------+
    c0->append_datum(Datum());
    c0->append_datum(Datum(1));
    c0->append_datum(Datum(2));
    c0->append_datum(Datum(3));

    ChunkPtr chunk = std::make_shared<Chunk>(Columns{c0}, schema);

    {
        std::vector<uint8_t> selection(chunk->num_rows(), 0);

        // c0 is not null
        PredicatePtr p0(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));

        ConjunctivePredicates conjuncts({p0.get()});

        conjuncts.evaluate_and(chunk.get(), selection.data());
        EXPECT_EQ("0,0,0,0", to_string(selection));

        selection.assign(chunk->num_rows(), 1);
        conjuncts.evaluate_and(chunk.get(), selection.data());
        EXPECT_EQ("0,1,1,1", to_string(selection));
    }
}

// NOLINTNEXTLINE
TEST(ConjunctivePredicatesTest, test_evaluate_or) {
    SchemaPtr schema(new Schema());
    schema->append(std::make_shared<Field>(0, "c0", TYPE_INT, true));

    auto c0 = ChunkHelper::column_from_field_type(TYPE_INT, true);

    // +------+
    // | c0   |
    // +------+
    // | NULL |
    // |    1 |
    // |    2 |
    // |    3 |
    // +------+
    c0->append_datum(Datum());
    c0->append_datum(Datum(1));
    c0->append_datum(Datum(2));
    c0->append_datum(Datum(3));

    ChunkPtr chunk = std::make_shared<Chunk>(Columns{c0}, schema);

    {
        std::vector<uint8_t> selection(chunk->num_rows(), 0);

        // c0 is not null
        PredicatePtr p0(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));

        ConjunctivePredicates conjuncts({p0.get()});

        selection.assign(chunk->num_rows(), 0);
        conjuncts.evaluate_or(chunk.get(), selection.data());
        EXPECT_EQ("0,1,1,1", to_string(selection));

        selection.assign(chunk->num_rows(), 1);
        conjuncts.evaluate_or(chunk.get(), selection.data());
        EXPECT_EQ("1,1,1,1", to_string(selection));
    }
}

struct MockConstExprBuilder {
    template <LogicalType ltype>
    Expr* operator()(ObjectPool* pool) {
        if constexpr (lt_is_decimal<ltype>) {
            CHECK(false) << "not supported";
            return nullptr;
        } else {
            TExprNode expr_node;
            expr_node.child_type = TPrimitiveType::INT;
            expr_node.node_type = TExprNodeType::INT_LITERAL;
            expr_node.num_children = 1;
            expr_node.__isset.opcode = true;
            expr_node.__isset.child_type = true;
            expr_node.type = gen_type_desc(TPrimitiveType::INT);

            using CppType = RunTimeCppType<ltype>;
            CppType literal_value;
            if constexpr (lt_is_string<ltype>) {
                literal_value = "123";
            } else if constexpr (lt_is_integer<ltype>) {
                literal_value = 123;
            } else {
                literal_value = CppType{};
            }
            Expr* expr = pool->add(new MockConstVectorizedExpr<ltype>(expr_node, literal_value));
            return expr;
        }
    }
};

class ConjunctiveTestFixture : public testing::TestWithParam<std::tuple<TExprOpcode::type, LogicalType>> {
public:
    TSlotDescriptor _create_slot_desc(LogicalType type, const std::string& col_name, int col_pos) {
        TSlotDescriptorBuilder builder;

        if (type == LogicalType::TYPE_VARCHAR || type == LogicalType::TYPE_CHAR) {
            return builder.string_type(1024).column_name(col_name).column_pos(col_pos).nullable(false).build();
        } else {
            return builder.type(type).column_name(col_name).column_pos(col_pos).nullable(false).build();
        }
    }

    TupleDescriptor* _create_tuple_desc(LogicalType ltype) {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(_create_slot_desc(ltype, "c1", 0));
        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        std::vector<bool> nullable_tuples = std::vector<bool>{true};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                      .ok());

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

    TabletSchemaPB create_tablet_schema(LogicalType ltype) {
        TabletSchemaPB tablet_schema;

        ColumnPB* col = tablet_schema.add_column();
        col->set_name("c1");
        std::string type_name = type_to_string(ltype);
        col->set_type(type_name);

        return tablet_schema;
    }

    // Build an expression: col < literal
    Expr* build_predicate(LogicalType ltype, TExprOpcode::type op, SlotDescriptor* slot) {
        TExprNode expr_node;
        expr_node.opcode = op;
        expr_node.child_type = to_thrift(ltype);
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        Expr* expr = _pool.add(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
        ColumnRef* column_ref = _pool.add(new ColumnRef(slot));
        Expr* col2 = type_dispatch_basic(ltype, MockConstExprBuilder(), &_pool);
        expr->_children.push_back(column_ref);
        expr->_children.push_back(col2);

        return expr;
    }

protected:
    RuntimeState _runtime_state;
    ObjectPool _pool;
};

// normalize a simple predicate: col op const
// NOLINTNEXTLINE
TEST_P(ConjunctiveTestFixture, test_parse_conjuncts) {
    auto [op, ltype] = GetParam();

    _pool.clear();
    TupleDescriptor* tuple_desc = _create_tuple_desc(ltype);
    std::vector<std::string> key_column_names = {"c1"};
    SlotDescriptor* slot = tuple_desc->slots()[0];
    std::vector<ExprContext*> conjunct_ctxs = {_pool.add(new ExprContext(build_predicate(ltype, op, slot)))};
    ASSERT_OK(Expr::prepare(conjunct_ctxs, &_runtime_state));
    ASSERT_OK(Expr::open(conjunct_ctxs, &_runtime_state));
    auto tablet_schema = TabletSchema::create(create_tablet_schema(ltype));

    OlapScanConjunctsManager cm;
    cm.conjunct_ctxs_ptr = &conjunct_ctxs;
    cm.tuple_desc = tuple_desc;
    cm.obj_pool = &_pool;
    cm.key_column_names = &key_column_names;
    cm.runtime_filters = _pool.add(new RuntimeFilterProbeCollector());

    ASSERT_OK(cm.parse_conjuncts(true, 1));
    // col >= false will be elimated
    if (ltype == TYPE_BOOLEAN && op == TExprOpcode::GE) {
        ASSERT_EQ(0, cm.olap_filters.size());
        return;
    } else {
        ASSERT_EQ(1, cm.olap_filters.size());
    }
    ASSERT_EQ(1, cm.column_value_ranges.size());
    ASSERT_EQ(1, cm.column_value_ranges.count(slot->col_name()));

    {
        PredicateParser pp(tablet_schema);
        std::unique_ptr<ColumnPredicate> predicate(pp.parse_thrift_cond(cm.olap_filters[0]));
        ASSERT_TRUE(!!predicate);

        // BOOLEAN is special, col <= false will be convert to col = false
        if (ltype == TYPE_BOOLEAN && op == TExprOpcode::LE) {
            ASSERT_EQ(TExprOpcode::EQ, convert_predicate_type_to_thrift(predicate->type()));
        } else {
            ASSERT_EQ(op, convert_predicate_type_to_thrift(predicate->type()));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(ConjunctiveTest, ConjunctiveTestFixture,
                         testing::Combine(testing::Values(TExprOpcode::LT, TExprOpcode::LE, TExprOpcode::GT,
                                                          TExprOpcode::GE, TExprOpcode::EQ, TExprOpcode::NE),
                                          testing::Values(LogicalType::TYPE_TINYINT, LogicalType::TYPE_SMALLINT,
                                                          LogicalType::TYPE_INT, LogicalType::TYPE_BIGINT,
                                                          LogicalType::TYPE_LARGEINT, LogicalType::TYPE_VARCHAR,
                                                          LogicalType::TYPE_CHAR, LogicalType::TYPE_BOOLEAN)));

} // namespace starrocks

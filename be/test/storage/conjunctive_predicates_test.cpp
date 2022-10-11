// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/conjunctive_predicates.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <unordered_map>
#include <vector>

#include "exec/vectorized/olap_scan_prepare.h"
#include "exprs/vectorized/binary_predicate.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "storage/chunk_helper.h"
#include "storage/predicate_parser.h"
#include "storage/vectorized_column_predicate.h"
#include "testutil//assert.h"

namespace starrocks::vectorized {

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
    auto c0_field = std::make_shared<Field>(0, "c0", OLAP_FIELD_TYPE_INT, true);
    auto c1_field = std::make_shared<Field>(1, "c1", OLAP_FIELD_TYPE_CHAR, true);
    auto c2_field = std::make_shared<Field>(2, "c2", OLAP_FIELD_TYPE_DATE_V2, true);
    auto c3_field = std::make_shared<Field>(3, "c3", OLAP_FIELD_TYPE_TIMESTAMP, true);
    auto c4_field = std::make_shared<Field>(4, "c4", OLAP_FIELD_TYPE_DECIMAL_V2, true);

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
        PredicatePtr p0(new_column_gt_predicate(get_type_info(OLAP_FIELD_TYPE_TIMESTAMP), 3, "1991-01-01 00:00:00"));

        ConjunctivePredicates conjuncts({p0.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,0,1,1", to_string(selection));
    }
    // c3 > '1991-01-01 00:00:00' and c3 <= '1992-02-10 00:00:00'
    {
        PredicatePtr p0(new_column_gt_predicate(get_type_info(OLAP_FIELD_TYPE_TIMESTAMP), 3, "1991-01-01 00:00:00"));
        PredicatePtr p1(new_column_le_predicate(get_type_info(OLAP_FIELD_TYPE_TIMESTAMP), 3, "1992-02-10 00:00:00"));

        ConjunctivePredicates conjuncts({p0.get(), p1.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,0,1,0", to_string(selection));
    }
    // c2 < '2020-01-01' and c0 is not null
    {
        PredicatePtr p0(new_column_lt_predicate(get_type_info(OLAP_FIELD_TYPE_DATE_V2), 2, "2020-01-01"));
        PredicatePtr p1(new_column_null_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 0, false));

        ConjunctivePredicates conjuncts({p0.get(), p1.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,1,0,1", to_string(selection));
    }
    // c0 is not null and c1 >= 'aaa' and c4 in ('0.000001', '0.000003')
    {
        PredicatePtr p0(new_column_null_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 0, false));
        PredicatePtr p1(new_column_ge_predicate(get_type_info(OLAP_FIELD_TYPE_CHAR), 1, "aaa"));
        PredicatePtr p2(
                new_column_in_predicate(get_type_info(OLAP_FIELD_TYPE_DECIMAL_V2), 4, {"0.000001", "0.000003"}));

        ConjunctivePredicates conjuncts({p0.get(), p1.get(), p2.get()});

        conjuncts.evaluate(chunk.get(), selection.data());
        EXPECT_EQ("0,1,0,0", to_string(selection));
    }
}

// NOLINTNEXTLINE
TEST(ConjunctivePredicatesTest, test_evaluate_and) {
    SchemaPtr schema(new Schema());
    schema->append(std::make_shared<Field>(0, "c0", OLAP_FIELD_TYPE_INT, true));

    auto c0 = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_INT, true);

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
        PredicatePtr p0(new_column_null_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 0, false));

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
    schema->append(std::make_shared<Field>(0, "c0", OLAP_FIELD_TYPE_INT, true));

    auto c0 = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_INT, true);

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
        PredicatePtr p0(new_column_null_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 0, false));

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
    template <PrimitiveType ptype>
    Expr* operator()(ObjectPool* pool) {
        if constexpr (pt_is_decimal<ptype>) {
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

            using CppType = RunTimeCppType<ptype>;
            CppType literal_value;
            if constexpr (pt_is_binary<ptype>) {
                literal_value = "123";
            } else if constexpr (pt_is_integer<ptype>) {
                literal_value = 123;
            } else {
                literal_value = CppType{};
            }
            Expr* expr = pool->add(new MockConstVectorizedExpr<ptype>(expr_node, literal_value));
            return expr;
        }
    }
};

class ConjunctiveTestFixture : public testing::TestWithParam<std::tuple<TExprOpcode::type, PrimitiveType>> {
public:
    TSlotDescriptor _create_slot_desc(PrimitiveType type, const std::string& col_name, int col_pos) {
        TSlotDescriptorBuilder builder;

        if (type == PrimitiveType::TYPE_VARCHAR || type == PrimitiveType::TYPE_CHAR) {
            return builder.string_type(1024).column_name(col_name).column_pos(col_pos).nullable(false).build();
        } else {
            return builder.type(type).column_name(col_name).column_pos(col_pos).nullable(false).build();
        }
    }

    TupleDescriptor* _create_tuple_desc(PrimitiveType ptype) {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(_create_slot_desc(ptype, "c1", 0));
        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        std::vector<bool> nullable_tuples = std::vector<bool>{true};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

    TabletSchemaPB create_tablet_schema(PrimitiveType ptype) {
        TabletSchemaPB tablet_schema;

        ColumnPB* col = tablet_schema.add_column();
        col->set_name("c1");
        std::string type_name = type_to_string(ptype);
        col->set_type(type_name);

        return tablet_schema;
    }

    // Build an expression: col < literal
    Expr* build_predicate(PrimitiveType ptype, TExprOpcode::type op, SlotDescriptor* slot) {
        TExprNode expr_node;
        expr_node.opcode = op;
        expr_node.child_type = to_thrift(ptype);
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        Expr* expr = _pool.add(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
        ColumnRef* column_ref = _pool.add(new ColumnRef(slot));
        Expr* col2 = type_dispatch_basic(ptype, MockConstExprBuilder(), &_pool);
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
    auto [op, ptype] = GetParam();

    _pool.clear();
    TupleDescriptor* tuple_desc = _create_tuple_desc(ptype);
    std::vector<std::string> key_column_names = {"c1"};
    SlotDescriptor* slot = tuple_desc->slots()[0];
    std::vector<ExprContext*> conjunct_ctxs = {_pool.add(new ExprContext(build_predicate(ptype, op, slot)))};
    auto tablet_schema = TabletSchema::create(create_tablet_schema(ptype));

    OlapScanConjunctsManager cm;
    cm.conjunct_ctxs_ptr = &conjunct_ctxs;
    cm.tuple_desc = tuple_desc;
    cm.obj_pool = &_pool;
    cm.key_column_names = &key_column_names;
    cm.runtime_filters = _pool.add(new RuntimeFilterProbeCollector());

    ASSERT_OK(cm.parse_conjuncts(true, 1));
    // col >= false will be elimated
    if (ptype == TYPE_BOOLEAN && op == TExprOpcode::GE) {
        ASSERT_EQ(0, cm.olap_filters.size());
        return;
    } else {
        ASSERT_EQ(1, cm.olap_filters.size());
    }
    ASSERT_EQ(1, cm.column_value_ranges.size());
    ASSERT_EQ(1, cm.column_value_ranges.count(slot->col_name()));

    {
        PredicateParser pp(*tablet_schema);
        std::unique_ptr<ColumnPredicate> predicate(pp.parse_thrift_cond(cm.olap_filters[0]));
        ASSERT_TRUE(!!predicate);

        // BOOLEAN is special, col <= false will be convert to col = false
        if (ptype == TYPE_BOOLEAN && op == TExprOpcode::LE) {
            ASSERT_EQ(TExprOpcode::EQ, convert_predicate_type_to_thrift(predicate->type()));
        } else {
            ASSERT_EQ(op, convert_predicate_type_to_thrift(predicate->type()));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(ConjunctiveTest, ConjunctiveTestFixture,
                         testing::Combine(testing::Values(TExprOpcode::LT, TExprOpcode::LE, TExprOpcode::GT,
                                                          TExprOpcode::GE, TExprOpcode::EQ, TExprOpcode::NE),
                                          testing::Values(PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_SMALLINT,
                                                          PrimitiveType::TYPE_INT, PrimitiveType::TYPE_BIGINT,
                                                          PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_VARCHAR,
                                                          PrimitiveType::TYPE_CHAR, PrimitiveType::TYPE_BOOLEAN)));

} // namespace starrocks::vectorized

// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/conjunctive_predicates.h"

#include <vector>

#include "gtest/gtest.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized

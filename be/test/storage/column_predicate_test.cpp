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

#include "storage/column_predicate.h"

#include <vector>

#include "gtest/gtest.h"
#include "storage/chunk_helper.h"
#include "storage/column_or_predicate.h"
#include "testutil/assert.h"

namespace starrocks {

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

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_eq) {
    // boolean
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_BOOLEAN), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_BOOLEAN, false);
        c->append_datum(Datum((uint8_t)1));
        c->append_datum(Datum((uint8_t)0));
        c->append_datum(Datum((uint8_t)0));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(1, p->value().get_int8());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(1, p->values()[0].get_int8());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // tinyint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_TINYINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_TINYINT, false);
        c->append_datum(Datum((int8_t)1));
        c->append_datum(Datum((int8_t)2));
        c->append_datum(Datum((int8_t)3));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(1, p->value().get_int8());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(1, p->values()[0].get_int8());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // smallint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_SMALLINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_SMALLINT, false);
        c->append_datum(Datum((int16_t)1));
        c->append_datum(Datum((int16_t)2));
        c->append_datum(Datum((int16_t)3));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(1, p->value().get_int16());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(1, p->values()[0].get_int16());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // int
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, false);
        c->append_datum(Datum((int32_t)1));
        c->append_datum(Datum((int32_t)2));
        c->append_datum(Datum((int32_t)3));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(1, p->value().get_int32());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(1, p->values()[0].get_int32());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // nullable int
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "101"));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum((int32_t)1));
        c->append_datum(Datum((int32_t)2));
        c->append_datum(Datum((int32_t)101));
        (void)c->append_nulls(1);
        c->append_datum(Datum((int32_t)101));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[2] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 4);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 5);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    // bigint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_BIGINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_BIGINT, false);
        c->append_datum(Datum((int64_t)1));
        c->append_datum(Datum((int64_t)2));
        c->append_datum(Datum((int64_t)3));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(1, p->value().get_int64());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(1, p->values()[0].get_int64());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // laregeint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_LARGEINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_LARGEINT, false);
        c->append_datum(Datum((int128_t)1));
        c->append_datum(Datum((int128_t)2));
        c->append_datum(Datum((int128_t)3));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(1, p->value().get_int128());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(1, p->values()[0].get_int128());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // float
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_FLOAT), 0, "1.0"));
        auto c = ChunkHelper::column_from_field_type(TYPE_FLOAT, false);
        c->append_datum(Datum((float)1.0));
        c->append_datum(Datum((float)2.1));
        c->append_datum(Datum((float)3.1));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_FLOAT_EQ(1.0, p->value().get_float());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_FLOAT_EQ(1.0, p->values()[0].get_float());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // double
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_DOUBLE), 0, "1.0"));
        auto c = ChunkHelper::column_from_field_type(TYPE_DOUBLE, false);
        c->append_datum(Datum((double)1.0));
        c->append_datum(Datum((double)2.1));
        c->append_datum(Datum((double)3.1));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_FLOAT_EQ(1.0, p->value().get_double());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_FLOAT_EQ(1.0, p->values()[0].get_double());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // date_v2
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_DATE), 0, "1990-01-01"));
        auto c = ChunkHelper::column_from_field_type(TYPE_DATE, false);
        c->append_datum(Datum(DateValue::create(1990, 1, 1)));
        c->append_datum(Datum(DateValue::create(1991, 1, 1)));
        c->append_datum(Datum(DateValue::create(1992, 1, 1)));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(DateValue::create(1990, 1, 1), p->value().get_date());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(DateValue::create(1990, 1, 1), p->values()[0].get_date());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // timestamp
    {
        std::unique_ptr<ColumnPredicate> p(
                new_column_eq_predicate(get_type_info(TYPE_DATETIME), 0, "1990-01-01 00:00:00"));
        auto c = ChunkHelper::column_from_field_type(TYPE_DATETIME, false);
        c->append_datum(Datum(TimestampValue::create(1990, 1, 1, 0, 0, 0)));
        c->append_datum(Datum(TimestampValue::create(1990, 1, 1, 0, 0, 1)));
        c->append_datum(Datum(TimestampValue::create(1990, 1, 1, 0, 0, 2)));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(TimestampValue::create(1990, 1, 1, 0, 0, 0), p->value().get_timestamp());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(TimestampValue::create(1990, 1, 1, 0, 0, 0), p->values()[0].get_timestamp());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // decimal_v2
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_DECIMALV2), 0, "1.23"));
        auto field = std::make_shared<Field>(1, "test", TYPE_DECIMALV2, 27, 9, false);
        auto c = ChunkHelper::column_from_field(*field);
        c->append_datum(Datum(DecimalV2Value("1.23")));
        c->append_datum(Datum(DecimalV2Value("1.24")));
        c->append_datum(Datum(DecimalV2Value("1.25")));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_TRUE(p->can_vectorized());
        ASSERT_EQ(DecimalV2Value("1.23"), p->value().get_decimal());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ(DecimalV2Value("1.23"), p->values()[0].get_decimal());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // varchar
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, "aa"));
        auto c = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
        c->append_datum(Datum("aa"));
        c->append_datum(Datum("ab"));
        c->append_datum(Datum("ac"));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_FALSE(p->can_vectorized());
        ASSERT_EQ("aa", p->value().get_slice());
        ASSERT_EQ(1u, p->values().size());
        ASSERT_EQ("aa", p->values()[0].get_slice());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 0, 0);
        ASSERT_EQ("1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 2);
        ASSERT_EQ("1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,1", to_string(buff));
    }
    // nullable char
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(get_type_info(TYPE_CHAR), 0, "abc"));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("xxx"));
        c->append_datum(Datum("yyy"));
        c->append_datum(Datum("abc"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("abc"));

        ASSERT_EQ(PredicateType::kEQ, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[2] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff[3] = 1;
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,1,1", to_string(buff));
    }
    // unsupported types.
    {
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_UNSIGNED_TINYINT), 0, "1") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_UNSIGNED_SMALLINT), 0, "1") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_UNSIGNED_INT), 0, "1") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_UNSIGNED_BIGINT), 0, "1") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_ARRAY), 0, "a") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_MAP), 0, "a") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_STRUCT), 0, "a") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_NONE), 0, "a") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_HLL), 0, "a") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_OBJECT), 0, "a") == nullptr);
        ASSERT_TRUE(new_column_eq_predicate(get_type_info(TYPE_MAX_VALUE), 0, "a") == nullptr);
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_ne) {
    // boolean
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ne_predicate(get_type_info(TYPE_BOOLEAN), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_BOOLEAN, false);
        c->append_datum(Datum((uint8_t)1));
        c->append_datum(Datum((uint8_t)0));
        c->append_datum(Datum((uint8_t)1));

        ASSERT_EQ(PredicateType::kNE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        buff[0] = 1;
        buff[1] = 0;
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff[0] = 1;
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,0", to_string(buff));
    }
    // tinyint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ne_predicate(get_type_info(TYPE_TINYINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_TINYINT, false);
        c->append_datum(Datum((int8_t)1));
        c->append_datum(Datum((int8_t)0));
        c->append_datum(Datum((int8_t)1));

        ASSERT_EQ(PredicateType::kNE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        buff[0] = 1;
        buff[1] = 0;
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff[0] = 1;
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,0", to_string(buff));
    }
    // smallint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ne_predicate(get_type_info(TYPE_SMALLINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_SMALLINT, false);
        c->append_datum(Datum((int16_t)1));
        c->append_datum(Datum((int16_t)0));
        c->append_datum(Datum((int16_t)1));

        ASSERT_EQ(PredicateType::kNE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        buff[0] = 1;
        buff[1] = 0;
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff[0] = 1;
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,0", to_string(buff));
    }
    // nullable int
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ne_predicate(get_type_info(TYPE_INT), 0, "101"));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(101));
        (void)c->append_nulls(1);
        c->append_datum(Datum(101));

        ASSERT_EQ(PredicateType::kNE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    // bigint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ne_predicate(get_type_info(TYPE_BIGINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_BIGINT, false);
        c->append_datum(Datum((int64_t)1));
        c->append_datum(Datum((int64_t)0));
        c->append_datum(Datum((int64_t)1));

        ASSERT_EQ(PredicateType::kNE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        buff[0] = 1;
        buff[1] = 0;
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff[0] = 1;
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,0", to_string(buff));
    }
    // largeint
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ne_predicate(get_type_info(TYPE_LARGEINT), 0, "1"));
        auto c = ChunkHelper::column_from_field_type(TYPE_LARGEINT, false);
        c->append_datum(Datum((int128_t)1));
        c->append_datum(Datum((int128_t)0));
        c->append_datum(Datum((int128_t)1));

        ASSERT_EQ(PredicateType::kNE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(3);
        p->evaluate(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(3, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff.assign(3, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        buff[0] = 1;
        buff[1] = 0;
        p->evaluate_and(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(3, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("0,1,0", to_string(buff));

        buff[0] = 1;
        p->evaluate_or(c.get(), buff.data(), 0, 3);
        ASSERT_EQ("1,1,0", to_string(buff));
    }
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ne_predicate(get_type_info(TYPE_CHAR), 0, "abc"));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("xyz"));
        c->append_datum(Datum("yyy"));
        c->append_datum(Datum("abc"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("abc"));

        ASSERT_EQ(PredicateType::kNE, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_gt) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_gt_predicate(get_type_info(TYPE_INT), 0, "3"));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        ASSERT_EQ(PredicateType::kGT, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    {
        std::unique_ptr<ColumnPredicate> p(new_column_gt_predicate(get_type_info(TYPE_CHAR), 0, "xyz"));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("abc"));
        c->append_datum(Datum("def"));
        c->append_datum(Datum("xyz"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("yyy"));

        ASSERT_EQ(PredicateType::kGT, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_ge) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ge_predicate(get_type_info(TYPE_INT), 0, "3"));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        ASSERT_EQ(PredicateType::kGE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ge_predicate(get_type_info(TYPE_CHAR), 0, "xyz"));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("abc"));
        c->append_datum(Datum("def"));
        c->append_datum(Datum("xyz"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("yyy"));

        ASSERT_EQ(PredicateType::kGE, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_lt) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_lt_predicate(get_type_info(TYPE_INT), 0, "3"));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        ASSERT_EQ(PredicateType::kLT, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    {
        std::unique_ptr<ColumnPredicate> p(new_column_lt_predicate(get_type_info(TYPE_CHAR), 0, "xyz"));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("abc"));
        c->append_datum(Datum("def"));
        c->append_datum(Datum("xyz"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("yyy"));

        ASSERT_EQ(PredicateType::kLT, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_le) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_le_predicate(get_type_info(TYPE_INT), 0, "3"));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        ASSERT_EQ(PredicateType::kLE, p->type());
        ASSERT_TRUE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    {
        std::unique_ptr<ColumnPredicate> p(new_column_le_predicate(get_type_info(TYPE_CHAR), 0, "xyz"));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("abc"));
        c->append_datum(Datum("def"));
        c->append_datum(Datum("xyz"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("yyy"));

        ASSERT_EQ(PredicateType::kLE, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_in) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_in_predicate(get_type_info(TYPE_INT), 0, {"3", "4"}));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        ASSERT_EQ(PredicateType::kInList, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    {
        std::unique_ptr<ColumnPredicate> p(new_column_in_predicate(get_type_info(TYPE_CHAR), 0, {"xyz", "yyy"}));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("abc"));
        c->append_datum(Datum("def"));
        c->append_datum(Datum("xyz"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("yyy"));

        ASSERT_EQ(PredicateType::kInList, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_no_in) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_not_in_predicate(get_type_info(TYPE_INT), 0, {"3", "4"}));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        ASSERT_EQ(PredicateType::kNotInList, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
    {
        std::unique_ptr<ColumnPredicate> p(new_column_not_in_predicate(get_type_info(TYPE_CHAR), 0, {"xyz", "yyy"}));
        auto c = ChunkHelper::column_from_field_type(TYPE_CHAR, true);
        c->append_datum(Datum("abc"));
        c->append_datum(Datum("def"));
        c->append_datum(Datum("xyz"));
        (void)c->append_nulls(1);
        c->append_datum(Datum("yyy"));

        ASSERT_EQ(PredicateType::kNotInList, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,0,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_is_null) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_null_predicate(get_type_info(TYPE_INT), 0, true));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[3] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,0,1,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_not_null) {
    {
        std::unique_ptr<ColumnPredicate> p(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));
        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(Datum(1));
        c->append_datum(Datum(2));
        c->append_datum(Datum(3));
        (void)c->append_nulls(1);
        c->append_datum(Datum(4));

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,1", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,1,0,1", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,1", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("1,1,1,0,1", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[2] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,0,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,0,1", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_or) {
    {
        std::unique_ptr<ColumnPredicate> p1(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "100"));
        std::unique_ptr<ColumnPredicate> p2(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "200"));

        auto p = std::make_unique<ColumnOrPredicate>(get_type_info(TYPE_INT), 0);
        p->add_child(p1.get());
        p->add_child(p2.get());

        auto c = ChunkHelper::column_from_field_type(TYPE_INT, true);
        c->append_datum(10);
        c->append_datum(100);
        c->append_datum(200);
        (void)c->append_nulls(2);

        ASSERT_EQ(PredicateType::kOr, p->type());
        ASSERT_FALSE(p->can_vectorized());

        // ---------------------------------------------
        // evaluate()
        // ---------------------------------------------
        std::vector<uint8_t> buff(5);
        p->evaluate(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,1,1,0,0", to_string(buff));

        p->evaluate(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,1,1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_and()
        // ---------------------------------------------
        buff.assign(5, 1);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,1,1,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 1, 3);
        ASSERT_EQ("0,1,1,0,0", to_string(buff));

        buff.assign(5, 0);
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,0,0,0", to_string(buff));

        buff[2] = 1;
        p->evaluate_and(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        p->evaluate_and(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,0,1,0,0", to_string(buff));

        // ---------------------------------------------
        // evaluate_or()
        // ---------------------------------------------
        buff.assign(5, 0);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("0,1,1,0,0", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("0,1,1,0,0", to_string(buff));

        buff.assign(5, 1);
        p->evaluate_or(c.get(), buff.data(), 0, 5);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));

        p->evaluate_or(c.get(), buff.data(), 2, 4);
        ASSERT_EQ("1,1,1,1,1", to_string(buff));
    }
}

#define ZMF(min, max) zone_map_filter(ZoneMapDetail(min, max))

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, zone_map_filter) {
    std::unique_ptr<ColumnPredicate> eq_100(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "100"));
    std::unique_ptr<ColumnPredicate> ne_100(new_column_ne_predicate(get_type_info(TYPE_INT), 0, "100"));
    std::unique_ptr<ColumnPredicate> gt_100(new_column_gt_predicate(get_type_info(TYPE_INT), 0, "100"));
    std::unique_ptr<ColumnPredicate> ge_100(new_column_ge_predicate(get_type_info(TYPE_INT), 0, "100"));
    std::unique_ptr<ColumnPredicate> lt_100(new_column_lt_predicate(get_type_info(TYPE_INT), 0, "100"));
    std::unique_ptr<ColumnPredicate> le_100(new_column_le_predicate(get_type_info(TYPE_INT), 0, "100"));
    std::unique_ptr<ColumnPredicate> is_null(new_column_null_predicate(get_type_info(TYPE_INT), 0, true));
    std::unique_ptr<ColumnPredicate> not_null(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));
    std::unique_ptr<ColumnPredicate> in_90_100(new_column_in_predicate(get_type_info(TYPE_INT), 0, {"90", "100"}));
    std::unique_ptr<ColumnPredicate> not_in_90_100(
            new_column_not_in_predicate(get_type_info(TYPE_INT), 0, {"90", "100"}));

    EXPECT_TRUE(eq_100->ZMF(Datum(90), Datum(100)));
    EXPECT_TRUE(eq_100->ZMF(Datum(100), Datum(100)));
    EXPECT_FALSE(eq_100->ZMF(Datum(101), Datum(200)));
    EXPECT_TRUE(eq_100->ZMF(Datum(), Datum(200)));
    EXPECT_FALSE(eq_100->ZMF(Datum(), Datum()));

    EXPECT_TRUE(ne_100->ZMF(Datum(90), Datum(99)));
    EXPECT_TRUE(ne_100->ZMF(Datum(90), Datum(200)));
    EXPECT_TRUE(ne_100->ZMF(Datum(), Datum()));
    EXPECT_TRUE(ne_100->ZMF(Datum(), Datum(99)));

    EXPECT_TRUE(gt_100->ZMF(Datum(90), Datum(101)));
    EXPECT_TRUE(gt_100->ZMF(Datum(), Datum(101)));
    EXPECT_FALSE(gt_100->ZMF(Datum(90), Datum(100)));
    EXPECT_FALSE(gt_100->ZMF(Datum(90), Datum(99)));
    EXPECT_FALSE(gt_100->ZMF(Datum(), Datum()));

    EXPECT_TRUE(ge_100->ZMF(Datum(90), Datum(101)));
    EXPECT_TRUE(ge_100->ZMF(Datum(), Datum(101)));
    EXPECT_TRUE(ge_100->ZMF(Datum(90), Datum(100)));
    EXPECT_FALSE(ge_100->ZMF(Datum(90), Datum(99)));
    EXPECT_FALSE(ge_100->ZMF(Datum(), Datum()));

    EXPECT_TRUE(lt_100->ZMF(Datum(), Datum(101)));
    EXPECT_TRUE(lt_100->ZMF(Datum(99), Datum(200)));
    EXPECT_FALSE(lt_100->ZMF(Datum(100), Datum(200)));
    EXPECT_FALSE(lt_100->ZMF(Datum(), Datum()));

    EXPECT_TRUE(le_100->ZMF(Datum(), Datum(101)));
    EXPECT_TRUE(le_100->ZMF(Datum(100), Datum(200)));
    EXPECT_FALSE(le_100->ZMF(Datum(101), Datum(200)));
    EXPECT_FALSE(le_100->ZMF(Datum(), Datum()));

    EXPECT_TRUE(is_null->ZMF(Datum(), Datum()));
    EXPECT_TRUE(is_null->ZMF(Datum(), Datum(100)));
    EXPECT_FALSE(is_null->ZMF(Datum(100), Datum(200)));

    EXPECT_TRUE(not_null->ZMF(Datum(), Datum(100)));
    EXPECT_TRUE(not_null->ZMF(Datum(100), Datum(200)));
    EXPECT_FALSE(not_null->ZMF(Datum(), Datum()));

    EXPECT_TRUE(in_90_100->ZMF(Datum(), Datum(100)));
    EXPECT_TRUE(in_90_100->ZMF(Datum(100), Datum(110)));
    EXPECT_TRUE(in_90_100->ZMF(Datum(90), Datum(99)));
    EXPECT_FALSE(in_90_100->ZMF(Datum(80), Datum(89)));
    EXPECT_FALSE(in_90_100->ZMF(Datum(101), Datum(110)));

    EXPECT_TRUE(not_in_90_100->ZMF(Datum(), Datum(100)));
    EXPECT_TRUE(not_in_90_100->ZMF(Datum(100), Datum(110)));
    EXPECT_TRUE(not_in_90_100->ZMF(Datum(90), Datum(99)));
    EXPECT_TRUE(not_in_90_100->ZMF(Datum(80), Datum(89)));
    EXPECT_TRUE(not_in_90_100->ZMF(Datum(101), Datum(110)));
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, zone_map_filter_char) {
    std::unique_ptr<ColumnPredicate> eq_xx(new_column_eq_predicate(get_type_info(TYPE_CHAR), 0, "xx\0\0\0"));
    std::unique_ptr<ColumnPredicate> ne_xx(new_column_ne_predicate(get_type_info(TYPE_CHAR), 0, "xx\0\0\0"));
    std::unique_ptr<ColumnPredicate> gt_xx(new_column_gt_predicate(get_type_info(TYPE_CHAR), 0, "xx\0\0\0"));
    std::unique_ptr<ColumnPredicate> ge_xx(new_column_ge_predicate(get_type_info(TYPE_CHAR), 0, "xx\0\0\0"));
    std::unique_ptr<ColumnPredicate> lt_xx(new_column_lt_predicate(get_type_info(TYPE_CHAR), 0, "xx\0\0\0"));
    std::unique_ptr<ColumnPredicate> le_xx(new_column_le_predicate(get_type_info(TYPE_CHAR), 0, "xx\0\0\0"));
    std::unique_ptr<ColumnPredicate> is_null(new_column_null_predicate(get_type_info(TYPE_CHAR), 0, true));
    std::unique_ptr<ColumnPredicate> not_null(new_column_null_predicate(get_type_info(TYPE_CHAR), 0, false));
    std::unique_ptr<ColumnPredicate> in_xx_yy(
            new_column_in_predicate(get_type_info(TYPE_CHAR), 0, {"xx\0\0", "yy\0\0"}));
    std::unique_ptr<ColumnPredicate> not_in_xx_yy(
            new_column_not_in_predicate(get_type_info(TYPE_CHAR), 0, {"xx\0\0", "yy\0\0"}));

    EXPECT_TRUE(eq_xx->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_TRUE(eq_xx->ZMF(Datum("xx"), Datum("xx")));
    EXPECT_FALSE(eq_xx->ZMF(Datum("xy"), Datum("yy")));
    EXPECT_TRUE(eq_xx->ZMF(Datum(), Datum("yy")));
    EXPECT_FALSE(eq_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(ne_xx->ZMF(Datum("tt"), Datum("xa")));
    EXPECT_TRUE(ne_xx->ZMF(Datum("tt"), Datum("yy")));
    EXPECT_TRUE(ne_xx->ZMF(Datum(), Datum()));
    EXPECT_TRUE(ne_xx->ZMF(Datum(), Datum("xa")));

    EXPECT_TRUE(gt_xx->ZMF(Datum("tt"), Datum("xy")));
    EXPECT_TRUE(gt_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_FALSE(gt_xx->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_FALSE(gt_xx->ZMF(Datum("tt"), Datum("xw")));
    EXPECT_FALSE(gt_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(ge_xx->ZMF(Datum("tt"), Datum("xy")));
    EXPECT_TRUE(ge_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_TRUE(ge_xx->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_FALSE(ge_xx->ZMF(Datum("tt"), Datum("xw")));
    EXPECT_FALSE(ge_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(lt_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_TRUE(lt_xx->ZMF(Datum("xw"), Datum("yy")));
    EXPECT_FALSE(lt_xx->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_FALSE(lt_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(le_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_TRUE(le_xx->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_FALSE(le_xx->ZMF(Datum("xy"), Datum("yy")));
    EXPECT_FALSE(le_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(is_null->ZMF(Datum(), Datum()));
    EXPECT_TRUE(is_null->ZMF(Datum(), Datum("xx")));
    EXPECT_FALSE(is_null->ZMF(Datum("xx"), Datum("yy")));

    EXPECT_TRUE(not_null->ZMF(Datum(), Datum("xx")));
    EXPECT_TRUE(not_null->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_FALSE(not_null->ZMF(Datum(), Datum()));

    EXPECT_TRUE(in_xx_yy->ZMF(Datum(), Datum("xx")));
    EXPECT_TRUE(in_xx_yy->ZMF(Datum("yy"), Datum("yy\0")));
    EXPECT_TRUE(in_xx_yy->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_FALSE(in_xx_yy->ZMF(Datum("ab"), Datum("tt")));
    EXPECT_FALSE(in_xx_yy->ZMF(Datum("yz"), Datum("zz")));

    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum(), Datum("xx")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("tt"), Datum("x")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("ab"), Datum("cd")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("xy"), Datum("zz")));
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, zone_map_filter_varchar) {
    std::unique_ptr<ColumnPredicate> eq_xx(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, "xx"));
    std::unique_ptr<ColumnPredicate> ne_xx(new_column_ne_predicate(get_type_info(TYPE_VARCHAR), 0, "xx"));
    std::unique_ptr<ColumnPredicate> gt_xx(new_column_gt_predicate(get_type_info(TYPE_VARCHAR), 0, "xx"));
    std::unique_ptr<ColumnPredicate> ge_xx(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 0, "xx"));
    std::unique_ptr<ColumnPredicate> lt_xx(new_column_lt_predicate(get_type_info(TYPE_VARCHAR), 0, "xx"));
    std::unique_ptr<ColumnPredicate> le_xx(new_column_le_predicate(get_type_info(TYPE_VARCHAR), 0, "xx"));
    std::unique_ptr<ColumnPredicate> is_null(new_column_null_predicate(get_type_info(TYPE_VARCHAR), 0, true));
    std::unique_ptr<ColumnPredicate> not_null(new_column_null_predicate(get_type_info(TYPE_VARCHAR), 0, false));
    std::unique_ptr<ColumnPredicate> in_xx_yy(new_column_in_predicate(get_type_info(TYPE_VARCHAR), 0, {"xx", "yy"}));
    std::unique_ptr<ColumnPredicate> not_in_xx_yy(
            new_column_not_in_predicate(get_type_info(TYPE_VARCHAR), 0, {"xx", "yy"}));

    EXPECT_TRUE(eq_xx->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_TRUE(eq_xx->ZMF(Datum("xx"), Datum("xx")));
    EXPECT_FALSE(eq_xx->ZMF(Datum("xy"), Datum("yy")));
    EXPECT_TRUE(eq_xx->ZMF(Datum(), Datum("yy")));
    EXPECT_FALSE(eq_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(ne_xx->ZMF(Datum("tt"), Datum("xa")));
    EXPECT_TRUE(ne_xx->ZMF(Datum("tt"), Datum("yy")));
    EXPECT_TRUE(ne_xx->ZMF(Datum(), Datum()));
    EXPECT_TRUE(ne_xx->ZMF(Datum(), Datum("xa")));

    EXPECT_TRUE(gt_xx->ZMF(Datum("tt"), Datum("xy")));
    EXPECT_TRUE(gt_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_FALSE(gt_xx->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_FALSE(gt_xx->ZMF(Datum("tt"), Datum("xw")));
    EXPECT_FALSE(gt_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(ge_xx->ZMF(Datum("tt"), Datum("xy")));
    EXPECT_TRUE(ge_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_TRUE(ge_xx->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_FALSE(ge_xx->ZMF(Datum("tt"), Datum("xw")));
    EXPECT_FALSE(ge_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(lt_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_TRUE(lt_xx->ZMF(Datum("xw"), Datum("yy")));
    EXPECT_FALSE(lt_xx->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_FALSE(lt_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(le_xx->ZMF(Datum(), Datum("xy")));
    EXPECT_TRUE(le_xx->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_FALSE(le_xx->ZMF(Datum("xy"), Datum("yy")));
    EXPECT_FALSE(le_xx->ZMF(Datum(), Datum()));

    EXPECT_TRUE(is_null->ZMF(Datum(), Datum()));
    EXPECT_TRUE(is_null->ZMF(Datum(), Datum("xx")));
    EXPECT_FALSE(is_null->ZMF(Datum("xx"), Datum("yy")));

    EXPECT_TRUE(not_null->ZMF(Datum(), Datum("xx")));
    EXPECT_TRUE(not_null->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_FALSE(not_null->ZMF(Datum(), Datum()));

    EXPECT_TRUE(in_xx_yy->ZMF(Datum(), Datum("xx")));
    EXPECT_TRUE(in_xx_yy->ZMF(Datum("yy"), Datum("yy\0")));
    EXPECT_TRUE(in_xx_yy->ZMF(Datum("tt"), Datum("xx")));
    EXPECT_FALSE(in_xx_yy->ZMF(Datum("ab"), Datum("tt")));
    EXPECT_FALSE(in_xx_yy->ZMF(Datum("yz"), Datum("zz")));

    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum(), Datum("xx")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("xx"), Datum("yy")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("tt"), Datum("x")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("ab"), Datum("cd")));
    EXPECT_TRUE(not_in_xx_yy->ZMF(Datum("xy"), Datum("zz")));
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_convert_cmp_predicate) {
    // clang-format off
    std::vector<PredicateType> testcases = {
        PredicateType::kEQ,
        PredicateType::kNE,
        PredicateType::kLT,
        PredicateType::kLE,
        PredicateType::kGT,
        PredicateType::kGE
    };
    // clang-format on

    for (auto predicate : testcases) {
        std::unique_ptr<ColumnPredicate> p(new_column_cmp_predicate(predicate, get_type_info(TYPE_BOOLEAN), 0, "1"));
        const ColumnPredicate* new_p;
        ObjectPool op;

        // different type
        {
            TypeInfoPtr new_type = get_type_info(TYPE_INT);
            ASSERT_OK(p->convert_to(&new_p, new_type, &op));
            EXPECT_EQ(new_p->type(), p->type());
        }

        // same type
        {
            TypeInfoPtr new_type = get_type_info(TYPE_BOOLEAN);
            ASSERT_OK(p->convert_to(&new_p, new_type, &op));
            EXPECT_EQ(new_p->type(), p->type());
        }
    }
}

// NOLINTNEXTLINE
TEST(ColumnPredicateTest, test_convert_cmp_binary_predicate) {
    // clang-format off
    std::vector<PredicateType> testcases = {
        PredicateType::kEQ,
        PredicateType::kNE,
        PredicateType::kLT,
        PredicateType::kLE,
        PredicateType::kGT,
        PredicateType::kGE
    };
    // clang-format on

    for (auto predicate : testcases) {
        std::unique_ptr<ColumnPredicate> p(new_column_cmp_predicate(predicate, get_type_info(TYPE_VARCHAR), 0, "1"));

        const ColumnPredicate* new_p;
        TypeInfoPtr new_type = get_type_info(TYPE_VARCHAR);
        ObjectPool op;

        ASSERT_OK(p->convert_to(&new_p, new_type, &op));
        EXPECT_EQ(new_p->type(), p->type());
    }
}
} // namespace starrocks

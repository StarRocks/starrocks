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

#include "storage/record_predicate/column_range.h"

#include <gtest/gtest.h>

#include "gen_cpp/types.pb.h"
#include "storage/chunk_helper.h"
#include "storage/record_predicate/record_predicate_helper.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"

namespace starrocks {

class ColumnRangeTest : public ::testing::Test {
public:
    void SetUp() override {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(PRIMARY_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(5);
        schema_pb.set_next_column_unique_id(3);

        ColumnPB& col1 = *(schema_pb.add_column());
        col1.set_unique_id(1);
        col1.set_name("col1");
        col1.set_type("INT");
        col1.set_is_key(true);
        col1.set_is_nullable(false);

        ColumnPB& col2 = *(schema_pb.add_column());
        col2.set_unique_id(2);
        col2.set_name("col2");
        col2.set_type("INT");
        col2.set_is_key(true);
        col2.set_is_nullable(false);

        _table_schema = std::make_shared<const TabletSchema>(schema_pb);
    }

protected:
    std::shared_ptr<const TabletSchema> _table_schema;
};

TEST_F(ColumnRangeTest, InitValidation) {
    RecordPredicatePB pb;
    pb.set_type(RecordPredicatePB::COLUMN_RANGE);
    auto* range_pb = pb.mutable_column_range();

    // Case 1: No columns
    {
        auto st = RecordPredicateHelper::create(pb);
        ASSERT_ERROR(st);
        EXPECT_TRUE(st.status().to_string().find("no range columns defined") != std::string::npos);
    }

    // Case 2: Empty column name
    {
        range_pb->add_column_names("");
        auto st = RecordPredicateHelper::create(pb);
        ASSERT_ERROR(st);
        EXPECT_TRUE(st.status().to_string().find("empty range column name") != std::string::npos);
        range_pb->clear_column_names();
    }

    // Case 3: Invalid range bounds (both empty)
    {
        range_pb->add_column_names("col1");
        auto st = RecordPredicateHelper::create(pb);
        ASSERT_ERROR(st);
        EXPECT_TRUE(st.status().to_string().find("invalid range bounds") != std::string::npos);
    }

    // Case 4: Invalid range bounds (both set)
    {
        auto* range = range_pb->mutable_range();
        range->mutable_lower_bound()->add_values()->set_int_value(1);
        range->mutable_upper_bound()->add_values()->set_int_value(10);
        auto st = RecordPredicateHelper::create(pb);
        ASSERT_ERROR(st);
        EXPECT_TRUE(st.status().to_string().find("invalid range bounds") != std::string::npos);
    }
}

TEST_F(ColumnRangeTest, SingleColumnEvaluate) {
    auto chunk = ChunkHelper::new_chunk(*_table_schema->schema(), 10);
    // col1 values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    for (int i = 0; i < 10; ++i) {
        chunk->get_column_by_name("col1")->append_datum(Datum((int32_t)i));
        chunk->get_column_by_name("col2")->append_datum(Datum((int32_t)i)); // dummy
    }

    // Test < 5
    // Boundary check: 4 should match, 5 should not match
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        auto* range = range_pb->mutable_range();
        range->mutable_upper_bound()->add_values()->set_int_value(5);
        range->set_upper_bound_included(false);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(selection[i], i < 5 ? 1 : 0);
        }
    }

    // Test <= 5
    // Boundary check: 5 should match, 6 should not match
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        auto* range = range_pb->mutable_range();
        range->mutable_upper_bound()->add_values()->set_int_value(5);
        range->set_upper_bound_included(true);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(selection[i], i <= 5 ? 1 : 0);
        }
    }

    // Test > 5
    // Boundary check: 6 should match, 5 should not match
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        auto* range = range_pb->mutable_range();
        range->mutable_lower_bound()->add_values()->set_int_value(5);
        range->set_lower_bound_included(false);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(selection[i], i > 5 ? 1 : 0);
        }
    }

    // Test >= 5
    // Boundary check: 5 should match, 4 should not match
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        auto* range = range_pb->mutable_range();
        range->mutable_lower_bound()->add_values()->set_int_value(5);
        range->set_lower_bound_included(true);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(selection[i], i >= 5 ? 1 : 0);
        }
    }
}

TEST_F(ColumnRangeTest, MultiColumnEvaluate) {
    auto chunk = ChunkHelper::new_chunk(*_table_schema->schema(), 10);
    // col1: 1, 1, 1, 2, 2, 2, 3, 3, 3, 4
    // col2: 1, 2, 3, 1, 2, 3, 1, 2, 3, 1
    std::vector<int> c1 = {1, 1, 1, 2, 2, 2, 3, 3, 3, 4};
    std::vector<int> c2 = {1, 2, 3, 1, 2, 3, 1, 2, 3, 1};
    
    for (int i = 0; i < 10; ++i) {
        chunk->get_column_by_name("col1")->append_datum(Datum((int32_t)c1[i]));
        chunk->get_column_by_name("col2")->append_datum(Datum((int32_t)c2[i]));
    }

    // Test < (2, 2)
    // Expected matches: (1,1), (1,2), (1,3), (2,1)
    // Boundary: (2,2) should not match. (2,1) matches because 2==2 and 1<2.
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        range_pb->add_column_names("col2");
        auto* range = range_pb->mutable_range();
        auto* val1 = range->mutable_upper_bound()->add_values();
        val1->set_int_value(2);
        auto* val2 = range->mutable_upper_bound()->add_values();
        val2->set_int_value(2);
        range->set_upper_bound_included(false);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            bool expected = (c1[i] < 2) || (c1[i] == 2 && c2[i] < 2);
            EXPECT_EQ(selection[i], expected ? 1 : 0) << "Row " << i << ": (" << c1[i] << "," << c2[i] << ")";
        }
    }

    // Test <= (2, 2)
    // Expected matches: (1,all), (2,1), (2,2)
    // Boundary: (2,2) should match. (2,3) should not match.
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        range_pb->add_column_names("col2");
        auto* range = range_pb->mutable_range();
        auto* val1 = range->mutable_upper_bound()->add_values();
        val1->set_int_value(2);
        auto* val2 = range->mutable_upper_bound()->add_values();
        val2->set_int_value(2);
        range->set_upper_bound_included(true);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            bool expected = (c1[i] < 2) || (c1[i] == 2 && c2[i] <= 2);
            EXPECT_EQ(selection[i], expected ? 1 : 0) << "Row " << i << ": (" << c1[i] << "," << c2[i] << ")";
        }
    }

    // Test > (2, 2)
    // Expected matches: (2,3), (3,all), (4,all)
    // Boundary: (2,2) should not match. (2,3) should match.
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        range_pb->add_column_names("col2");
        auto* range = range_pb->mutable_range();
        auto* val1 = range->mutable_lower_bound()->add_values();
        val1->set_int_value(2);
        auto* val2 = range->mutable_lower_bound()->add_values();
        val2->set_int_value(2);
        range->set_lower_bound_included(false);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            bool expected = (c1[i] > 2) || (c1[i] == 2 && c2[i] > 2);
            EXPECT_EQ(selection[i], expected ? 1 : 0) << "Row " << i << ": (" << c1[i] << "," << c2[i] << ")";
        }
    }

    // Test >= (2, 2)
    // Expected matches: (2,2), (2,3), (3,all), (4,all)
    // Boundary: (2,2) should match. (2,1) should not match.
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("col1");
        range_pb->add_column_names("col2");
        auto* range = range_pb->mutable_range();
        auto* val1 = range->mutable_lower_bound()->add_values();
        val1->set_int_value(2);
        auto* val2 = range->mutable_lower_bound()->add_values();
        val2->set_int_value(2);
        range->set_lower_bound_included(true);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(10);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        for (int i = 0; i < 10; ++i) {
            bool expected = (c1[i] > 2) || (c1[i] == 2 && c2[i] >= 2);
            EXPECT_EQ(selection[i], expected ? 1 : 0) << "Row " << i << ": (" << c1[i] << "," << c2[i] << ")";
        }
    }
}

TEST_F(ColumnRangeTest, MultiColumnStringEvaluate) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_num_short_key_columns(2);
    schema_pb.set_num_rows_per_row_block(5);
    schema_pb.set_next_column_unique_id(3);

    ColumnPB& col1 = *(schema_pb.add_column());
    col1.set_unique_id(1);
    col1.set_name("s1");
    col1.set_type("VARCHAR");
    col1.set_is_key(true);
    col1.set_is_nullable(false);

    ColumnPB& col2 = *(schema_pb.add_column());
    col2.set_unique_id(2);
    col2.set_name("s2");
    col2.set_type("VARCHAR");
    col2.set_is_key(true);
    col2.set_is_nullable(false);

    auto schema = std::make_shared<const TabletSchema>(schema_pb);
    auto chunk = ChunkHelper::new_chunk(*schema->schema(), 5);

    // Data:
    // 0: ("a", "b")
    // 1: ("b", "a")
    // 2: ("b", "b")
    // 3: ("b", "c")
    // 4: ("c", "a")
    std::vector<std::string> s1 = {"a", "b", "b", "b", "c"};
    std::vector<std::string> s2 = {"b", "a", "b", "c", "a"};

    for (int i = 0; i < 5; ++i) {
        chunk->get_column_by_name("s1")->append_datum(Datum(Slice(s1[i])));
        chunk->get_column_by_name("s2")->append_datum(Datum(Slice(s2[i])));
    }

    // Target: ("b", "b")

    // Test < ("b", "b")
    // Expect: ("a", "b"), ("b", "a") -> indices 0, 1
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("s1");
        range_pb->add_column_names("s2");
        auto* range = range_pb->mutable_range();
        range->mutable_upper_bound()->add_values()->set_string_value("b");
        range->mutable_upper_bound()->add_values()->set_string_value("b");
        range->set_upper_bound_included(false);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(5);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        EXPECT_EQ(selection[0], 1); // a < b
        EXPECT_EQ(selection[1], 1); // b == b, a < b
        EXPECT_EQ(selection[2], 0); // b == b, b < b (False)
        EXPECT_EQ(selection[3], 0); // b == b, c < b (False)
        EXPECT_EQ(selection[4], 0); // c < b (False)
    }

    // Test >= ("b", "b")
    // Expect: ("b", "b"), ("b", "c"), ("c", "a") -> indices 2, 3, 4
    {
        RecordPredicatePB pb;
        pb.set_type(RecordPredicatePB::COLUMN_RANGE);
        auto* range_pb = pb.mutable_column_range();
        range_pb->add_column_names("s1");
        range_pb->add_column_names("s2");
        auto* range = range_pb->mutable_range();
        range->mutable_lower_bound()->add_values()->set_string_value("b");
        range->mutable_lower_bound()->add_values()->set_string_value("b");
        range->set_lower_bound_included(true);

        ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
        std::vector<uint8_t> selection(5);
        ASSERT_OK(pred->evaluate(chunk.get(), selection.data()));

        EXPECT_EQ(selection[0], 0);
        EXPECT_EQ(selection[1], 0);
        EXPECT_EQ(selection[2], 1); // b == b, b >= b
        EXPECT_EQ(selection[3], 1); // b == b, c >= b
        EXPECT_EQ(selection[4], 1); // c > b
    }
}

TEST_F(ColumnRangeTest, SchemaMismatch) {
    RecordPredicatePB pb;
    pb.set_type(RecordPredicatePB::COLUMN_RANGE);
    auto* range_pb = pb.mutable_column_range();
    range_pb->add_column_names("non_existent_col");
    auto* range = range_pb->mutable_range();
    range->mutable_upper_bound()->add_values()->set_int_value(5);
    range->set_upper_bound_included(false);

    ASSIGN_OR_ABORT(auto pred, RecordPredicateHelper::create(pb));
    auto chunk = ChunkHelper::new_chunk(*_table_schema->schema(), 10);
    std::vector<uint8_t> selection(10);
    
    auto st = pred->evaluate(chunk.get(), selection.data());
    ASSERT_ERROR(st);
    EXPECT_TRUE(st.to_string().find("does not contains all columns which record predicate need") != std::string::npos);
}

} // namespace starrocks

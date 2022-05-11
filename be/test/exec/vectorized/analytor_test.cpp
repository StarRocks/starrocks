// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <gtest/gtest.h>

#include "exec/vectorized/analytor.h"

namespace starrocks::vectorized {
class AnalytorTest : public ::testing::Test {
public:
    void SetUp() override {
        config::vector_chunk_size = 1024;
    }
};

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_partition_end) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    auto c2 = Int32Column::create();
    v = 3;
    c2->append_value_multiple_times(&v, 5);
    v = 4;
    c2->append_value_multiple_times(&v, 15);

    analytor.update_input_rows(20);
    analytor._partition_columns.emplace_back(c1);
    analytor._partition_columns.emplace_back(c2);

    analytor.find_partition_end();
    ASSERT_EQ(analytor.found_partition_end(), 5);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_peer_group_end) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    analytor.update_input_rows(20);
    analytor._order_columns.emplace_back(c1);
    analytor._partition_end = 20;

    analytor.find_peer_group_end();
    ASSERT_EQ(analytor.peer_group_end(), 10);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, reset_state_for_cur_partition) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr);

    analytor._partition_start = 3;
    analytor._partition_end = 10;
    analytor._found_partition_end = 20;
    analytor.reset_state_for_cur_partition();
    ASSERT_EQ(analytor.partition_start(), 10);
    ASSERT_EQ(analytor.partition_end(), 20);
    ASSERT_EQ(analytor.current_row_position(), 10);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, reset_state_for_next_partition) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr);

    analytor._partition_start = 10;
    analytor._partition_end = 10;
    analytor._found_partition_end = 20;
    analytor.reset_state_for_next_partition();
    ASSERT_EQ(analytor.partition_start(), 20);
    ASSERT_EQ(analytor.partition_end(), 20);
    ASSERT_EQ(analytor.current_row_position(), 20);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_and_check_partition_end) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor1(plan_node, row_desc, nullptr);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    auto c2 = Int32Column::create();
    v = 3;
    c2->append_value_multiple_times(&v, 5);
    v = 4;
    c2->append_value_multiple_times(&v, 15);

    analytor1.update_input_rows(20);
    analytor1._partition_columns.emplace_back(c1);
    analytor1._partition_columns.emplace_back(c2);

    bool end = analytor1.find_and_check_partition_end();
    ASSERT_TRUE(end);
    ASSERT_EQ(analytor1.found_partition_end(), 5);

    analytor1.reset_state_for_cur_partition();

    end = analytor1.find_and_check_partition_end();
    ASSERT_TRUE(end);
    ASSERT_EQ(analytor1.found_partition_end(), 10);

    analytor1.reset_state_for_cur_partition();

    end = analytor1.find_and_check_partition_end();
    ASSERT_FALSE(end);
    ASSERT_EQ(analytor1.found_partition_end(), 20);

    // partition columns is empty
    Analytor analytor2(plan_node, row_desc, nullptr);
    analytor2.update_input_rows(20);

    end = analytor2.find_and_check_partition_end();
    ASSERT_FALSE(end);
    ASSERT_EQ(analytor2.found_partition_end(), 20);

    // input rows = 0
    Analytor analytor3(plan_node, row_desc, nullptr);
    analytor3.update_input_rows(0);

    end = analytor3.find_and_check_partition_end();
    ASSERT_FALSE(end);
    ASSERT_EQ(analytor3.found_partition_end(), 0);
}

}

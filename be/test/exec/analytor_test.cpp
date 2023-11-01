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

#include "exec/analytor.h"

#include <gtest/gtest.h>

#include "column/fixed_length_column.h"

namespace starrocks {
class AnalytorTest : public ::testing::Test {
public:
    void SetUp() override { config::vector_chunk_size = 1024; }
};

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_peer_group_end) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr, false);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    analytor._update_input_rows(20);
    analytor._order_columns.emplace_back(c1);
    analytor._found_partition_end = {true, 20};

    analytor._find_peer_group_end();
    ASSERT_EQ(analytor._peer_group_end, 10);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, reset_state_for_cur_partition) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr, false);

    analytor._partition_start = 3;
    analytor._partition_end = 10;
    analytor._found_partition_end = {true, 20};
    analytor._reset_state_for_cur_partition();
    ASSERT_EQ(analytor._partition_start, 10);
    ASSERT_EQ(analytor._partition_end, 20);
    ASSERT_EQ(analytor._current_row_position, 10);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, reset_state_for_next_partition) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr, false);

    analytor._partition_start = 10;
    analytor._partition_end = 10;
    analytor._found_partition_end = {true, 20};
    analytor._reset_state_for_next_partition();
    ASSERT_EQ(analytor._partition_start, 20);
    ASSERT_EQ(analytor._partition_end, 20);
    ASSERT_EQ(analytor._current_row_position, 20);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_partition_end) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor1(plan_node, row_desc, nullptr, false);

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

    analytor1._update_input_rows(20);
    analytor1._partition_columns.emplace_back(c1);
    analytor1._partition_columns.emplace_back(c2);

    analytor1._current_row_position = analytor1._found_partition_end.second;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._found_partition_end.first);
    ASSERT_EQ(analytor1._found_partition_end.second, 5);

    analytor1._reset_state_for_cur_partition();

    analytor1._current_row_position = analytor1._found_partition_end.second;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._found_partition_end.first);
    ASSERT_EQ(analytor1._found_partition_end.second, 10);

    analytor1._reset_state_for_cur_partition();

    analytor1._current_row_position = analytor1._found_partition_end.second;
    analytor1._find_partition_end();
    ASSERT_FALSE(analytor1._found_partition_end.first);
    ASSERT_EQ(analytor1._found_partition_end.second, 20);

    // partition columns is empty
    Analytor analytor2(plan_node, row_desc, nullptr, false);
    analytor2._update_input_rows(20);

    analytor2._current_row_position = analytor2._found_partition_end.second;
    analytor2._find_partition_end();
    ASSERT_FALSE(analytor2._found_partition_end.first);
    ASSERT_EQ(analytor2._found_partition_end.second, 20);

    // input rows = 0
    Analytor analytor3(plan_node, row_desc, nullptr, false);
    analytor3._update_input_rows(0);

    analytor2._current_row_position = analytor2._found_partition_end.second;
    analytor3._find_partition_end();
    ASSERT_FALSE(analytor3._found_partition_end.first);
    ASSERT_EQ(analytor3._found_partition_end.second, 0);
}

} // namespace starrocks

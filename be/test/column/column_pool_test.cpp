// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/column/column_pool_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "column/column_pool.h"

#include "gtest/gtest.h"

namespace starrocks::vectorized {

class ColumnPoolTest : public ::testing::Test {
protected:
    void SetUp() override { TEST_clear_all_columns_this_thread(); }
    void TearDown() override { TEST_clear_all_columns_this_thread(); }
};

// NOLINTNEXTLINE
TEST_F(ColumnPoolTest, single_thread) {
    auto c1 = get_column<Int32Column>();
    ASSERT_EQ(0u, c1->get_data().capacity());

    c1->reserve(3);
    c1->append_datum(Datum((int32_t)1));
    c1->append_datum(Datum((int32_t)2));
    c1->set_delete_state(DEL_PARTIAL_SATISFIED);
    return_column<Int32Column>(c1);

    auto c2 = get_column<Int32Column>();
    ASSERT_EQ(c1, c2);
    ASSERT_EQ(0u, c2->size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c2->delete_state());
    ASSERT_EQ(3, c2->get_data().capacity());

    auto c3 = get_column<Int32Column>();
    ASSERT_NE(c2, c3);

    auto c4 = get_column<Int32Column>();
    ASSERT_NE(c3, c4);

    auto c5 = get_column<Int32Column>();
    ASSERT_NE(c4, c5);

    auto c6 = get_column<Int32Column>();
    ASSERT_NE(c5, c6);

    return_column<Int32Column>(c6);
    return_column<Int32Column>(c5);

    auto c7 = get_column<Int32Column>();
    auto c8 = get_column<Int32Column>();
    ASSERT_EQ(c5, c7);
    ASSERT_EQ(c6, c8);
    c5 = nullptr;
    c6 = nullptr;

    return_column<Int32Column>(c8);
    return_column<Int32Column>(c7);

    delete c2;
    delete c3;
    delete c4;
}

} // namespace starrocks::vectorized

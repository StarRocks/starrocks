// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/column/object_column_test.cpp

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

#include "column/object_column.h"

#include <gtest/gtest.h>

#include "column/const_column.h"

namespace starrocks::vectorized {

// NOLINTNEXTLINE
TEST(ObjectColumnTest, HLL_test_filter) {
    // keep all.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 1);
        c->filter(filter);
        ASSERT_EQ(100, c->size());
    }
    // filter all.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);

        Column::Filter filter(100, 0);
        c->filter(filter);
        ASSERT_EQ(0, c->size());
    }
    // filter out the last 10 elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 1);
        for (int i = 90; i < 100; i++) {
            filter[i] = 0;
        }
        c->filter(filter);
        ASSERT_EQ(90, c->size());
    }
    // filter out the first 10 elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 1);
        for (int i = 0; i < 10; i++) {
            filter[i] = 0;
        }
        c->filter(filter);
        ASSERT_EQ(90, c->size());
    }
    // filter out half elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 1);
        for (int i = 0; i < 100; i++) {
            filter[i] = i % 2;
        }
        c->filter(filter);
        ASSERT_EQ(50, c->size());
    }
}

// NOLINTNEXTLINE
TEST(ObjectColumnTest, HLL_test_filter_range) {
    // keep all.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 1);
        c->filter_range(filter, 0, 100);
        ASSERT_EQ(100, c->size());
    }
    // filter all.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);

        Column::Filter filter(100, 0);
        c->filter_range(filter, 0, 100);
        ASSERT_EQ(0, c->size());
    }
    // filter out the last 10 elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 0);
        c->filter_range(filter, 90, 100);
        ASSERT_EQ(90, c->size());
    }
    // filter out the first 10 elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 0);
        c->filter_range(filter, 0, 10);
        ASSERT_EQ(90, c->size());
    }
    // filter 12 elements in the middle
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Column::Filter filter(100, 0);
        c->filter_range(filter, 20, 32);
        ASSERT_EQ(88, c->size());
    }
}

// NOLINTNEXTLINE
TEST(ObjectColumnTest, HLL_test_reset_column) {
    auto c = HyperLogLogColumn::create();

    c->append(HyperLogLog());
    c->append(HyperLogLog());
    c->append(HyperLogLog());
    const std::vector<HyperLogLog*>& data = c->get_data();
    ASSERT_EQ(3, data.size());
    c->set_delete_state(DEL_PARTIAL_SATISFIED);

    ASSERT_EQ(DEL_PARTIAL_SATISFIED, c->delete_state());

    c->reset_column();
    ASSERT_EQ(0, c->size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c->delete_state());
    ASSERT_EQ(0, c->get_data().size());
}

// NOLINTNEXTLINE
TEST(ObjectColumnTest, HLL_test_swap_column) {
    auto c1 = HyperLogLogColumn::create();
    auto c2 = HyperLogLogColumn::create();

    c1->append(HyperLogLog());
    c1->append(HyperLogLog());
    c1->append(HyperLogLog());

    c2->append(HyperLogLog());

    c1->swap_column(*c2);

    ASSERT_EQ(1, c1->size());
    ASSERT_EQ(1, c1->get_data().size());
    ASSERT_EQ(3, c2->size());
    ASSERT_EQ(3, c2->get_data().size());

    c2->swap_column(*c1);
    ASSERT_EQ(1, c2->size());
    ASSERT_EQ(1, c2->get_data().size());
    ASSERT_EQ(3, c1->size());
    ASSERT_EQ(3, c1->get_data().size());
}

} // namespace starrocks::vectorized

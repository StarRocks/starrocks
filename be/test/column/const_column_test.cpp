// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/column/const_column_test.cpp

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

#include "column/const_column.h"

#include <gtest/gtest.h>

#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "testutil/parallel_test.h"

namespace starrocks::vectorized {

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_basic) {
    auto data_column = FixedLengthColumn<int32_t>::create();
    data_column->append(2020);

    auto column = ConstColumn::create(std::move(data_column), 1024);

    ASSERT_EQ(true, column->is_constant());
    ASSERT_EQ(1024, column->size());
    ASSERT_EQ(sizeof(int32_t) + sizeof(size_t), column->byte_size());

    column->resize(100);
    ASSERT_EQ(100, column->size());

    column->append_default();
    ASSERT_EQ(101, column->size());

    auto data = reinterpret_cast<const int32_t*>(column->raw_data());
    ASSERT_EQ(data[0], 2020);

    int num = 10;
    ASSERT_EQ(-1, column->append_numbers(&num, sizeof(num)));

    ASSERT_FALSE(column->append_nulls(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_compare_at) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(100, 10);
    auto c2 = create_const_column(200, 10);
    ASSERT_EQ(0, c1->compare_at(1, 2, *c1, -1));
    ASSERT_LT(c1->compare_at(1, 9, *c2, -1), 0);
    ASSERT_GT(c2->compare_at(1, 9, *c1, -1), 0);
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_assign) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(100, 1);
    auto c2 = create_const_column(101, 10);

    c1->assign(1024, 0);
    for (size_t i = 0; i < 1024; i++) {
        ASSERT_EQ(c1->get(i).get_int32(), 100);
    }

    c2->assign(1024, 8);
    for (size_t i = 0; i < 1024; i++) {
        ASSERT_EQ(c2->get(i).get_int32(), 101);
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_reset_column) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c = create_const_column(1, 10);

    c->reset_column();
    ASSERT_EQ(0, c->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_swap_column) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);
    auto c2 = create_const_column(2, 200);

    c1->swap_column(*c2);

    ASSERT_EQ(200, c1->size());
    ASSERT_EQ(100, c2->size());

    ASSERT_EQ(1, c2->get(10).get_int32());
    ASSERT_EQ(2, c1->get(199).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_copy_constructor) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2(*c1);
    ASSERT_EQ(100, c2.size());
    ASSERT_TRUE(c2.data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2.get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2.size());
    ASSERT_TRUE(c2.data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2.get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_move_constructor) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2(std::move(*c1));
    ASSERT_EQ(100, c2.size());
    ASSERT_TRUE(c2.data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2.get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_copy_assignment) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2 = create_const_column(100, 1);
    *c2 = *c1;

    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_move_assignment) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2 = create_const_column(100, 1);
    *c2 = std::move(*c1);

    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_clone) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto cloned_col = c1->clone();
    auto c2 = down_cast<ConstColumn*>(cloned_col.get());
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_clone_shared) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto cloned_col = c1->clone_shared();
    ASSERT_TRUE(cloned_col.unique());
    auto c2 = down_cast<ConstColumn*>(cloned_col.get());
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_clone_empty) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto cloned_col = c1->clone_empty();
    auto c2 = down_cast<ConstColumn*>(cloned_col.get());
    ASSERT_EQ(0, c2->size());
    ASSERT_TRUE(c2->data_column().unique());
}

PARALLEL_TEST(ConstColumnTest, test_element_memory_usage) {
    auto create_const_column = [](int32_t value, size_t size) {
        auto c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto column = create_const_column(1, 10);
    ASSERT_EQ(4, column->element_memory_usage());

    for (size_t start = 0; start < column->size(); start++) {
        ASSERT_EQ(0, column->element_memory_usage(start, 0));
        for (size_t size = 1; start + size <= column->size(); size++) {
            ASSERT_EQ(4, column->element_memory_usage(start, size));
        }
    }
}

} // namespace starrocks::vectorized

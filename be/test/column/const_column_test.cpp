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

#include "column/const_column.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "testutil/parallel_test.h"

namespace starrocks {

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_const_column_upgrade_if_overflow) {
    Int32Column::Ptr data_column = Int32Column::create();
    data_column->append(1);

    ConstColumn::Ptr column = ConstColumn::create(std::move(data_column), 1024);
    auto ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);

    data_column = Int32Column::create();
    data_column->append(1);
    column = ConstColumn::create(std::move(data_column), 2ul << 32u);
    ret = column->upgrade_if_overflow();
    ASSERT_FALSE(ret.ok());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_const_column_downgrade) {
    BinaryColumn::Ptr data_column = BinaryColumn::create();
    ASSERT_FALSE(data_column->has_large_column());
    data_column->append_string("1");
    ConstColumn::Ptr const_column = ConstColumn::create(data_column, 1024);
    auto ret = const_column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);

    LargeBinaryColumn::Ptr large_data_column = LargeBinaryColumn::create();
    large_data_column->append_string("1");
    const_column = ConstColumn::create(large_data_column, 1024);
    ASSERT_TRUE(const_column->has_large_column());
    ret = const_column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_FALSE(const_column->has_large_column());
    ASSERT_FALSE(const_column->has_large_column());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_basic) {
    auto data_column = FixedLengthColumn<int32_t>::create();
    data_column->append(2020);

    ConstColumn::Ptr column = ConstColumn::create(std::move(data_column), 1024);

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
        Int32Column::Ptr c = Int32Column::create();
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
        Int32Column::Ptr c = Int32Column::create();
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
        Int32Column::Ptr c = Int32Column::create();
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
        Int32Column::Ptr c = Int32Column::create();
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
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2(*c1);
    ASSERT_EQ(100, c2.size());
    ASSERT_TRUE(c2.data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2.get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2.size());
    ASSERT_TRUE(c2.data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2.get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_move_constructor) {
    auto create_const_column = [](int32_t value, size_t size) {
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2(std::move(*c1));
    ASSERT_EQ(100, c2.size());
    ASSERT_TRUE(c2.data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2.get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_copy_assignment) {
    auto create_const_column = [](int32_t value, size_t size) {
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2 = create_const_column(100, 1);
    *c2 = *c1;

    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_move_assignment) {
    auto create_const_column = [](int32_t value, size_t size) {
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto c2 = create_const_column(100, 1);
    *c2 = std::move(*c1);

    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_clone) {
    auto create_const_column = [](int32_t value, size_t size) {
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto cloned_col = c1->clone();
    auto c2 = down_cast<ConstColumn*>(cloned_col.get());
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_clone_shared) {
    auto create_const_column = [](int32_t value, size_t size) {
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto cloned_col = c1->clone();
    ASSERT_TRUE(cloned_col->use_count() == 1);
    auto c2 = down_cast<ConstColumn*>(cloned_col.get());
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }

    c1->reset_column();
    ASSERT_EQ(100, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(1, c2->get(i).get_int32());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_clone_empty) {
    auto create_const_column = [](int32_t value, size_t size) {
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 100);

    ASSERT_EQ(100, c1->size());

    auto cloned_col = c1->clone_empty();
    auto c2 = down_cast<ConstColumn*>(cloned_col.get());
    ASSERT_EQ(0, c2->size());
    ASSERT_TRUE(c2->data_column()->use_count() == 1);
}

// NOLINTNEXTLINE
PARALLEL_TEST(ConstColumnTest, test_replicate) {
    auto create_const_column = [](int32_t value, size_t size) {
        Int32Column::Ptr c = Int32Column::create();
        c->append_numbers(&value, sizeof(value));
        return ConstColumn::create(c, size);
    };

    auto c1 = create_const_column(1, 3);

    ASSERT_EQ(3, c1->size());

    Offsets offsets;
    offsets.push_back(0);
    offsets.push_back(2);
    offsets.push_back(5);
    offsets.push_back(7);

    auto c2 = c1->replicate(offsets).value();

    ASSERT_EQ(7, c2->size());
    ASSERT_EQ(1, c2->get(6).get_int32());
}

PARALLEL_TEST(ConstColumnTest, test_reference_memory_usage) {
    {
        auto create_int_const_column = [](int32_t value, size_t size) {
            Int32Column::Ptr c = Int32Column::create();
            c->append_numbers(&value, sizeof(value));
            return ConstColumn::create(c, size);
        };

        auto column = create_int_const_column(1, 10);
        ASSERT_EQ(0, column->reference_memory_usage());
    }
    {
        auto create_json_const_column = [](const std::string& json_str, size_t size) {
            JsonColumn::Ptr c = JsonColumn::create();
            auto json_value = JsonValue::parse(json_str).value();
            c->append_datum(&json_value);
            return ConstColumn::create(c, size);
        };
        auto column = create_json_const_column("1", 10);
        ASSERT_EQ(2, column->reference_memory_usage());
    }
}

} // namespace starrocks

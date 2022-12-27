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

#include "column/nullable_column.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <utility>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "exec/sorting/sorting.h"
#include "testutil/parallel_test.h"

namespace starrocks {

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_nullable_column_upgrade_if_overflow) {
    auto c0 = NullableColumn::create(UInt32Column::create(), NullColumn::create());
    c0->append_datum((uint32_t)1);

    auto ret = c0->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_nullable_column_downgrade) {
    auto c0 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    c0->append_datum(Slice("1"));

    ASSERT_FALSE(c0->has_large_column());
    auto ret = c0->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);

    c0 = NullableColumn::create(LargeBinaryColumn::create(), NullColumn::create());
    c0->append_datum(Slice("1"));

    ASSERT_TRUE(c0->has_large_column());
    ret = c0->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_FALSE(c0->has_large_column());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_copy_constructor) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(*c0);
    c0->reset_column();

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_move_constructor) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(std::move(*c0));

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_copy_assignment) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(Int32Column::create(), NullColumn::create());
    c1 = *c0;
    c0->reset_column();

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_move_assignment) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(Int32Column::create(), NullColumn::create());
    c1 = *c0;

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_clone) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto c1 = c0->clone();
    ASSERT_TRUE(c1->is_nullable());
    ASSERT_EQ(0, c1->size());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get()) != nullptr);
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->data_column().unique());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->null_column().unique());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->data_column()->size());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->null_column()->size());

    c1->append_datum({}); // NULL
    c1->append_datum({(int32_t)1});
    c1->append_datum({(int32_t)2});
    c1->append_datum({(int32_t)3});

    auto c2 = c1->clone();
    c1->reset_column();

    ASSERT_TRUE(c2->is_nullable());
    ASSERT_EQ(4, c2->size());
    ASSERT_EQ(4, down_cast<NullableColumn*>(c2.get())->data_column()->size());
    ASSERT_EQ(4, down_cast<NullableColumn*>(c2.get())->null_column()->size());
    ASSERT_TRUE(c2->get(0).is_null());
    ASSERT_EQ(1, c2->get(1).get_int32());
    ASSERT_EQ(2, c2->get(2).get_int32());
    ASSERT_EQ(3, c2->get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_clone_shared) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto c1 = c0->clone_shared();
    ASSERT_TRUE(c1.unique());
    ASSERT_TRUE(c1->is_nullable());
    ASSERT_EQ(0, c1->size());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c1) != nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c1)->data_column().unique());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c1)->null_column().unique());
    ASSERT_EQ(0, std::dynamic_pointer_cast<NullableColumn>(c1)->data_column()->size());
    ASSERT_EQ(0, std::dynamic_pointer_cast<NullableColumn>(c1)->null_column()->size());

    c1->append_datum({}); // NULL
    c1->append_datum({(int32_t)1});
    c1->append_datum({(int32_t)2});
    c1->append_datum({(int32_t)3});

    auto c2 = c1->clone_shared();
    c1->reset_column();

    ASSERT_TRUE(c2.unique());
    ASSERT_TRUE(c2->is_nullable());
    ASSERT_EQ(4, c2->size());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c2) != nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c2)->data_column().unique());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c2)->null_column().unique());
    ASSERT_EQ(4, std::dynamic_pointer_cast<NullableColumn>(c2)->data_column()->size());
    ASSERT_EQ(4, std::dynamic_pointer_cast<NullableColumn>(c2)->null_column()->size());
    ASSERT_TRUE(c2->get(0).is_null());
    ASSERT_EQ(1, c2->get(1).get_int32());
    ASSERT_EQ(2, c2->get(2).get_int32());
    ASSERT_EQ(3, c2->get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_clone_empty) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto c1 = c0->clone_empty();
    ASSERT_TRUE(c1->is_nullable());
    ASSERT_EQ(0, c1->size());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get()) != nullptr);
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->data_column().unique());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->null_column().unique());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->data_column()->size());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->null_column()->size());

    c1->append_datum({}); // NULL
    c1->append_datum({(int32_t)1});
    c1->append_datum({(int32_t)2});
    c1->append_datum({(int32_t)3});

    auto c2 = c1->clone_empty();

    ASSERT_TRUE(c2->is_nullable());
    ASSERT_EQ(0, c2->size());
    ASSERT_TRUE(down_cast<NullableColumn*>(c2.get()) != nullptr);
    ASSERT_TRUE(down_cast<NullableColumn*>(c2.get())->data_column().unique());
    ASSERT_TRUE(down_cast<NullableColumn*>(c2.get())->null_column().unique());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c2.get())->data_column()->size());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c2.get())->null_column()->size());
}

PARALLEL_TEST(NullableColumnTest, test_update_rows) {
    auto column = NullableColumn::create(Int32Column::create(), NullColumn::create());
    column->append_datum((int32_t)1);
    column->append_datum((int32_t)2);
    column->append_datum({});
    column->append_datum((int32_t)4);
    column->append_datum({});

    auto replace_col1 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    replace_col1->append_datum({});
    replace_col1->append_datum((int32_t)5);

    std::vector<uint32_t> replace_idxes = {1, 4};
    ASSERT_TRUE(column->update_rows(*replace_col1.get(), replace_idxes.data()).ok());
    ASSERT_EQ(5, column->size());
    ASSERT_TRUE(column->data_column().unique());
    ASSERT_TRUE(column->null_column().unique());
    ASSERT_EQ(5, column->data_column()->size());
    ASSERT_EQ(5, column->null_column()->size());

    ASSERT_EQ(1, column->get(0).get_int32());
    ASSERT_TRUE(column->get(1).is_null());
    ASSERT_TRUE(column->get(2).is_null());
    ASSERT_EQ(4, column->get(3).get_int32());
    ASSERT_EQ(5, column->get(4).get_int32());

    auto column1 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    column1->append_datum("abc");
    column1->append_datum("def");
    column1->append_datum({});
    column1->append_datum("ghi");
    column1->append_datum({});

    auto replace_col2 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    replace_col2->append_datum({});
    replace_col2->append_datum("jk");

    ASSERT_TRUE(column1->update_rows(*replace_col2.get(), replace_idxes.data()).ok());
    ASSERT_EQ(5, column1->size());
    ASSERT_TRUE(column1->data_column().unique());
    ASSERT_TRUE(column1->null_column().unique());
    ASSERT_EQ(5, column1->data_column()->size());
    ASSERT_EQ(5, column1->null_column()->size());

    ASSERT_EQ("abc", column1->get(0).get_slice().to_string());
    ASSERT_TRUE(column1->get(1).is_null());
    ASSERT_TRUE(column1->get(2).is_null());
    ASSERT_EQ("ghi", column1->get(3).get_slice().to_string());
    ASSERT_EQ("jk", column1->get(4).get_slice().to_string());
}

PARALLEL_TEST(NullableColumnTest, test_xor_checksum) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    for (int i = 0; i <= 1000; i++) {
        c0->append_datum((int32_t)i);
    }

    int64_t checksum = c0->xor_checksum(0, 1001);
    int64_t expected_checksum = 1001;

    ASSERT_EQ(checksum, expected_checksum);
}

PARALLEL_TEST(NullableColumnTest, test_compare_row) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    c0->append_datum({});
    c0->append_datum(1);
    c0->append_datum(2);
    c0->append_datum({});
    c0->append_datum({});
    c0->append_datum(7);
    c0->append_datum({});
    c0->append_datum(8);
    c0->append_datum({});
    auto correct = [&](const Datum& rhs_value, int sort_order, int null_first) {
        CompareVector res;
        auto rhs_column = NullableColumn::create(Int32Column::create(), NullColumn::create());
        rhs_column->append_datum(rhs_value);

        for (size_t i = 0; i < c0->size(); i++) {
            if (c0->is_null(i) || rhs_value.is_null()) {
                res.push_back(c0->compare_at(i, 0, *rhs_column, null_first));
            } else {
                res.push_back(c0->compare_at(i, 0, *rhs_column, null_first) * sort_order);
            }
        }
        return res;
    };
    auto execute = [&](Datum rhs_value, int sort_order, int null_first) {
        CompareVector cmp_result(c0->size(), 0);
        compare_column(c0, cmp_result, std::move(rhs_value), SortDesc(sort_order, null_first));
        return cmp_result;
    };

    std::vector<Datum> rhs_values = {{0}, {1}, {3}, {4}, {7}, {10}, {}};
    for (const Datum& datum : rhs_values) {
        std::string datum_str = datum.is_null() ? "NULL" : std::to_string(datum.get_int32());
        for (int sort_order : std::vector<int>{1, -1}) {
            for (int null_first : std::vector<int>{1, -1}) {
                fmt::print("Column::compare_row rhs={} sort_order={} null_first={}\n", datum_str, sort_order,
                           null_first);
                EXPECT_EQ(correct(datum, sort_order, null_first), execute(datum, sort_order, null_first));
            }
        }
    }
}

PARALLEL_TEST(NullableColumnTest, test_replicate) {
    auto column = NullableColumn::create(Int32Column::create(), NullColumn::create());
    column->append_datum((int32_t)1);
    column->append_datum({});
    column->append_datum((int32_t)4);

    Offsets offsets;
    offsets.push_back(0);
    offsets.push_back(2);
    offsets.push_back(4);
    offsets.push_back(7);
    auto c2 = column->replicate(offsets);

    ASSERT_EQ(1, c2->get(0).get_int32());
    ASSERT_EQ(1, c2->get(1).get_int32());
    ASSERT_TRUE(c2->get(2).is_null());
    ASSERT_TRUE(c2->get(3).is_null());
    ASSERT_EQ(4, c2->get(4).get_int32());
    ASSERT_EQ(4, c2->get(5).get_int32());
    ASSERT_EQ(4, c2->get(6).get_int32());
}

} // namespace starrocks

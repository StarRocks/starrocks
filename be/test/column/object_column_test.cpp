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

#include "column/object_column.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/percentile_functions.h"
#include "runtime/types.h"
#include "types/hll.h"
#include "util/percentile_value.h"

namespace starrocks {

// NOLINTNEXTLINE
TEST(ObjectColumnTest, HLL_test_filter) {
    // keep all.
    {
        auto c = ColumnHelper::create_column(TypeDescriptor::create_hll_type(), false);
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Filter filter(100, 1);
        c->filter(filter);
        ASSERT_EQ(100, c->size());
    }
    // filter all.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);

        Filter filter(100, 0);
        c->filter(filter);
        ASSERT_EQ(0, c->size());
    }
    // filter out the last 10 elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Filter filter(100, 1);
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

        Filter filter(100, 1);
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

        Filter filter(100, 1);
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

        Filter filter(100, 1);
        c->filter_range(filter, 0, 100);
        ASSERT_EQ(100, c->size());
    }
    // filter all.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);

        Filter filter(100, 0);
        c->filter_range(filter, 0, 100);
        ASSERT_EQ(0, c->size());
    }
    // filter out the last 10 elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Filter filter(100, 0);
        c->filter_range(filter, 90, 100);
        ASSERT_EQ(90, c->size());
    }
    // filter out the first 10 elements.
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Filter filter(100, 0);
        c->filter_range(filter, 0, 10);
        ASSERT_EQ(90, c->size());
    }
    // filter 12 elements in the middle
    {
        auto c = HyperLogLogColumn::create();
        c->resize(100);
        ASSERT_EQ(100, c->size());

        Filter filter(100, 0);
        c->filter_range(filter, 20, 32);
        ASSERT_EQ(88, c->size());
    }
}

// NOLINTNEXTLINE
TEST(ObjectColumnTest, test_object_column_upgrade_if_overflow) {
    auto c = HyperLogLogColumn::create();
    c->append(HyperLogLog());

    auto ret = c->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
}

// NOLINTNEXTLINE
TEST(ObjectColumnTest, test_append_value_multiple_times) {
    auto src_col = BitmapColumn::create();
    auto deep_copy_col = BitmapColumn::create();
    auto shallow_copy_col = BitmapColumn::create();

    BitmapValue bitmap;
    for (size_t i = 0; i < 64; i++) {
        bitmap.add(i);
    }
    src_col->append(&bitmap);

    deep_copy_col->append_value_multiple_times(*src_col, 0, 4);
    shallow_copy_col->append_value_multiple_times(*src_col, 0, 4);
    src_col->get_object(0)->add(64);

    for (size_t i = 0; i < 4; i++) {
        ASSERT_EQ(deep_copy_col->get_object(0)->cardinality(), 64);
        ASSERT_EQ(shallow_copy_col->get_object(0)->cardinality(), 65);
    }
}

// NOLINTNEXTLINE
TEST(ObjectColumnTest, test_object_column_downgrade) {
    auto c = HyperLogLogColumn::create();
    c->append(HyperLogLog());

    auto ret = c->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_FALSE(c->has_large_column());
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

TEST(ObjectColumnTest, Percentile_test_swap_column) {
    Columns columns;
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto s = DoubleColumn::create();
    s->append(1);
    s->append(2);
    s->append(3);
    columns.push_back(s);

    auto column = PercentileFunctions::percentile_hash(ctx, columns).value();
    ASSERT_TRUE(column->is_object());

    auto percentile = ColumnHelper::cast_to<TYPE_PERCENTILE>(column);
    ASSERT_EQ(1, percentile->get_object(0)->quantile(1));
    ASSERT_EQ(2, percentile->get_object(1)->quantile(1));
    ASSERT_EQ(3, percentile->get_object(2)->quantile(1));

    auto s1 = DoubleColumn::create();
    s1->append(4);
    columns.clear();
    columns.push_back(s1);
    auto column1 = PercentileFunctions::percentile_hash(ctx, columns).value();
    ASSERT_TRUE(column1->is_object());

    std::vector<uint32_t> idx = {1};
    column->update_rows(*column1.get(), idx.data());

    percentile = ColumnHelper::cast_to<TYPE_PERCENTILE>(column);
    ASSERT_EQ(1, percentile->get_object(0)->quantile(1));
    ASSERT_EQ(4, percentile->get_object(1)->quantile(1));
    ASSERT_EQ(3, percentile->get_object(2)->quantile(1));

    delete ctx;
}

} // namespace starrocks

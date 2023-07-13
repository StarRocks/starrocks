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

#include "exprs/bitmap_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/vectorized_fwd.h"
#include "exprs/base64.h"
#include "exprs/function_context.h"
#include "types/bitmap_value.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class VecBitmapFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        ctx_ptr.reset(FunctionContext::create_test_context());
        ctx = ctx_ptr.get();
    }

private:
    std::unique_ptr<FunctionContext> ctx_ptr;
    FunctionContext* ctx;
};

TEST_F(VecBitmapFunctionsTest, bitmapEmptyTest) {
    {
        Columns c;
        auto column = BitmapFunctions::bitmap_empty(ctx, c).value();

        ASSERT_TRUE(column->is_constant());

        auto* bitmap = ColumnHelper::get_const_value<TYPE_OBJECT>(column);

        ASSERT_EQ(1, bitmap->getSizeInBytes());
    }
}

TEST_F(VecBitmapFunctionsTest, toBitmapTest) {
    {
        Columns columns;

        auto s = BinaryColumn::create();

        s->append(Slice("12312313"));
        s->append(Slice("1"));
        s->append(Slice("0"));

        columns.push_back(s);

        auto column = BitmapFunctions::to_bitmap<TYPE_VARCHAR>(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ(5, p->get_object(0)->serialize_size());
        ASSERT_EQ(5, p->get_object(1)->serialize_size());
        ASSERT_EQ(5, p->get_object(2)->serialize_size());
    }

    {
        Columns columns;

        auto s = BinaryColumn::create();

        s->append(Slice("-1"));
        s->append(Slice("1"));
        s->append(Slice("0"));

        columns.push_back(s);

        auto v = BitmapFunctions::to_bitmap<TYPE_VARCHAR>(ctx, columns).value();

        ASSERT_TRUE(v->is_nullable());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(ColumnHelper::as_column<NullableColumn>(v)->data_column());

        ASSERT_TRUE(v->is_null(0));
        ASSERT_EQ(5, p->get_object(1)->serialize_size());
        ASSERT_EQ(5, p->get_object(2)->serialize_size());
    }
}

TEST_F(VecBitmapFunctionsTest, toBitmapTest_Int) {
    // to_bitmap(int32)
    {
        Columns columns;

        auto s = Int32Column::create();

        s->append(-1);
        s->append(1);
        s->append(0);

        columns.push_back(s);

        auto v = BitmapFunctions::to_bitmap<TYPE_INT>(ctx, columns).value();

        ASSERT_TRUE(v->is_nullable());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(ColumnHelper::as_column<NullableColumn>(v)->data_column());

        ASSERT_TRUE(v->is_null(0));
        ASSERT_EQ(5, p->get_object(1)->serialize_size());
        ASSERT_EQ(5, p->get_object(2)->serialize_size());
    }

    // to_bitmap(int64)
    {
        Columns columns;

        auto s = Int64Column::create();

        s->append(12312313);
        s->append(1);
        s->append(0);

        columns.push_back(s);

        auto column = BitmapFunctions::to_bitmap<TYPE_BIGINT>(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ(5, p->get_object(0)->serialize_size());
        ASSERT_EQ(5, p->get_object(1)->serialize_size());
        ASSERT_EQ(5, p->get_object(2)->serialize_size());
    }

    // to_bitmap(largeint)
    {
        Columns columns;

        auto s = Int128Column::create();

        int128_t inputs[] = {-1, 1, 0, int128_t(std::numeric_limits<uint64_t>::max()),
                             int128_t(std::numeric_limits<uint64_t>::max()) + 1};
        for (int128_t input : inputs) {
            s->append(input);
        }
        columns.push_back(s);

        auto v = BitmapFunctions::to_bitmap<TYPE_LARGEINT>(ctx, columns).value();

        ASSERT_TRUE(v->is_nullable());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(ColumnHelper::as_column<NullableColumn>(v)->data_column());

        ASSERT_TRUE(v->is_null(0));
        ASSERT_FALSE(v->is_null(1));
        ASSERT_FALSE(v->is_null(2));
        ASSERT_FALSE(v->is_null(3));
        ASSERT_TRUE(v->is_null(4));

        ASSERT_EQ(5, p->get_object(1)->serialize_size());
        ASSERT_EQ(5, p->get_object(2)->serialize_size());
        ASSERT_EQ(9, p->get_object(3)->serialize_size());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapHashTest) {
    {
        Columns columns;

        auto s = BinaryColumn::create();

        s->append(Slice("12312313"));
        s->append(Slice("1"));
        s->append(Slice("0"));

        columns.push_back(s);

        auto column = BitmapFunctions::bitmap_hash(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ(5, p->get_object(0)->serialize_size());
        ASSERT_EQ(5, p->get_object(1)->serialize_size());
        ASSERT_EQ(5, p->get_object(2)->serialize_size());
    }

    {
        Columns columns;

        auto s = BinaryColumn::create();
        auto n = NullColumn::create();

        s->append(Slice("-1"));
        s->append(Slice("1"));
        s->append(Slice("0"));

        n->append(0);
        n->append(0);
        n->append(1);

        columns.push_back(NullableColumn::create(s, n));

        auto v = BitmapFunctions::bitmap_hash(ctx, columns).value();

        ASSERT_FALSE(v->is_nullable());
        ASSERT_TRUE(v->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(v);

        ASSERT_EQ(5, p->get_object(0)->serialize_size());
        ASSERT_EQ(5, p->get_object(1)->serialize_size());
        ASSERT_EQ(1, p->get_object(2)->serialize_size());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapCountTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(2);
    b2.add(3);
    b2.add(4);
    b2.add(2);

    b3.add(0);
    b3.add(0);
    b3.add(0);
    b3.add(0);

    b4.add(4123102120);
    b4.add(23074);
    b4.add(4123123);
    b4.add(23074);

    {
        Columns columns;

        auto s = BitmapColumn::create();

        s->append(&b1);
        s->append(&b2);
        s->append(&b3);
        s->append(&b4);

        columns.push_back(s);

        auto column = BitmapFunctions::bitmap_count(ctx, columns).value();

        ASSERT_TRUE(column->is_numeric());

        auto p = ColumnHelper::cast_to<TYPE_BIGINT>(column);

        ASSERT_EQ(4, p->get_data()[0]);
        ASSERT_EQ(3, p->get_data()[1]);
        ASSERT_EQ(1, p->get_data()[2]);
        ASSERT_EQ(3, p->get_data()[3]);
    }

    {
        Columns columns;
        auto s = BitmapColumn::create();

        s->append(&b1);
        s->append(&b2);
        s->append(&b3);
        s->append(&b4);

        auto n = NullColumn::create();

        n->append(0);
        n->append(0);
        n->append(1);
        n->append(1);

        columns.push_back(NullableColumn::create(s, n));

        auto v = BitmapFunctions::bitmap_count(ctx, columns).value();

        ASSERT_FALSE(v->is_nullable());
        ASSERT_TRUE(v->is_numeric());

        auto p = ColumnHelper::cast_to<TYPE_BIGINT>(v);

        ASSERT_EQ(4, p->get_data()[0]);
        ASSERT_EQ(3, p->get_data()[1]);
        ASSERT_EQ(0, p->get_data()[2]);
        ASSERT_EQ(0, p->get_data()[3]);
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapOrTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(1);
    b2.add(2);
    b2.add(3);
    b2.add(4);

    b3.add(1);
    b3.add(2);
    b3.add(3);
    b3.add(4);

    b4.add(4);
    b4.add(5);
    b4.add(6);
    b4.add(7);

    {
        Columns columns;

        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);
        s2->append(&b3);
        s2->append(&b4);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_or(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ(4, p->get_object(0)->cardinality());
        ASSERT_EQ(7, p->get_object(1)->cardinality());
    }

    {
        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);
        s2->append(&b3);
        s2->append(&b4);

        auto n = NullColumn::create();

        n->append(0);
        n->append(1);

        columns.push_back(NullableColumn::create(s1, n));
        columns.push_back(s2);

        auto v = BitmapFunctions::bitmap_or(ctx, columns).value();

        ASSERT_TRUE(v->is_nullable());
        ASSERT_FALSE(v->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(ColumnHelper::as_column<NullableColumn>(v)->data_column());

        ASSERT_EQ(4, p->get_object(0)->cardinality());
        ASSERT_TRUE(v->is_null(1));
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapAndTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(1);
    b2.add(2);
    b2.add(3);
    b2.add(4);

    b3.add(1);
    b3.add(2);
    b3.add(3);
    b3.add(4);

    b4.add(4);
    b4.add(5);
    b4.add(6);
    b4.add(7);

    {
        Columns columns;

        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);
        s2->append(&b3);
        s2->append(&b4);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_and(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ(4, p->get_object(0)->cardinality());
        ASSERT_EQ(1, p->get_object(1)->cardinality());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapToStringTest) {
    BitmapValue b1;
    BitmapValue b2;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(4);
    b2.add(5);
    b2.add(6);
    b2.add(7);

    {
        Columns columns;

        auto s1 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);

        columns.push_back(s1);

        auto column = BitmapFunctions::bitmap_to_string(ctx, columns).value();

        ASSERT_TRUE(column->is_binary());

        auto p = ColumnHelper::cast_to<TYPE_VARCHAR>(column);

        ASSERT_EQ("1,2,3,4", p->get_slice(0).to_string());
        ASSERT_EQ("4,5,6,7", p->get_slice(1).to_string());
    }

    BitmapValue b3;
    BitmapValue b4;

    // enable bitmap with SET.
    config::enable_bitmap_union_disk_format_with_set = true;
    b3.add(1);
    b3.add(2);
    b3.add(3);
    b3.add(4);

    b4.add(4);
    b4.add(5);
    b4.add(6);
    b4.add(7);

    {
        Columns columns;

        auto s1 = BitmapColumn::create();

        s1->append(&b3);
        s1->append(&b4);

        columns.push_back(s1);

        auto column = BitmapFunctions::bitmap_to_string(ctx, columns).value();

        ASSERT_TRUE(column->is_binary());

        auto p = ColumnHelper::cast_to<TYPE_VARCHAR>(column);

        ASSERT_EQ("1,2,3,4", p->get_slice(0).to_string());
        ASSERT_EQ("4,5,6,7", p->get_slice(1).to_string());
    }
    config::enable_bitmap_union_disk_format_with_set = false;
}

TEST_F(VecBitmapFunctionsTest, bitmapFromStringTest) {
    {
        Columns columns;
        auto s1 = BinaryColumn::create();

        s1->append(Slice("1,2,3,4"));
        s1->append(Slice("4,5,6,7"));

        columns.push_back(s1);

        auto column = BitmapFunctions::bitmap_from_string(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,4", p->get_object(0)->to_string());
        ASSERT_EQ("4,5,6,7", p->get_object(1)->to_string());
    }

    {
        Columns columns;
        auto s1 = BinaryColumn::create();

        s1->append(Slice("1,2,3,4"));
        s1->append(Slice("asdf,7"));

        columns.push_back(s1);

        auto v = BitmapFunctions::bitmap_from_string(ctx, columns).value();
        ASSERT_TRUE(v->is_nullable());
        ASSERT_FALSE(v->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(ColumnHelper::as_column<NullableColumn>(v)->data_column());

        ASSERT_EQ("1,2,3,4", p->get_object(0)->to_string());
        ASSERT_TRUE(v->is_null(1));
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapContainsTest) {
    BitmapValue b1;
    BitmapValue b2;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(4);
    b2.add(5);
    b2.add(6);
    b2.add(7);
    {
        Columns columns;
        auto s1 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);

        auto b1 = Int64Column::create();

        b1->append(4);
        b1->append(1);

        columns.push_back(s1);
        columns.push_back(b1);

        auto column = BitmapFunctions::bitmap_contains(ctx, columns).value();

        ASSERT_TRUE(column->is_numeric());

        auto p = ColumnHelper::cast_to<TYPE_BOOLEAN>(column);

        ASSERT_EQ(1, p->get_data()[0]);
        ASSERT_EQ(0, p->get_data()[1]);
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapHasAnyTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(4);
    b2.add(5);
    b2.add(6);
    b2.add(7);

    b3.add(1);
    b3.add(2);
    b3.add(3);
    b3.add(4);

    b4.add(14);
    b4.add(15);
    b4.add(16);
    b4.add(17);
    {
        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);

        s2->append(&b3);
        s2->append(&b4);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_has_any(ctx, columns).value();

        ASSERT_TRUE(column->is_numeric());

        auto p = ColumnHelper::cast_to<TYPE_BOOLEAN>(column);

        ASSERT_EQ(1, p->get_data()[0]);
        ASSERT_EQ(0, p->get_data()[1]);
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapNotTest) {
    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,4", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,4", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,4", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_andnot(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("", p->get_object(0)->to_string());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapXorTest) {
    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,15,22", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,4,12,40,634", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,4,6", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);
        b1_column0.add(4);

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,4", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,4,15,22", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,12,40,634", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3,6", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);
        b1_column0.add(2);
        b1_column0.add(3);

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,2,3", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,3,4,15,22", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,12,40,634", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,6", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;
        b1_column0.add(1);

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;
        b1_column1.add(15);
        b1_column1.add(22);
        b1_column1.add(3);
        b1_column1.add(4);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("3,4,15,22", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;
        b1_column1.add(12);
        b1_column1.add(40);
        b1_column1.add(634);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("12,40,634", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;
        b1_column1.add(6);

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("6", p->get_object(0)->to_string());
    }

    {
        BitmapValue b1_column0;

        BitmapValue b1_column1;

        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = BitmapColumn::create();

        s1->append(&b1_column0);
        s2->append(&b1_column1);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_xor(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("", p->get_object(0)->to_string());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapRemoveTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(1);
    b2.add(4);
    b2.add(634);

    b3.add(634);

    {
        Columns columns;
        auto s1 = BitmapColumn::create();
        auto s2 = Int64Column::create();

        s1->append(&b1);
        s1->append(&b2);
        s1->append(&b3);
        s1->append(&b4);

        s2->append(2);
        s2->append(4);
        s2->append(634);
        s2->append(632);

        columns.push_back(s1);
        columns.push_back(s2);

        auto column = BitmapFunctions::bitmap_remove(ctx, columns).value();

        ASSERT_TRUE(column->is_object());

        auto p = ColumnHelper::cast_to<TYPE_OBJECT>(column);

        ASSERT_EQ("1,3,4", p->get_object(0)->to_string());
        ASSERT_EQ("1,634", p->get_object(1)->to_string());
        ASSERT_EQ("", p->get_object(2)->to_string());
        ASSERT_EQ("", p->get_object(3)->to_string());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapToArrayTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(1);
    b2.add(4);
    b2.add(634);

    b3.add(634);

    {
        Columns columns;
        auto s1 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);
        s1->append(&b3);
        s1->append(&b4);

        columns.push_back(s1);

        auto column = BitmapFunctions::bitmap_to_array(ctx, columns).value();
        auto array_column = ColumnHelper::as_column<ArrayColumn>(column);

        auto a1 = array_column->get(0).get_array();
        ASSERT_EQ(a1.size(), 4);
        ASSERT_EQ(a1[0].get_int64(), 1);
        ASSERT_EQ(a1[1].get_int64(), 2);
        ASSERT_EQ(a1[2].get_int64(), 3);
        ASSERT_EQ(a1[3].get_int64(), 4);

        auto a2 = array_column->get(1).get_array();
        ASSERT_EQ(a2.size(), 3);
        ASSERT_EQ(a2[0].get_int64(), 1);
        ASSERT_EQ(a2[1].get_int64(), 4);
        ASSERT_EQ(a2[2].get_int64(), 634);

        auto a3 = array_column->get(2).get_array();
        ASSERT_EQ(a3.size(), 1);
        ASSERT_EQ(a3[0].get_int64(), 634);

        auto a4 = array_column->get(3).get_array();
        ASSERT_EQ(a4.size(), 0);
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapToArrayNullTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    b2.add(1);
    b2.add(4);
    b2.add(634);

    b3.add(634);

    {
        Columns columns;
        auto s1 = BitmapColumn::create();

        s1->append(&b1);
        s1->append(&b2);
        s1->append(&b3);
        s1->append(&b4);

        auto n = NullColumn::create();

        n->append(0);
        n->append(0);
        n->append(1);
        n->append(1);

        columns.push_back(NullableColumn::create(s1, n));

        auto column = BitmapFunctions::bitmap_to_array(ctx, columns).value();
        auto null_column = ColumnHelper::as_column<NullableColumn>(column);
        auto array_column = ColumnHelper::as_column<ArrayColumn>(null_column->data_column());

        auto a1 = array_column->get(0).get_array();
        ASSERT_EQ(a1.size(), 4);
        ASSERT_EQ(a1[0].get_int64(), 1);
        ASSERT_EQ(a1[1].get_int64(), 2);
        ASSERT_EQ(a1[2].get_int64(), 3);
        ASSERT_EQ(a1[3].get_int64(), 4);

        auto a2 = array_column->get(1).get_array();
        ASSERT_EQ(a2.size(), 3);
        ASSERT_EQ(a2[0].get_int64(), 1);
        ASSERT_EQ(a2[1].get_int64(), 4);
        ASSERT_EQ(a2[2].get_int64(), 634);

        ASSERT_TRUE(null_column->is_null(2));
        ASSERT_TRUE(null_column->is_null(3));
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapToArrayConstTest) {
    BitmapValue b1;

    b1.add(1);
    b1.add(2);
    b1.add(3);
    b1.add(4);

    {
        Columns columns;
        auto s1 = BitmapColumn::create();

        s1->append(&b1);

        columns.push_back(ConstColumn::create(s1, 4));

        auto column = BitmapFunctions::bitmap_to_array(ctx, columns).value();
        auto array_column = ColumnHelper::as_column<ArrayColumn>(column);

        for (size_t i = 0; i < 4; ++i) {
            auto a1 = array_column->get(i).get_array();
            ASSERT_EQ(a1.size(), 4);
            ASSERT_EQ(a1[0].get_int64(), 1);
            ASSERT_EQ(a1[1].get_int64(), 2);
            ASSERT_EQ(a1[2].get_int64(), 3);
            ASSERT_EQ(a1[3].get_int64(), 4);
        }
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapToArrayOnlyNullTest) {
    {
        Columns columns;
        size_t size = 8;
        auto s1 = ColumnHelper::create_const_null_column(size);
        columns.push_back(s1);

        auto column = BitmapFunctions::bitmap_to_array(ctx, columns).value();
        // auto null_column = ColumnHelper::as_column<NullableColumn>(column);

        for (size_t i = 0; i < size; ++i) {
            ASSERT_TRUE(column->is_null(i));
        }
    }
}

//BitmapValue test
TEST_F(VecBitmapFunctionsTest, bitmapValueUnionOperator) {
    BitmapValue b1;
    int promote_to_bitmap = 33;
    for (int i = 0; i < promote_to_bitmap; ++i) {
        b1.add(i);
    }

    BitmapValue b2;
    b2.add(99);

    {
        BitmapValue a1;
        a1 |= b1;

        BitmapValue a2;
        a2 |= b2;

        BitmapValue a3;
        a3 |= b1;
        a3 |= b2;

        ASSERT_TRUE(a1.cardinality() == promote_to_bitmap);
        ASSERT_TRUE(a2.cardinality() == 1);
        ASSERT_TRUE(a3.cardinality() == (promote_to_bitmap + 1));
    }
}

//BitmapValue test
TEST_F(VecBitmapFunctionsTest, bitmapValueXorOperator) {
    int promote_to_bitmap = 33;

    BitmapValue b1;
    for (int i = 0; i < promote_to_bitmap; ++i) {
        b1.add(i);
    }

    BitmapValue b2;
    for (int i = 0; i < promote_to_bitmap; ++i) {
        b2.add(i);
    }

    {
        b1 ^= b2;
        ASSERT_TRUE(b2.cardinality() == promote_to_bitmap);
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapMaxTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(0);
    b1.add(0);
    b1.add(0);
    b1.add(0);

    b3.add(1);
    b3.add(2);
    b3.add(3);
    b3.add(4);

    b4.add(4123102120);
    b4.add(23074);
    b4.add(4123123);
    b4.add(23074);

    {
        Columns columns;

        auto s = BitmapColumn::create();

        s->append(&b1);
        s->append(&b2);
        s->append(&b3);
        s->append(&b4);

        columns.push_back(s);

        auto column = BitmapFunctions::bitmap_max(ctx, columns).value();

        ASSERT_FALSE(column->is_numeric());

        ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

        auto max_value0 = viewer.value(0)->max();
        ASSERT_TRUE(max_value0.has_value());
        ASSERT_EQ(0, max_value0.value());

        ASSERT_TRUE(column->is_null(1));

        auto max_value2 = viewer.value(2)->max();
        ASSERT_TRUE(max_value2.has_value());
        ASSERT_EQ(4, max_value2.value());

        auto max_value3 = viewer.value(3)->max();
        ASSERT_TRUE(max_value3.has_value());
        ASSERT_EQ(4123102120, max_value3.value());
    }

    {
        Columns columns;
        auto s = BitmapColumn::create();

        s->append(&b1);
        s->append(&b2);
        s->append(&b3);
        s->append(&b4);

        auto n = NullColumn::create();

        n->append(0);
        n->append(1);
        n->append(1);
        n->append(0);

        columns.push_back(NullableColumn::create(s, n));

        auto v = BitmapFunctions::bitmap_max(ctx, columns).value();

        ASSERT_TRUE(v->is_nullable());

        ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

        auto max_value0 = viewer.value(0)->max();
        ASSERT_TRUE(max_value0.has_value());
        ASSERT_EQ(0, max_value0.value());

        ASSERT_TRUE(v->is_null(1));
        ASSERT_TRUE(v->is_null(2));

        auto max_value3 = viewer.value(3)->max();
        ASSERT_TRUE(max_value3.has_value());
        ASSERT_EQ(4123102120, max_value3.value());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmapMinTest) {
    BitmapValue b1;
    BitmapValue b2;
    BitmapValue b3;
    BitmapValue b4;

    b1.add(0);
    b1.add(0);
    b1.add(0);
    b1.add(0);

    b3.add(1);
    b3.add(2);
    b3.add(3);
    b3.add(4);

    b4.add(4123102120);
    b4.add(23074);
    b4.add(4123123);
    b4.add(23074);

    {
        Columns columns;

        auto s = BitmapColumn::create();

        s->append(&b1);
        s->append(&b2);
        s->append(&b3);
        s->append(&b4);

        columns.push_back(s);

        auto column = BitmapFunctions::bitmap_min(ctx, columns).value();

        ASSERT_FALSE(column->is_numeric());

        ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

        auto min_value0 = viewer.value(0)->min();
        ASSERT_TRUE(min_value0.has_value());
        ASSERT_EQ(0, min_value0.value());

        ASSERT_TRUE(column->is_null(1));

        auto min_value2 = viewer.value(2)->min();
        ASSERT_TRUE(min_value2.has_value());
        ASSERT_EQ(1, min_value2.value());

        auto min_value3 = viewer.value(3)->min();
        ASSERT_TRUE(min_value3.has_value());
        ASSERT_EQ(23074, min_value3.value());
    }

    {
        Columns columns;
        auto s = BitmapColumn::create();

        s->append(&b1);
        s->append(&b2);
        s->append(&b3);
        s->append(&b4);

        auto n = NullColumn::create();

        n->append(0);
        n->append(1);
        n->append(1);
        n->append(0);

        columns.push_back(NullableColumn::create(s, n));

        auto v = BitmapFunctions::bitmap_min(ctx, columns).value();

        ASSERT_TRUE(v->is_nullable());

        auto p = ColumnHelper::cast_to<TYPE_LARGEINT>(ColumnHelper::as_column<NullableColumn>(v)->data_column());

        ASSERT_EQ(NULL, p->get_data()[0]);
        ASSERT_TRUE(v->is_null(1));
        ASSERT_TRUE(v->is_null(2));
        ASSERT_EQ(23074, p->get_data()[3]);
    }
}

TEST_F(VecBitmapFunctionsTest, base64ToBitmapTest) {
    // init bitmap
    BitmapValue bitmap_src({1, 100, 256});

    // init and malloc space
    int size = 1024;
    int len = (size_t)(4.0 * ceil((double)size / 3.0)) + 1;
    char p[len];
    uint8_t* src;
    src = (uint8_t*)malloc(sizeof(uint8_t) * size);

    // serialize and encode bitmap, return char*
    bitmap_src.serialize(src);
    base64_encode2((unsigned char*)src, size, (unsigned char*)p);

    std::unique_ptr<char[]> p1;
    p1.reset(new char[len + 3]);

    // decode and deserialize
    base64_decode2(p, len, p1.get());
    BitmapValue bitmap_decode;
    bitmap_decode.deserialize(p1.get());

    // judge encode and decode bitmap data
    ASSERT_EQ(bitmap_src.to_string(), bitmap_decode.to_string());
    free(src);
}

TEST_F(VecBitmapFunctionsTest, array_to_bitmap_test) {
    auto builder = [](const Buffer<int64_t>& val) {
        auto ele_column = NullableColumn::create(Int64Column::create(), NullColumn::create());
        for (auto& v : val) {
            ele_column->append_datum(v);
        }
        auto offset_column = UInt32Column::create();
        offset_column->append(0);
        offset_column->append(val.size());
        return ArrayColumn::create(ele_column, offset_column);
    };

    auto nullable_builder = [](const Buffer<int64_t>& val, const Buffer<int32_t>& null_idx) {
        auto ele_column = Int64Column::create();
        ele_column->append(val);

        auto nullable_column = NullableColumn::create(ele_column, NullColumn::create(val.size()));
        for (auto idx : null_idx) {
            nullable_column->set_null(idx);
        }
        auto offset_column = UInt32Column::create();
        offset_column->append(0);
        offset_column->append(val.size());
        return ArrayColumn::create(nullable_column, offset_column);
    };

    Columns columns = {builder(Buffer<int64_t>{1, 2, 3, 4})};
    auto res = BitmapFunctions::array_to_bitmap(nullptr, columns).value();
    ASSERT_EQ(res->debug_item(0), "1,2,3,4");
    columns = {ColumnHelper::create_const_null_column(1)};
    res = BitmapFunctions::array_to_bitmap(nullptr, columns).value();
    ASSERT_EQ(res->debug_item(0), "CONST: NULL");
    columns = {nullable_builder(Buffer<int64_t>{1, 2, 3, 4}, {0})};
    res = BitmapFunctions::array_to_bitmap(nullptr, columns).value();
    ASSERT_EQ(res->debug_item(0), "2,3,4");
    columns = {nullable_builder(Buffer<int64_t>{1, 2, 3, 4}, {0, 1, 2, 3})};
    res = BitmapFunctions::array_to_bitmap(nullptr, columns).value();
    ASSERT_EQ(res->debug_item(0), "");
}
TEST_F(VecBitmapFunctionsTest, bitmapToBase64Test) {
    { // Empty Bitmap
        Columns columns;
        auto s = BitmapColumn::create();
        BitmapValue empty;
        empty.clear();
        s->append(&empty);
        columns.push_back(s);

        auto sliceCol = BitmapFunctions::bitmap_to_base64(ctx, columns);

        ColumnViewer<TYPE_VARCHAR> viewer(sliceCol.value());
        Columns columns2;
        columns2.push_back(sliceCol.value());

        auto bitmapCol = BitmapFunctions::base64_to_bitmap(ctx, columns2);

        ColumnViewer<TYPE_OBJECT> viewer2(bitmapCol.value());
        auto bmp = viewer2.value(0);
        ASSERT_EQ(0, bmp->cardinality());
    }

    { // Single Bitmap
        Columns columns;
        auto s = BitmapColumn::create();
        BitmapValue single({1});
        s->append(&single);

        columns.push_back(s);

        auto sliceCol = BitmapFunctions::bitmap_to_base64(ctx, columns);

        ColumnViewer<TYPE_VARCHAR> viewer(sliceCol.value());

        Columns columns2;
        columns2.push_back(sliceCol.value());

        auto bitmapCol = BitmapFunctions::base64_to_bitmap(ctx, columns2);

        ColumnViewer<TYPE_OBJECT> viewer2(bitmapCol.value());
        auto bmp = viewer2.value(0);
        ASSERT_EQ(1, bmp->cardinality());
        ASSERT_TRUE(bmp->contains(1));
    }

    { // Set Bitmap
        Columns columns;
        auto s = BitmapColumn::create();
        // Adding values one by one, no more than 32, which makes the bitmap stores value with set
        BitmapValue set;
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        s->append(&set);

        columns.push_back(s);

        auto sliceCol = BitmapFunctions::bitmap_to_base64(ctx, columns);

        ColumnViewer<TYPE_VARCHAR> viewer(sliceCol.value());

        Columns columns2;
        columns2.push_back(sliceCol.value());
        auto bitmapCol = BitmapFunctions::base64_to_bitmap(ctx, columns2);

        ColumnViewer<TYPE_OBJECT> viewer2(bitmapCol.value());
        auto bmp = viewer2.value(0);
        ASSERT_EQ(4, bmp->cardinality());
        ASSERT_TRUE(bmp->contains(1));
        ASSERT_TRUE(bmp->contains(2));
        ASSERT_TRUE(bmp->contains(3));
        ASSERT_TRUE(bmp->contains(4));
    }

    { // 32bit Bitmap
        Columns columns;
        auto s = BitmapColumn::create();
        // Constructing bitmap with vector which contains 32bit values makes it a 32bit bitmap
        BitmapValue bmp32bit({1, 2, 3, 4});
        s->append(&bmp32bit);

        columns.push_back(s);

        auto sliceCol = BitmapFunctions::bitmap_to_base64(ctx, columns);

        ColumnViewer<TYPE_VARCHAR> viewer(sliceCol.value());

        Columns columns2;
        columns2.push_back(sliceCol.value());
        auto bitmapCol = BitmapFunctions::base64_to_bitmap(ctx, columns2);

        ColumnViewer<TYPE_OBJECT> viewer2(bitmapCol.value());
        auto bmp = viewer2.value(0);
        ASSERT_EQ(4, bmp->cardinality());
        ASSERT_TRUE(bmp->contains(1));
        ASSERT_TRUE(bmp->contains(2));
        ASSERT_TRUE(bmp->contains(3));
        ASSERT_TRUE(bmp->contains(4));
    }

    { // 64bit Bitmap
        Columns columns;
        auto s = BitmapColumn::create();
        // Constructing bitmap with vector which contains 64bit values makes it a 64bit bitmap
        BitmapValue bmp64bit({600123456781, 600123456782, 600123456783, 600123456784});
        s->append(&bmp64bit);

        columns.push_back(s);

        auto sliceCol = BitmapFunctions::bitmap_to_base64(ctx, columns);

        ColumnViewer<TYPE_VARCHAR> viewer(sliceCol.value());

        Columns columns2;
        columns2.push_back(sliceCol.value());
        auto bitmapCol = BitmapFunctions::base64_to_bitmap(ctx, columns2);

        ColumnViewer<TYPE_OBJECT> viewer2(bitmapCol.value());
        auto bmp = viewer2.value(0);
        ASSERT_EQ(4, bmp->cardinality());
        ASSERT_TRUE(bmp->contains(600123456781));
        ASSERT_TRUE(bmp->contains(600123456782));
        ASSERT_TRUE(bmp->contains(600123456783));
        ASSERT_TRUE(bmp->contains(600123456784));
    }
}

TEST_F(VecBitmapFunctionsTest, sub_bitmap) {
    BitmapValue bitmap({1, 2, 3, 4, 5, 64, 128, 256, 512, 1024});
    auto bitmap_column = BitmapColumn::create();
    bitmap_column->append(&bitmap);

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("3,4,5", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(5);
        len->append(100);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(5);
        len->append(INT64_MAX);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(0);
        len->append(2);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1,2", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(-1);
        len->append(1);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(-1);
        len->append(100);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(-6);
        len->append(100);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("5,64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(-6);
        len->append(5);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("5,64,128,256,512", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(0);
        len->append(0);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(100);
        len->append(5);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(-100);
        len->append(5);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(5);
        len->append(INT64_MIN);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        BitmapValue bitmap1;
        auto bitmap_column1 = BitmapColumn::create();
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        bitmap_column1->append(&bitmap1);
        offset->append(5);
        len->append(5);

        columns.emplace_back(bitmap_column1);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }
}

TEST_F(VecBitmapFunctionsTest, sub_bitmap_special_cases) {
    // empty bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        BitmapValue empty;
        empty.clear();
        bitmap_column->append(&empty);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }
    // single bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        BitmapValue single({1});
        bitmap_column->append(&single);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(0);
        len->append(1);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1", res->get_object(0)->to_string());
    }
    // set bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Adding values one by one, no more than 32, which makes the bitmap stores value with set
        BitmapValue set;
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        bitmap_column->append(&set);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("3,4", res->get_object(0)->to_string());
    }
    // 32bit Bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Constructing bitmap with vector which contains 32bit values makes it a 32bit bitmap
        BitmapValue bmp32bit({1, 2, 3, 4});
        bitmap_column->append(&bmp32bit);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("3,4", res->get_object(0)->to_string());
    }
    // 64bit bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Constructing bitmap with vector which contains 64bit values makes it a 64bit bitmap
        BitmapValue bmp64bit({600123456781, 600123456782, 600123456783, 600123456784});
        bitmap_column->append(&bmp64bit);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::sub_bitmap(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("600123456783,600123456784", res->get_object(0)->to_string());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmap_subset_limit) {
    BitmapValue bitmap({1, 2, 3, 4, 5, 64, 128, 256, 512, 1024});
    auto bitmap_column = BitmapColumn::create();
    bitmap_column->append(&bitmap);

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(2);
        limit->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("2,3,4", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(5);
        limit->append(100);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("5,64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(5);
        limit->append(INT64_MAX);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("5,64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(0);
        limit->append(2);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1,2", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(-1);
        limit->append(1);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(5);
        limit->append(-2);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("4,5", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(1025);
        limit->append(-100);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1,2,3,4,5,64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(1025);
        limit->append(-2);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(0);
        limit->append(0);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(1025);
        limit->append(5);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(0);
        limit->append(-5);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        start->append(1025);
        limit->append(INT64_MAX);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        BitmapValue bitmap1;
        auto bitmap_column1 = BitmapColumn::create();
        auto start = Int64Column::create();
        auto limit = Int64Column::create();
        bitmap_column1->append(&bitmap1);
        start->append(5);
        limit->append(5);

        columns.emplace_back(bitmap_column1);
        columns.emplace_back(start);
        columns.emplace_back(limit);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }
}

TEST_F(VecBitmapFunctionsTest, sub_bitmap_limit_special_cases) {
    // empty bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        BitmapValue empty;
        empty.clear();
        bitmap_column->append(&empty);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }
    // single bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        BitmapValue single({1});
        bitmap_column->append(&single);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(0);
        len->append(1);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1", res->get_object(0)->to_string());
    }
    // set bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Adding values one by one, no more than 32, which makes the bitmap stores value with set
        BitmapValue set;
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        bitmap_column->append(&set);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("2,3,4", res->get_object(0)->to_string());
    }
    // 32bit Bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Constructing bitmap with vector which contains 32bit values makes it a 32bit bitmap
        BitmapValue bmp32bit({1, 2, 3, 4});
        bitmap_column->append(&bmp32bit);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("2,3,4", res->get_object(0)->to_string());
    }
    // 64bit bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Constructing bitmap with vector which contains 64bit values makes it a 64bit bitmap
        BitmapValue bmp64bit({600123456781, 600123456782, 600123456783, 600123456784});
        bitmap_column->append(&bmp64bit);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_limit(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("600123456781,600123456782,600123456783", res->get_object(0)->to_string());
    }
}

TEST_F(VecBitmapFunctionsTest, bitmap_subset_in_range) {
    BitmapValue bitmap({1, 2, 3, 4, 5, 64, 128, 256, 512, 1024});
    auto bitmap_column = BitmapColumn::create();
    bitmap_column->append(&bitmap);

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(2);
        end->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("2", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(5);
        end->append(100);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("5,64", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(5);
        end->append(INT64_MAX);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("5,64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(0);
        end->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1,2", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(1);
        end->append(2);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(4);
        end->append(6);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("4,5", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(0);
        end->append(1025);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1,2,3,4,5,64,128,256,512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(510);
        end->append(1025);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("512,1024", res->get_object(0)->to_string());
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(0);
        end->append(0);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(1025);
        end->append(5);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(0);
        end->append(-5);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        start->append(1025);
        end->append(INT64_MAX);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }

    {
        Columns columns;
        BitmapValue bitmap1;
        auto bitmap_column1 = BitmapColumn::create();
        auto start = Int64Column::create();
        auto end = Int64Column::create();
        bitmap_column1->append(&bitmap1);
        start->append(5);
        end->append(5);

        columns.emplace_back(bitmap_column1);
        columns.emplace_back(start);
        columns.emplace_back(end);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }
}

TEST_F(VecBitmapFunctionsTest, bitmap_subset_in_range_special_cases) {
    // empty bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        BitmapValue empty;
        empty.clear();
        bitmap_column->append(&empty);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        ASSERT_TRUE(column->is_null(0));
    }
    // single bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        BitmapValue single({1});
        bitmap_column->append(&single);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(0);
        len->append(2);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("1", res->get_object(0)->to_string());
    }
    // set bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Adding values one by one, no more than 32, which makes the bitmap stores value with set
        BitmapValue set;
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        bitmap_column->append(&set);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("2", res->get_object(0)->to_string());
    }
    // 32bit Bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Constructing bitmap with vector which contains 32bit values makes it a 32bit bitmap
        BitmapValue bmp32bit({1, 2, 3, 4});
        bitmap_column->append(&bmp32bit);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(2);
        len->append(3);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("2", res->get_object(0)->to_string());
    }
    // 64bit bitmap
    {
        auto bitmap_column = BitmapColumn::create();
        // Constructing bitmap with vector which contains 64bit values makes it a 64bit bitmap
        BitmapValue bmp64bit({600123456781, 600123456782, 600123456783, 600123456784});
        bitmap_column->append(&bmp64bit);

        Columns columns;
        auto offset = Int64Column::create();
        auto len = Int64Column::create();
        offset->append(600123456781);
        len->append(600123456782);

        columns.emplace_back(bitmap_column);
        columns.emplace_back(offset);
        columns.emplace_back(len);

        auto column = BitmapFunctions::bitmap_subset_in_range(ctx, columns).value();
        auto res = ColumnHelper::cast_to<TYPE_OBJECT>(column);
        ASSERT_EQ("600123456781", res->get_object(0)->to_string());
    }
}
} // namespace starrocks

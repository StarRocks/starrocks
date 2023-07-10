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

#include "exprs/time_functions.h"

#include <cctz/civil_time.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "exprs/function_context.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "runtime/time_types.h"
#include "testutil/function_utils.h"
#include "types/logical_type.h"

namespace starrocks {

class TimeFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        _state = std::make_shared<RuntimeState>(globals);
        _utils = std::make_shared<FunctionUtils>(_state.get());
    }

public:
    TExprNode expr_node;

private:
    std::shared_ptr<RuntimeState> _state;
    std::shared_ptr<FunctionUtils> _utils;
};

TEST_F(TimeFunctionsTest, yearTest) {
    Columns columns;

    auto tc = TimestampColumn::create();
    for (int j = 0; j < 20; ++j) {
        tc->append(TimestampValue::create(2000 + j, 1, 1, 0, 30, 30));
    }

    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::year(_utils->get_fn_ctx(), columns).value();

    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(2000 + k, v->get_data()[k]);
    }
}

TEST_F(TimeFunctionsTest, quarterNullTest) {
    Columns columns;

    auto null = NullColumn::create();
    auto tc = TimestampColumn::create();
    for (int j = 0; j < 10; ++j) {
        tc->append(TimestampValue::create(2000, j + 1, 1, 0, 30, 30));
        null->append(j % 2 == 0);
    }

    columns.emplace_back(NullableColumn::create(tc, null));

    ColumnPtr result = TimeFunctions::quarter(_utils->get_fn_ctx(), columns).value();

    ASSERT_TRUE(result->is_nullable());
    ASSERT_FALSE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

    for (int k = 0; k < 10; ++k) {
        if (k % 2 != 0) {
            result->is_null(k);
        }

        ASSERT_EQ((k) / 3 + 1, v->get_data()[k]);
    }
}

TEST_F(TimeFunctionsTest, yearAddTest) {
    Columns columns;

    auto tc = TimestampColumn::create();
    auto year = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        tc->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
        year->append(j);
    }

    columns.emplace_back(tc);
    columns.emplace_back(year);

    ColumnPtr result = TimeFunctions::years_add(_utils->get_fn_ctx(), columns).value();

    ASSERT_FALSE(result->is_timestamp());
    ASSERT_TRUE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(TimestampValue::create(2000 + k, 1, 1, 0, 30, 30), v->get_data()[k]);
        ASSERT_FALSE(result->is_null(k));
    }
}

TEST_F(TimeFunctionsTest, quarterAddTest) {
    Columns columns;

    auto tc = TimestampColumn::create();
    auto quarter = Int32Column::create();

    tc->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    quarter->append(2);

    columns.emplace_back(tc);
    columns.emplace_back(quarter);

    ColumnPtr result = TimeFunctions::quarters_add(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_nullable());
    auto v = ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

    ASSERT_EQ(TimestampValue::create(2000, 7, 1, 0, 30, 30), v->get_data()[0]);
    ASSERT_FALSE(result->is_null(0));
}

TEST_F(TimeFunctionsTest, millisAddTest) {
    Columns columns;

    auto tc = TimestampColumn::create();
    auto millis = Int32Column::create();

    tc->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    millis->append(200);

    columns.emplace_back(tc);
    columns.emplace_back(millis);

    ColumnPtr result = TimeFunctions::millis_add(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_nullable());
    auto v = ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

    TimestampValue check_ts;
    check_ts.from_timestamp(2000, 1, 1, 0, 30, 30, 200 * 1000);

    ASSERT_EQ(check_ts, v->get_data()[0]);
    ASSERT_FALSE(result->is_null(0));
}

TEST_F(TimeFunctionsTest, yearOverflowTest) {
    Columns columns;

    auto tc = TimestampColumn::create();
    auto year = Int32Column::create();
    tc->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    year->append(8000);

    tc->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
    year->append(7999);

    columns.emplace_back(tc);
    columns.emplace_back(year);

    ColumnPtr result = TimeFunctions::years_add(_utils->get_fn_ctx(), columns).value();

    ASSERT_FALSE(result->is_timestamp());
    ASSERT_TRUE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

    ASSERT_TRUE(result->is_null(0));

    ASSERT_EQ(TimestampValue::create(9999, 1, 1, 0, 30, 30), v->get_data()[1]);
    ASSERT_FALSE(result->is_null(1));
}

TEST_F(TimeFunctionsTest, monthTest) {
    Columns columns;
    auto tc = TimestampColumn::create();
    for (int j = 0; j < 20; ++j) {
        tc->append(TimestampValue::create(2000, j + 1, 1, 0, 1, 1));
    }
    columns.emplace_back(tc);

    ColumnPtr years = TimeFunctions::year(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(years->is_numeric());
    ASSERT_FALSE(years->is_nullable());
    ColumnPtr result = TimeFunctions::month(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto year_values = ColumnHelper::cast_to<TYPE_INT>(years);
    auto month_values = ColumnHelper::cast_to<TYPE_INT>(result);
    for (size_t j = 0; j < tc->size(); ++j) {
        ASSERT_EQ(2000 + (j / 12), year_values->get_data()[j]);
        ASSERT_EQ((j + 1) % 13 + (j + 1) / 13, month_values->get_data()[j]);
    }
}

TEST_F(TimeFunctionsTest, dayOfWeekTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 7, 5, 0, 1, 1));  // Sunday
    tc->append(TimestampValue::create(2020, 7, 6, 0, 1, 1));  // Monday
    tc->append(TimestampValue::create(2020, 7, 7, 0, 1, 1));  // Tuesday
    tc->append(TimestampValue::create(2020, 7, 8, 0, 1, 1));  // Wednesday
    tc->append(TimestampValue::create(2020, 7, 9, 0, 1, 1));  // Thursday
    tc->append(TimestampValue::create(2020, 7, 10, 0, 1, 1)); // Friday
    tc->append(TimestampValue::create(2020, 7, 11, 0, 1, 1)); // Saturday
    tc->append(TimestampValue::create(2020, 7, 12, 0, 1, 1)); // Sunday
    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::day_of_week(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto week_days = ColumnHelper::cast_to<TYPE_INT>(result);
    for (size_t i = 0; i < tc->size(); ++i) {
        ASSERT_EQ((i + 1) == 7 ? 7 : (i + 1) % 7, week_days->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, dayOfYearTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 0, 1, 1));
    tc->append(TimestampValue::create(2020, 2, 2, 0, 0, 1));
    tc->append(TimestampValue::create(2020, 3, 6, 0, 1, 1));
    tc->append(TimestampValue::create(2020, 4, 8, 0, 1, 1));
    tc->append(TimestampValue::create(2020, 5, 9, 0, 1, 1));
    tc->append(TimestampValue::create(2020, 11, 3, 0, 1, 1));

    int days[] = {1, 33, 66, 99, 130, 308};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::day_of_year(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto year_days = ColumnHelper::cast_to<TYPE_INT>(result);

    for (size_t i = 0; i < sizeof(days) / sizeof(days[0]); ++i) {
        ASSERT_EQ(days[i], year_days->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, weekOfYearTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 0, 1, 1));
    tc->append(TimestampValue::create(2020, 7, 5, 3, 0, 1));
    tc->append(TimestampValue::create(2020, 3, 28, 7, 12, 1));
    tc->append(TimestampValue::create(2020, 4, 8, 2, 3, 34));
    tc->append(TimestampValue::create(2020, 8, 9, 5, 1, 1));
    tc->append(TimestampValue::create(2020, 12, 31, 8, 8, 13));

    int weeks[] = {1, 27, 13, 15, 32, 53};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::week_of_year(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto year_weeks = ColumnHelper::cast_to<TYPE_INT>(result);

    for (size_t i = 0; i < sizeof(weeks) / sizeof(weeks[0]); ++i) {
        ASSERT_EQ(weeks[i], year_weeks->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, weekOfYearIsoTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2023, 1, 5, 0, 5, 0));
    tc->append(TimestampValue::create(2023, 1, 9, 0, 9, 0));
    tc->append(TimestampValue::create(2023, 1, 2, 0, 2, 0));
    tc->append(TimestampValue::create(2023, 1, 6, 0, 6, 0));
    tc->append(TimestampValue::create(2023, 1, 3, 0, 3, 0));
    tc->append(TimestampValue::create(2023, 1, 7, 0, 7, 0));
    tc->append(TimestampValue::create(2023, 1, 10, 0, 10, 0));
    tc->append(TimestampValue::create(2023, 1, 1, 0, 1, 0));
    tc->append(TimestampValue::create(2023, 1, 4, 0, 4, 0));
    tc->append(TimestampValue::create(2023, 1, 8, 0, 8, 0));

    int weeks[] = {1, 2, 1, 1, 1, 1, 2, 52, 1, 1};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::week_of_year_iso(_utils->get_fn_ctx(), columns).value();

    auto year_weeks = ColumnHelper::cast_to<TYPE_INT>(result);
    for (size_t i = 0; i < sizeof(weeks) / sizeof(weeks[0]); ++i) {
        ASSERT_EQ(weeks[i], year_weeks->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, weekWithDefaultModeTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2007, 1, 1, 0, 0, 0));
    tc->append(TimestampValue::create(2017, 5, 1, 0, 0, 0));
    tc->append(TimestampValue::create(2020, 9, 23, 0, 0, 0));
    tc->append(TimestampValue::create(2015, 10, 11, 0, 0, 0));

    int weeks[] = {0, 18, 38, 41};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::week_of_year_with_default_mode(_utils->get_fn_ctx(), columns).value();

    auto year_weeks = ColumnHelper::cast_to<TYPE_INT>(result);
    for (size_t i = 0; i < sizeof(weeks) / sizeof(weeks[0]); ++i) {
        ASSERT_EQ(weeks[i], year_weeks->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, weekWithModeTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2007, 1, 1, 0, 0, 0));
    tc->append(TimestampValue::create(2017, 5, 1, 0, 0, 0));
    tc->append(TimestampValue::create(2020, 9, 23, 0, 0, 0));
    tc->append(TimestampValue::create(2015, 10, 11, 0, 0, 0));
    tc->append(TimestampValue::create(2014, 12, 11, 0, 0, 0));
    tc->append(TimestampValue::create(2001, 5, 3, 0, 0, 0));
    tc->append(TimestampValue::create(2005, 2, 3, 0, 0, 0));
    tc->append(TimestampValue::create(2003, 9, 3, 0, 0, 0));

    auto mode_column = Int32Column::create();
    mode_column->append(3);
    mode_column->append(2);
    mode_column->append(1);
    mode_column->append(0);
    mode_column->append(5);
    mode_column->append(7);
    mode_column->append(4);
    mode_column->append(6);

    int weeks[] = {1, 18, 39, 41, 49, 18, 5, 36};

    Columns columns;
    columns.emplace_back(tc);
    columns.emplace_back(mode_column);

    ColumnPtr result = TimeFunctions::week_of_year_with_mode(_utils->get_fn_ctx(), columns).value();

    auto year_weeks = ColumnHelper::cast_to<TYPE_INT>(result);
    for (size_t i = 0; i < sizeof(weeks) / sizeof(weeks[0]); ++i) {
        ASSERT_EQ(weeks[i], year_weeks->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, toDateTest) {
    const int year = 2020, month = 6, day = 18;
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(year, month, day, 19, 21, 21));
    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::to_date(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_date());
    ASSERT_FALSE(result->is_nullable());

    DateValue date = DateValue::create(year, month, day);
    auto dates = ColumnHelper::cast_to<TYPE_DATE>(result);
    ASSERT_TRUE(date == dates->get_data()[0]);
}

TEST_F(TimeFunctionsTest, dateAndDaysDiffTest) {
    auto tc1 = TimestampColumn::create();
    auto tc2 = TimestampColumn::create();
    // group 1
    tc1->append(TimestampValue::create(2012, 8, 30, 0, 0, 0));
    tc2->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    // group 2
    tc1->append(TimestampValue::create(2012, 8, 30, 0, 0, 1));
    tc2->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    // group 3
    tc1->append(TimestampValue::create(2012, 9, 1, 0, 0, 1));
    tc2->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    // group 4
    tc1->append(TimestampValue::create(2012, 8, 23, 0, 0, 5));
    tc2->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    // group 5
    tc1->append(TimestampValue::create(2020, 6, 20, 13, 48, 25));
    tc2->append(TimestampValue::create(2020, 6, 20, 13, 48, 30));
    // group 6
    tc1->append(TimestampValue::create(2020, 6, 20, 13, 48, 30));
    tc2->append(TimestampValue::create(2020, 6, 20, 13, 48, 25));

    Columns columns;
    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    // date_diff
    {
        ColumnPtr result = TimeFunctions::date_diff(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(6, v->get_data()[0]);
        ASSERT_EQ(6, v->get_data()[1]);
        ASSERT_EQ(8, v->get_data()[2]);
        ASSERT_EQ(-1, v->get_data()[3]);
        ASSERT_EQ(0, v->get_data()[4]);
        ASSERT_EQ(0, v->get_data()[5]);
    }

    // days_diff
    {
        ColumnPtr result = TimeFunctions::days_diff(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(5, v->get_data()[0]);
        ASSERT_EQ(6, v->get_data()[1]);
        ASSERT_EQ(8, v->get_data()[2]);
        ASSERT_EQ(0, v->get_data()[3]);
        ASSERT_EQ(0, v->get_data()[4]);
        ASSERT_EQ(0, v->get_data()[5]);
    }
}

TEST_F(TimeFunctionsTest, timeDiffTest) {
    auto tc1 = TimestampColumn::create();
    auto tc2 = TimestampColumn::create();
    // group 1: from TimestampFunctionsTest.time_diff_test
    tc1->append(TimestampValue::create(2019, 7, 18, 12, 0, 0));
    tc2->append(TimestampValue::create(2019, 7, 18, 13, 1, 2));
    // group 2
    auto t1 = TimestampValue::create(2020, 6, 20, 0, 1, 1);
    auto t2 = TimestampValue::create(2019, 5, 10, 13, 12, 11);

    cctz::civil_second s1(2020, 6, 20, 0, 1, 1);
    cctz::civil_second s2(2019, 5, 10, 13, 12, 11);

    tc1->append(t1);
    tc2->append(t2);

    Columns columns;
    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    ColumnPtr result = TimeFunctions::time_diff(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_TIME>(result);
    ASSERT_EQ(-3662, v->get_data()[0]);
    ASSERT_EQ(s1 - s2, v->get_data()[1]);
}

TEST_F(TimeFunctionsTest, yearsDiffTest) {
    {
        Columns columns;

        auto tc1 = TimestampColumn::create();
        auto tc2 = TimestampColumn::create();
        for (int j = 0; j < 20; ++j) {
            tc1->append(TimestampValue::create(2001, 11, 1, 0, 30, 30));
            tc2->append(TimestampValue::create(2000, 12, 1, 0, 30, 30));
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        ColumnPtr result = TimeFunctions::years_diff(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        for (int k = 0; k < 20; ++k) {
            ASSERT_EQ(0, v->get_data()[k]);
        }
    }

    {
        Columns columns;

        auto tc1 = TimestampColumn::create();
        auto tc2 = TimestampColumn::create();
        for (int j = 0; j < 20; ++j) {
            tc1->append(TimestampValue::create(2002, 12, 1, 0, 30, 30));
            tc2->append(TimestampValue::create(2000, 11, 1, 0, 30, 30));
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        ColumnPtr result = TimeFunctions::years_diff(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        for (int k = 0; k < 20; ++k) {
            ASSERT_EQ(2, v->get_data()[k]);
        }
    }

    {
        Columns columns;

        auto tc1 = TimestampColumn::create();
        auto tc2 = TimestampColumn::create();
        for (int j = 0; j < 20; ++j) {
            tc1->append(TimestampValue::create(2000, 11, 1, 0, 30, 30));
            tc2->append(TimestampValue::create(2001, 12, 1, 0, 30, 30));
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        ColumnPtr result = TimeFunctions::years_diff(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        for (int k = 0; k < 20; ++k) {
            ASSERT_EQ(-1, v->get_data()[k]);
        }
    }
}

TEST_F(TimeFunctionsTest, monthsDiffTest) {
    {
        Columns columns;

        auto tc1 = TimestampColumn::create();
        auto tc2 = TimestampColumn::create();
        for (int j = 0; j < 20; ++j) {
            tc1->append(TimestampValue::create(2000, 1, 1, 0, 30, 30));
            tc2->append(TimestampValue::create(2000, 12, 1, 0, 30, 30));
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        ColumnPtr result = TimeFunctions::months_diff(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        for (int k = 0; k < 20; ++k) {
            ASSERT_EQ(-11, v->get_data()[k]);
        }
    }

    {
        Columns columns;

        auto tc1 = TimestampColumn::create();
        auto tc2 = TimestampColumn::create();
        for (int j = 0; j < 20; ++j) {
            tc1->append(TimestampValue::create(2002, 1, 1, 0, 30, 30));
            tc2->append(TimestampValue::create(2000, 12, 1, 0, 30, 30));
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        ColumnPtr result = TimeFunctions::months_diff(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        for (int k = 0; k < 20; ++k) {
            ASSERT_EQ(13, v->get_data()[k]);
        }
    }
}

TEST_F(TimeFunctionsTest, now) {
    {
        ColumnPtr ptr = TimeFunctions::now(_utils->get_fn_ctx(), Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_timestamp());
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);
        ASSERT_EQ("2019-08-06 01:38:57", v->get(0).get_timestamp().to_string());
    }

    {
        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        starrocks::RuntimeState state(globals);
        starrocks::FunctionUtils futils(&state);
        FunctionContext* ctx = futils.get_fn_ctx();
        ColumnPtr ptr = TimeFunctions::now(ctx, Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_timestamp());
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);
        ASSERT_EQ(TimestampValue::create(2019, 8, 6, 1, 38, 57), v->get(0).get_timestamp());
    }
}

TEST_F(TimeFunctionsTest, curtime) {
    // without RuntimeState
    {
        ColumnPtr now = TimeFunctions::now(_utils->get_fn_ctx(), Columns()).value();
        ColumnPtr ptr = TimeFunctions::curtime(_utils->get_fn_ctx(), Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_numeric());
        TimestampValue ts = ColumnHelper::as_column<ConstColumn>(now)->get(0).get_timestamp();
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);

        int h, m, s, us;
        ts.to_time(&h, &m, &s, &us);
        ASSERT_EQ(h * 3600 + m * 60 + s, v->get(0).get_double());
    }
    // with RuntimeState
    {
        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        starrocks::RuntimeState state(globals);
        starrocks::FunctionUtils futils(&state);
        ColumnPtr ptr = TimeFunctions::curtime(futils.get_fn_ctx(), Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_numeric());
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);
        ASSERT_EQ(1 * 3600 + 38 * 60 + 57, v->get(0).get_double());
    }
}

TEST_F(TimeFunctionsTest, curdate) {
    // without RuntimeState
    {
        ColumnPtr now = TimeFunctions::now(_utils->get_fn_ctx(), Columns()).value();
        ColumnPtr cur_date = TimeFunctions::curdate(_utils->get_fn_ctx(), Columns()).value();
        ASSERT_TRUE(cur_date->is_constant());
        ASSERT_FALSE(cur_date->is_date());
        TimestampValue ts = ColumnHelper::as_column<ConstColumn>(now)->get(0).get_timestamp();
        auto v = ColumnHelper::as_column<ConstColumn>(cur_date);
        ASSERT_EQ((DateValue)ts, v->get(0).get_date());
    }
    // with RuntimeState
    {
        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        starrocks::RuntimeState state(globals);
        starrocks::FunctionUtils futils(&state);
        ColumnPtr cur_date = TimeFunctions::curdate(futils.get_fn_ctx(), Columns()).value();
        ASSERT_TRUE(cur_date->is_constant());
        ASSERT_FALSE(cur_date->is_date());
        auto v = ColumnHelper::as_column<ConstColumn>(cur_date);
        ASSERT_EQ(DateValue::create(2019, 8, 6), v->get(0).get_date());
    }
}

TEST_F(TimeFunctionsTest, weeks_diff) {
    auto tc1 = TimestampColumn::create();
    auto tc2 = TimestampColumn::create();
    // case 1: 0 week, from TimestampFunctionsTest.time_diff_test
    tc1->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    tc2->append(TimestampValue::create(2012, 8, 30, 0, 0, 0));
    // case 2: 0 week
    tc1->append(TimestampValue::create(2012, 8, 31, 0, 0, 0));
    tc2->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    // case 3: -1 week
    tc1->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    tc2->append(TimestampValue::create(2012, 8, 31, 0, 0, 1));
    // case 4: 1 week
    tc1->append(TimestampValue::create(2012, 8, 31, 0, 0, 1));
    tc2->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    // case 5: 1 week
    tc1->append(TimestampValue::create(2020, 6, 23, 16, 55, 25));
    tc2->append(TimestampValue::create(2020, 6, 13, 16, 55, 26));
    // case 6: 1 week
    tc1->append(TimestampValue::create(2020, 1, 2, 3, 4, 5));
    tc2->append(TimestampValue::create(2019, 12, 23, 10, 10, 10));
    // case 7: 2 week
    tc1->append(TimestampValue::create(2020, 1, 2, 3, 4, 5));
    tc2->append(TimestampValue::create(2019, 12, 19, 3, 4, 5));
    // case 8: -14 week
    tc1->append(TimestampValue::create(2020, 1, 1, 3, 4, 5));
    tc2->append(TimestampValue::create(2020, 4, 8, 3, 4, 5));

    Columns columns;
    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    ColumnPtr result = TimeFunctions::weeks_diff(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_EQ(8, result->size());

    auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
    ASSERT_EQ(0, v->get_data()[0]);
    ASSERT_EQ(0, v->get_data()[1]);
    ASSERT_EQ(-1, v->get_data()[2]);
    ASSERT_EQ(1, v->get_data()[3]);
    ASSERT_EQ(1, v->get_data()[4]);
    ASSERT_EQ(1, v->get_data()[5]);
    ASSERT_EQ(2, v->get_data()[6]);
    ASSERT_EQ(-14, v->get_data()[7]);
}

TEST_F(TimeFunctionsTest, quarters_diff) {
    auto tc1 = TimestampColumn::create();
    auto tc2 = TimestampColumn::create();
    // case 1: 0 quarter
    tc1->append(TimestampValue::create(2020, 1, 2, 3, 4, 5));
    tc2->append(TimestampValue::create(2020, 4, 2, 3, 4, 4));
    // case 2: 0 quarter
    tc1->append(TimestampValue::create(2020, 4, 2, 3, 4, 4));
    tc2->append(TimestampValue::create(2020, 1, 2, 3, 4, 5));
    // case 3: -1 quarter
    tc1->append(TimestampValue::create(2020, 1, 2, 3, 4, 5));
    tc2->append(TimestampValue::create(2020, 4, 2, 3, 4, 5));
    // case 4: 1 quarter
    tc1->append(TimestampValue::create(2020, 4, 2, 3, 4, 5));
    tc2->append(TimestampValue::create(2020, 1, 2, 3, 4, 5));
    // case 5: 1 quarter
    tc1->append(TimestampValue::create(2020, 7, 2, 3, 4, 4));
    tc2->append(TimestampValue::create(2020, 1, 2, 3, 4, 5));
    // case 6: 4 quarters
    tc1->append(TimestampValue::create(2020, 6, 23, 17, 44, 23));
    tc2->append(TimestampValue::create(2019, 6, 23, 17, 44, 23));

    Columns columns;
    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    ColumnPtr result = TimeFunctions::quarters_diff(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_EQ(6, result->size());

    auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
    ASSERT_EQ(0, v->get_data()[0]);
    ASSERT_EQ(0, v->get_data()[1]);
    ASSERT_EQ(-1, v->get_data()[2]);
    ASSERT_EQ(1, v->get_data()[3]);
    ASSERT_EQ(1, v->get_data()[4]);
    ASSERT_EQ(4, v->get_data()[5]);
}

TEST_F(TimeFunctionsTest, hours_minutes_seconds_diff) {
    auto tc1 = TimestampColumn::create();
    auto tc2 = TimestampColumn::create();
    // case 1: from TimestampFunctionsTest.timestampdiff_test
    tc1->append(TimestampValue::create(2012, 8, 30, 0, 0, 0));
    tc2->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    // case 2
    tc1->append(TimestampValue::create(2012, 8, 24, 0, 0, 1));
    tc2->append(TimestampValue::create(2012, 8, 30, 0, 0, 0));
    // case 3
    tc1->append(TimestampValue::create(2020, 3, 1, 0, 0, 1));
    tc2->append(TimestampValue::create(2020, 2, 28, 22, 0, 2));
    // case 4
    tc1->append(TimestampValue::create(2019, 3, 1, 0, 0, 1));
    tc2->append(TimestampValue::create(2019, 2, 28, 22, 0, 2));
    // case 5
    tc1->append(TimestampValue::create(2020, 1, 1, 12, 30, 30));
    tc2->append(TimestampValue::create(2019, 12, 31, 12, 30, 30));

    Columns columns;
    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    // hours_diff
    {
        ColumnPtr result = TimeFunctions::hours_diff(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(result->is_numeric());
        ASSERT_EQ(5, result->size());
        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(143, v->get_data()[0]);
        ASSERT_EQ(-143, v->get_data()[1]);
        ASSERT_EQ(25, v->get_data()[2]);
        ASSERT_EQ(1, v->get_data()[3]);
        ASSERT_EQ(24, v->get_data()[4]);
    }
    // minutes_diff
    {
        ColumnPtr result = TimeFunctions::minutes_diff(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(result->is_numeric());
        ASSERT_EQ(5, result->size());
        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(8639, v->get_data()[0]);
        ASSERT_EQ(-8639, v->get_data()[1]);
        ASSERT_EQ(1559, v->get_data()[2]);
        ASSERT_EQ(119, v->get_data()[3]);
        ASSERT_EQ(1440, v->get_data()[4]);
    }
    // seconds_diff
    {
        ColumnPtr result = TimeFunctions::seconds_diff(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(result->is_numeric());
        ASSERT_EQ(5, result->size());
        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(518399, v->get_data()[0]);
        ASSERT_EQ(-518399, v->get_data()[1]);
        ASSERT_EQ(93599, v->get_data()[2]);
        ASSERT_EQ(7199, v->get_data()[3]);
        ASSERT_EQ(86400, v->get_data()[4]);
    }
}

TEST_F(TimeFunctionsTest, toUnixForNow) {
    {
        Columns columns;

        ColumnPtr result = TimeFunctions::to_unix_for_now_32(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_constant());

        auto v = std::static_pointer_cast<ConstColumn>(result)->data_column();
        //auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(1565080737, std::static_pointer_cast<Int32Column>(v)->get_data()[0]);
    }
}

TEST_F(TimeFunctionsTest, toUnixFromDatetime) {
    {
        Columns columns;

        auto tc1 = TimestampColumn::create();
        tc1->append(TimestampValue::create(1970, 1, 1, 16, 0, 0));
        tc1->append(TimestampValue::create(2019, 8, 6, 1, 38, 57));

        columns.emplace_back(tc1);

        ColumnPtr result = TimeFunctions::to_unix_from_datetime_32(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(24 * 60 * 60, v->get_data()[0]);
        ASSERT_EQ(1565080737, v->get_data()[1]);
    }
}

TEST_F(TimeFunctionsTest, toUnixFromDate) {
    {
        Columns columns;

        auto tc1 = DateColumn::create();
        tc1->append(DateValue::create(1970, 1, 1));
        tc1->append(DateValue::create(1970, 1, 2));

        columns.emplace_back(tc1);

        ColumnPtr result = TimeFunctions::to_unix_from_date_32(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(8 * 60 * 60, v->get_data()[0]);
        ASSERT_EQ(32 * 60 * 60, v->get_data()[1]);
    }
}

TEST_F(TimeFunctionsTest, toUnixFromDatetimeWithFormat) {
    {
        Columns columns;
        //     ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, StringVal("2019-08-06 01:38:57"), "%Y-%m-%d %H:%i:%S").val);
        auto tc1 = BinaryColumn::create();
        tc1->append("2019-08-06 01:38:57");
        tc1->append("2019-08-06 01:38:58");
        auto tc2 = BinaryColumn::create();
        tc2->append("%Y-%m-%d %H:%i:%S");
        tc2->append("%Y-%m-%d %H:%i:%S");

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        ColumnPtr result = TimeFunctions::to_unix_from_datetime_with_format_32(_utils->get_fn_ctx(), columns).value();

        ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(1565080737, v->get_data()[0]);
        ASSERT_EQ(1565080738, v->get_data()[1]);
    }
}

TEST_F(TimeFunctionsTest, fromUnixToDatetime) {
    {
        Columns columns;
        //     ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, StringVal("2019-08-06 01:38:57"), "%Y-%m-%d %H:%i:%S").val);
        auto tc1 = Int32Column::create();
        tc1->append(1565080737);
        tc1->append(1565080797);
        tc1->append(1565084337);

        columns.emplace_back(tc1);

        ColumnPtr result = TimeFunctions::from_unix_to_datetime_32(_utils->get_fn_ctx(), columns).value();

        //ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ("2019-08-06 01:38:57", v->get_data()[0]);
        ASSERT_EQ("2019-08-06 01:39:57", v->get_data()[1]);
        ASSERT_EQ("2019-08-06 02:38:57", v->get_data()[2]);
    }
}

TEST_F(TimeFunctionsTest, fromUnixToDatetimeWithFormat) {
    {
        Columns columns;
        //     ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, StringVal("2019-08-06 01:38:57"), "%Y-%m-%d %H:%i:%S").val);
        auto tc1 = Int32Column::create();
        tc1->append(24 * 60 * 60);
        tc1->append(61 + 24 * 60 * 60);
        tc1->append(3789 + 24 * 60 * 60);
        auto tc2 = BinaryColumn::create();
        tc2->append("%Y-%m-%d %H:%i:%S");
        tc2->append("%Y-%m-%d %H:%i:%S");
        tc2->append("%Y-%m-%d %H:%i:%S");

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::from_unix_prepare(_utils->get_fn_ctx(),
                                                     FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::from_unix_to_datetime_with_format_32(_utils->get_fn_ctx(), columns).value();

        //ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ("1970-01-01 16:00:00", v->get_data()[0]);
        ASSERT_EQ("1970-01-01 16:01:01", v->get_data()[1]);
        ASSERT_EQ("1970-01-01 17:03:09", v->get_data()[2]);

        ASSERT_TRUE(TimeFunctions::from_unix_close(_utils->get_fn_ctx(),
                                                   FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }
}

TEST_F(TimeFunctionsTest, fromUnixToDatetimeWithConstFormat) {
    {
        Columns columns;
        //     ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, StringVal("2019-08-06 01:38:57"), "%Y-%m-%d %H:%i:%S").val);
        auto tc1 = Int32Column::create();
        tc1->append(24 * 60 * 60);
        tc1->append(61 + 24 * 60 * 60);
        tc1->append(3789 + 24 * 60 * 60);
        auto tc2 = ColumnHelper::create_const_column<TYPE_VARCHAR>("%Y-%m-%d %H:%i:%S", 1);

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::from_unix_prepare(_utils->get_fn_ctx(),
                                                     FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::from_unix_to_datetime_with_format_32(_utils->get_fn_ctx(), columns).value();

        //ASSERT_TRUE(result->is_numeric());

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ("1970-01-01 16:00:00", v->get_data()[0]);
        ASSERT_EQ("1970-01-01 16:01:01", v->get_data()[1]);
        ASSERT_EQ("1970-01-01 17:03:09", v->get_data()[2]);

        ASSERT_TRUE(TimeFunctions::from_unix_close(_utils->get_fn_ctx(),
                                                   FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    // Test empty string
    {
        auto tc1 = ColumnHelper::create_const_column<TYPE_INT>(24 * 60 * 60, 1);
        auto tc2 = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);

        Columns columns = {tc1, tc2};
        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::from_unix_prepare(_utils->get_fn_ctx(),
                                                     FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
        ColumnPtr result = TimeFunctions::from_unix_to_datetime_with_format_32(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(result->only_null());
        ASSERT_TRUE(TimeFunctions::from_unix_close(_utils->get_fn_ctx(),
                                                   FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }
}

TEST_F(TimeFunctionsTest, from_days) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> x(ctx);

    const int Year = 2020, Month = 6, Day = 24;
    DateTimeValue dtv(TIME_DATE, Year, Month, Day, 0, 0, 0, 0);
    DateValue date = DateValue::create(Year, Month, Day);
    // nullable
    {
        auto tc = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        tc->append_datum((RunTimeTypeTraits<TYPE_INT>::CppType)dtv.daynr());
        (void)tc->append_nulls(1);

        Columns columns;
        columns.emplace_back(tc);
        ColumnPtr result = TimeFunctions::from_days(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(2, nullable_col->size());
        ASSERT_EQ(date, nullable_col->get(0).get_date());
        ASSERT_FALSE(nullable_col->is_null(0));
    }
    // const
    {
        auto tc = ColumnHelper::create_const_column<TYPE_INT>((RunTimeTypeTraits<TYPE_INT>::CppType)dtv.daynr(), 1);
        Columns columns;
        columns.emplace_back(tc);
        ColumnPtr result = TimeFunctions::from_days(ctx, columns).value();
        ASSERT_TRUE(result->is_constant());

        ConstColumn::Ptr const_col = ColumnHelper::as_column<ConstColumn>(result);
        ASSERT_EQ(1, const_col->size());
        ASSERT_FALSE(const_col->is_date());
        ASSERT_EQ(date, const_col->get(0).get_date());
    }
    // null
    {
        auto tc = ColumnHelper::create_const_null_column(1);
        Columns columns;
        columns.emplace_back(tc);
        ColumnPtr result = TimeFunctions::from_days(ctx, columns).value();
        ASSERT_TRUE(result->only_null());
    }
    // normal
    {
        auto col = Int32Column::create();
        col->append(730850);

        Columns columns;
        columns.emplace_back(col);
        ColumnPtr result = TimeFunctions::from_days(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(1, nullable_col->size());
        ASSERT_EQ(DateValue::create(2000, 12, 31), nullable_col->get(0).get_date());
        ASSERT_FALSE(nullable_col->is_null(0));
    }
    // overflow
    {
        auto tc = Int32Column::create();
        tc->append(3652425);

        Columns columns;
        columns.emplace_back(tc);
        ColumnPtr result = TimeFunctions::from_days(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());
        auto col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(1, col->size());
        ASSERT_FALSE(col->is_null(0));
        ASSERT_EQ(col->get(0).get_date().to_string(), "0000-00-00");
    }
    // from_days(negative) return "0000-00-00"
    {
        auto tc = Int32Column::create();
        tc->append(-1);
        tc->append(-2);
        tc->append(-2147483648);
        Columns columns;
        columns.push_back(tc);

        ColumnPtr result = TimeFunctions::from_days(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        auto col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(3, col->size());
        for (auto i = 0; i < 3; ++i) {
            ASSERT_FALSE(col->is_null(i));
            ASSERT_EQ(col->get(i).get_date().to_string(), "0000-00-00");
        }
    }
}

TEST_F(TimeFunctionsTest, to_days) {
    const int Year = 2020, Month = 6, Day = 24;
    auto tc = RunTimeTypeTraits<TYPE_DATE>::ColumnType::create();
    tc->append(DateValue::create(Year, Month, Day));

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::to_days(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_EQ(1, result->size());
    auto v = ColumnHelper::cast_to<TYPE_INT>(result);
    DateTimeValue dtv(TIME_DATE, Year, Month, Day, 0, 0, 0, 0);
    ASSERT_EQ(dtv.daynr(), v->get_data()[0]);
}

TEST_F(TimeFunctionsTest, str_to_date) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    const char* str1 = "01,5,2013";
    const char* str2 = "2020-06-24 17:10:25";
    const char* fmt1 = "%d,%m,%Y";
    const char* fmt2 = "%Y-%m-%d %H:%i:%s";
    TimestampValue ts1 = TimestampValue::create(2013, 5, 1, 0, 0, 0);
    TimestampValue ts2 = TimestampValue::create(2020, 6, 24, 17, 10, 25);

    const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    // nullable
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_column(varchar_type_desc, true);
        str_col->append_datum(Slice(str1)); // str1 <=> fmt1
        fmt_col->append_datum(Slice(fmt1));
        str_col->append_datum(Slice(str2)); // str2 <=> fmt2
        fmt_col->append_datum(Slice(fmt2));
        (void)str_col->append_nulls(1); // null <=> fmt1
        fmt_col->append_datum(Slice(fmt1));
        str_col->append_datum(Slice(str1)); // str1 <=> null
        (void)fmt_col->append_nulls(1);
        (void)str_col->append_nulls(1); // null <=> null
        (void)fmt_col->append_nulls(1);

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);
        ColumnPtr result = TimeFunctions::str_to_date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(5, nullable_col->size());
        ASSERT_EQ(ts1, nullable_col->get(0).get_timestamp());
        ASSERT_EQ(ts2, nullable_col->get(1).get_timestamp());
        for (int i = 2; i < 5; ++i) {
            ASSERT_TRUE(nullable_col->is_null(i));
        }
    }
    // const
    {
        auto str_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(str1, 1);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);
        ColumnPtr result = TimeFunctions::str_to_date(ctx, columns).value();
        ASSERT_TRUE(result->is_constant());

        ConstColumn::Ptr const_col = ColumnHelper::as_column<ConstColumn>(result);
        ASSERT_FALSE(const_col->is_timestamp());
        ASSERT_EQ(1, const_col->size());
        ASSERT_EQ(ts1, const_col->get(0).get_timestamp());
    }
    // const <=> non-const
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        str_col->append_datum(Slice(str1));
        (void)str_col->append_nulls(1);
        str_col->append_datum(Slice("25,06,2020"));

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);

        ColumnPtr result = TimeFunctions::str_to_date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(3, nullable_col->size());
        ASSERT_EQ(ts1, nullable_col->get(0).get_timestamp());
        ASSERT_TRUE(nullable_col->is_null(1));
        ASSERT_EQ(TimestampValue::create(2020, 6, 25, 0, 0, 0), nullable_col->get(2).get_timestamp());
    }
}

TEST_F(TimeFunctionsTest, str_to_date_of_dateformat) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    const char* str1 = "2013-05-01";
    const char* fmt1 = "%Y-%m-%d";
    TimestampValue ts1 = TimestampValue::create(2013, 5, 1, 0, 0, 0);
    const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    // const <=> non-const
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        str_col->append_datum(Slice(str1));
        (void)str_col->append_nulls(1);
        str_col->append_datum(Slice("2020-06-25"));
        str_col->append_datum(Slice("     2020-03-12"));
        str_col->append_datum(Slice("   2020-03-12    11:35:23  "));
        str_col->append_datum(Slice("   2020-0  "));

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);

        ColumnPtr result = TimeFunctions::str_to_date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);

        ASSERT_EQ(ts1, nullable_col->get(0).get_timestamp());
        ASSERT_TRUE(nullable_col->is_null(1));
        ASSERT_EQ(TimestampValue::create(2020, 6, 25, 0, 0, 0), nullable_col->get(2).get_timestamp());
        ASSERT_EQ(TimestampValue::create(2020, 3, 12, 0, 0, 0), nullable_col->get(3).get_timestamp());
        ASSERT_EQ(TimestampValue::create(2020, 3, 12, 0, 0, 0), nullable_col->get(4).get_timestamp());
        ASSERT_TRUE(nullable_col->is_null(5));
    }
}

TEST_F(TimeFunctionsTest, str_to_date_of_datetimeformat) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    const char* str1 = "2013-05-01 11:12:13";
    const char* fmt1 = "%Y-%m-%d %H:%i:%s";
    TimestampValue ts1 = TimestampValue::create(2013, 5, 1, 11, 12, 13);
    [[maybe_unused]] TimestampValue ts2 = TimestampValue::create(2020, 6, 24, 17, 10, 25);
    const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    // const <=> non-const
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        str_col->append_datum(Slice(str1));
        (void)str_col->append_nulls(1);
        str_col->append_datum(Slice("2020-06-25 12:05:39"));
        str_col->append_datum(Slice("     2020-03-12 08:19:39"));
        str_col->append_datum(Slice("   2020-03-12    11:35:23  "));
        str_col->append_datum(Slice("   2020-03-12    11:  "));

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);

        ColumnPtr result = TimeFunctions::str_to_date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);

        ASSERT_EQ(ts1, nullable_col->get(0).get_timestamp());
        ASSERT_TRUE(nullable_col->is_null(1));
        ASSERT_EQ(TimestampValue::create(2020, 6, 25, 12, 5, 39), nullable_col->get(2).get_timestamp());
        ASSERT_EQ(TimestampValue::create(2020, 3, 12, 8, 19, 39), nullable_col->get(3).get_timestamp());
        ASSERT_EQ(TimestampValue::create(2020, 3, 12, 11, 35, 23), nullable_col->get(4).get_timestamp());
        ASSERT_EQ(TimestampValue::create(2020, 3, 12, 11, 0, 0), nullable_col->get(5).get_timestamp());
    }
}

TEST_F(TimeFunctionsTest, date_format) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    auto date_col = DateColumn::create();
    date_col->append(DateValue::create(2013, 5, 1));
    auto dt_col = TimestampColumn::create();
    dt_col->append(TimestampValue::create(2020, 6, 25, 15, 58, 21));
    // date_format
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("%d,%m,%Y"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("01,05,2013"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("%Y-%m-%d %H:%i:%s"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013-05-01 00:00:00"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyyMMdd"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("20130501"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013-05-01"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd HH:mm:ss"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013-05-01 00:00:00"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("%Y-%m-%dT%H:%i:%s"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013-05-01T00:00:00"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("abcdef"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("abcdef"), v->get_data()[0]);
    }

    // datetime_format
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("%d,%m,%Y"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("25,06,2020"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("%Y-%m-%d %H:%i:%s"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2020-06-25 15:58:21"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyyMMdd"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("20200625"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2020-06-25"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd HH:mm:ss"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2020-06-25 15:58:21"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("%Y-%m-%dT%H:%i:%s"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2020-06-25T15:58:21"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("abcdef"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("abcdef"), v->get_data()[0]);
    }
    {
        // stack-buffer-overflow test
        std::string test_string;
        for (int i = 0; i < 10000; ++i) {
            test_string.append("x");
        }
        auto fmt_col =
                ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(test_string.c_str(), test_string.size()), 1);
        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);

        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_EQ(true, result->is_null(0));
    }
    {
        // Test(const,var)
        auto datetime_col =
                ColumnHelper::create_const_column<TYPE_DATETIME>(TimestampValue::create(2020, 12, 18, 10, 9, 8), 2);
        auto string_col = BinaryColumn::create();
        string_col->append_string(std::string("a"));
        string_col->append_string(std::string("b"));

        Columns columns;
        columns.emplace_back(datetime_col);
        columns.emplace_back(string_col);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

        ASSERT_FALSE(result->is_constant());
        auto binary_col = down_cast<BinaryColumn*>(result.get());
        ASSERT_EQ(Slice("a"), binary_col->get_slice(0));
        ASSERT_EQ(Slice("b"), binary_col->get_slice(1));
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("", 0), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->only_null());
    }
    {
        auto fmt_col = BinaryColumn::create();
        fmt_col->append_string(std::string(""));

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::date_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_nullable());
        ASSERT_EQ(1, result->size());
        ASSERT_TRUE(result->is_null(0));
    }
    {
        auto string_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("", 0), 1);
        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(string_col);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->only_null());
    }
    {
        auto string_col = BinaryColumn::create();
        string_col->append_string(std::string(""));

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(string_col);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::datetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_nullable());
        ASSERT_EQ(1, result->size());
        ASSERT_TRUE(result->is_null(0));
    }
}

TEST_F(TimeFunctionsTest, jodatime_format) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    auto date_col = DateColumn::create();
    date_col->append(DateValue::create(2013, 5, 1));
    auto dt_col = TimestampColumn::create();
    dt_col->append(TimestampValue::create(2020, 6, 25, 15, 58, 21));
    // jodadate_format
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("dd,MM,yy"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("01,05,2013"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyyMMdd"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("20130501"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013-05-01"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd HH:mm:ss"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013-05-01 00:00:00"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-ddTHH:mm:ss"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013-05-01T00:00:00"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("bcfbcf"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("bcfbcf"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("", 0), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->only_null());
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("G C Y x w e E y D M d"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("AD 20 2013 2013 18 3 Wed 2013 121 5 1"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy MMM dd EEEE ee"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013 May 01 Wednesday 03"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy MMM 'abcd'"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2013 May abcd"), v->get_data()[0]);
    }

    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("'abcd' yyyyMM"), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadate_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("abcd 201305"), v->get_data()[0]);
    }

    // datetime_format
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("dd,MM,yy"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("25,06,2020"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyyMMdd"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("20200625"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2020-06-25"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-dd HH:mm:ss"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2020-06-25 15:58:21"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("yyyy-MM-ddTHH:mm:ss"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("2020-06-25T15:58:21"), v->get_data()[0]);
    }
    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("bcfbcf"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("bcfbcf"), v->get_data()[0]);
    }
    {
        // stack-buffer-overflow test
        std::string test_string;
        for (int i = 0; i < 10000; ++i) {
            test_string.append("x");
        }
        auto fmt_col =
                ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(test_string.c_str(), test_string.size()), 1);
        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);

        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_EQ(true, result->is_null(0));
    }

    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("", 0), 1);

        Columns columns;
        columns.emplace_back(date_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->only_null());
    }

    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("a K h H k m s S"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("PM 3 3 15 15 58 21 0"), v->get_data()[0]);
    }

    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("a K 'abcd'"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("PM 3 abcd"), v->get_data()[0]);
    }

    {
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("'abcd' yyyyMM"), 1);

        Columns columns;
        columns.emplace_back(dt_col);
        columns.emplace_back(fmt_col);
        ctx->set_constant_columns(columns);
        TimeFunctions::format_prepare(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ColumnPtr result = TimeFunctions::jodadatetime_format(ctx, columns).value();
        TimeFunctions::format_close(ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        ASSERT_TRUE(result->is_binary());
        ASSERT_EQ(1, result->size());
        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        ASSERT_EQ(Slice("abcd 202006"), v->get_data()[0]);
    }
}

TEST_F(TimeFunctionsTest, daynameTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 21, 22, 1));
    tc->append(TimestampValue::create(2020, 2, 2, 14, 17, 1));
    tc->append(TimestampValue::create(2020, 3, 6, 11, 54, 1));
    tc->append(TimestampValue::create(2020, 4, 8, 9, 13, 1));
    tc->append(TimestampValue::create(2020, 5, 9, 8, 8, 1));
    tc->append(TimestampValue::create(2020, 11, 3, 23, 41, 1));

    std::string days[] = {"Wednesday", "Sunday", "Friday", "Wednesday", "Saturday", "Tuesday"};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::day_name(_utils->get_fn_ctx(), columns).value();
    auto day_names = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (size_t i = 0; i < sizeof(days) / sizeof(days[0]); ++i) {
        ASSERT_EQ(days[i], day_names->get_data()[i].to_string());
    }
}

TEST_F(TimeFunctionsTest, monthnameTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 21, 22, 1));
    tc->append(TimestampValue::create(2020, 2, 2, 14, 17, 1));
    tc->append(TimestampValue::create(2020, 3, 6, 11, 54, 1));
    tc->append(TimestampValue::create(2020, 4, 8, 9, 13, 1));
    tc->append(TimestampValue::create(2020, 5, 9, 8, 8, 1));
    tc->append(TimestampValue::create(2020, 11, 3, 23, 41, 1));

    std::string months[] = {"January", "February", "March", "April", "May", "November"};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::month_name(_utils->get_fn_ctx(), columns).value();
    auto day_names = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (size_t i = 0; i < sizeof(months) / sizeof(months[0]); ++i) {
        ASSERT_EQ(months[i], day_names->get_data()[i].to_string());
    }
}

TEST_F(TimeFunctionsTest, convertTzGeneralTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2019, 8, 1, 13, 21, 3));
    tc->append(TimestampValue::create(2019, 8, 1, 13, 21, 3));
    tc->append(TimestampValue::create(2019, 8, 1, 13, 21, 3));
    tc->append(TimestampValue::create(2019, 8, 1, 8, 21, 3));
    tc->append(TimestampValue::create(2019, 8, 1, 8, 21, 3));

    auto tc_from = BinaryColumn::create();
    tc_from->append(Slice("Asia/Shanghai"));
    tc_from->append(Slice("Asia/Urumqi"));
    tc_from->append(Slice("America/Los_Angeles"));
    tc_from->append(Slice("Asia/Shanghai"));
    tc_from->append(Slice("Asia/Shanghai"));

    auto tc_to = BinaryColumn::create();
    tc_to->append(Slice("America/Los_Angeles"));
    tc_to->append(Slice("America/Los_Angeles"));
    tc_to->append(Slice("Asia/Urumqi"));
    tc_to->append(Slice("UTC"));
    tc_to->append(Slice("+08:00"));

    TimestampValue res[] = {TimestampValue::create(2019, 7, 31, 22, 21, 3),
                            TimestampValue::create(2019, 8, 1, 0, 21, 3), TimestampValue::create(2019, 8, 2, 2, 21, 3),
                            TimestampValue::create(2019, 8, 1, 0, 21, 3), TimestampValue::create(2019, 8, 1, 8, 21, 3)};
    Columns columns;
    columns.emplace_back(tc);
    columns.emplace_back(tc_from);
    columns.emplace_back(tc_to);

    _utils->get_fn_ctx()->set_constant_columns(columns);

    ASSERT_TRUE(
            TimeFunctions::convert_tz_prepare(_utils->get_fn_ctx(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                    .ok());

    ColumnPtr result = TimeFunctions::convert_tz(_utils->get_fn_ctx(), columns).value();

    auto day_names = ColumnHelper::cast_to<TYPE_DATETIME>(result);
    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) ASSERT_EQ(res[i], day_names->get_data()[i]);

    ASSERT_TRUE(
            TimeFunctions::convert_tz_close(_utils->get_fn_ctx(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                    .ok());
}

TEST_F(TimeFunctionsTest, convertTzConstTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2019, 4, 7, 21, 21, 3));
    tc->append(TimestampValue::create(2019, 8, 1, 13, 8, 7));
    tc->append(TimestampValue::create(2019, 6, 18, 9, 13, 27));

    auto tc_from = ColumnHelper::create_const_column<TYPE_VARCHAR>("Asia/Urumqi", 1);

    const char* literal = "America/Los_Angeles";
    auto to_col = BinaryColumn::create();
    to_col->append(Slice(literal));
    to_col->get_bytes().emplace_back('X');
    to_col->get_bytes().resize(to_col->get_offset().back());
    auto tc_to = ConstColumn::create(std::move(to_col));

    TimestampValue res[] = {TimestampValue::create(2019, 4, 7, 8, 21, 3), TimestampValue::create(2019, 8, 1, 0, 8, 7),
                            TimestampValue::create(2019, 6, 17, 20, 13, 27)};
    Columns columns;
    columns.emplace_back(tc);
    columns.emplace_back(tc_from);
    columns.emplace_back(tc_to);

    _utils->get_fn_ctx()->set_constant_columns(columns);
    _utils->get_fn_ctx()->_arg_types.emplace_back(FunctionContext::TypeDesc{TYPE_DATETIME});
    _utils->get_fn_ctx()->_arg_types.emplace_back(FunctionContext::TypeDesc{TYPE_VARCHAR});
    _utils->get_fn_ctx()->_arg_types.emplace_back(FunctionContext::TypeDesc{TYPE_VARCHAR});

    ASSERT_TRUE(
            TimeFunctions::convert_tz_prepare(_utils->get_fn_ctx(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                    .ok());

    ColumnPtr result = TimeFunctions::convert_tz(_utils->get_fn_ctx(), columns).value();

    auto day_names = ColumnHelper::cast_to<TYPE_DATETIME>(result);
    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) ASSERT_EQ(res[i], day_names->get_data()[i]);

    ASSERT_TRUE(
            TimeFunctions::convert_tz_close(_utils->get_fn_ctx(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                    .ok());
}

TEST_F(TimeFunctionsTest, utctimestampTest) {
    {
        ColumnPtr ptr = TimeFunctions::utc_timestamp(_utils->get_fn_ctx(), Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_timestamp());
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);
        ASSERT_EQ("2019-08-06 08:38:57", v->get(0).get_timestamp().to_string());
    }

    {
        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        starrocks::RuntimeState state(globals);
        starrocks::FunctionUtils futils(&state);
        FunctionContext* ctx = futils.get_fn_ctx();
        ColumnPtr ptr = TimeFunctions::utc_timestamp(ctx, Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_timestamp());
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);
        ASSERT_EQ(TimestampValue::create(2019, 8, 6, 8, 38, 57), v->get(0).get_timestamp());
    }
}

TEST_F(TimeFunctionsTest, utctimeTest) {
    // without RuntimeState
    {
        ColumnPtr utc_timestamp = TimeFunctions::utc_timestamp(_utils->get_fn_ctx(), Columns()).value();
        ColumnPtr ptr = TimeFunctions::utc_time(_utils->get_fn_ctx(), Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_numeric());
        TimestampValue ts = ColumnHelper::as_column<ConstColumn>(utc_timestamp)->get(0).get_timestamp();
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);

        int h, m, s, us;
        ts.to_time(&h, &m, &s, &us);
        ASSERT_EQ(h * 3600 + m * 60 + s, v->get(0).get_double());
    }
    // with RuntimeState
    {
        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        starrocks::RuntimeState state(globals);
        starrocks::FunctionUtils futils(&state);
        FunctionContext* ctx = futils.get_fn_ctx();
        ColumnPtr ptr = TimeFunctions::utc_time(ctx, Columns()).value();
        ASSERT_TRUE(ptr->is_constant());
        ASSERT_FALSE(ptr->is_numeric());
        auto v = ColumnHelper::as_column<ConstColumn>(ptr);
        ASSERT_EQ(8 * 3600 + 38 * 60 + 57, v->get(0).get_double());
    }
}

TEST_F(TimeFunctionsTest, hourTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 21, 1, 1));
    tc->append(TimestampValue::create(2020, 2, 2, 14, 0, 1));
    tc->append(TimestampValue::create(2020, 3, 6, 11, 1, 1));
    tc->append(TimestampValue::create(2020, 4, 8, 9, 1, 1));
    tc->append(TimestampValue::create(2020, 5, 9, 8, 1, 1));
    tc->append(TimestampValue::create(2020, 11, 3, 23, 1, 1));

    int days[] = {21, 14, 11, 9, 8, 23};

    Columns columns;
    columns.emplace_back(tc);
    ColumnPtr result = TimeFunctions::hour(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto year_days = ColumnHelper::cast_to<TYPE_INT>(result);

    for (size_t i = 0; i < sizeof(days) / sizeof(days[0]); ++i) {
        ASSERT_EQ(days[i], year_days->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, minuteTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 21, 22, 1));
    tc->append(TimestampValue::create(2020, 2, 2, 14, 17, 1));
    tc->append(TimestampValue::create(2020, 3, 6, 11, 54, 1));
    tc->append(TimestampValue::create(2020, 4, 8, 9, 13, 1));
    tc->append(TimestampValue::create(2020, 5, 9, 8, 8, 1));
    tc->append(TimestampValue::create(2020, 11, 3, 23, 41, 1));

    int days[] = {22, 17, 54, 13, 8, 41};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::minute(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto year_days = ColumnHelper::cast_to<TYPE_INT>(result);

    for (size_t i = 0; i < sizeof(days) / sizeof(days[0]); ++i) {
        ASSERT_EQ(days[i], year_days->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, secondTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 21, 22, 51));
    tc->append(TimestampValue::create(2020, 2, 2, 14, 17, 28));
    tc->append(TimestampValue::create(2020, 3, 6, 11, 54, 23));
    tc->append(TimestampValue::create(2020, 4, 8, 9, 13, 19));
    tc->append(TimestampValue::create(2020, 5, 9, 8, 8, 16));
    tc->append(TimestampValue::create(2020, 11, 3, 23, 41, 37));

    int days[] = {51, 28, 23, 19, 16, 37};

    Columns columns;
    columns.emplace_back(tc);

    ColumnPtr result = TimeFunctions::second(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto year_days = ColumnHelper::cast_to<TYPE_INT>(result);

    for (size_t i = 0; i < sizeof(days) / sizeof(days[0]); ++i) {
        ASSERT_EQ(days[i], year_days->get_data()[i]);
    }
}

TEST_F(TimeFunctionsTest, timestampTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 21, 22, 51));

    //second
    {
        Columns columns;
        columns.emplace_back(tc);

        ColumnPtr result = TimeFunctions::timestamp(_utils->get_fn_ctx(), columns).value();

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[] = {TimestampValue::create(2020, 1, 1, 21, 22, 51)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }
}

TEST_F(TimeFunctionsTest, datetimeTruncTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(2020, 1, 1, 21, 22, 51));
    tc->append(TimestampValue::create(2020, 2, 2, 14, 17, 28));
    tc->append(TimestampValue::create(2020, 3, 6, 11, 54, 23));
    tc->append(TimestampValue::create(2020, 4, 8, 9, 13, 19));
    tc->append(TimestampValue::create(2020, 5, 9, 8, 8, 16));
    tc->append(TimestampValue::create(2020, 11, 3, 23, 41, 37));

    //second
    {
        auto text = BinaryColumn::create();
        text->append("second");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2020, 1, 1, 21, 22, 51), TimestampValue::create(2020, 2, 2, 14, 17, 28),
                TimestampValue::create(2020, 3, 6, 11, 54, 23), TimestampValue::create(2020, 4, 8, 9, 13, 19),
                TimestampValue::create(2020, 5, 9, 8, 8, 16),   TimestampValue::create(2020, 11, 3, 23, 41, 37)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //minute
    {
        auto text = BinaryColumn::create();
        text->append("minute");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2020, 1, 1, 21, 22, 0), TimestampValue::create(2020, 2, 2, 14, 17, 0),
                TimestampValue::create(2020, 3, 6, 11, 54, 0), TimestampValue::create(2020, 4, 8, 9, 13, 0),
                TimestampValue::create(2020, 5, 9, 8, 8, 0),   TimestampValue::create(2020, 11, 3, 23, 41, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //hour
    {
        auto text = BinaryColumn::create();
        text->append("hour");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2020, 1, 1, 21, 0, 0), TimestampValue::create(2020, 2, 2, 14, 0, 0),
                TimestampValue::create(2020, 3, 6, 11, 0, 0), TimestampValue::create(2020, 4, 8, 9, 0, 0),
                TimestampValue::create(2020, 5, 9, 8, 0, 0),  TimestampValue::create(2020, 11, 3, 23, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //day
    {
        auto text = BinaryColumn::create();
        text->append("day");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2020, 1, 1, 0, 0, 0), TimestampValue::create(2020, 2, 2, 0, 0, 0),
                TimestampValue::create(2020, 3, 6, 0, 0, 0), TimestampValue::create(2020, 4, 8, 0, 0, 0),
                TimestampValue::create(2020, 5, 9, 0, 0, 0), TimestampValue::create(2020, 11, 3, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //month
    {
        auto text = BinaryColumn::create();
        text->append("month");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2020, 1, 1, 0, 0, 0), TimestampValue::create(2020, 2, 1, 0, 0, 0),
                TimestampValue::create(2020, 3, 1, 0, 0, 0), TimestampValue::create(2020, 4, 1, 0, 0, 0),
                TimestampValue::create(2020, 5, 1, 0, 0, 0), TimestampValue::create(2020, 11, 1, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //year
    {
        auto text = BinaryColumn::create();
        text->append("year");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2020, 1, 1, 0, 0, 0), TimestampValue::create(2020, 1, 1, 0, 0, 0),
                TimestampValue::create(2020, 1, 1, 0, 0, 0), TimestampValue::create(2020, 1, 1, 0, 0, 0),
                TimestampValue::create(2020, 1, 1, 0, 0, 0), TimestampValue::create(2020, 1, 1, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //week
    {
        auto text = BinaryColumn::create();
        text->append("week");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2019, 12, 30, 0, 0, 0), TimestampValue::create(2020, 1, 27, 0, 0, 0),
                TimestampValue::create(2020, 3, 2, 0, 0, 0),   TimestampValue::create(2020, 4, 6, 0, 0, 0),
                TimestampValue::create(2020, 5, 4, 0, 0, 0),   TimestampValue::create(2020, 11, 2, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //quarter
    {
        auto text = BinaryColumn::create();
        text->append("quarter");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::datetime_trunc_prepare(_utils->get_fn_ctx(),
                                                          FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::datetime_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::datetime_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATETIME>(result);

        TimestampValue check_result[6] = {
                TimestampValue::create(2020, 1, 1, 0, 0, 0), TimestampValue::create(2020, 1, 1, 0, 0, 0),
                TimestampValue::create(2020, 1, 1, 0, 0, 0), TimestampValue::create(2020, 4, 1, 0, 0, 0),
                TimestampValue::create(2020, 4, 1, 0, 0, 0), TimestampValue::create(2020, 10, 1, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }
}

TEST_F(TimeFunctionsTest, dateTruncTest) {
    auto tc = DateColumn::create();
    tc->append(DateValue::create(2020, 1, 1));
    tc->append(DateValue::create(2020, 2, 2));
    tc->append(DateValue::create(2020, 3, 6));
    tc->append(DateValue::create(2020, 4, 8));
    tc->append(DateValue::create(2020, 5, 9));
    tc->append(DateValue::create(2020, 11, 3));

    //day
    {
        auto text = BinaryColumn::create();
        text->append("day");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::date_trunc_prepare(_utils->get_fn_ctx(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::date_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::date_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATE>(result);

        DateValue check_result[6] = {DateValue::create(2020, 1, 1), DateValue::create(2020, 2, 2),
                                     DateValue::create(2020, 3, 6), DateValue::create(2020, 4, 8),
                                     DateValue::create(2020, 5, 9), DateValue::create(2020, 11, 3)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //month
    {
        auto text = BinaryColumn::create();
        text->append("month");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::date_trunc_prepare(_utils->get_fn_ctx(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::date_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::date_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATE>(result);

        DateValue check_result[6] = {DateValue::create(2020, 1, 1), DateValue::create(2020, 2, 1),
                                     DateValue::create(2020, 3, 1), DateValue::create(2020, 4, 1),
                                     DateValue::create(2020, 5, 1), DateValue::create(2020, 11, 1)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //year
    {
        auto text = BinaryColumn::create();
        text->append("year");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::date_trunc_prepare(_utils->get_fn_ctx(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::date_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::date_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATE>(result);

        DateValue check_result[6] = {DateValue::create(2020, 1, 1), DateValue::create(2020, 1, 1),
                                     DateValue::create(2020, 1, 1), DateValue::create(2020, 1, 1),
                                     DateValue::create(2020, 1, 1), DateValue::create(2020, 1, 1)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //week
    {
        auto text = BinaryColumn::create();
        text->append("week");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::date_trunc_prepare(_utils->get_fn_ctx(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::date_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::date_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATE>(result);

        DateValue check_result[6] = {DateValue::create(2019, 12, 30), DateValue::create(2020, 1, 27),
                                     DateValue::create(2020, 3, 2),   DateValue::create(2020, 4, 6),
                                     DateValue::create(2020, 5, 4),   DateValue::create(2020, 11, 2)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //quarter
    {
        auto text = BinaryColumn::create();
        text->append("quarter");
        auto format = ConstColumn::create(text, 1);

        Columns columns;
        columns.emplace_back(format);
        columns.emplace_back(tc);

        _utils->get_fn_ctx()->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::date_trunc_prepare(_utils->get_fn_ctx(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::date_trunc(_utils->get_fn_ctx(), columns).value();
        ASSERT_TRUE(TimeFunctions::date_trunc_close(
                            _utils->get_fn_ctx(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        auto datetimes = ColumnHelper::cast_to<TYPE_DATE>(result);

        DateValue check_result[6] = {DateValue::create(2020, 1, 1), DateValue::create(2020, 1, 1),
                                     DateValue::create(2020, 1, 1), DateValue::create(2020, 4, 1),
                                     DateValue::create(2020, 4, 1), DateValue::create(2020, 10, 1)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }
}

TEST_F(TimeFunctionsTest, str2date) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    const char* str1 = "01,5,2013";
    const char* str2 = "2020-06-24 17:10:25";
    const char* fmt1 = "%d,%m,%Y";
    const char* fmt2 = "%Y-%m-%d %H:%i:%s";
    DateValue ts1 = DateValue::create(2013, 5, 1);
    DateValue ts2 = DateValue::create(2020, 6, 24);

    const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    // nullable
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_column(varchar_type_desc, true);
        str_col->append_datum(Slice(str1)); // str1 <=> fmt1
        fmt_col->append_datum(Slice(fmt1));
        str_col->append_datum(Slice(str2)); // str2 <=> fmt2
        fmt_col->append_datum(Slice(fmt2));
        (void)str_col->append_nulls(1); // null <=> fmt1
        fmt_col->append_datum(Slice(fmt1));
        str_col->append_datum(Slice(str1)); // str1 <=> null
        (void)fmt_col->append_nulls(1);
        (void)str_col->append_nulls(1); // null <=> null
        (void)fmt_col->append_nulls(1);

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);
        ColumnPtr result = TimeFunctions::str2date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(5, nullable_col->size());
        ASSERT_EQ(ts1, nullable_col->get(0).get_date());
        ASSERT_EQ(ts2, nullable_col->get(1).get_date());
        for (int i = 2; i < 5; ++i) {
            ASSERT_TRUE(nullable_col->is_null(i));
        }
    }
    // const
    {
        auto str_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(str1, 1);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);
        ColumnPtr result = TimeFunctions::str2date(ctx, columns).value();
        ASSERT_TRUE(result->is_constant());

        ConstColumn::Ptr const_col = ColumnHelper::as_column<ConstColumn>(result);
        ASSERT_FALSE(const_col->is_date());
        ASSERT_EQ(1, const_col->size());
        ASSERT_EQ(ts1, const_col->get(0).get_date());
    }
    // const <=> non-const
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        str_col->append_datum(Slice(str1));
        (void)str_col->append_nulls(1);
        str_col->append_datum(Slice("25,06,2020"));

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);

        ColumnPtr result = TimeFunctions::str2date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);
        ASSERT_EQ(3, nullable_col->size());
        ASSERT_EQ(ts1, nullable_col->get(0).get_date());
        ASSERT_TRUE(nullable_col->is_null(1));
        ASSERT_EQ(DateValue::create(2020, 6, 25), nullable_col->get(2).get_date());
    }
}

TEST_F(TimeFunctionsTest, str2date_of_dateformat) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    const char* str1 = "2013-05-01";
    const char* fmt1 = "%Y-%m-%d";
    DateValue ts1 = DateValue::create(2013, 5, 1);
    const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    // const <=> non-const
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        str_col->append_datum(Slice(str1));
        (void)str_col->append_nulls(1);
        str_col->append_datum(Slice("2020-06-25"));
        str_col->append_datum(Slice("     2020-03-12"));
        str_col->append_datum(Slice("   2020-03-12    11:35:23  "));
        str_col->append_datum(Slice("   2020-0  "));

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);

        ColumnPtr result = TimeFunctions::str2date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);

        ASSERT_EQ(ts1, nullable_col->get(0).get_date());
        ASSERT_TRUE(nullable_col->is_null(1));
        ASSERT_EQ(DateValue::create(2020, 6, 25), nullable_col->get(2).get_date());
        ASSERT_EQ(DateValue::create(2020, 3, 12), nullable_col->get(3).get_date());
        ASSERT_EQ(DateValue::create(2020, 3, 12), nullable_col->get(4).get_date());
        ASSERT_TRUE(nullable_col->is_null(5));
    }
}

TEST_F(TimeFunctionsTest, str2date_of_datetimeformat) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    const char* str1 = "2013-05-01 11:12:13";
    const char* fmt1 = "%Y-%m-%d %H:%i:%s";
    DateValue ts1 = DateValue::create(2013, 5, 1);
    [[maybe_unused]] DateValue ts2 = DateValue::create(2020, 6, 24);
    const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    // const <=> non-const
    {
        auto str_col = ColumnHelper::create_column(varchar_type_desc, true);
        auto fmt_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fmt1, 1);
        str_col->append_datum(Slice(str1));
        (void)str_col->append_nulls(1);
        str_col->append_datum(Slice("2020-06-25 12:05:39"));
        str_col->append_datum(Slice("     2020-03-12 08:19:39"));
        str_col->append_datum(Slice("   2020-03-12    11:35:23  "));
        str_col->append_datum(Slice("   2020-03-12    11:  "));

        Columns columns;
        columns.emplace_back(str_col);
        columns.emplace_back(fmt_col);

        ColumnPtr result = TimeFunctions::str2date(ctx, columns).value();
        ASSERT_TRUE(result->is_nullable());

        NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);

        ASSERT_EQ(ts1, nullable_col->get(0).get_date());
        ASSERT_TRUE(nullable_col->is_null(1));
        ASSERT_EQ(DateValue::create(2020, 6, 25), nullable_col->get(2).get_date());
        ASSERT_EQ(DateValue::create(2020, 3, 12), nullable_col->get(3).get_date());
        ASSERT_EQ(DateValue::create(2020, 3, 12), nullable_col->get(4).get_date());
        ASSERT_EQ(DateValue::create(2020, 3, 12), nullable_col->get(5).get_date());
    }
}

TEST_F(TimeFunctionsTest, timeSliceFloorTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(0001, 1, 1, 21, 22, 51));
    tc->append(TimestampValue::create(0001, 3, 2, 14, 17, 28));
    tc->append(TimestampValue::create(0001, 5, 6, 11, 54, 23));
    tc->append(TimestampValue::create(2022, 7, 8, 9, 13, 19));
    tc->append(TimestampValue::create(2022, 9, 9, 8, 8, 16));
    tc->append(TimestampValue::create(2022, 11, 3, 23, 41, 37));

    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATETIME))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATETIME));
    std::unique_ptr<FunctionContext> time_slice_context(
            FunctionContext::create_test_context(std::move(arg_types), return_type));

    //second
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("second");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 21, 22, 50), TimestampValue::create(0001, 3, 2, 14, 17, 25),
                TimestampValue::create(0001, 5, 6, 11, 54, 20), TimestampValue::create(2022, 7, 8, 9, 13, 15),
                TimestampValue::create(2022, 9, 9, 8, 8, 15),   TimestampValue::create(2022, 11, 3, 23, 41, 35)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //minute
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("minute");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 21, 20, 0), TimestampValue::create(0001, 3, 2, 14, 15, 0),
                TimestampValue::create(0001, 5, 6, 11, 50, 0), TimestampValue::create(2022, 7, 8, 9, 10, 0),
                TimestampValue::create(2022, 9, 9, 8, 5, 0),   TimestampValue::create(2022, 11, 3, 23, 40, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //hour
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("hour");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 20, 0, 0), TimestampValue::create(0001, 3, 2, 10, 0, 0),
                TimestampValue::create(0001, 5, 6, 10, 0, 0), TimestampValue::create(2022, 7, 8, 8, 0, 0),
                TimestampValue::create(2022, 9, 9, 6, 0, 0),  TimestampValue::create(2022, 11, 3, 21, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //day
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("day");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 0, 0, 0), TimestampValue::create(0001, 3, 2, 0, 0, 0),
                TimestampValue::create(0001, 5, 6, 0, 0, 0), TimestampValue::create(2022, 7, 5, 0, 0, 0),
                TimestampValue::create(2022, 9, 8, 0, 0, 0), TimestampValue::create(2022, 11, 2, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //month
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("month");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 0, 0, 0), TimestampValue::create(0001, 1, 1, 0, 0, 0),
                TimestampValue::create(0001, 1, 1, 0, 0, 0), TimestampValue::create(2022, 4, 1, 0, 0, 0),
                TimestampValue::create(2022, 9, 1, 0, 0, 0), TimestampValue::create(2022, 9, 1, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //year
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("year");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 0, 0, 0), TimestampValue::create(0001, 1, 1, 0, 0, 0),
                TimestampValue::create(0001, 1, 1, 0, 0, 0), TimestampValue::create(2021, 1, 1, 0, 0, 0),
                TimestampValue::create(2021, 1, 1, 0, 0, 0), TimestampValue::create(2021, 1, 1, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //week
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("week");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 0, 0, 0),  TimestampValue::create(0001, 2, 5, 0, 0, 0),
                TimestampValue::create(0001, 4, 16, 0, 0, 0), TimestampValue::create(2022, 6, 20, 0, 0, 0),
                TimestampValue::create(2022, 8, 29, 0, 0, 0), TimestampValue::create(2022, 10, 3, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //quarter
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("quarter");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 0, 0, 0), TimestampValue::create(0001, 1, 1, 0, 0, 0),
                TimestampValue::create(0001, 1, 1, 0, 0, 0), TimestampValue::create(2022, 4, 1, 0, 0, 0),
                TimestampValue::create(2022, 4, 1, 0, 0, 0), TimestampValue::create(2022, 4, 1, 0, 0, 0)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }
}

TEST_F(TimeFunctionsTest, timeSliceCeilTest) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(0001, 1, 1, 21, 22, 51));
    tc->append(TimestampValue::create(0001, 3, 2, 14, 17, 28));
    tc->append(TimestampValue::create(0001, 5, 6, 11, 54, 23));
    tc->append(TimestampValue::create(2022, 7, 8, 9, 13, 19));
    tc->append(TimestampValue::create(2022, 9, 9, 8, 8, 16));
    tc->append(TimestampValue::create(2022, 11, 3, 23, 41, 37));

    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATETIME))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATETIME));
    std::unique_ptr<FunctionContext> time_slice_context(
            FunctionContext::create_test_context(std::move(arg_types), return_type));

    //second
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("second");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("ceil");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATETIME>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        TimestampValue check_result[6] = {
                TimestampValue::create(0001, 1, 1, 21, 22, 55), TimestampValue::create(0001, 3, 2, 14, 17, 30),
                TimestampValue::create(0001, 5, 6, 11, 54, 25), TimestampValue::create(2022, 7, 8, 9, 13, 20),
                TimestampValue::create(2022, 9, 9, 8, 8, 20),   TimestampValue::create(2022, 11, 3, 23, 41, 40)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }
}

TEST_F(TimeFunctionsTest, timeSliceTestWithThrowExceptions) {
    auto tc = TimestampColumn::create();
    tc->append(TimestampValue::create(0000, 1, 1, 0, 0, 0));

    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATETIME))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATETIME));
    std::unique_ptr<FunctionContext> time_slice_context(
            FunctionContext::create_test_context(std::move(arg_types), return_type));

    //second
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("second");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        StatusOr<ColumnPtr> result = TimeFunctions::time_slice(time_slice_context.get(), columns);
        ASSERT_TRUE(result.status().is_invalid_argument());
        ASSERT_EQ(result.status().get_error_msg(), "time used with time_slice can't before 0001-01-01 00:00:00");

        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    }
}

TEST_F(TimeFunctionsTest, DateSliceFloorTest) {
    auto tc = DateColumn::create();
    tc->append(DateValue::create(0001, 1, 1));
    tc->append(DateValue::create(0001, 3, 2));
    tc->append(DateValue::create(0001, 5, 6));
    tc->append(DateValue::create(2022, 7, 8));
    tc->append(DateValue::create(2022, 9, 9));
    tc->append(DateValue::create(2022, 11, 3));

    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATE))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATE));
    std::unique_ptr<FunctionContext> time_slice_context(
            FunctionContext::create_test_context(std::move(arg_types), return_type));

    //day
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("day");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATE>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        DateValue check_result[6] = {DateValue::create(0001, 1, 1), DateValue::create(0001, 3, 2),
                                     DateValue::create(0001, 5, 6), DateValue::create(2022, 7, 5),
                                     DateValue::create(2022, 9, 8), DateValue::create(2022, 11, 2)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //month
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("month");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATE>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        DateValue check_result[6] = {DateValue::create(0001, 1, 1), DateValue::create(0001, 1, 1),
                                     DateValue::create(0001, 1, 1), DateValue::create(2022, 4, 1),
                                     DateValue::create(2022, 9, 1), DateValue::create(2022, 9, 1)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //year
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("year");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATE>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        DateValue check_result[6] = {DateValue::create(0001, 1, 1), DateValue::create(0001, 1, 1),
                                     DateValue::create(0001, 1, 1), DateValue::create(2021, 1, 1),
                                     DateValue::create(2021, 1, 1), DateValue::create(2021, 1, 1)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //week
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("week");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATE>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        DateValue check_result[6] = {DateValue::create(0001, 1, 1),  DateValue::create(0001, 2, 5),
                                     DateValue::create(0001, 4, 16), DateValue::create(2022, 6, 20),
                                     DateValue::create(2022, 8, 29), DateValue::create(2022, 10, 3)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }

    //quarter
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("quarter");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("floor");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATE>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        DateValue check_result[6] = {DateValue::create(0001, 1, 1), DateValue::create(0001, 1, 1),
                                     DateValue::create(0001, 1, 1), DateValue::create(2022, 4, 1),
                                     DateValue::create(2022, 4, 1), DateValue::create(2022, 4, 1)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }
}

TEST_F(TimeFunctionsTest, DateSliceCeilTest) {
    auto tc = DateColumn::create();
    tc->append(DateValue::create(0001, 1, 1));
    tc->append(DateValue::create(0001, 3, 2));
    tc->append(DateValue::create(0001, 5, 6));
    tc->append(DateValue::create(2022, 7, 8));
    tc->append(DateValue::create(2022, 9, 9));
    tc->append(DateValue::create(2022, 11, 3));

    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATE))};
    auto return_type = AnyValUtil::column_type_to_type_desc(TypeDescriptor::from_logical_type(TYPE_DATE));
    std::unique_ptr<FunctionContext> time_slice_context(
            FunctionContext::create_test_context(std::move(arg_types), return_type));

    //day
    {
        auto period_value = Int32Column::create();
        period_value->append(5);
        auto period_column = ConstColumn::create(period_value, 1);

        auto unit_text = BinaryColumn::create();
        unit_text->append("day");
        auto unit_column = ConstColumn::create(unit_text, 1);

        auto boundary_text = BinaryColumn::create();
        boundary_text->append("ceil");
        auto boundary_column = ConstColumn::create(boundary_text, 1);

        Columns columns;
        columns.emplace_back(tc);
        columns.emplace_back(period_column);
        columns.emplace_back(unit_column);
        columns.emplace_back(boundary_column);

        time_slice_context->set_constant_columns(columns);

        ASSERT_TRUE(TimeFunctions::time_slice_prepare(time_slice_context.get(),
                                                      FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());

        ColumnPtr result = TimeFunctions::time_slice(time_slice_context.get(), columns).value();
        ASSERT_TRUE(
                TimeFunctions::time_slice_close(time_slice_context.get(),
                                                FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        auto datetimes =
                ColumnHelper::cast_to<TYPE_DATE>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        DateValue check_result[6] = {DateValue::create(0001, 1, 6),  DateValue::create(0001, 3, 7),
                                     DateValue::create(0001, 5, 11), DateValue::create(2022, 7, 10),
                                     DateValue::create(2022, 9, 13), DateValue::create(2022, 11, 7)};

        for (size_t i = 0; i < sizeof(check_result) / sizeof(check_result[0]); ++i) {
            ASSERT_EQ(check_result[i], datetimes->get_data()[i]);
        }
    }
}

TEST_F(TimeFunctionsTest, MakeDateTest) {
    auto year_value = Int32Column::create();
    auto day_of_year_value = Int32Column::create();

    year_value->append(0);
    day_of_year_value->append(1);

    year_value->append(2023);
    day_of_year_value->append(0);

    year_value->append(2023);
    day_of_year_value->append(32);

    year_value->append(2023);
    day_of_year_value->append(365);

    year_value->append(2023);
    day_of_year_value->append(366);

    year_value->append(9999);
    day_of_year_value->append(1);

    year_value->append(9999);
    day_of_year_value->append(365);

    year_value->append(9999);
    day_of_year_value->append(366);

    year_value->append(10000);
    day_of_year_value->append(1);

    year_value->append(1);
    day_of_year_value->append(-1);

    year_value->append(1);
    (void)day_of_year_value->append_nulls(1);

    Columns columns;
    columns.emplace_back(year_value);
    columns.emplace_back(day_of_year_value);

    ColumnPtr result = TimeFunctions::make_date(_utils->get_fn_ctx(), columns).value();
    ASSERT_TRUE(result->is_nullable());

    NullableColumn::Ptr nullable_col = ColumnHelper::as_column<NullableColumn>(result);

    ASSERT_EQ(DateValue::create(0000, 1, 1), nullable_col->get(0).get_date());
    ASSERT_TRUE(nullable_col->is_null(1));
    ASSERT_EQ(DateValue::create(2023, 2, 1), nullable_col->get(2).get_date());
    ASSERT_EQ(DateValue::create(2023, 12, 31), nullable_col->get(3).get_date());
    ASSERT_TRUE(nullable_col->is_null(4));
    ASSERT_EQ(DateValue::create(9999, 1, 1), nullable_col->get(5).get_date());
    ASSERT_EQ(DateValue::create(9999, 12, 31), nullable_col->get(6).get_date());
    ASSERT_TRUE(nullable_col->is_null(7));
    ASSERT_TRUE(nullable_col->is_null(8));
    ASSERT_TRUE(nullable_col->is_null(9));
    ASSERT_TRUE(nullable_col->is_null(10));
}
} // namespace starrocks

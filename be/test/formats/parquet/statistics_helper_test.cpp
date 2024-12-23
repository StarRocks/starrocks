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

#include "formats/parquet/statistics_helper.h"

#include <gtest/gtest.h>

#include "formats/parquet/parquet_ut_base.h"
#include "formats/parquet/schema.h"
#include "testutil/assert.h"

namespace starrocks::parquet {

class StatisticsHelperTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }
    void TearDown() override {}

    template <typename T>
    std::string int_to_string(T value) {
        char chararray[sizeof(T)];
        memcpy(chararray, &value, sizeof(T));
        return std::string(chararray, sizeof(T));
    }

protected:
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
};

TEST_F(StatisticsHelperTest, TestInFilterInt) {
    std::string timezone = "Asia/Shanghai";
    ParquetField field;
    field.physical_type = tparquet::Type::type::INT32;
    std::vector<std::string> min_values;
    min_values.emplace_back(int_to_string<int32_t>(1));
    min_values.emplace_back(int_to_string<int32_t>(4));
    std::vector<std::string> max_values;
    max_values.emplace_back(int_to_string<int32_t>(4));
    max_values.emplace_back(int_to_string<int32_t>(6));

    std::set<int32_t> in_oprands{2, 3, 7};
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_in_predicate_int_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, in_oprands, &t_conjuncts);
    EXPECT_EQ(t_conjuncts.size(), 1);
    std::vector<ExprContext*> ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctxs);
    EXPECT_EQ(ctxs.size(), 1);

    std::vector<int64_t> null_counts{0, 0};
    std::vector<bool> null_pages{false, false};
    Filter selected(min_values.size(), true);
    auto st = StatisticsHelper::in_filter_on_min_max_stat(min_values, max_values, null_pages, null_counts, ctxs[0],
                                                          &field, timezone, selected);
    ASSERT_OK(st);
    ASSERT_TRUE(selected[0]);
    ASSERT_FALSE(selected[1]);
}

TEST_F(StatisticsHelperTest, TestInFilterString) {
    std::string timezone = "Asia/Shanghai";
    ParquetField field;
    field.physical_type = tparquet::Type::type::BYTE_ARRAY;
    std::vector<std::string> min_values{"abc"};
    std::vector<std::string> max_values{"def"};

    {
        std::set<std::string> in_oprands{"a", "ab"};
        std::vector<TExpr> t_conjuncts;
        ParquetUTBase::create_in_predicate_string_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, in_oprands, &t_conjuncts);
        EXPECT_EQ(t_conjuncts.size(), 1);
        std::vector<ExprContext*> ctxs;
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctxs);
        EXPECT_EQ(ctxs.size(), 1);

        std::vector<int64_t> null_counts{0};
        std::vector<bool> null_pages{false};
        Filter selected(min_values.size(), true);
        auto st = StatisticsHelper::in_filter_on_min_max_stat(min_values, max_values, null_pages, null_counts, ctxs[0],
                                                              &field, timezone, selected);
        ASSERT_OK(st);
        ASSERT_FALSE(selected[0]);
    }

    {
        std::set<std::string> in_oprands{"ac", "de"};
        std::vector<TExpr> t_conjuncts;
        ParquetUTBase::create_in_predicate_string_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, in_oprands, &t_conjuncts);
        EXPECT_EQ(t_conjuncts.size(), 1);
        std::vector<ExprContext*> ctxs;
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctxs);
        EXPECT_EQ(ctxs.size(), 1);

        std::vector<int64_t> null_counts{0};
        std::vector<bool> null_pages{false};
        Filter selected(min_values.size(), true);
        auto st = StatisticsHelper::in_filter_on_min_max_stat(min_values, max_values, null_pages, null_counts, ctxs[0],
                                                              &field, timezone, selected);
        ASSERT_OK(st);
        ASSERT_TRUE(selected[0]);
    }
}

TEST_F(StatisticsHelperTest, TestInFilterDate) {
    std::string timezone = "Asia/Shanghai";
    ParquetField field;
    field.physical_type = tparquet::Type::type::INT32;
    std::vector<std::string> min_values;
    // 2020-01-01, 2021-01-01
    min_values.emplace_back(int_to_string<int32_t>(18262));
    min_values.emplace_back(int_to_string<int32_t>(18628));
    std::vector<std::string> max_values;
    // 2020-12-31, 2021-12-31
    max_values.emplace_back(int_to_string<int32_t>(18627));
    max_values.emplace_back(int_to_string<int32_t>(18992));

    std::set<std::string> in_oprands{"2020-01-01", "2022-01-01"};
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_in_predicate_date_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, TPrimitiveType::DATE, in_oprands,
                                                          &t_conjuncts);
    EXPECT_EQ(t_conjuncts.size(), 1);
    std::vector<ExprContext*> ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctxs);
    EXPECT_EQ(ctxs.size(), 1);

    std::vector<int64_t> null_counts{0, 0};
    std::vector<bool> null_pages{false, false};
    Filter selected(min_values.size(), true);
    auto st = StatisticsHelper::in_filter_on_min_max_stat(min_values, max_values, null_pages, null_counts, ctxs[0],
                                                          &field, timezone, selected);
    ASSERT_OK(st);
    ASSERT_TRUE(selected[0]);
    ASSERT_FALSE(selected[1]);
}

TEST_F(StatisticsHelperTest, TestInFilterDatetime) {
    std::string timezone = "Asia/Shanghai";
    ParquetField field;
    field.physical_type = tparquet::Type::type::INT64;
    field.schema_element.type = tparquet::Type::INT64;
    field.schema_element.__isset.logicalType = true;
    field.schema_element.logicalType.__isset.TIMESTAMP = true;
    field.schema_element.logicalType.TIMESTAMP.isAdjustedToUTC = true;
    field.schema_element.logicalType.TIMESTAMP.unit.__isset.MILLIS = true;

    std::vector<std::string> min_values;
    // 2020-01-01 00:00:00, 2021-01-01 00:00:00
    min_values.emplace_back(int_to_string<int64_t>(1577808000000));
    min_values.emplace_back(int_to_string<int64_t>(1609430400000));
    std::vector<std::string> max_values;
    // 2020-12-31 23:59:59, 2021-12-31 23:59:59
    max_values.emplace_back(int_to_string<int64_t>(1609430399000));
    max_values.emplace_back(int_to_string<int64_t>(1640966399000));

    std::set<std::string> in_oprands{"2020-01-01 00:00:00", "2022-01-01 00:00:00"};
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::create_in_predicate_date_conjunct_ctxs(TExprOpcode::FILTER_IN, 0, TPrimitiveType::DATETIME,
                                                          in_oprands, &t_conjuncts);
    EXPECT_EQ(t_conjuncts.size(), 1);
    std::vector<ExprContext*> ctxs;
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctxs);
    EXPECT_EQ(ctxs.size(), 1);

    std::vector<int64_t> null_counts{0, 0};
    std::vector<bool> null_pages{false, false};
    Filter selected(min_values.size(), true);
    auto st = StatisticsHelper::in_filter_on_min_max_stat(min_values, max_values, null_pages, null_counts, ctxs[0],
                                                          &field, timezone, selected);
    ASSERT_OK(st);
    ASSERT_TRUE(selected[0]);
    ASSERT_FALSE(selected[1]);
}

} // namespace starrocks::parquet

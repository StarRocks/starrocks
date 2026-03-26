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

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "types/logical_type.h"

namespace starrocks::connector {

std::vector<std::string> rewrite_oracle_datetime_filters_for_test(
        const std::vector<std::string>& filters, const std::vector<std::pair<std::string, LogicalType>>& columns);

TEST(JDBCConnectorTest, RewriteDateColumnWithNLSFallback) {
    std::vector<std::string> filters = {"date_col = '2022-01-1'"};
    std::vector<std::pair<std::string, LogicalType>> columns = {{"date_col", TYPE_DATE}};

    auto rewritten = rewrite_oracle_datetime_filters_for_test(filters, columns);
    ASSERT_EQ(1, rewritten.size());
    EXPECT_EQ("date_col = TO_DATE('2022-01-1')", rewritten[0]);
}

TEST(JDBCConnectorTest, RewriteDatetimeColumnWithMicroseconds) {
    std::vector<std::string> filters = {"ts_col = '2026-03-12 09:30:15.123456'"};
    std::vector<std::pair<std::string, LogicalType>> columns = {{"ts_col", TYPE_DATETIME}};

    auto rewritten = rewrite_oracle_datetime_filters_for_test(filters, columns);
    ASSERT_EQ(1, rewritten.size());
    EXPECT_EQ("ts_col = TO_TIMESTAMP('2026-03-12 09:30:15.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')", rewritten[0]);
}

TEST(JDBCConnectorTest, RewriteTemporalVarcharColumnByNameInference) {
    std::vector<std::string> filters = {"tstz_col = '2026-03-12 09:30:15.123456'"};
    std::vector<std::pair<std::string, LogicalType>> columns = {{"tstz_col", TYPE_VARCHAR}};

    auto rewritten = rewrite_oracle_datetime_filters_for_test(filters, columns);
    ASSERT_EQ(1, rewritten.size());
    EXPECT_EQ("tstz_col = TO_TIMESTAMP('2026-03-12 09:30:15.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')", rewritten[0]);
}

TEST(JDBCConnectorTest, KeepNonTemporalVarcharColumnUnchanged) {
    std::vector<std::string> filters = {"note = '2026-03-12 09:30:15.123456'"};
    std::vector<std::pair<std::string, LogicalType>> columns = {{"note", TYPE_VARCHAR}};

    auto rewritten = rewrite_oracle_datetime_filters_for_test(filters, columns);
    ASSERT_EQ(1, rewritten.size());
    EXPECT_EQ("note = '2026-03-12 09:30:15.123456'", rewritten[0]);
}

TEST(JDBCConnectorTest, RewriteLiteralColumnPredicate) {
    std::vector<std::string> filters = {"'2026-03-12 09:30:15' = ts_col"};
    std::vector<std::pair<std::string, LogicalType>> columns = {{"ts_col", TYPE_DATETIME}};

    auto rewritten = rewrite_oracle_datetime_filters_for_test(filters, columns);
    ASSERT_EQ(1, rewritten.size());
    EXPECT_EQ("TO_TIMESTAMP('2026-03-12 09:30:15', 'YYYY-MM-DD HH24:MI:SS') = ts_col", rewritten[0]);
}

} // namespace starrocks::connector

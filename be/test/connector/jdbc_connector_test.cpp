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

#include "connector/jdbc_connector.h"

#include <gtest/gtest.h>

namespace starrocks::connector {

TEST(JdbcConnectorTest, resolve_jdbc_scan_sql_prefers_fe_sql) {
    TJDBCScanNode node;
    node.__set_sql("SELECT c1, c2 FROM t1 WHERE c1 = 1 LIMIT 1");
    Slice url("jdbc:mysql://localhost:3306");
    EXPECT_EQ(resolve_jdbc_scan_sql(node, url, 99), "SELECT c1, c2 FROM t1 WHERE c1 = 1 LIMIT 1");
}

TEST(JdbcConnectorTest, resolve_jdbc_scan_sql_fallback_mysql_limit) {
    TJDBCScanNode node;
    node.__set_table_name("t");
    node.__set_columns(std::vector<std::string>{"c1"});
    Slice url("jdbc:mysql://localhost:3306");
    EXPECT_EQ(resolve_jdbc_scan_sql(node, url, 5), "SELECT c1 FROM t LIMIT 5");
}

TEST(JdbcConnectorTest, resolve_jdbc_scan_sql_fallback_sqlserver_top) {
    TJDBCScanNode node;
    node.__set_table_name("t");
    node.__set_columns(std::vector<std::string>{"c1"});
    Slice url("jdbc:sqlserver://localhost:1433;");
    EXPECT_EQ(resolve_jdbc_scan_sql(node, url, 10), "SELECT TOP(10) c1 FROM t");
}

TEST(JdbcConnectorTest, resolve_jdbc_scan_sql_fallback_oracle_rownum) {
    TJDBCScanNode node;
    node.__set_table_name("t");
    node.__set_columns(std::vector<std::string>{"c1"});
    Slice url("jdbc:oracle:thin:@localhost:1521:orcl");
    EXPECT_EQ(resolve_jdbc_scan_sql(node, url, 3), "SELECT * FROM (SELECT c1 FROM t) WHERE ROWNUM <= 3");
}

} // namespace starrocks::connector

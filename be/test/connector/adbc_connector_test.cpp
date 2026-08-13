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

#include "connector/adbc_connector.h"

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace starrocks::connector {

// Forward-declare the free function defined in adbc_connector.cpp for testing.
std::string get_adbc_sql(const std::string& table, const std::vector<std::string>& columns,
                         const std::vector<std::string>& filters, int64_t limit);

class ADBCConnectorTest : public ::testing::Test {};

TEST_F(ADBCConnectorTest, ConnectorType) {
    ADBCConnector connector;
    EXPECT_EQ(connector.connector_type(), ConnectorType::ADBC_CONN);
}

TEST_F(ADBCConnectorTest, DataSourceName) {
    TScanRange scan_range;
    ADBCDataSource ds(nullptr, scan_range);
    EXPECT_EQ(ds.name(), "ADBCDataSource");
}

TEST_F(ADBCConnectorTest, DriverManagerUsesLogicalNameDiscovery) {
    AdbcDatabase database{};
    AdbcError error = ADBC_ERROR_INIT;
    ASSERT_EQ(ADBC_STATUS_OK, AdbcDatabaseNew(&database, &error));
    ASSERT_EQ(ADBC_STATUS_OK, AdbcDriverManagerDatabaseSetLoadFlags(&database, ADBC_LOAD_FLAG_DEFAULT, &error));

    constexpr const char* kMissingDriver = "starrocks_adbc_missing_driver_for_test";
    ASSERT_EQ(ADBC_STATUS_OK, AdbcDatabaseSetOption(&database, "driver", kMissingDriver, &error));

    AdbcStatusCode status = AdbcDatabaseInit(&database, &error);
    EXPECT_NE(ADBC_STATUS_OK, status);
    std::string message = error.message == nullptr ? "" : error.message;
    EXPECT_FALSE(message.empty());
    EXPECT_NE(std::string::npos, message.find(kMissingDriver));
    if (error.release != nullptr) {
        error.release(&error);
    }

    error = ADBC_ERROR_INIT;
    EXPECT_EQ(ADBC_STATUS_OK, AdbcDatabaseRelease(&database, &error));
    if (error.release != nullptr) {
        error.release(&error);
    }
}

TEST_F(ADBCConnectorTest, SqlAssemblyBasic) {
    std::vector<std::string> columns = {"col1", "col2", "col3"};
    std::vector<std::string> filters;
    std::string sql = get_adbc_sql("schema1.table1", columns, filters, -1);
    EXPECT_EQ(sql, "SELECT col1, col2, col3 FROM schema1.table1");
}

TEST_F(ADBCConnectorTest, SqlAssemblyWithFilters) {
    std::vector<std::string> columns = {"id", "name"};
    std::vector<std::string> filters = {"id > 10", "name = 'test'"};
    std::string sql = get_adbc_sql("db.users", columns, filters, -1);
    EXPECT_EQ(sql, "SELECT id, name FROM db.users WHERE (id > 10) AND (name = 'test')");
}

TEST_F(ADBCConnectorTest, SqlAssemblyWithLimit) {
    std::vector<std::string> columns = {"*"};
    std::vector<std::string> filters;
    std::string sql = get_adbc_sql("t", columns, filters, 100);
    EXPECT_EQ(sql, "SELECT * FROM t LIMIT 100");
}

TEST_F(ADBCConnectorTest, SqlAssemblyWithFiltersAndLimit) {
    std::vector<std::string> columns = {"a", "b"};
    std::vector<std::string> filters = {"a > 0"};
    std::string sql = get_adbc_sql("schema.tbl", columns, filters, 5);
    EXPECT_EQ(sql, "SELECT a, b FROM schema.tbl WHERE (a > 0) LIMIT 5");
}

TEST_F(ADBCConnectorTest, SqlAssemblySingleColumn) {
    std::vector<std::string> columns = {"col1"};
    std::vector<std::string> filters;
    std::string sql = get_adbc_sql("t", columns, filters, -1);
    EXPECT_EQ(sql, "SELECT col1 FROM t");
}

} // namespace starrocks::connector

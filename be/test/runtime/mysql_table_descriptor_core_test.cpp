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

#include "runtime/descriptors.h"

namespace starrocks {

namespace {

TTableDescriptor make_mysql_table_descriptor() {
    TMySQLTable mysql_table;
    mysql_table.__set_host("127.0.0.1");
    mysql_table.__set_port("3306");
    mysql_table.__set_user("root");
    mysql_table.__set_passwd("secret");
    mysql_table.__set_db("mysql_db");
    mysql_table.__set_table("mysql_table");

    TTableDescriptor table_desc;
    table_desc.__set_id(1);
    table_desc.__set_tableType(TTableType::MYSQL_TABLE);
    table_desc.__set_numCols(1);
    table_desc.__set_numClusteringCols(0);
    table_desc.__set_tableName("mysql_table");
    table_desc.__set_dbName("mysql_db");
    table_desc.__set_mysqlTable(mysql_table);
    return table_desc;
}

} // namespace

TEST(MySQLTableDescriptorCoreTest, KeepsConnectionFieldsInRuntimeCore) {
    MySQLTableDescriptor descriptor(make_mysql_table_descriptor());

    EXPECT_EQ("mysql_db", descriptor.mysql_db());
    EXPECT_EQ("mysql_table", descriptor.mysql_table());
    EXPECT_EQ("127.0.0.1", descriptor.host());
    EXPECT_EQ("3306", descriptor.port());
    EXPECT_EQ("root", descriptor.user());
    EXPECT_EQ("secret", descriptor.passwd());
    EXPECT_NE(std::string::npos, descriptor.debug_string().find("MySQLTable"));
}

} // namespace starrocks

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
package com.starrocks.sql.ast;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.SlotRef;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateTableStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testOrderBy() throws Exception {
        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` string,\n" +
                "    `tag_name` string\n" +
                ") ENGINE = OLAP PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "ORDER BY(`id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";

        CreateTableStmt stmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, 32).get(0);

        Assertions.assertEquals(stmt.getDbName(), "test_create_table_db");
        Assertions.assertEquals(stmt.getTableName(), "starrocks_test_table");
        Assertions.assertEquals(stmt.getProperties().get("replication_num"), "1");

        Assertions.assertEquals(stmt.getOrderByElements().size(), 1);
        OrderByElement orderByElement = stmt.getOrderByElements().get(0);
        SlotRef slotRef = (SlotRef) orderByElement.getExpr();
        Assertions.assertEquals(slotRef.getColumnName(), "id");
        Assertions.assertTrue(orderByElement.getIsAsc());
        // If ascending, nulls are first by default
        Assertions.assertTrue(orderByElement.getNullsFirstParam());
    }

    @Test
    public void testOrderByWithDirection() throws Exception {
        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` string,\n" +
                "    `tag_name` string\n" +
                ") ENGINE = OLAP PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "ORDER BY (`id` DESC)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";

        CreateTableStmt stmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, 32).get(0);

        Assertions.assertEquals(stmt.getDbName(), "test_create_table_db");
        Assertions.assertEquals(stmt.getTableName(), "starrocks_test_table");
        Assertions.assertEquals(stmt.getProperties().get("replication_num"), "1");

        Assertions.assertEquals(stmt.getOrderByElements().size(), 1);
        OrderByElement orderByElement = stmt.getOrderByElements().get(0);
        SlotRef slotRef = (SlotRef) orderByElement.getExpr();
        Assertions.assertEquals(slotRef.getColumnName(), "id");
        Assertions.assertFalse(orderByElement.getIsAsc());
        // If descending, nulls are last by default
        Assertions.assertFalse(orderByElement.getNullsFirstParam());
    }

    @Test
    public void testOrderByWithNullOrder() throws Exception {
        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` string,\n" +
                "    `tag_name` string\n" +
                ") ENGINE = OLAP PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "ORDER BY (`id` DESC NULLS FIRST)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";

        CreateTableStmt stmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, 32).get(0);

        Assertions.assertEquals(stmt.getDbName(), "test_create_table_db");
        Assertions.assertEquals(stmt.getTableName(), "starrocks_test_table");
        Assertions.assertEquals(stmt.getProperties().get("replication_num"), "1");

        Assertions.assertEquals(stmt.getOrderByElements().size(), 1);
        OrderByElement orderByElement = stmt.getOrderByElements().get(0);
        SlotRef slotRef = (SlotRef) orderByElement.getExpr();
        Assertions.assertEquals(slotRef.getColumnName(), "id");
        Assertions.assertFalse(orderByElement.getIsAsc());
        Assertions.assertTrue(orderByElement.getNullsFirstParam());
    }

    @Test
    public void testOrderByMultipleColumns() throws Exception {
        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` string,\n" +
                "    `tag_name` string\n" +
                ") ENGINE = OLAP PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "ORDER BY (`id`, `tag_name` DESC NULLS FIRST)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";

        CreateTableStmt stmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser.parse(
                sql, 32).get(0);

        Assertions.assertEquals(stmt.getDbName(), "test_create_table_db");
        Assertions.assertEquals(stmt.getTableName(), "starrocks_test_table");
        Assertions.assertEquals(stmt.getProperties().get("replication_num"), "1");

        Assertions.assertEquals(stmt.getOrderByElements().size(), 2);
        OrderByElement orderByElement = stmt.getOrderByElements().get(1);
        SlotRef slotRef = (SlotRef) orderByElement.getExpr();
        Assertions.assertEquals(slotRef.getColumnName(), "tag_name");
        Assertions.assertFalse(orderByElement.getIsAsc());
        Assertions.assertTrue(orderByElement.getNullsFirstParam());
    }
}

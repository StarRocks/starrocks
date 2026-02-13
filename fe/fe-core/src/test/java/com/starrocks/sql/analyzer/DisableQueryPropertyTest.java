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
//

package com.starrocks.sql.analyzer;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for table property "disable_query".
 *
 * 1. SHOW CREATE TABLE should display "disable_query" in PROPERTIES only when it's true (default is false).
 * 2. ALTER TABLE ... SET ("disable_query" = "true") should succeed and be reflected in SHOW CREATE TABLE.
 * 3. When disable_query is true, any query which reads that table should fail at analyze phase
 *    with message "the table <name> disable_query is true".
 */
public class DisableQueryPropertyTest {

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testShowCreateTableContainsDisableQuery() throws Exception {
        starRocksAssert
                .withTable("CREATE TABLE `t_disable_query_1` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `v1` int(11) NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"disable_query\" = \"true\"\n" +
                        ");");

        String sql = "show create table test.t_disable_query_1";
        ShowCreateTableStmt showStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        String createSql = resultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(createSql.contains("\"disable_query\" = \"true\""),
                "SHOW CREATE TABLE should contain disable_query property. Got: " + createSql);
    }

    @Test
    public void testAlterTableDisableQueryAndBlockQuery() throws Exception {
        // create table with default disable_query (false, meaning query is allowed)
        starRocksAssert
                .withTable("CREATE TABLE `t_disable_query_2` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `v1` int(11) NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        // alter disable_query to true (disable query)
        String alterSql = "ALTER TABLE test.t_disable_query_2 SET (\"disable_query\" = \"true\")";
        starRocksAssert.alterTableProperties(alterSql);
        
        // verify the property is set correctly
        String sql = "show create table test.t_disable_query_2";
        ShowCreateTableStmt showStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        String createSql = resultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(createSql.contains("\"disable_query\" = \"true\""),
                "After ALTER TABLE, SHOW CREATE TABLE should contain disable_query = true. Got: " + createSql);

        // now query should be rejected because disable_query is true
        String querySql = "SELECT * FROM test.t_disable_query_2";
        Exception ex = Assertions.assertThrows(Exception.class, () ->
                UtFrameUtils.getPlanAndFragment(ctx, querySql));
        // message format is checked in outer error handling; we only check core text here
        Assertions.assertTrue(ex.getMessage().contains("the table test.t_disable_query_2 property disable_query is true") ||
                        ex.getMessage().contains("disable_query is true"),
                "Query error message should mention disable_query is true. Got: " + ex.getMessage());
    }

    @Test
    public void testCreateTableWithInvalidDisableQuery() throws Exception {
        // Test that CREATE TABLE with invalid disable_query value is silently parsed as false
        // (Boolean.parseBoolean returns false for any non-"true" string)
        String createSqlInvalid = "CREATE TABLE `t_disable_query_invalid` (\n" +
                "  `id` int(11) NOT NULL\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"disable_query\" = \"yes\"\n" +
                ");";

        // Table creation should succeed (invalid value is silently parsed as false)
        starRocksAssert.withTable(createSqlInvalid);

        // Verify that disable_query is false (default) and not shown in SHOW CREATE TABLE
        String sql = "show create table test.t_disable_query_invalid";
        ShowCreateTableStmt showStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        String createSql = resultSet.getResultRows().get(0).get(1);
        boolean hasDisableQueryProperty = createSql.contains("\"disable_query\" = \"true\"") ||
                createSql.contains("\"disable_query\" = \"false\"");
        Assertions.assertFalse(hasDisableQueryProperty,
                "SHOW CREATE TABLE should not contain disable_query when invalid value is parsed as false. Got: " + createSql);

        // Test with another invalid value
        String createSqlInvalid2 = "CREATE TABLE `t_disable_query_invalid2` (\n" +
                "  `id` int(11) NOT NULL\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"disable_query\" = \"invalid\"\n" +
                ");";

        // Table creation should succeed (invalid value is silently parsed as false)
        starRocksAssert.withTable(createSqlInvalid2);

        // Verify that disable_query is false (default) and not shown in SHOW CREATE TABLE
        String sql2 = "show create table test.t_disable_query_invalid2";
        ShowCreateTableStmt showStmt2 =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        ShowResultSet resultSet2 = ShowExecutor.execute(showStmt2, ctx);
        String createSql2 = resultSet2.getResultRows().get(0).get(1);
        boolean hasDisableQueryProperty2 = createSql2.contains("\"disable_query\" = \"true\"") ||
                createSql2.contains("\"disable_query\" = \"false\"");
        Assertions.assertFalse(hasDisableQueryProperty2,
                "SHOW CREATE TABLE should not contain disable_query when invalid value is parsed as false. Got: " + createSql2);
    }

    @Test
    public void testAlterTableDisableQueryFromTrueToFalse() throws Exception {
        // create table with disable_query = true
        starRocksAssert
                .withTable("CREATE TABLE `t_disable_query_toggle` (\n" +
                        "  `id` int(11) NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"disable_query\" = \"true\"\n" +
                        ");");

        // verify it's shown when true
        String sql1 = "show create table test.t_disable_query_toggle";
        ShowCreateTableStmt showStmt1 =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        ShowResultSet resultSet1 = ShowExecutor.execute(showStmt1, ctx);
        String createSql1 = resultSet1.getResultRows().get(0).get(1);
        Assertions.assertTrue(createSql1.contains("\"disable_query\" = \"true\""),
                "SHOW CREATE TABLE should contain disable_query = true. Got: " + createSql1);

        // alter disable_query to false
        String alterSql = "ALTER TABLE test.t_disable_query_toggle SET (\"disable_query\" = \"false\")";
        starRocksAssert.alterTableProperties(alterSql);

        // verify it's not shown when false
        String sql2 = "show create table test.t_disable_query_toggle";
        ShowCreateTableStmt showStmt2 =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        ShowResultSet resultSet2 = ShowExecutor.execute(showStmt2, ctx);
        String createSql2 = resultSet2.getResultRows().get(0).get(1);
        boolean hasDisableQueryProperty = createSql2.contains("\"disable_query\" = \"true\"") ||
                createSql2.contains("\"disable_query\" = \"false\"");
        Assertions.assertFalse(hasDisableQueryProperty,
                "SHOW CREATE TABLE should not contain disable_query property when set to false. Got: " + createSql2);
    }

    @Test
    public void testUpdateStmtBlockedByDisableQuery() throws Exception {
        // create table with disable_query = true
        starRocksAssert
                .withTable("CREATE TABLE `t_disable_query_update` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `v1` int(11) NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "PRIMARY KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"disable_query\" = \"true\"\n" +
                        ");");

        // UPDATE should be rejected because disable_query is true
        String updateSql = "UPDATE test.t_disable_query_update SET v1 = 100 WHERE id = 1";
        Exception ex = Assertions.assertThrows(Exception.class, () ->
                UtFrameUtils.getPlanAndFragment(ctx, updateSql));
        Assertions.assertTrue(ex.getMessage().contains("the table test.t_disable_query_update property disable_query is true") ||
                        ex.getMessage().contains("disable_query is true"),
                "UPDATE error message should mention disable_query is true. Got: " + ex.getMessage());
    }

    @Test
    public void testDeleteStmtBlockedByDisableQuery() throws Exception {
        starRocksAssert
                .withTable("CREATE TABLE `t_disable_query_delete_pk` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `v1` int(11) NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "PRIMARY KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"disable_query\" = \"true\"\n" +
                        ");");

        String deleteSql = "DELETE FROM test.t_disable_query_delete_pk WHERE id = 1";
        Exception ex = Assertions.assertThrows(Exception.class, () ->
                UtFrameUtils.getPlanAndFragment(ctx, deleteSql));
        Assertions.assertTrue(
                ex.getMessage().contains("the table test.t_disable_query_delete_pk property disable_query is true") ||
                        ex.getMessage().contains("disable_query is true"),
                "DELETE error message should mention disable_query is true. Got: " + ex.getMessage());
    }
}







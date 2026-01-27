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
 * Tests for table property "enable_query".
 *
 * 1. SHOW CREATE TABLE should display "enable_query" in PROPERTIES.
 * 2. ALTER TABLE ... SET ("enable_query" = "false") should succeed and be reflected in SHOW CREATE TABLE.
 * 3. When enable_query is false, any query which reads that table should fail at analyze phase
 *    with message "the table <name> enable_query is false".
 */
public class EnableQueryPropertyTest {

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
    public void testShowCreateTableContainsEnableQuery() throws Exception {
        starRocksAssert
                .withTable("CREATE TABLE `t_enable_query_1` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `v1` int(11) NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"enable_query\" = \"false\"\n" +
                        ");");

        String sql = "show create table test.t_enable_query_1";
        ShowCreateTableStmt showStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        String createSql = resultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(createSql.contains("\"enable_query\" = \"false\""),
                "SHOW CREATE TABLE should contain enable_query property. Got: " + createSql);
    }

    @Test
    public void testAlterTableEnableQueryAndBlockQuery() throws Exception {
        // create table with default enable_query (true)
        starRocksAssert
                .withTable("CREATE TABLE `t_enable_query_2` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `v1` int(11) NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        // alter enable_query to false
        String alterSql = "ALTER TABLE test.t_enable_query_2 SET (\"enable_query\" = \"false\")";
        starRocksAssert.alterTableProperties(alterSql);
        
        // verify the property is set correctly
        String sql = "show create table test.t_enable_query_2";
        ShowCreateTableStmt showStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        String createSql = resultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(createSql.contains("\"enable_query\" = \"false\""),
                "After ALTER TABLE, SHOW CREATE TABLE should contain enable_query = false. Got: " + createSql);

        // now query should be rejected because enable_query is false
        String querySql = "SELECT * FROM test.t_enable_query_2";
        Exception ex = Assertions.assertThrows(Exception.class, () ->
                UtFrameUtils.getPlanAndFragment(ctx, querySql));
        // message format is checked in outer error handling; we only check core text here
        Assertions.assertTrue(ex.getMessage().contains("the table test.t_enable_query_2 property enable_query is false") ||
                        ex.getMessage().contains("enable_query is false"),
                "Query error message should mention enable_query is false. Got: " + ex.getMessage());
    }

    @Test
    public void testCreateTableWithInvalidEnableQuery() throws Exception {
        // Test that CREATE TABLE with invalid enable_query value should fail
        String createSqlInvalid = "CREATE TABLE `t_enable_query_invalid` (\n" +
                "  `id` int(11) NOT NULL\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"enable_query\" = \"yes\"\n" +
                ");";

        Exception ex = Assertions.assertThrows(Exception.class, () ->
                starRocksAssert.withTable(createSqlInvalid));
        Assertions.assertTrue(ex.getMessage().contains("must be bool type(false/true)") ||
                        ex.getMessage().contains("enable_query"),
                "CREATE TABLE with invalid enable_query should fail. Got: " + ex.getMessage());

        // Test with another invalid value
        String createSqlInvalid2 = "CREATE TABLE `t_enable_query_invalid2` (\n" +
                "  `id` int(11) NOT NULL\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"enable_query\" = \"invalid\"\n" +
                ");";

        Exception ex2 = Assertions.assertThrows(Exception.class, () ->
                starRocksAssert.withTable(createSqlInvalid2));
        Assertions.assertTrue(ex2.getMessage().contains("must be bool type(false/true)") ||
                        ex2.getMessage().contains("enable_query"),
                "CREATE TABLE with invalid enable_query should fail. Got: " + ex2.getMessage());
    }
}







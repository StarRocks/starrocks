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

package com.starrocks.catalog;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test SHOW CREATE TABLE includes flat_json properties
 */
public class ShowCreateTableFlatJsonTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test_flat_json_db").useDatabase("test_flat_json_db");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        starRocksAssert.dropDatabase("test_flat_json_db");
    }

    @Test
    public void testShowCreateTableWithFlatJsonEnabled() throws Exception {
        // Test 1: Create table with flat_json.enable=true and other properties
        String createTableSql = "CREATE TABLE test_flat_json_enabled (\n" +
                "    id INT,\n" +
                "    data JSON\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "    'replication_num' = '1',\n" +
                "    'flat_json.enable' = 'true',\n" +
                "    'flat_json.null.factor' = '0.2',\n" +
                "    'flat_json.sparsity.factor' = '0.7',\n" +
                "    'flat_json.column.max' = '75'\n" +
                ");";
        
        starRocksAssert.withTable(createTableSql);
        
        // Execute SHOW CREATE TABLE
        String showCreateSql = "SHOW CREATE TABLE test_flat_json_enabled";
        ShowCreateTableStmt showStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        
        Assertions.assertNotNull(resultSet);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        
        String createTableResult = resultSet.getResultRows().get(0).get(1);
        
        // Verify all flat_json properties are present in the output
        Assertions.assertTrue(createTableResult.contains("\"flat_json.enable\" = \"true\""),
                "SHOW CREATE TABLE should include flat_json.enable property");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.null.factor\" = \"0.2\""),
                "SHOW CREATE TABLE should include flat_json.null.factor property");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.sparsity.factor\" = \"0.7\""),
                "SHOW CREATE TABLE should include flat_json.sparsity.factor property");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.column.max\" = \"75\""),
                "SHOW CREATE TABLE should include flat_json.column.max property");
        
        starRocksAssert.dropTable("test_flat_json_enabled");
    }

    @Test
    public void testShowCreateTableWithFlatJsonDisabled() throws Exception {
        // Test 2: Create table with flat_json.enable=false but other properties set
        String createTableSql = "CREATE TABLE test_flat_json_disabled (\n" +
                "    id INT,\n" +
                "    data JSON\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "    'replication_num' = '1',\n" +
                "    'flat_json.enable' = 'false',\n" +
                "    'flat_json.null.factor' = '0.3',\n" +
                "    'flat_json.sparsity.factor' = '0.8',\n" +
                "    'flat_json.column.max' = '80'\n" +
                ");";
        
        starRocksAssert.withTable(createTableSql);
        
        // Execute SHOW CREATE TABLE
        String showCreateSql = "SHOW CREATE TABLE test_flat_json_disabled";
        ShowCreateTableStmt showStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        
        Assertions.assertNotNull(resultSet);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        
        String createTableResult = resultSet.getResultRows().get(0).get(1);
        
        // After the fix, all flat_json properties should be present even when enable=false
        Assertions.assertTrue(createTableResult.contains("\"flat_json.enable\" = \"false\""),
                "SHOW CREATE TABLE should include flat_json.enable property");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.null.factor\" = \"0.3\""),
                "SHOW CREATE TABLE should include flat_json.null.factor property even when enable=false");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.sparsity.factor\" = \"0.8\""),
                "SHOW CREATE TABLE should include flat_json.sparsity.factor property even when enable=false");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.column.max\" = \"80\""),
                "SHOW CREATE TABLE should include flat_json.column.max property even when enable=false");
        
        starRocksAssert.dropTable("test_flat_json_disabled");
    }

    @Test
    public void testShowCreateTableWithDefaultFlatJsonValues() throws Exception {
        // Test 3: Create table with only flat_json.enable=true (should use default values)
        String createTableSql = "CREATE TABLE test_flat_json_defaults (\n" +
                "    id INT,\n" +
                "    data JSON\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "    'replication_num' = '1',\n" +
                "    'flat_json.enable' = 'true'\n" +
                ");";
        
        starRocksAssert.withTable(createTableSql);
        
        // Execute SHOW CREATE TABLE
        String showCreateSql = "SHOW CREATE TABLE test_flat_json_defaults";
        ShowCreateTableStmt showStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, ctx);
        ShowResultSet resultSet = ShowExecutor.execute(showStmt, ctx);
        
        Assertions.assertNotNull(resultSet);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        
        String createTableResult = resultSet.getResultRows().get(0).get(1);
        
        // Should include flat_json.enable and default values for other properties
        Assertions.assertTrue(createTableResult.contains("\"flat_json.enable\" = \"true\""),
                "SHOW CREATE TABLE should include flat_json.enable property");
        // Default values should be present
        Assertions.assertTrue(createTableResult.contains("\"flat_json.null.factor\""),
                "SHOW CREATE TABLE should include flat_json.null.factor with default value");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.sparsity.factor\""),
                "SHOW CREATE TABLE should include flat_json.sparsity.factor with default value");
        Assertions.assertTrue(createTableResult.contains("\"flat_json.column.max\""),
                "SHOW CREATE TABLE should include flat_json.column.max with default value");
        
        starRocksAssert.dropTable("test_flat_json_defaults");
    }
}

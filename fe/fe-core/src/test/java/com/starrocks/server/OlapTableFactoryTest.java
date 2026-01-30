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

package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for OlapTableFactory, specifically for table_query_timeout property handling
 * during CREATE TABLE (covers OlapTableFactory.java lines 775-781).
 */
public class OlapTableFactoryTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static final String DB_NAME = "test_olap_table_factory_db";

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    @AfterAll
    public static void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * Test CREATE TABLE with valid table_query_timeout property.
     * This test covers OlapTableFactory.java lines 773-778 (success path).
     */
    @Test
    public void testCreateTableWithValidTableQueryTimeout() throws Exception {
        String createTableSql = "CREATE TABLE `test_timeout_valid` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"200\"\n" +
                ");";
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        // Call OlapTableFactory directly to ensure we hit OlapTableFactory.java's table_query_timeout branch.
        Table table = OlapTableFactory.INSTANCE.createTable(GlobalStateMgr.getCurrentState().getLocalMetastore(), db, stmt);
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table instanceof OlapTable);
        Assertions.assertEquals(200, ((OlapTable) table).getTableQueryTimeout());
    }

    /**
     * Test CREATE TABLE with table_query_timeout = 0 (invalid, should throw DdlException).
     * This test covers OlapTableFactory.java lines 779-781 (exception path).
     */
    @Test
    public void testCreateTableWithZeroTableQueryTimeout() throws Exception {
        String createTableSql = "CREATE TABLE `test_timeout_zero` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"0\"\n" +
                ");";
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> OlapTableFactory.INSTANCE.createTable(GlobalStateMgr.getCurrentState().getLocalMetastore(), db, stmt));
        Assertions.assertTrue(e.getMessage().contains("must be greater than 0"),
                "Expected error message about value must be greater than 0, but got: " + e.getMessage());
    }
}


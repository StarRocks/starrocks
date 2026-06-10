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

package com.starrocks.sql.common;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class RangeColocateColumnsTest {

    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = "create database test_colocate_columns;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(
                createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        // Create a range distribution table with sort key (k1, k2, k3)
        String sql = "CREATE TABLE test_colocate_columns.t1 (\n" +
                "    k1 INT,\n" +
                "    k2 VARCHAR(32),\n" +
                "    k3 BIGINT,\n" +
                "    v1 INT\n" +
                ") DUPLICATE KEY(k1, k2, k3)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    @AfterAll
    public static void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private OlapTable getTable() {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test_colocate_columns").getTable("t1");
    }

    @Test
    public void testGetRangeColocateColumnsDefault() throws DdlException {
        OlapTable table = getTable();
        // null colocateColumnNames -> returns all sort key columns
        List<Column> columns = MetaUtils.getRangeColocateColumns(table, null);
        Assertions.assertFalse(columns.isEmpty());
        // Should be the key columns: k1, k2, k3
        Assertions.assertEquals("k1", columns.get(0).getName());
        Assertions.assertEquals("k2", columns.get(1).getName());
        Assertions.assertEquals("k3", columns.get(2).getName());
    }

    @Test
    public void testGetRangeColocateColumnsPrefix() throws DdlException {
        OlapTable table = getTable();
        // Specify first two columns as colocate columns
        List<Column> columns = MetaUtils.getRangeColocateColumns(table,
                Arrays.asList("k1", "k2"));
        Assertions.assertEquals(2, columns.size());
        Assertions.assertEquals("k1", columns.get(0).getName());
        Assertions.assertEquals("k2", columns.get(1).getName());
    }

    @Test
    public void testGetRangeColocateColumnsSingleColumn() throws DdlException {
        OlapTable table = getTable();
        List<Column> columns = MetaUtils.getRangeColocateColumns(table,
                Arrays.asList("k1"));
        Assertions.assertEquals(1, columns.size());
        Assertions.assertEquals("k1", columns.get(0).getName());
    }

    @Test
    public void testGetRangeColocateColumnsNotPrefix() {
        OlapTable table = getTable();
        // k2 is not the first sort key column
        Assertions.assertThrows(DdlException.class,
                () -> MetaUtils.getRangeColocateColumns(table, Arrays.asList("k2")));
    }

    @Test
    public void testGetRangeColocateColumnsWrongOrder() {
        OlapTable table = getTable();
        // k2, k1 is not a valid prefix (wrong order)
        Assertions.assertThrows(DdlException.class,
                () -> MetaUtils.getRangeColocateColumns(table, Arrays.asList("k2", "k1")));
    }

    @Test
    public void testGetRangeColocateColumnsTooMany() {
        OlapTable table = getTable();
        // More columns than sort key has
        Assertions.assertThrows(DdlException.class,
                () -> MetaUtils.getRangeColocateColumns(table,
                        Arrays.asList("k1", "k2", "k3", "v1")));
    }

    @Test
    public void testGetRangeColocateColumnsNonExistent() {
        OlapTable table = getTable();
        // Column that doesn't exist
        Assertions.assertThrows(DdlException.class,
                () -> MetaUtils.getRangeColocateColumns(table,
                        Arrays.asList("nonexistent")));
    }
}

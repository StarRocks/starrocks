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

package com.starrocks.alter.lake;

import com.starrocks.catalog.Database;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AlterTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_schema_change_test";
    private static Database db;
    private LakeTable table;

    public AlterTest() {
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Before
    public void before() throws Exception {
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getServingState().getLocalMetastore().getDb(DB_NAME);
        table = createTable(connectContext, "CREATE TABLE t0(c0 INT) primary key(c0) distributed by hash(c0)");
    }

    @After
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    private static LakeTable createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), createTableStmt.getTableName());
    }

    @Test
    public void testAlterPKTableAddingBitmapColumn() throws Exception {
        String sql = "ALTER TABLE t0 ADD COLUMN c1 bitmap null";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail("Should not throw exception when adding bitmap column to pk table in shared-data mode");
        }
    }

    @Test
    public void testAlterPKTableAddingBitmapColumns() throws Exception {
        String sql = "ALTER TABLE t0 ADD COLUMN (c1 bitmap null, c2 bitmap null, c3 INT DEFAULT \"1\")";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(
                    "Should not throw exception when adding multiple bitmap columns to pk table in shared-data mode");
        }
    }

    @Test
    public void testAlterPKTableAddingBitmapColumnWithModifyClause() throws Exception {
        String sql = "ALTER TABLE t0 MODIFY COLUMN c0 bitmap null";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(
                    "Should not throw exception when modifying column to bitmap type with pk table in shared-data mode");
        }
    }
}

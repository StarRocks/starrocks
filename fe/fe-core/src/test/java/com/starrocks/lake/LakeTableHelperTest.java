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

package com.starrocks.lake;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class LakeTableHelperTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_table_helper";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
    }

    @AfterClass
    public static void afterClass() {
    }

    private static LakeTable createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Table table = testDb().getTable(createTableStmt.getTableName());
        return (LakeTable) table;
    }

    private static Database testDb() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
    }

    @Test
    public void testSupportCombinedTxnLog() throws Exception {
        Config.lake_use_combined_txn_log = true;
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.INSERT_STREAMING));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.LAKE_COMPACTION));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.FRONTEND_STREAMING));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BYPASS_WRITE));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.DELETE));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.MV_REFRESH));
        Config.lake_use_combined_txn_log = false;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
    }
}

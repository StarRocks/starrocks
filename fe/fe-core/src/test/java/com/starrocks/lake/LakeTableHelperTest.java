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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

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
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
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
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        Config.lake_use_combined_txn_log = true;

        Database db = testDb();
        LakeTable table = createTable(
                String.format("CREATE TABLE %s.t0(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1",
                        DB_NAME));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.INSERT_STREAMING));

        Config.lake_use_combined_txn_log = false;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));

        Config.lake_use_combined_txn_log = true;
        long invalidDbId = db.getId() + 1;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(invalidDbId, Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));

        long invalidTableId = table.getId() + 1;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(invalidTableId),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));

        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));
    }
}

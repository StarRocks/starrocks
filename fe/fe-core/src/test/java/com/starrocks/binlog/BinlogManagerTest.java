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

package com.starrocks.binlog;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.system.Backend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BinlogManagerTest {
    private static ConnectContext connectContext;
    private static BinlogManager binlogManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Backend be = UtFrameUtils.addMockBackend(10002);
        be.setDecommissioned(true);
        Config.enable_strict_storage_medium_check = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        binlogManager = new BinlogManager();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        String createTableStmtStr = "CREATE TABLE test.binlog_test(k1 int, v1 int) " +
                "duplicate key(k1) distributed by hash(k1) buckets 2 properties('replication_num' = '1', " +
                "'binlog_enable' = 'false', 'binlog_ttl_second' = '100', 'binlog_max_size' = '100');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.
                parseStmtWithNewParser(createTableStmtStr, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    @Test
    public void testTryEnableBinlog() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "binlog_test");
        boolean result = binlogManager.tryEnableBinlog(db, table.getId(), 200L, -1L);
        Assert.assertTrue(result);
        Assert.assertEquals(1, table.getBinlogVersion());
        Assert.assertEquals(200, table.getCurBinlogConfig().getBinlogTtlSecond());
        Assert.assertEquals(100,
                table.getCurBinlogConfig().getBinlogMaxSize());

    }

    @Test
    public void testTryDisableBinlog() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "binlog_test");
        boolean result = binlogManager.tryDisableBinlog(db, table.getId());
        Assert.assertFalse(table.isBinlogEnabled());
    }

    @Test
    public void testCheckAndSetBinlogAvailableVersion() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "binlog_test");
        table.setBinlogTxnId(2);
        long totalNum = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(10001).size();
        binlogManager.checkAndSetBinlogAvailableVersion(db, table, 1, 10002);
        Assert.assertFalse(binlogManager.isBinlogAvailable(db.getId(), table.getId()));

        binlogManager.checkAndSetBinlogAvailableVersion(db, table, 2, 10002);
        Assert.assertTrue(binlogManager.isBinlogAvailable(db.getId(), table.getId()));
    }

}

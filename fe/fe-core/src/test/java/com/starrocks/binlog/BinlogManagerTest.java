// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.binlog;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.system.Backend;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class BinlogManagerTest {
    private static ConnectContext connectContext;
    private static BinlogManager binlogManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Backend be = UtFrameUtils.addMockBackend(10002);
        be.setIsDecommissioned(true);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        Config.enable_strict_storage_medium_check = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        binlogManager = new BinlogManager();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        String createTableStmtStr = "CREATE TABLE test.binlog_test(k1 int, v1 int) " +
                "duplicate key(k1) distributed by hash(k1) properties('replication_num' = '1', " +
                "'binlog_enable' = 'false', 'binlog_ttl' = '100', 'binlog_max_size' = '100');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.
                parseStmtWithNewParser(createTableStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createTable(createTableStmt);
    }
    @Test
    public void testTryEnableBinlog() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("binlog_test");
        boolean result = binlogManager.tryEnableBinlog(db, table.getId(), 200L, -1L);
        Assert.assertTrue(result);
        Assert.assertEquals(2, table.getBinlogVersion().longValue());
        Assert.assertEquals(200, table.getCurBinlogConfig().getBinlogTtlSecond());
        Assert.assertEquals(100,
                table.getCurBinlogConfig().getBinlogMaxSize());

    }

    @Test
    public void testTryCloseBinlog() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("binlog_test");
        boolean result = binlogManager.tryCloseBinlog(db, table.getId());
        Assert.assertFalse(table.enableBinlog());
    }

    @Test
    public void testCheckAndSetBinlogAvailableVersion() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("binlog_test");
        Set<Long> transactionIds = new HashSet<>();
        transactionIds.add(1L);
        transactionIds.add(2L);
        transactionIds.add(3L);
        binlogManager.recordBinlogTxnId(4, db.getId(), table.getId(), transactionIds);

        Set<Long> runningTransactionIds = new HashSet<Long>();
        runningTransactionIds.add(1L);
        runningTransactionIds.add(2L);
        binlogManager.checkAndSetBinlogAvailableVersion(2, runningTransactionIds, db);
        Assert.assertFalse(binlogManager.isBinlogAvailable(table.getId(), db.getId()));

        runningTransactionIds.remove(2L);
        binlogManager.checkAndSetBinlogAvailableVersion(1, runningTransactionIds, db);
        Assert.assertTrue(binlogManager.isBinlogAvailable(table.getId(), db.getId()));
    }

}

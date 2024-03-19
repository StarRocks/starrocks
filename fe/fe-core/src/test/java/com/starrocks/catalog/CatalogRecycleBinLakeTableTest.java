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

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class CatalogRecycleBinLakeTableTest {
    @BeforeClass
    public static void beforeClass() {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
    }

    private static Table createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb(createTableStmt.getDbName());
        return db.getTable(createTableStmt.getTableName());
    }

    private static void dropTable(ConnectContext connectContext, String sql) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    private static void recoverDatabase(ConnectContext connectContext, String sql) throws Exception {
        RecoverDbStmt stmt = (RecoverDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().recoverDatabase(stmt);
    }

    private static Future<DropTableResponse> buildDropTableResponse(int errCode, String msg) {
        DropTableResponse response = new DropTableResponse();
        response.status = new StatusPB();
        response.status.statusCode = errCode;
        response.status.errorMsgs = Lists.newArrayList(msg);
        return CompletableFuture.completedFuture(response);
    }

    @Test
    public void testRecycleLakeTable(@Mocked LakeService lakeService) throws Exception {
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database recycle_bin_test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getMetadata().getDb("recycle_bin_test");

        Table table1 = createTable(connectContext, "create table recycle_bin_test.t0" +
                "(key1 int," +
                " key2 varchar(10)" +
                ") distributed by hash(key1) buckets 3 " +
                "properties('replication_num' = '1');");
        Assert.assertTrue(table1.isCloudNativeTable());
        Assert.assertTrue(table1.isDeleteRetryable());

        dropTable(connectContext, "DROP TABLE recycle_bin_test.t0");
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        Assert.assertNotNull(GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeIdOfTable(table1.getId()));

        Table table2 = createTable(connectContext, "create table recycle_bin_test.t0" +
                "(key1 int," +
                " key2 varchar(10)" +
                ") distributed by hash(key1) buckets 3 " +
                "properties('replication_num' = '1');");
        Assert.assertTrue(table1.isCloudNativeTable());

        dropTable(connectContext, "DROP TABLE recycle_bin_test.t0");
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table2.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        Assert.assertNotNull(GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeIdOfTable(table2.getId()));
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 3;
                maxTimes = 3;
                result = buildDropTableResponse(10, "mocked error");
                result = buildDropTableResponse(0, "ok");
                result = buildDropTableResponse(0, "ok");
            }
        };

        // table1 cannot be deleted because LakeService.dropTable() will return an error status
        recycleBin.eraseTable(System.currentTimeMillis());
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table2.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        Assert.assertNull(GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeIdOfTable(table1.getId()));

        // table1 cannot be deleted because the retry interval hasn't expired yet.
        recycleBin.eraseTable(System.currentTimeMillis());
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table2.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));

        // Now the retry interval has reached
        recycleBin.eraseTable(System.currentTimeMillis() + CatalogRecycleBin.getFailRetryInterval() + 1);
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table2.getId()));
        Assert.assertNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));

        Table table3 = createTable(connectContext, "create table recycle_bin_test.t1" +
                "(key1 int," +
                " key2 varchar(10)" +
                ") distributed by hash(key1) buckets 3 " +
                "properties('replication_num' = '1');");
        Assert.assertTrue(table3.isCloudNativeTable());

        dropTable(connectContext, "DROP TABLE recycle_bin_test.t1 FORCE");
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table3.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table3.getId()));
        Assert.assertFalse(recycleBin.recoverTable(db, "t1"));
        Assert.assertNull(GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeIdOfTable(table3.getId()));

        Assert.assertTrue(recycleBin.recoverTable(db, "t0"));
        Assert.assertSame(table2, db.getTable("t0"));
        Assert.assertNull(recycleBin.getTable(db.getId(), table2.getId()));

        recycleBin.eraseTable(System.currentTimeMillis());
        Assert.assertEquals(0, recycleBin.getTables(db.getId()).size());
    }

    @Test
    public void testReplayRecycleLakeTable(@Mocked LakeService lakeService) throws Exception {
        final String dbName = "replay_recycle_lake_table_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getMetadata().getDb(dbName);

        Table table1 = createTable(connectContext, String.format("create table %s.t0" +
                "(key1 int," +
                " key2 varchar(10)" +
                ") distributed by hash(key1) buckets 3 " +
                "properties('replication_num' = '1');", dbName));
        recycleBin.recycleTable(db.getId(), table1, true);
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        recycleBin.replayDisableTableRecovery(Lists.newArrayList(table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));

        recycleBin.replayEraseTable(Lists.newArrayList(table1.getId()));
        Assert.assertNull(recycleBin.getTable(db.getId(), table1.getId()));
    }

    @Test
    public void testRecycleLakeDatabase(@Mocked LakeService lakeService) throws Exception {
        final String dbName = "recycle_lake_database_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getMetadata().getDb(dbName);

        Table table1 = createTable(connectContext, String.format("create table %s.t1" +
                "(key1 int," +
                " key2 varchar(10)" +
                ") distributed by hash(key1) buckets 3 " +
                "properties('replication_num' = '1');", dbName));

        Table table2 = createTable(connectContext, String.format("create table %s.t2" +
                "(key1 int," +
                " key2 varchar(10)" +
                ") distributed by hash(key1) buckets 3 " +
                "properties('replication_num' = '1');", dbName));

        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(dbName, false);
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));

        // Recover the database.
        final String recoverDbSql = String.format("recover database %s", dbName);
        recoverDatabase(connectContext, recoverDbSql);
        Assert.assertNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertNull(recycleBin.getTable(db.getId(), table2.getId()));

        // Drop the database again.
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(dbName, false);
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 2;
                maxTimes = 2;
                result = buildDropTableResponse(1, "injected error");
                result = buildDropTableResponse(1, "injected error");
            }
        };

        // Now the retry interval has reached
        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        recycleBin.eraseTable(System.currentTimeMillis() + delay);
        Assert.assertThrows(DdlException.class, () -> recoverDatabase(connectContext, recoverDbSql));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
    }
}

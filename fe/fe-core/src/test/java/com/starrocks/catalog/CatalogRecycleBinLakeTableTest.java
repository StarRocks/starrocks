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
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
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
        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();
    }

    private static Table createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), createTableStmt.getTableName());
    }

    private static void dropTable(ConnectContext connectContext, String sql) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    private static void alterTable(ConnectContext connectContext, String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        DDLStmtExecutor.execute(stmt, connectContext);
    }

    private static void recoverDatabase(ConnectContext connectContext, String sql) throws Exception {
        RecoverDbStmt stmt = (RecoverDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().recoverDatabase(stmt);
    }

    private static void recoverPartition(ConnectContext connectContext, String sql) throws Exception {
        RecoverPartitionStmt stmt = (RecoverPartitionStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().recoverPartition(stmt);
    }

    private static Future<DropTableResponse> buildDropTableResponse(int errCode, String msg) {
        DropTableResponse response = new DropTableResponse();
        response.status = new StatusPB();
        response.status.statusCode = errCode;
        response.status.errorMsgs = Lists.newArrayList(msg);
        return CompletableFuture.completedFuture(response);
    }

    private static void checkTableTablet(Table table, boolean expectExist) {
        for (Partition partition : table.getPartitions()) {
            checkPartitionTablet(partition, expectExist);
        }
    }

    private static void checkPartitionTablet(Partition partition, boolean expectExist) {
        TabletInvertedIndex tabletIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (MaterializedIndex index :
                partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                TabletMeta meta = tabletIndex.getTabletMeta(tablet.getId());
                if (expectExist) {
                    Assert.assertNotNull(meta);
                } else {
                    Assert.assertNull(meta);
                }
            }
        }
    }

    private static String getStorageVolumeIdOfTable(long tableId) {
        return GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeIdOfTable(tableId);
    }

    private static void waitTableClearFinished(CatalogRecycleBin recycleBin, long id,
                                               long time) {
        while (recycleBin.getRecycleTableInfo(id) != null) {
            recycleBin.eraseTable(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    private static void waitPartitionClearFinished(CatalogRecycleBin recycleBin, long id,
                                                   long time) {
        while (recycleBin.getRecyclePartitionInfo(id) != null) {
            recycleBin.erasePartition(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    private static void waitTableToBeDone(CatalogRecycleBin recycleBin, long id, long time) {
        while (recycleBin.isDeletingTable(id)) {
            recycleBin.eraseTable(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    private static void waitPartitionToBeDone(CatalogRecycleBin recycleBin, long id, long time) {
        while (recycleBin.isDeletingPartition(id)) {
            recycleBin.erasePartition(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    @Test
    public void testRecycleLakeTable(@Mocked LakeService lakeService) throws Exception {
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database recycle_bin_test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("recycle_bin_test");

        Table table1 = createTable(connectContext, "create table recycle_bin_test.t0" +
                "(key1 int," +
                " key2 varchar(10)" +
                ") distributed by hash(key1) buckets 3 " +
                "properties('replication_num' = '1');");
        Assert.assertTrue(table1.isCloudNativeTable());
        Assert.assertTrue(table1.isDeleteRetryable());
        checkTableTablet(table1, true);

        dropTable(connectContext, "DROP TABLE recycle_bin_test.t0");
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        Assert.assertNotNull(getStorageVolumeIdOfTable(table1.getId()));
        checkTableTablet(table1, true);

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
        Assert.assertNotNull(getStorageVolumeIdOfTable(table2.getId()));
        checkTableTablet(table1, true);
        checkTableTablet(table2, true);

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
        waitTableToBeDone(recycleBin, table1.getId(), System.currentTimeMillis());
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table2.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        Assert.assertNull(getStorageVolumeIdOfTable(table1.getId()));
        checkTableTablet(table1, true);
        checkTableTablet(table2, true);

        // table1 cannot be deleted because the retry interval hasn't expired yet.
        recycleBin.eraseTable(System.currentTimeMillis());
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table2.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        checkTableTablet(table1, true);
        checkTableTablet(table2, true);

        // Now the retry interval has reached, table1 should be deleted after return
        recycleBin.eraseTable(System.currentTimeMillis() + CatalogRecycleBin.getFailRetryInterval() + 1);
        waitTableClearFinished(recycleBin, table1.getId(),
                               System.currentTimeMillis() + CatalogRecycleBin.getFailRetryInterval() + 1);
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assert.assertTrue(recycleBin.isTableRecoverable(db.getId(), table2.getId()));
        Assert.assertNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertFalse(recycleBin.isTableRecoverable(db.getId(), table1.getId()));
        checkTableTablet(table1, false);
        checkTableTablet(table2, true);

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
        Assert.assertNull(getStorageVolumeIdOfTable(table3.getId()));
        checkTableTablet(table2, true);
        checkTableTablet(table3, true);

        // Recover table2
        Assert.assertTrue(recycleBin.recoverTable(db, "t0"));
        Assert.assertSame(table2, GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0"));
        Assert.assertNull(recycleBin.getTable(db.getId(), table2.getId()));
        checkTableTablet(table2, true);

        // table3 should be deleted after return
        recycleBin.eraseTable(System.currentTimeMillis());
        waitTableClearFinished(recycleBin, table3.getId(), System.currentTimeMillis());
        Assert.assertEquals(0, recycleBin.getTables(db.getId()).size());
        Assert.assertNull(recycleBin.getTable(db.getId(), table3.getId()));
        checkTableTablet(table3, false);
    }

    @Test
    public void testReplayRecycleLakeTable(@Mocked LakeService lakeService) throws Exception {
        final String dbName = "replay_recycle_lake_table_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

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
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

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

        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(new ConnectContext(), dbName, false);
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));

        // Recover the database.
        final String recoverDbSql = String.format("recover database %s", dbName);
        recoverDatabase(connectContext, recoverDbSql);
        Assert.assertNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertNull(recycleBin.getTable(db.getId(), table2.getId()));

        // Drop the database again.
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(new ConnectContext(), dbName, false);
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
        waitTableToBeDone(recycleBin, table1.getId(), System.currentTimeMillis() + delay);
        waitTableToBeDone(recycleBin, table2.getId(), System.currentTimeMillis() + delay);
        Assert.assertThrows(DdlException.class, () -> recoverDatabase(connectContext, recoverDbSql));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assert.assertNotNull(recycleBin.getTable(db.getId(), table2.getId()));
    }

    @Test
    public void testRecycleLakePartition(@Mocked LakeService lakeService) throws Exception {
        final String dbName = "recycle_lake_partition_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        Table table1 = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')," +
                        "  PARTITION p3 VALUES LESS THAN('2024-03-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Partition p1 = table1.getPartition("p1");
        Partition p2 = table1.getPartition("p2");
        Partition p3 = table1.getPartition("p3");
        Assert.assertNotNull(p1);
        Assert.assertFalse(LakeTableHelper.isSharedPartitionDirectory(p1.getDefaultPhysicalPartition(),
                WarehouseManager.DEFAULT_WAREHOUSE_ID));
        Assert.assertFalse(LakeTableHelper.isSharedPartitionDirectory(p2.getDefaultPhysicalPartition(),
                WarehouseManager.DEFAULT_WAREHOUSE_ID));
        Assert.assertFalse(LakeTableHelper.isSharedPartitionDirectory(p3.getDefaultPhysicalPartition(),
                WarehouseManager.DEFAULT_WAREHOUSE_ID));

        // Drop partition "p1"
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p1", dbName));
        Assert.assertNull(table1.getPartition("p1"));
        checkPartitionTablet(p1, true);

        // Recover "p1"
        recoverPartition(connectContext, String.format("RECOVER PARTITION p1 FROM %s.t1", dbName));
        Assert.assertSame(p1, table1.getPartition("p1"));
        checkPartitionTablet(p1, true);

        // Drop partition "p1"
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p1", dbName));
        Assert.assertNull(table1.getPartition("p1"));
        checkPartitionTablet(p1, true);

        // Drop partition "p2" force
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p2 FORCE", dbName));
        Assert.assertNull(table1.getPartition("p2"));
        checkPartitionTablet(p2, true);

        // Recover partition "p2", should fail
        Assert.assertThrows(DdlException.class, () -> {
            recoverPartition(connectContext, String.format("RECOVER PARTITION p2 FROM %s.t1", dbName));
        });

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = buildDropTableResponse(0, "");
            }
        };
        // Erase time not reached for p1 but not p2
        recycleBin.erasePartition(System.currentTimeMillis());
        Assert.assertTrue(!recycleBin.isDeletingPartition(p1.getId()));
        waitPartitionClearFinished(recycleBin, p2.getId(), System.currentTimeMillis());
        Assert.assertSame(p1, recycleBin.getPartition(p1.getId()));
        Assert.assertNull(recycleBin.getPartition(p2.getId()));
        checkPartitionTablet(p1, true);
        checkPartitionTablet(p2, false);

        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = buildDropTableResponse(1, "injected error");
            }
        };

        // Erase time reached but LakeService.dropTable() failed
        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        recycleBin.erasePartition(System.currentTimeMillis() + delay);
        waitPartitionToBeDone(recycleBin, p1.getId(), System.currentTimeMillis() + delay);
        Assert.assertSame(p1, recycleBin.getPartition(p1.getId()));
        Assert.assertNull(recycleBin.getPartition(p2.getId()));
        Assert.assertThrows(DdlException.class, () -> {
            recoverPartition(connectContext, String.format("RECOVER PARTITION p1 FROM %s.t1", dbName));
        });
        Assert.assertThrows(DdlException.class, () -> {
            recoverPartition(connectContext, String.format("RECOVER PARTITION p2 FROM %s.t1", dbName));
        });
        checkPartitionTablet(p1, true);
        checkPartitionTablet(p2, false);

        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = buildDropTableResponse(0, "");
            }
        };
        recycleBin.erasePartition(System.currentTimeMillis() + delay);
        waitPartitionClearFinished(recycleBin, p1.getId(), System.currentTimeMillis() + delay);
        waitPartitionClearFinished(recycleBin, p2.getId(), System.currentTimeMillis() + delay);
        Assert.assertNull(recycleBin.getPartition(p1.getId()));
        Assert.assertNull(recycleBin.getPartition(p2.getId()));
        checkPartitionTablet(p1, false);
        checkPartitionTablet(p2, false);

        // List partition
        Table table2 = createTable(connectContext, String.format(
                "CREATE TABLE %s.t2" +
                        "(" +
                        "  k1 DATE NOT NULL," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY LIST(k1) (" +
                        "  PARTITION p1 VALUES IN ('2024-01-01')," +
                        "  PARTITION p2 VALUES IN ('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        p1 = table2.getPartition("p1");
        Assert.assertFalse(LakeTableHelper.isSharedPartitionDirectory(p1.getDefaultPhysicalPartition(),
                WarehouseManager.DEFAULT_WAREHOUSE_ID));
        // Drop partition "p1"
        alterTable(connectContext, String.format("ALTER TABLE %s.t2 DROP PARTITION p1", dbName));
        Assert.assertNull(table2.getPartition("p1"));
        Assert.assertNotNull(recycleBin.getPartition(p1.getId()));
        checkPartitionTablet(p1, true);
        // List partition is unrecoverable now.
        Assert.assertThrows(DdlException.class, () -> {
            recoverPartition(connectContext, String.format("RECOVER PARTITION p1 from %s.t2", dbName));
        });

        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = buildDropTableResponse(0, "");
            }
        };
        recycleBin.erasePartition(System.currentTimeMillis() + delay);
        waitPartitionClearFinished(recycleBin, p1.getId(), System.currentTimeMillis() + delay);
        Assert.assertNull(recycleBin.getPartition(p1.getId()));
        checkPartitionTablet(p1, false);
    }

    @Test
    public void testRecycleLakePartitionWithSharedDirectory(@Mocked LakeService lakeService) throws Exception {
        final String dbName = "recycle_partition_shared_directory_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        Table table1 = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')," +
                        "  PARTITION p3 VALUES LESS THAN('2024-03-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Partition p1 = table1.getPartition("p1");
        Partition p2 = table1.getPartition("p2");
        Assert.assertNotNull(p1);
        System.out.printf("p1=%d p2=%d%n", p1.getId(), p2.getId());

        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p1 FORCE", dbName));
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p2 FORCE", dbName));
        Assert.assertNull(table1.getPartition("p1"));
        Assert.assertNull(table1.getPartition("p2"));
        checkPartitionTablet(p1, true);
        checkPartitionTablet(p2, true);

        new MockUp<LakeTableHelper>() {
            @Mock
            public boolean isSharedDirectory(String path, long partitionId) {
                Assert.assertTrue(path.endsWith("/" + partitionId));
                return partitionId == p1.getDefaultPhysicalPartition().getId();
            }
        };
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        recycleBin.erasePartition(System.currentTimeMillis());
        waitPartitionClearFinished(recycleBin, p1.getId(), System.currentTimeMillis());
        waitPartitionClearFinished(recycleBin, p2.getId(), System.currentTimeMillis());
        Assert.assertNull(recycleBin.getPartition(p1.getId()));
        Assert.assertNull(recycleBin.getPartition(p2.getId()));
        checkPartitionTablet(p1, false);
        checkPartitionTablet(p2, false);
    }
}

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


package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.LakeTableSchemaChangeJob;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.MockedBackend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LakePublishBatchTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private static final String DB = "db_for_test";
    private static final String TABLE_AGG_ON = "table_for_test_agg_on";
    private static final String TABLE_AGG_OFF = "table_for_test_agg_off";
    private static final String TABLE_SCHEMA_CHANGE = "table_for_test_schema_change";
    private TransactionState.TxnCoordinator transactionSource =
            new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe");

    private static boolean enable_batch_publish_version;
    private static int batch_publish_min_version_num;
    private static int alterSchedulerIntervalMs;

    private void generateSimpleTabletCommitInfo(Database db, Table table,
                                                List<TabletCommitInfo> transTablets1,
                                                List<TabletCommitInfo> transTablets2) {
        int num = 0;
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIds()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    if (num % 2 == 0) {
                        transTablets1.add(tabletCommitInfo);
                    } else {
                        transTablets2.add(tabletCommitInfo);
                    }
                }
            }
            num++;
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        enable_batch_publish_version = Config.lake_enable_batch_publish_version;
        batch_publish_min_version_num = Config.lake_batch_publish_min_version_num;
        alterSchedulerIntervalMs = Config.alter_scheduler_interval_millisecond;
        Config.lake_enable_batch_publish_version = true;
        Config.lake_batch_publish_min_version_num = 2;
        Config.alter_scheduler_interval_millisecond = 100;

        new MockUp<PublishVersionDaemon>() {
            @Mock
            public void runOneCycle() {

            }
        };

        new MockUp<Utils>() {
            @Mock
            public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                    long newVersion, Map<Long, Double> compactionScores,
                    List<String> distributionColumns, ComputeResource computeResource,
                    Map<Long, Long> tabletRowNums, boolean useAggregatePublish)
                    throws NoAliveBackendException, RpcException {

            }
        };

        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        String sql1 = "create table " + TABLE_AGG_OFF +
                " (dt date NOT NULL, pk bigint NOT NULL, v0 string not null) primary KEY (dt, pk) " +
                "PARTITION BY RANGE(`dt`) (\n" +
                "    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),\n" +
                "    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),\n" +
                "    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),\n" +
                "    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))\n" +
                ")" +
                "DISTRIBUTED BY HASH(pk) BUCKETS 3" +
                " PROPERTIES(\"replication_num\" = \"" + 3 +
                "\", \"storage_medium\" = \"SSD\", \"file_bundling\" = \"false\")";
        starRocksAssert.withTable(sql1);

        String sql2 = "create table " + TABLE_AGG_ON +
                " (dt date NOT NULL, pk bigint NOT NULL, v0 string not null) primary KEY (dt, pk) " +
                "PARTITION BY RANGE(`dt`) (\n" +
                "    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),\n" +
                "    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),\n" +
                "    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),\n" +
                "    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))\n" +
                ")" +
                "DISTRIBUTED BY HASH(pk) BUCKETS 3" +
                " PROPERTIES(\"replication_num\" = \"" + 3 +
                "\", \"storage_medium\" = \"SSD\", \"file_bundling\" = \"true\")";
        starRocksAssert.withTable(sql2);

        String sql3 = "create table " + TABLE_SCHEMA_CHANGE +
                " (pk int NOT NULL, v0 int not null) primary KEY (pk) " +
                "DISTRIBUTED BY HASH(pk) BUCKETS 1;";
        starRocksAssert.withTable(sql3);
    }

    @AfterAll
    public static void afterClass() {
        Config.lake_enable_batch_publish_version = enable_batch_publish_version;
        Config.lake_batch_publish_min_version_num = batch_publish_min_version_num;
        Config.alter_scheduler_interval_millisecond = alterSchedulerIntervalMs;
        FeConstants.runningUnitTest = false;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNormal(boolean enableAggregation) throws Exception {
        String tableName = enableAggregation ? TABLE_AGG_ON : TABLE_AGG_OFF;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<TabletCommitInfo> transTablets1 = Lists.newArrayList();
        List<TabletCommitInfo> transTablets2 = Lists.newArrayList();
        generateSimpleTabletCommitInfo(db, table, transTablets1, transTablets2);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId1 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable1 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), transactionId1, transTablets1,
                Lists.newArrayList(), null);

        long transactionId2 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable2 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter2 = globalTransactionMgr.commitTransaction(db.getId(), transactionId2, transTablets2,
                Lists.newArrayList(), null);

        long transactionId3 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable3 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter3 = globalTransactionMgr.commitTransaction(db.getId(), transactionId3, transTablets1,
                Lists.newArrayList(), null);

        long transactionId4 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable4 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter4 = globalTransactionMgr.commitTransaction(db.getId(), transactionId4, transTablets2,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        Assertions.assertTrue(waiter1.await(10, TimeUnit.SECONDS));
        Assertions.assertTrue(waiter2.await(10, TimeUnit.SECONDS));
        Assertions.assertTrue(waiter3.await(10, TimeUnit.SECONDS));
        Assertions.assertTrue(waiter4.await(10, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPublishTransactionState(boolean enableAggregation) throws Exception {
        String tableName = enableAggregation ? TABLE_AGG_ON : TABLE_AGG_OFF;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();

        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIds()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets.add(tabletCommitInfo);
                }
            }
        }

        // test publish transactionStateBatch which size is one
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Config.lake_batch_publish_min_version_num = 1;
        long transactionId9 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable9 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter9 = globalTransactionMgr.commitTransaction(db.getId(), transactionId9, transTablets,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();
        TransactionState transactionState9 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId9);
        boolean success = false;
        for (int i = 0; i < 10; i++) {
            publishVersionDaemon.runAfterCatalogReady();
            if (waiter9.await(1, TimeUnit.SECONDS)) {
                success = true;
                break;
            }
        }
        Assertions.assertTrue(success, 
                String.format("Transaction publish timeout after 10 seconds. " +
                        "TransactionId: %d, Table: %s, DB: %s, status: %s", 
                        transactionId9, tableName, db.getFullName(), transactionState9.getTransactionStatus()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPublishDbDroped(boolean enableAggregation) throws Exception {
        String tableName = enableAggregation ? TABLE_AGG_ON : TABLE_AGG_OFF;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIds()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets.add(tabletCommitInfo);
                }
            }
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        long transactionId5 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable5 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        globalTransactionMgr.commitTransaction(db.getId(), transactionId5, transTablets,
                Lists.newArrayList(), null);

        long transactionId6 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable6 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        globalTransactionMgr.commitTransaction(db.getId(), transactionId6, transTablets,
                Lists.newArrayList(), null);

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return null;
            }
        };

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        TransactionState transactionState1 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId5);
        TransactionState transactionState2 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId6);

        // wait publish complete
        Thread.sleep(1000);
        assertEquals(transactionState1.getTransactionStatus(), TransactionStatus.ABORTED);
        assertEquals(transactionState2.getTransactionStatus(), TransactionStatus.ABORTED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPublishTableDropped(boolean enableAggregation) throws Exception {
        String tableName = enableAggregation ? TABLE_AGG_ON : TABLE_AGG_OFF;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIds()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets.add(tabletCommitInfo);
                }
            }
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        long transactionId7 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable7 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), transactionId7, transTablets,
                Lists.newArrayList(), null);

        long transactionId8 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable8 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter2 = globalTransactionMgr.commitTransaction(db.getId(), transactionId8, transTablets,
                Lists.newArrayList(), null);

        new MockUp<Database>() {
            @Mock
            public Table getTable(String tableName) {
                return null;
            }
        };

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        Assertions.assertTrue(waiter1.await(1, TimeUnit.MINUTES));
        Assertions.assertTrue(waiter2.await(1, TimeUnit.MINUTES));

        TransactionState transactionState1 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId7);
        TransactionState transactionState2 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId8);
        assertEquals(transactionState1.getTransactionStatus(), TransactionStatus.VISIBLE);
        assertEquals(transactionState2.getTransactionStatus(), TransactionStatus.VISIBLE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTransformBatchToSingle(boolean enableAggregation) throws Exception {
        String tableName = enableAggregation ? TABLE_AGG_ON : TABLE_AGG_OFF;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<TabletCommitInfo> transTablets1 = Lists.newArrayList();
        List<TabletCommitInfo> transTablets2 = Lists.newArrayList();

        generateSimpleTabletCommitInfo(db, table, transTablets1, transTablets2);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId1 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label1" + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), transactionId1, transTablets1,
                Lists.newArrayList(), null);

        long transactionId2 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label2" + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter2 = globalTransactionMgr.commitTransaction(db.getId(), transactionId2, transTablets2,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        Assertions.assertTrue(waiter1.await(10, TimeUnit.SECONDS));
        Assertions.assertTrue(waiter2.await(10, TimeUnit.SECONDS));

        // Ensure publishingLakeTransactionsBatchTableId has been cleared, otherwise the following single publish may fail.
        publishVersionDaemon.publishingLakeTransactionsBatchTableId.clear();

        Config.lake_enable_batch_publish_version = false;
        long transactionId3 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label3" + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter3 = globalTransactionMgr.commitTransaction(db.getId(), transactionId3, transTablets1,
                Lists.newArrayList(), null);

        publishVersionDaemon.runAfterCatalogReady();
        Assertions.assertTrue(waiter3.await(10, TimeUnit.SECONDS));

        Config.lake_enable_batch_publish_version = true;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTransformSingleToBatch(boolean enableAggregation) throws Exception {
        String tableName = enableAggregation ? TABLE_AGG_ON : TABLE_AGG_OFF;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<TabletCommitInfo> transTablets1 = Lists.newArrayList();
        List<TabletCommitInfo> transTablets2 = Lists.newArrayList();
        generateSimpleTabletCommitInfo(db, table, transTablets1, transTablets2);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId5 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label5" + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter5 = globalTransactionMgr.commitTransaction(db.getId(), transactionId5, transTablets1,
                Lists.newArrayList(), null);

        Config.lake_enable_batch_publish_version = false;
        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();
        Assertions.assertTrue(waiter5.await(10, TimeUnit.SECONDS));

        long transactionId6 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label6" + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter6 = globalTransactionMgr.commitTransaction(db.getId(), transactionId6, transTablets2,
                Lists.newArrayList(), null);

        long transactionId7 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label7" + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter7 = globalTransactionMgr.commitTransaction(db.getId(), transactionId7, transTablets1,
                Lists.newArrayList(), null);

        // mock the switch from single to batch by adding transactionId to publishingLakeTransactions
        publishVersionDaemon.publishingLakeTransactions.clear();
        publishVersionDaemon.publishingLakeTransactions.add(transactionId6);

        Config.lake_enable_batch_publish_version = true;
        publishVersionDaemon.runAfterCatalogReady();
        Assertions.assertFalse(waiter6.await(10, TimeUnit.SECONDS));
        Assertions.assertFalse(waiter7.await(10, TimeUnit.SECONDS));

        publishVersionDaemon.publishingLakeTransactions.clear();
        publishVersionDaemon.runAfterCatalogReady();
        Assertions.assertTrue(waiter6.await(10, TimeUnit.SECONDS));
        Assertions.assertTrue(waiter7.await(10, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCheckStateBatchConsistent(boolean enableAggregation) throws Exception {
        String tableName = enableAggregation ? TABLE_AGG_ON : TABLE_AGG_OFF;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIds()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets.add(tabletCommitInfo);
                }
            }
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId1 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable1 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), transactionId1, transTablets,
                Lists.newArrayList(), null);

        long transactionId2 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable2 + "_" + UUIDUtil.genUUID().toString(),
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter2 = globalTransactionMgr.commitTransaction(db.getId(), transactionId2, transTablets,
                Lists.newArrayList(), null);

        {
            TransactionStateBatch readyStateBatch = globalTransactionMgr.getReadyPublishTransactionsBatch().get(0);
            Assertions.assertEquals(2, readyStateBatch.size());

            DatabaseTransactionMgr transactionMgr = globalTransactionMgr.getDatabaseTransactionMgr(db.getId());
            Assertions.assertTrue(transactionMgr.checkTxnStateBatchConsistent(db, readyStateBatch));

            // keep origin version
            Map<Partition, Long> partitionVersions = new HashMap<>();
            for (Partition partition : table.getPartitions()) {
                partitionVersions.put(partition, partition.getDefaultPhysicalPartition().getVisibleVersion());
                partition.getDefaultPhysicalPartition().setVisibleVersion(0, System.currentTimeMillis());
            }
            Assertions.assertFalse(transactionMgr.checkTxnStateBatchConsistent(db, readyStateBatch));

            // restore partition version
            for (Map.Entry<Partition, Long> entry : partitionVersions.entrySet()) {
                entry.getKey().getDefaultPhysicalPartition()
                        .setVisibleVersion(entry.getValue(), System.currentTimeMillis());
            }
            Assertions.assertTrue(transactionMgr.checkTxnStateBatchConsistent(db, readyStateBatch));

            TransactionState transactionState2 = readyStateBatch.getTransactionStates().get(1);
            Collection<PartitionCommitInfo> partitionCommitInfos = transactionState2.getTableCommitInfo(table.getId())
                    .getIdToPartitionCommitInfo().values();
            Map<PartitionCommitInfo, Long> originPartitionCommitInfos = new HashMap<>();
            for (PartitionCommitInfo partitionCommitInfo : partitionCommitInfos) {
                originPartitionCommitInfos.put(partitionCommitInfo, partitionCommitInfo.getVersion());
                partitionCommitInfo.setVersion(99);
            }
            Assertions.assertFalse(transactionMgr.checkTxnStateBatchConsistent(db, readyStateBatch));

            // restore
            for (Map.Entry<PartitionCommitInfo, Long> entry : originPartitionCommitInfos.entrySet()) {
                entry.getKey().setVersion(entry.getValue());
            }
            Assertions.assertTrue(transactionMgr.checkTxnStateBatchConsistent(db, readyStateBatch));

            PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
            publishVersionDaemon.runAfterCatalogReady();
        }
    }

    @Test
    public void testBatchPublishShadowIndex() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), TABLE_SCHEMA_CHANGE);
        assertEquals(1, table.getPartitions().size());
        PhysicalPartition physicalPartition = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        List<MaterializedIndex> normalIndices =
                physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        assertEquals(1, normalIndices.size());
        MaterializedIndex normalIndex = normalIndices.get(0);
        assertEquals(1, normalIndex.getTabletIds().size());
        LakeTablet normalTablet = (LakeTablet) normalIndex.getTablets().get(0);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        // txn1 only includes tablets of base index
        long txn1 = globalTransactionMgr.beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "txn1" + "_" + UUIDUtil.genUUID().toString(), transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState txnState1 = globalTransactionMgr.getTransactionState(db.getId(), txn1);
        txnState1.addTableIndexes((OlapTable) table);
        List<TabletCommitInfo> commitInfo1 = commitAllTablets(List.of(normalTablet));

        // do a schema change, which will create a shadow index
        String alterSql = String.format("alter table %s add index idx (v0) using bitmap", TABLE_SCHEMA_CHANGE);
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
        List<AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        assertEquals(1, alterJobs.size());
        assertInstanceOf(LakeTableSchemaChangeJob.class, alterJobs.get(0));
        LakeTableSchemaChangeJob schemaChangeJob = (LakeTableSchemaChangeJob) alterJobs.get(0);
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(
                () -> schemaChangeJob.getJobState() == AlterJobV2.JobState.WAITING_TXN);

        List<MaterializedIndex> shadowIndices =
                physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        assertEquals(1, shadowIndices.size());
        MaterializedIndex shadowIndex = shadowIndices.get(0);
        assertEquals(1, shadowIndex.getTabletIds().size());
        LakeTablet shadowTablet = (LakeTablet) shadowIndex.getTablets().get(0);

        // txn2 includes tablets of both base index and shadow index
        long txn2 = globalTransactionMgr.beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "txn2" + "_" + UUIDUtil.genUUID().toString(), transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState txnState2 = globalTransactionMgr.getTransactionState(db.getId(), txn2);
        txnState2.addTableIndexes((OlapTable) table);
        List<TabletCommitInfo> commitInfo2 = commitAllTablets(List.of(normalTablet, shadowTablet));

        // txn3 includes tablets of both base index and shadow index
        long txn3 = globalTransactionMgr.beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                "txn3" + "_" + UUIDUtil.genUUID().toString(), transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState txnState3 = globalTransactionMgr.getTransactionState(db.getId(), txn3);
        txnState3.addTableIndexes((OlapTable) table);
        List<TabletCommitInfo> commitInfo3 = commitAllTablets(List.of(normalTablet, shadowTablet));

        // commit in the order of txn2, tnx1, and txn3
        VisibleStateWaiter waiter2 = globalTransactionMgr.commitTransaction(db.getId(), txn2, commitInfo2,
                Lists.newArrayList(), null);
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), txn1, commitInfo1,
                Lists.newArrayList(), null);
        VisibleStateWaiter waiter3 = globalTransactionMgr.commitTransaction(db.getId(), txn3, commitInfo3,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        Assertions.assertTrue(waiter1.await(1, TimeUnit.MINUTES));
        Assertions.assertTrue(waiter2.await(1, TimeUnit.MINUTES));
        Assertions.assertTrue(waiter3.await(1, TimeUnit.MINUTES));

        ComputeNode shadowTabletNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getComputeNodeAssignedToTablet(WarehouseManager.DEFAULT_RESOURCE, shadowTablet.getId());
        LakeService lakeService = BrpcProxy.getLakeService(shadowTabletNode.getHost(), shadowTabletNode.getBrpcPort());
        assertInstanceOf(MockedBackend.MockLakeService.class, lakeService);
        MockedBackend.MockLakeService mockLakeService = (MockedBackend.MockLakeService) lakeService;
        PublishLogVersionBatchRequest request = mockLakeService.pollPublishLogVersionBatchRequests();
        assertNotNull(request);
        assertEquals(List.of(shadowTablet.getId()), request.getTabletIds());
        assertEquals(2, request.getTxnInfos().size());
        assertEquals(txn2, request.getTxnInfos().get(0).getTxnId());
        assertEquals(txn3, request.getTxnInfos().get(1).getTxnId());
    }

    private List<TabletCommitInfo> commitAllTablets(List<LakeTablet> tablets) {
        List<TabletCommitInfo> commitInfos = Lists.newArrayList();
        List<Long> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds();
        for (LakeTablet tablet : tablets) {
            TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tablet.getId(), backends.get(0));
            commitInfos.add(tabletCommitInfo);
        }
        return commitInfos;
    }
}

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
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CatalogRecycleBinLakeTableDropTableTest extends CatalogRecycleBinLakeTableTest {
    private static final Logger LOG = LogManager.getLogger(CatalogRecycleBinLakeTableDropTableTest.class);

    /**
     * Test erasing a Lake Table that has no partitions.
     */
    @Test
    public void testEraseLakeTableWithNoPartitions(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_lake_table_no_partitions_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a range-partitioned table with one partition
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Assertions.assertNotNull(p1);

        // Drop partition first (force), so the table becomes empty
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p1 FORCE", dbName));
        Assertions.assertNull(table.getPartition("p1"));

        // Force drop the now-empty table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        // One dropTable call expected for the individually dropped partition p1
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // Erase the individually dropped partition p1 first
        waitPartitionClearFinished(recycleBin, p1.getId(), futureTime);

        // eraseTable should handle the empty table in a single call:
        // addLakeTablePartitionsToRecycleBin returns false (no partitions),
        // so table is directly finished without partition-level deletion.
        recycleBin.eraseTable(futureTime);

        // Table should be erased
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        // No partition tracking should exist
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        // Tablet meta should be removed
        checkTableTablet(table, false);
    }

    /**
     * Test that replayEraseTable properly cleans up lakeTableToPartitions and partitionsFromTableDeletion.
     * This covers the cleanup code in removeTableFromRecycleBin() that was added for lake table support.
     */
    @Test
    public void testReplayEraseTableCleansUpPartitionTracking(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "replay_erase_cleanup_tracking_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First eraseTable call: adds partitions to tracking
        recycleBin.eraseTable(futureTime);

        // Verify tracking data exists
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p2.getId()));

        // Simulate follower replay: replayEraseTable should clean up all tracking data
        recycleBin.replayEraseTable(Lists.newArrayList(table.getId()));

        // Verify table is removed
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        // Verify all tracking data is cleaned up
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        // Verify partitions are removed from idToPartition
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p2.getId()));
        // Verify recycle times are cleaned
        Assertions.assertFalse(recycleBin.isContainedInidToRecycleTime(p1.getId()));
        Assertions.assertFalse(recycleBin.isContainedInidToRecycleTime(p2.getId()));
    }

    /**
     * Test that eraseTable() does nothing when partitions are still being deleted asynchronously.
     */
    @Test
    public void testEraseTableWaitsForPartitionsStillDeleting(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_table_waits_for_partitions_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        // Exactly 2 dropTable calls expected for the 2 partitions
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First eraseTable call: adds partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Second eraseTable call: partitions are still in idToPartition (not processed yet)
        // eraseTable should do nothing and wait
        recycleBin.eraseTable(futureTime);
        // Table should still exist
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Now process the partitions, waiting for async deletion to complete
        waitTableToBeDone(recycleBin, table.getId(), futureTime);

        // All partitions should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Third eraseTable call: all partitions deleted, should clean up and finish
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
    }

    /**
     * Test that erasePartition handles CompletableFuture.get() exception correctly for
     * partitions from table deletion. The partition should remain in the recycle bin for retry.
     */
    @Test
    public void testErasePartitionFromTableDeletionWithException(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_partition_table_deletion_exception_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 1 partition
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First call throws exception, second call succeeds
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = new RuntimeException("mocked RPC exception"); // throws exception
                result = buildDropTableResponse(0, ""); // succeeds on retry
            }
        };

        // First eraseTable call: adds partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));

        // Submit async task; wait for it to fail (exception path)
        // finished stays false, asyncDeleteForPartitions is cleaned up for retry
        waitTableToBeDone(recycleBin, table.getId(), futureTime);

        // Partition should still be pending (failed, not removed)
        Assertions.assertEquals(1, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));

        // Retry: submit new async task and wait for success
        waitTableToBeDone(recycleBin, table.getId(), futureTime);

        // Now partition should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up table
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
    }

    /**
     * Test erasing a lake table that is unpartitioned (single default partition).
     */
    @Test
    public void testEraseLakeUnpartitionedTable(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_lake_unpartitioned_table_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create an unpartitioned table
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 INT," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        // Unpartitioned table has exactly one partition
        Assertions.assertEquals(1, table.getPartitions().size());
        Partition defaultPartition = table.getPartitions().iterator().next();
        checkTableTablet(table, true);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        // Exactly 1 dropTable call expected for the single partition
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds the single partition to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertEquals(1, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(defaultPartition.getId()));

        // Process the partition, waiting for async deletion to complete
        waitTableToBeDone(recycleBin, table.getId(), futureTime);

        // Partition should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up table
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        checkTableTablet(table, false);
    }

    /**
     * Test erasing a lake table with list partitions.
     */
    @Test
    public void testEraseLakeListPartitionedTable(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_lake_list_partitioned_table_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a list-partitioned table
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
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

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);
        checkTableTablet(table, true);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        // Exactly 2 dropTable calls expected for the 2 list partitions
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds list partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));

        // Process the partitions, waiting for async deletion to complete
        waitTableToBeDone(recycleBin, table.getId(), futureTime);

        // Partitions should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up table
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        checkTableTablet(table, false);
    }

    /**
     * Test that RecyclePartitionInfo.forceRemoveDirectory defaults to false.
     * Partitions dropped individually (not as part of table deletion) should not
     * have forceRemoveDirectory set, so shared directories are skipped.
     */
    @Test
    public void testPartitionDroppedIndividuallyHasForceRemoveDirectoryFalse(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "partition_individual_drop_no_force_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Assertions.assertNotNull(p1);

        // Drop partition individually (force)
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p1 FORCE", dbName));
        Assertions.assertNull(table.getPartition("p1"));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));

        // Verify it's NOT marked as from table deletion
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()),
                "Individually dropped partition should not be marked as from table deletion");

        // Clean up
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        // Exactly 1 dropTable call expected for the individually dropped partition
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        waitPartitionClearFinished(recycleBin, p1.getId(), System.currentTimeMillis() + delay);
    }

    /**
     * Test that removeTableFromRecycleBin (called during replay or erase) properly cleans up
     * async delete futures for lake table partitions along with other tracking data.
     */
    @Test
    public void testRemoveTableFromRecycleBinCleansAsyncDeleteFutures(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "remove_table_cleans_async_futures_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));

        // erasePartition: submits async tasks (futures are created)
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 0;
                result = buildDropTableResponse(0, "");
            }
        };
        recycleBin.erasePartition(futureTime);

        // At this point, asyncDeleteForPartitions should have entries
        Assertions.assertTrue(containsAsyncDeletePartition(recycleBin, p1.getId()) ||
                        containsAsyncDeletePartition(recycleBin, p2.getId()),
                "At least one partition should have an async delete future");

        // removeTableFromRecycleBin should clean up everything including async futures
        recycleBin.removeTableFromRecycleBin(Lists.newArrayList(table.getId()));

        // Verify all tracking is cleaned up
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p2.getId()));
        Assertions.assertFalse(containsAsyncDeletePartition(recycleBin, p1.getId()));
        Assertions.assertFalse(containsAsyncDeletePartition(recycleBin, p2.getId()));
    }


    /**
     * Test that force dropping a Lake Table with shared partition directories
     * properly cleans up the shared directories. Without the forceRemoveDirectory flag,
     * shared directories would be skipped by removePartitionDirectory().
     */
    @Test
    public void testForceDropLakeTableCleansSharedDirectories(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "force_drop_shared_directory_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 3 partitions
        Table table = createTable(connectContext, String.format(
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

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        Partition p3 = table.getPartition("p3");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);
        Assertions.assertNotNull(p3);
        checkTableTablet(table, true);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        // Mock isSharedDirectory to always return true (simulating all partitions share a directory).
        // With forceRemoveDirectory=true (set during table deletion), isSharedDirectory should NOT
        // even be called because the && short-circuits in removePartitionDirectory.
        AtomicBoolean isSharedDirectoryCalled = new AtomicBoolean(false);
        new MockUp<LakeTableHelper>() {
            @Mock
            public boolean isSharedDirectory(String path, long partitionId) {
                isSharedDirectoryCalled.set(true);
                return true; // all dirs are "shared"
            }
        };
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        // All 3 partitions should have their directories removed even though they are "shared",
        // because forceRemoveDirectory=true bypasses the shared directory check.
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 3;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First eraseTable call: adds partitions with forceRemoveDirectory=true
        recycleBin.eraseTable(futureTime);

        // Verify partitions are tracked and have forceRemoveDirectory set
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertEquals(3, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p3.getId()));

        // Process the partitions, waiting for async deletion to complete
        waitTableToBeDone(recycleBin, table.getId(), futureTime);

        // Verify all partitions are deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Verify isSharedDirectory was NOT called (short-circuited by forceRemoveDirectory=true)
        Assertions.assertFalse(isSharedDirectoryCalled.get(),
                "isSharedDirectory should not be called when forceRemoveDirectory is true");

        // Final eraseTable call: cleans up table
        recycleBin.eraseTable(futureTime);

        // Table should be fully erased
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        checkTableTablet(table, false);
    }

    /**
     * Test all new @VisibleForTesting utility methods with non-existent IDs.
     */
    @Test
    public void testUtilityMethodsWithNonExistentIds() {
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        long nonExistentTableId = 999999L;
        long nonExistentPartitionId = 888888L;

        // getLakeTablePendingPartitionCount with non-existent tableId should return 0
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(nonExistentTableId));

        // isAnyLakeTablePartitionDeleting with non-existent tableId should return false
        Assertions.assertFalse(recycleBin.isAnyLakeTablePartitionDeleting(nonExistentTableId));

        // isLakeTableDeletingInProgress with non-existent tableId should return false
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(nonExistentTableId));

        // isPartitionFromTableDeletion with non-existent partitionId should return false
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(nonExistentPartitionId));
    }

    /**
     * Test that clear() properly cleans up all lake table tracking data structures.
     */
    @Test
    public void testClearCleansAllLakeTableTrackingData(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "clear_cleans_tracking_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        // Create a table with partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds partitions to tracking data structures
        recycleBin.eraseTable(futureTime);

        // Verify tracking data is populated BEFORE clear
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p2.getId()));

        // Call clear() - should clean up ALL data structures
        recycleBin.clear();

        // Verify all tracking data is cleaned up by clear()
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p2.getId()));
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertFalse(recycleBin.isAnyLakeTablePartitionDeleting(table.getId()));
    }

    /**
     * Test estimateCount() returns correct counts when lake table partitions are tracked.
     * This verifies that partitions added from table deletion are counted in "Partition" metric.
     */
    @Test
    public void testEstimateCountWithLakeTablePartitionsFromTableDeletion(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "estimate_count_with_tracking_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Verify initial state
        Map<String, Long> initialCounts = recycleBin.estimateCount();
        Assertions.assertEquals(0L, initialCounts.get("Partition"));
        Assertions.assertEquals(0L, initialCounts.get("AsyncDeletePartition"));
        Assertions.assertTrue(initialCounts.containsKey("Database"));
        Assertions.assertTrue(initialCounts.containsKey("Table"));
        // Verify AsyncDeleteTable key was removed (it should NOT exist)
        Assertions.assertFalse(initialCounts.containsKey("AsyncDeleteTable"));

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds 2 partitions to idToPartition from table deletion
        recycleBin.eraseTable(futureTime);

        // Verify estimateCount reflects the added partitions
        Map<String, Long> countsAfterErase = recycleBin.estimateCount();
        Assertions.assertEquals(2L, countsAfterErase.get("Partition"));
        Assertions.assertEquals(1L, countsAfterErase.get("Table"));
    }

    /**
     * Test removeTableFromRecycleBin when partitions have already been removed from idToPartition
     * by erasePartition().
     */
    @Test
    public void testRemoveTableFromRecycleBinWhenPartitionsAlreadyDeleted(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "remove_table_partitions_already_deleted_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Process partitions, waiting for async deletion to complete (don't call eraseTable yet)
        waitUntilNoAsyncDeletion(recycleBin, table.getId(), futureTime);

        // Verify partitions are removed from idToPartition
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p2.getId()));
        // But table is still in lakeTableToPartitions (hasn't been cleaned up by eraseTable yet)
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));

        // Now simulate a follower replay: replayEraseTable calls removeTableFromRecycleBin.
        // At this point, partitions are already removed from idToPartition,
        // so idToPartition.remove(partitionId) returns null inside the cleanup loop.
        recycleBin.replayEraseTable(Lists.newArrayList(table.getId()));

        // Verify table is removed
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        // Verify all tracking data is cleaned up
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p2.getId()));
    }

    /**
     * Test that erasePartition properly handles the case when partition.delete() returns false
     * (non-exception failure, e.g., dropTable RPC returns error status code).
     */
    @Test
    public void testErasePartitionDeleteReturnsFalseForLakeTablePartition(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_partition_delete_returns_false_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 1 partition
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        // First call returns error status (delete returns false), second call succeeds
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = buildDropTableResponse(1, "mocked error: storage unavailable");
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds partition to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));

        // Submit async task and wait for it to fail (dropTable returns error, future.get() returns false)
        // asyncDeleteForPartitions entry is removed for retry, setNextEraseMinTime schedules retry
        waitTableToBeDone(recycleBin, table.getId(), futureTime);

        // Partition should still be in idToPartition (failed, not removed)
        Assertions.assertEquals(1, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        // No async future should be running
        Assertions.assertFalse(recycleBin.isAnyLakeTablePartitionDeleting(table.getId()));

        // Use a far future time to overcome the retry delay (FAIL_RETRY_INTERVAL = 60s)
        long farFutureTime = System.currentTimeMillis() + CatalogRecycleBin.getFailRetryInterval() + delay + 1000;

        // Retry: submit new async task, wait for success
        waitTableToBeDone(recycleBin, table.getId(), farFutureTime);

        // Now partition should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up table
        recycleBin.eraseTable(farFutureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
    }

    /**
     * Test isAnyLakeTablePartitionDeleting returns true during active async deletion
     * and returns false after all async tasks complete (both when partitions are still
     * in idToPartition and when they've been removed).
     */
    @Test
    public void testIsAnyLakeTablePartitionDeletingDuringAndAfterAsyncDelete(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "any_partition_deleting_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 1 partition
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds partition to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        // Before any async task, no partition should be "deleting"
        Assertions.assertFalse(recycleBin.isAnyLakeTablePartitionDeleting(table.getId()));

        // erasePartition: submits async task
        recycleBin.erasePartition(futureTime);
        // Async task is running, isAnyLakeTablePartitionDeleting should return true
        Assertions.assertTrue(recycleBin.isAnyLakeTablePartitionDeleting(table.getId()));

        // Wait for async task to complete and process the result
        waitUntilNoAsyncDeletion(recycleBin, table.getId(), futureTime);

        // After processing, partition is removed from idToPartition and async future is cleaned up
        // isAnyLakeTablePartitionDeleting should return false (info is null in idToPartition)
        Assertions.assertFalse(recycleBin.isAnyLakeTablePartitionDeleting(table.getId()));

        // But table is still tracked (partitions still in lakeTableToPartitions set)
        Assertions.assertTrue(recycleBin.isLakeTableDeletingInProgress(table.getId()));
        // getPendingPartitionCount should be 0 (partitions removed from idToPartition)
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
    }

    /**
     * Test the complete lifecycle of dropping a Lake Table followed by dropping the same-named
     * re-created table. This ensures the tracking data structures are properly isolated between
     * different table instances with the same name.
     */
    @Test
    public void testDropRecreateThenDropLakeTableAgain(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "drop_recreate_drop_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 0;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;

        // First table creation and force drop
        String createSql = String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName);
        Table table1 = createTable(connectContext, createSql);
        Partition table1p1 = table1.getPartition("p1");
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        // Erase the first table completely
        long futureTime = System.currentTimeMillis() + delay;
        waitTableClearFinished(recycleBin, table1.getId(), futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table1.getId()));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table1.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(table1p1.getId()));

        // Re-create and force drop the same-named table
        Table table2 = createTable(connectContext, createSql);
        Partition table2p1 = table2.getPartition("p1");
        Assertions.assertNotEquals(table1.getId(), table2.getId());
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        // Erase the second table
        futureTime = System.currentTimeMillis() + delay;
        waitTableClearFinished(recycleBin, table2.getId(), futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table2.getId()));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table2.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(table2p1.getId()));
    }

    /**
     * Test that removeTableFromRecycleBin handles correctly when called for a table
     * that has NO lake partition tracking (i.e., lakeTableToPartitions does not contain
     * the table).
     */
    @Test
    public void testRemoveTableFromRecycleBinWithNoLakeTracking(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "remove_table_no_lake_tracking_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a lake table
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());

        // Force drop the table (puts it in recycle bin)
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));

        // Do NOT call eraseTable, so lakeTableToPartitions does NOT contain this table
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(table.getId()));

        // Directly call removeTableFromRecycleBin (simulating a replay before eraseTable was called)
        // This should handle the case where lakeTableToPartitions.remove(tableId) returns null
        recycleBin.removeTableFromRecycleBin(Lists.newArrayList(table.getId()));

        // Table should be removed without error
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
    }

    /**
     * Test that addLakeTablePartitionsToRecycleBin correctly marks partitions as
     * non-recoverable and sets recycle time to 0 for immediate processing.
     */
    @Test
    public void testAddLakeTablePartitionsToRecycleBinSetsCorrectProperties(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "add_partitions_properties_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: triggers addLakeTablePartitionsToRecycleBin
        recycleBin.eraseTable(futureTime);

        // Verify partition properties are set correctly by addLakeTablePartitionsToRecycleBin
        RecyclePartitionInfo p1Info = recycleBin.getRecyclePartitionInfo(p1.getId());
        RecyclePartitionInfo p2Info = recycleBin.getRecyclePartitionInfo(p2.getId());
        Assertions.assertNotNull(p1Info);
        Assertions.assertNotNull(p2Info);

        // Partitions should be non-recoverable (table is being erased)
        Assertions.assertFalse(p1Info.isRecoverable());
        Assertions.assertFalse(p2Info.isRecoverable());

        // Partitions should have forceRemoveDirectory=true (table-level deletion)
        Assertions.assertTrue(p1Info.isForceRemoveDirectory());
        Assertions.assertTrue(p2Info.isForceRemoveDirectory());

        // Recycle time should be set to 0 for immediate processing
        Assertions.assertTrue(recycleBin.isContainedInidToRecycleTime(p1.getId()));
        Assertions.assertTrue(recycleBin.isContainedInidToRecycleTime(p2.getId()));

        // Partitions should be marked as from table deletion
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));

        // Clean up
        recycleBin.clear();
    }
}

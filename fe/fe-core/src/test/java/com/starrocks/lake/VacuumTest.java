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

import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.vacuum.AutovacuumDaemon;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.Warehouse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Future;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class VacuumTest {
    private static Database db;
    private static OlapTable olapTable;
    private static PhysicalPartition partition;
    private static WarehouseManager warehouseManager;
    private static ComputeNode computeNode;
    private static LakeService lakeService;
    private static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;


    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(GlobalStateMgrTestUtil.testDb1)
                    .useDatabase(GlobalStateMgrTestUtil.testDb1);

        starRocksAssert.withTable("CREATE TABLE testTable1\n" +
                    "(\n" +
                    "    v1 date,\n" +
                    "    v2 int,\n" +
                    "    v3 int\n" +
                    ")\n" +
                    "DUPLICATE KEY(`v1`)\n" +
                    "DISTRIBUTED BY HASH(v1) BUCKETS 1\n" +
                    "PROPERTIES('replication_num' = '1');");

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);

        warehouseManager = mock(WarehouseManager.class);
        computeNode = mock(ComputeNode.class);
        

        when(warehouseManager.getBackgroundWarehouse()).thenReturn(mock(Warehouse.class));
        when(warehouseManager.getComputeNodeAssignedToTablet(anyString(), any(LakeTablet.class))).thenReturn(computeNode);

        when(computeNode.getHost()).thenReturn("localhost");
        when(computeNode.getBrpcPort()).thenReturn(8080);
        
    }

    @AfterAll
    public static void clear() {
        db.dropTable(olapTable.getName());
    }

    @Test
    public void testLastSuccVacuumVersionUpdate() throws Exception {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);

        AutovacuumDaemon autovacuumDaemon = new AutovacuumDaemon();

        VacuumResponse mockResponse = new VacuumResponse();
        mockResponse.status = new StatusPB();
        mockResponse.status.statusCode = 0;
        mockResponse.vacuumedFiles = 10L;
        mockResponse.vacuumedFileSize = 1024L;
        mockResponse.vacuumedVersion = 5L;
        mockResponse.extraFileSize = 1024L;
        mockResponse.tabletInfos = new ArrayList<>();

        Future<VacuumResponse> mockFuture = mock(Future.class);
        when(mockFuture.get()).thenReturn(mockResponse);

        lakeService = mock(LakeService.class);
        when(lakeService.vacuum(any(VacuumRequest.class))).thenReturn(mockFuture);
        try (MockedStatic<BrpcProxy> mockBrpcProxyStatic = mockStatic(BrpcProxy.class)) {
            mockBrpcProxyStatic.when(() -> BrpcProxy.getLakeService(anyString(), anyInt())).thenReturn(lakeService);
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable, partition);
        }
        
        Assertions.assertEquals(5L, partition.getLastSuccVacuumVersion());

        mockResponse.vacuumedVersion = 7L;
        try (MockedStatic<BrpcProxy> mockBrpcProxyStatic = mockStatic(BrpcProxy.class)) {
            mockBrpcProxyStatic.when(() -> BrpcProxy.getLakeService(anyString(), anyInt())).thenReturn(lakeService);
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable, partition);
        }
        Assertions.assertEquals(7L, partition.getLastSuccVacuumVersion());
    }

    @Test
    public void testLastSuccVacuumVersionUpdateFailed() throws Exception {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);
        AutovacuumDaemon autovacuumDaemon = new AutovacuumDaemon();

        VacuumResponse mockResponse = new VacuumResponse();
        mockResponse.status = new StatusPB();
        mockResponse.status.statusCode = 1;
        mockResponse.status.errorMsgs = Arrays.asList("internal failed");
        mockResponse.vacuumedFiles = 10L;
        mockResponse.vacuumedFileSize = 1024L;
        mockResponse.vacuumedVersion = 5L;
        mockResponse.extraFileSize = 1024L;
        mockResponse.tabletInfos = new ArrayList<>();

        Future<VacuumResponse> mockFuture = mock(Future.class);
        when(mockFuture.get()).thenReturn(mockResponse);

        lakeService = mock(LakeService.class);
        when(lakeService.vacuum(any(VacuumRequest.class))).thenReturn(mockFuture);
        try (MockedStatic<BrpcProxy> mockBrpcProxyStatic = mockStatic(BrpcProxy.class)) {
            mockBrpcProxyStatic.when(() -> BrpcProxy.getLakeService(anyString(), anyInt())).thenReturn(lakeService);
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable, partition);
        }

        Assertions.assertEquals(4L, partition.getLastSuccVacuumVersion());
    }

    @Test
    public void testVacuumCheck() throws Exception {
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);
        AutovacuumDaemon autovacuumDaemon = new AutovacuumDaemon();
        long current = System.currentTimeMillis();
        // static
        partition.setVisibleVersion(1L, current - Config.lake_autovacuum_stale_partition_threshold * 3600 * 1000);
        Assertions.assertFalse(autovacuumDaemon.shouldVacuum(partition));
        // empty
        partition.setVisibleVersion(1L, current);
        Assertions.assertFalse(autovacuumDaemon.shouldVacuum(partition));
        // too frequency
        partition.setVisibleVersion(10L, current);
        partition.setLastVacuumTime(current);
        Assertions.assertFalse(autovacuumDaemon.shouldVacuum(partition));
        // already vacuum success
        partition.setLastVacuumTime(current - Config.lake_autovacuum_partition_naptime_seconds * 1000 * 6);
        partition.setLastSuccVacuumVersion(10L);
        Assertions.assertFalse(autovacuumDaemon.shouldVacuum(partition));
        // disable
        Config.lake_autovacuum_detect_vaccumed_version = false;
        Assertions.assertTrue(autovacuumDaemon.shouldVacuum(partition));
    }

    /**
     * Test LakeTableHelper.computeMinActiveTxnId which is used by both AutovacuumDaemon and FullVacuumDaemon.
     * This method computes the minimum active transaction ID across:
     * 1. Database-level minimum active transaction ID
     * 2. Schema change handler's active transaction ID
     * 3. Rollup handler's active transaction ID
     */
    @Test
    public void testLakeTableHelperComputeMinActiveTxnId() throws Exception {
        long dbId = db.getId();
        long tableId = olapTable.getId();
        long minTxnId = 100L;
        long schemaChangeTxnId = 150L;
        long rollupTxnId = 200L;

        GlobalTransactionMgr globalTransactionMgr = mock(GlobalTransactionMgr.class);
        when(globalTransactionMgr.getMinActiveTxnIdOfDatabase(dbId)).thenReturn(minTxnId);

        SchemaChangeHandler schemaChangeHandler = mock(SchemaChangeHandler.class);
        when(schemaChangeHandler.getActiveTxnIdOfTable(tableId)).thenReturn(java.util.Optional.of(schemaChangeTxnId));

        MaterializedViewHandler rollupHandler = mock(MaterializedViewHandler.class);
        when(rollupHandler.getActiveTxnIdOfTable(tableId)).thenReturn(java.util.Optional.of(rollupTxnId));

        GlobalStateMgr globalStateMgr = mock(GlobalStateMgr.class);
        when(globalStateMgr.getGlobalTransactionMgr()).thenReturn(globalTransactionMgr);
        when(globalStateMgr.getSchemaChangeHandler()).thenReturn(schemaChangeHandler);
        when(globalStateMgr.getRollupHandler()).thenReturn(rollupHandler);

        try (MockedStatic<GlobalStateMgr> mockGlobalStateMgr = mockStatic(GlobalStateMgr.class)) {
            mockGlobalStateMgr.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);

            // Case 1: Database minTxnId is the smallest
            long result = LakeTableHelper.computeMinActiveTxnId(dbId, tableId);
            Assertions.assertEquals(minTxnId, result);

            // Case 2: schemaChangeTxnId is the smallest
            when(globalTransactionMgr.getMinActiveTxnIdOfDatabase(dbId)).thenReturn(300L);
            result = LakeTableHelper.computeMinActiveTxnId(dbId, tableId);
            Assertions.assertEquals(schemaChangeTxnId, result);

            // Case 3: rollupTxnId is the smallest
            when(schemaChangeHandler.getActiveTxnIdOfTable(tableId)).thenReturn(java.util.Optional.empty());
            result = LakeTableHelper.computeMinActiveTxnId(dbId, tableId);
            Assertions.assertEquals(rollupTxnId, result);

            // Case 4: All handlers return empty, use database min txn
            when(rollupHandler.getActiveTxnIdOfTable(tableId)).thenReturn(java.util.Optional.empty());
            result = LakeTableHelper.computeMinActiveTxnId(dbId, tableId);
            Assertions.assertEquals(300L, result);

            // Case 5: rollupTxnId is smallest (verifies rollup handler is actually checked)
            when(globalTransactionMgr.getMinActiveTxnIdOfDatabase(dbId)).thenReturn(500L);
            when(schemaChangeHandler.getActiveTxnIdOfTable(tableId)).thenReturn(java.util.Optional.of(400L));
            when(rollupHandler.getActiveTxnIdOfTable(tableId)).thenReturn(java.util.Optional.of(50L));
            result = LakeTableHelper.computeMinActiveTxnId(dbId, tableId);
            Assertions.assertEquals(50L, result);
        }
    }
}

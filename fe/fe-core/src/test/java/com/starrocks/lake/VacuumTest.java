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
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
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
    private static OlapTable olapTable2;
    private static OlapTable olapTable7;
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

        starRocksAssert.withTable("CREATE TABLE testTable2\n" +
                    "(\n" +
                    "    v1 date,\n" +
                    "    v2 int,\n" +
                    "    v3 int\n" +
                    ")\n" +
                    "DUPLICATE KEY(`v1`)\n" +
                    "DISTRIBUTED BY HASH(v1) BUCKETS 1\n" +
                    "PROPERTIES('file_bundling' = 'true');");

        starRocksAssert.withTable("CREATE TABLE testTable7\n" +
                    "(\n" +
                    "    v1 date,\n" +
                    "    v2 int,\n" +
                    "    v3 int\n" +
                    ")\n" +
                    "DUPLICATE KEY(`v1`)\n" +
                    "DISTRIBUTED BY HASH(v1) BUCKETS 1\n" +
                    "PROPERTIES('file_bundling' = 'true');");

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        olapTable2 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable2);
        olapTable7 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        warehouseManager = mock(WarehouseManager.class);
        computeNode = mock(ComputeNode.class);
        

        when(warehouseManager.getBackgroundWarehouse()).thenReturn(mock(Warehouse.class));
        when(warehouseManager.getComputeNodeAssignedToTablet((ComputeResource) any(), any(LakeTablet.class)))
                .thenReturn(computeNode);

        when(computeNode.getHost()).thenReturn("localhost");
        when(computeNode.getBrpcPort()).thenReturn(8080);
        
    }

    @AfterAll
    public static void clear() {
        db.dropTable(olapTable.getName());
        db.dropTable(olapTable2.getName());
        db.dropTable(olapTable7.getName());
    }

    @Test
    public void testLastSuccVacuumVersionUpdate() throws Exception {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setMetadataSwitchVersion(5L);
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
        Assertions.assertEquals(0L, partition.getMetadataSwitchVersion());
    }

    @Test
    public void testAggregateVacuum() throws Exception {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        partition = olapTable2.getPhysicalPartitions().stream().findFirst().orElse(null);
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
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable2, partition);
        }
        
        Assertions.assertEquals(5L, partition.getLastSuccVacuumVersion());

        mockResponse.vacuumedVersion = 7L;
        try (MockedStatic<BrpcProxy> mockBrpcProxyStatic = mockStatic(BrpcProxy.class)) {
            mockBrpcProxyStatic.when(() -> BrpcProxy.getLakeService(anyString(), anyInt())).thenReturn(lakeService);
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable2, partition);
        }
        Assertions.assertEquals(7L, partition.getLastSuccVacuumVersion());
    }

    @Test
    public void testMetadataSwitchVersionVacuum() throws Exception {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        partition = olapTable2.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(0L);
        partition.setLastSuccVacuumVersion(4L);
        partition.setMetadataSwitchVersion(6L);

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
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable2, partition);
        }

        Assertions.assertEquals(5L, partition.getLastSuccVacuumVersion());
        Assertions.assertEquals(6L, partition.getMetadataSwitchVersion());

        mockResponse.vacuumedVersion = 7L;
        try (MockedStatic<BrpcProxy> mockBrpcProxyStatic = mockStatic(BrpcProxy.class)) {
            mockBrpcProxyStatic.when(() -> BrpcProxy.getLakeService(anyString(), anyInt())).thenReturn(lakeService);
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable2, partition);
        }
        Assertions.assertEquals(7L, partition.getLastSuccVacuumVersion());
        Assertions.assertEquals(0L, partition.getMetadataSwitchVersion());
    }

    @Test
    public void testLastSuccVacuumVersionUpdateFailed() throws Exception {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);
        partition.setMetadataSwitchVersion(5L);
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
        Assertions.assertEquals(5L, partition.getMetadataSwitchVersion());
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
        partition.setVisibleVersion(10L, current - Config.lake_autovacuum_stale_partition_threshold * 3600 * 1000);
        Assertions.assertFalse(autovacuumDaemon.shouldVacuum(partition));
        // metaSwitchVersion is not 0
        partition.setMetadataSwitchVersion(5);
        Assertions.assertTrue(autovacuumDaemon.shouldVacuum(partition));
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
        // metadataSwitchVersion is not 0
        partition.setVisibleVersion(10L, current - Config.lake_autovacuum_stale_partition_threshold * 3600 * 1000 * 2);
        partition.setMinRetainVersion(0L);
        partition.setMetadataSwitchVersion(5L);
        partition.setLastSuccVacuumVersion(5L);
        Assertions.assertFalse(autovacuumDaemon.shouldVacuum(partition));
        partition.setMetadataSwitchVersion(6L);
        Assertions.assertTrue(autovacuumDaemon.shouldVacuum(partition));
        partition.setMinRetainVersion(0L);
        partition.setMetadataSwitchVersion(0L);
        partition.setVisibleVersion(10L, current);
        Assertions.assertTrue(autovacuumDaemon.shouldVacuum(partition));
        // disable
        Config.lake_autovacuum_detect_vaccumed_version = false;
        Assertions.assertTrue(autovacuumDaemon.shouldVacuum(partition));
    }

    @Test
    public void testVacuumImmediatelyGraceTimestamp() throws Exception {
        PhysicalPartition partition2 = olapTable7.getPhysicalPartitions().stream().findFirst().orElse(null);
        AutovacuumDaemon autovacuumDaemon = new AutovacuumDaemon();
        long current = System.currentTimeMillis();
        partition2.setVisibleVersion(10L, current);
        partition2.setMinRetainVersion(5L);
        partition2.setLastSuccVacuumVersion(10L);
        partition2.setLastVacuumTime(current);
        Assertions.assertFalse(autovacuumDaemon.shouldVacuum(partition2));

        Config.lake_vacuum_immediately_partition_ids = String.valueOf(partition2.getId());
        Assertions.assertTrue(autovacuumDaemon.shouldVacuum(partition2));
    }
}

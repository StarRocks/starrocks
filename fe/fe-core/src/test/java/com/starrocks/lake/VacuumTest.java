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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.lake.vacuum.AutovacuumDaemon;
import com.starrocks.lake.vacuum.FullVacuumDaemon;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.VacuumFullRequest;
import com.starrocks.proto.VacuumFullResponse;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.LakeServiceWithMetrics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyLong;
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
        when(warehouseManager.getComputeNodeAssignedToTablet((ComputeResource) any(), anyLong()))
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
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setMetadataSwitchVersion(5L);
        partition.setLastSuccVacuumVersion(4L);
        partition.setLastMinActiveTxnId(1L);  // confirmed predecessor so the round runs (Option A debounce)

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
    public void testVacuumRequestCarriesTimeout() throws Exception {
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);
        partition.setLastMinActiveTxnId(1L);  // confirmed predecessor so the round runs (Option A debounce)

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
        ArgumentCaptor<VacuumRequest> requestCaptor = ArgumentCaptor.forClass(VacuumRequest.class);
        when(lakeService.vacuum(requestCaptor.capture())).thenReturn(mockFuture);
        try (MockedStatic<BrpcProxy> mockBrpcProxyStatic = mockStatic(BrpcProxy.class)) {
            mockBrpcProxyStatic.when(() -> BrpcProxy.getLakeService(anyString(), anyInt())).thenReturn(lakeService);
            autovacuumDaemon.testVacuumPartitionImpl(db, olapTable, partition);
        }

        Assertions.assertFalse(requestCaptor.getAllValues().isEmpty());
        for (VacuumRequest request : requestCaptor.getAllValues()) {
            // The BE aborts the vacuum task once this duration has elapsed, so it must match
            // the longest the FE actually waits, i.e. the brpc timeout of the vacuum RPC.
            Assertions.assertEquals(LakeService.TIMEOUT_VACUUM, (long) request.timeoutMs);
        }
    }

    @Test
    public void testTxnLogSweepDebounce() throws Exception {
        // Option A (complete-round skip): an unconfirmed minActiveTxnId never drives a vacuum round; only a
        // value confirmed by the previous round (non-decreasing) is acted on, sweeping with that older value.
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);

        // Round 1: no confirmed predecessor (lastMinActiveTxnId == 0) -> the whole round is skipped.
        partition.setLastMinActiveTxnId(0L);
        List<VacuumRequest> round1 = runVacuumCaptureRequests();
        Assertions.assertTrue(round1.isEmpty(), "first round must be skipped entirely");
        long cur = partition.getLastMinActiveTxnId();   // seeded with the observed computeMinActiveTxnId()
        Assertions.assertTrue(cur > 0);

        // Round 2: confirmed non-decreasing -> run, sweeping with the PREVIOUS (older, lower) value, not cur.
        long former = Math.max(1L, cur - 1);
        partition.setLastMinActiveTxnId(former);
        List<VacuumRequest> round2 = runVacuumCaptureRequests();
        Assertions.assertFalse(round2.isEmpty(), "confirmed round must run");
        boolean swept = false;
        for (VacuumRequest req : round2) {
            Assertions.assertEquals(former, (long) req.minActiveTxnId,
                    "must act on the confirmed previous-round watermark, not the current observation");
            if (Boolean.TRUE.equals(req.deleteTxnLog)) {
                swept = true;
            }
        }
        Assertions.assertTrue(swept, "confirmed round must sweep txn logs on one node");

        // Round 3: regression (former set far above any real txn id) -> the whole round is skipped.
        partition.setLastMinActiveTxnId(Long.MAX_VALUE);
        List<VacuumRequest> round3 = runVacuumCaptureRequests();
        Assertions.assertTrue(round3.isEmpty(), "regression round must be skipped entirely");
    }

    private List<VacuumRequest> runVacuumCaptureRequests() throws Exception {
        VacuumResponse mockResponse = new VacuumResponse();
        mockResponse.status = new StatusPB();
        mockResponse.status.statusCode = 0;
        mockResponse.vacuumedFiles = 0L;
        mockResponse.vacuumedFileSize = 0L;
        mockResponse.vacuumedVersion = 0L;
        mockResponse.extraFileSize = 0L;
        mockResponse.tabletInfos = new ArrayList<>();

        Future<VacuumResponse> mockFuture = mock(Future.class);
        when(mockFuture.get()).thenReturn(mockResponse);

        LakeService svc = mock(LakeService.class);
        ArgumentCaptor<VacuumRequest> captor = ArgumentCaptor.forClass(VacuumRequest.class);
        when(svc.vacuum(captor.capture())).thenReturn(mockFuture);

        AutovacuumDaemon daemon = new AutovacuumDaemon();
        try (MockedStatic<BrpcProxy> mockBrpc = mockStatic(BrpcProxy.class)) {
            mockBrpc.when(() -> BrpcProxy.getLakeService(anyString(), anyInt())).thenReturn(svc);
            daemon.testVacuumPartitionImpl(db, olapTable, partition);
        }
        return captor.getAllValues();
    }

    @Test
    public void testAggregateVacuum() throws Exception {
        partition = olapTable2.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);
        partition.setLastMinActiveTxnId(1L);  // confirmed predecessor so the round runs (Option A debounce)

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
        partition = olapTable2.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(0L);
        partition.setLastSuccVacuumVersion(4L);
        partition.setMetadataSwitchVersion(6L);
        partition.setLastMinActiveTxnId(1L);  // confirmed predecessor so the round runs (Option A debounce)

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
    public void testFullVacuumBasic() {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        FullVacuumDaemon fullVacuumDaemon = new FullVacuumDaemon();

        Set<Long> allTabletIds = new HashSet<>();
        long testDbId = 0;
        List<Long> dbIds = currentState.getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database currDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (currDb != null && !currDb.isSystemDatabase()) {
                testDbId = dbId;
                break;
            }
        }
        final Database sourceDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        for (Table tbl : sourceDb.getTables()) {
            if (tbl.isOlapTable()) {
                OlapTable olapTbl = (OlapTable) tbl;
                for (PhysicalPartition part : olapTbl.getPhysicalPartitions()) {
                    part.setLastFullVacuumTime(1L);
                    for (MaterializedIndex index : part.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        allTabletIds.addAll(index.getTablets().stream().map(Tablet::getId).toList());
                    }
                }
            }
        }

        new MockUp<PhysicalPartition>() {
            @Mock
            public long getVisibleVersion() {
                return 10L;
            }
        };

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return sourceDb;
            }

            @Mock
            public List<Table> getTables(Long dbId) {
                return sourceDb.getTables();
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return new ComputeNode();
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) {
                return new LakeServiceWithMetrics(null);
            }
        };
        
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<VacuumFullResponse> vacuumFull(VacuumFullRequest request) {
                VacuumFullResponse resp = new VacuumFullResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = 0;
                resp.vacuumedFiles = 1L;
                resp.vacuumedFileSize = 1L;
                return CompletableFuture.completedFuture(resp);
            }
        };

        final StarOSAgent starOSAgent = new StarOSAgent();
        final ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        final WarehouseManager curWarehouseManager = new WarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public ClusterSnapshotMgr getClusterSnapshotMgr() {
                return clusterSnapshotMgr;
            }

            @Mock
            public WarehouseManager getWarehouseMgr() {
                return curWarehouseManager;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<VacuumFullRequest>() {
            @Mock
            public void setRetainVersions(List<Long> retainVersions) {
                Assertions.assertEquals(1, retainVersions.size());
                return;
            }
        };

        new MockUp<ClusterSnapshotMgr>() {
            @Mock
            public long getSafeDeletionTimeMs() {
                return 454545L;
            }
        };

        Config.lake_enable_fullvacuum = true;
        FeConstants.runningUnitTest = false;
        int oldValue1 = Config.lake_fullvacuum_parallel_partitions;
        long oldValue2 = Config.lake_fullvacuum_partition_naptime_seconds;
        Config.lake_fullvacuum_parallel_partitions = 1;
        Config.lake_fullvacuum_partition_naptime_seconds = 0;
        Deencapsulation.invoke(fullVacuumDaemon, "runAfterCatalogReady");
        Config.lake_fullvacuum_partition_naptime_seconds = oldValue2;
        Config.lake_fullvacuum_parallel_partitions = oldValue1;
        FeConstants.runningUnitTest = true;
        Config.lake_enable_fullvacuum = false;
    }

    @Test
    public void testFullVacuumMaxCheckVersionRespectsMinRetainVersion() {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        FullVacuumDaemon fullVacuumDaemon = new FullVacuumDaemon();

        long expectedVisibleVersion = 10L;
        long expectedMinRetainVersion = 6L;

        long testDbId = 0;
        List<Long> dbIds = currentState.getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database currDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (currDb != null && !currDb.isSystemDatabase()) {
                testDbId = dbId;
                break;
            }
        }
        final Database sourceDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        for (Table tbl : sourceDb.getTables()) {
            if (tbl.isOlapTable()) {
                OlapTable olapTbl = (OlapTable) tbl;
                for (PhysicalPartition part : olapTbl.getPhysicalPartitions()) {
                    part.setLastFullVacuumTime(1L);
                    part.setMinRetainVersion(expectedMinRetainVersion);
                }
            }
        }

        new MockUp<PhysicalPartition>() {
            @Mock
            public long getVisibleVersion() {
                return expectedVisibleVersion;
            }
        };

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return sourceDb;
            }

            @Mock
            public List<Table> getTables(Long dbId) {
                return sourceDb.getTables();
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return new ComputeNode();
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) {
                return new LakeServiceWithMetrics(null);
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<VacuumFullResponse> vacuumFull(VacuumFullRequest request) {
                VacuumFullResponse resp = new VacuumFullResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = 0;
                resp.vacuumedFiles = 1L;
                resp.vacuumedFileSize = 1L;
                return CompletableFuture.completedFuture(resp);
            }
        };

        final StarOSAgent starOSAgent = new StarOSAgent();
        final ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        final WarehouseManager curWarehouseManager = new WarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public ClusterSnapshotMgr getClusterSnapshotMgr() {
                return clusterSnapshotMgr;
            }

            @Mock
            public WarehouseManager getWarehouseMgr() {
                return curWarehouseManager;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<ClusterSnapshotMgr>() {
            @Mock
            public long getSafeDeletionTimeMs() {
                return 454545L;
            }
        };

        new MockUp<VacuumFullRequest>() {
            @Mock
            public void setMaxCheckVersion(long v) {
                // Expect min(visibleVersion, minRetainVersion)
                Assertions.assertEquals(Math.min(expectedVisibleVersion, expectedMinRetainVersion), v);
            }
        };

        FeConstants.runningUnitTest = false;
        int oldValue1 = Config.lake_fullvacuum_parallel_partitions;
        long oldValue2 = Config.lake_fullvacuum_partition_naptime_seconds;
        Config.lake_fullvacuum_parallel_partitions = 1;
        Config.lake_fullvacuum_partition_naptime_seconds = 0;
        Deencapsulation.invoke(fullVacuumDaemon, "runAfterCatalogReady");
        Config.lake_fullvacuum_partition_naptime_seconds = oldValue2;
        Config.lake_fullvacuum_parallel_partitions = oldValue1;
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testLastSuccVacuumVersionUpdateFailed() throws Exception {
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);
        partition.setMetadataSwitchVersion(5L);
        partition.setLastMinActiveTxnId(1L);  // confirmed predecessor so the round runs (Option A debounce)
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

            // Verify that AutovacuumDaemon and FullVacuumDaemon delegate to LakeTableHelper correctly
            // by calling their methods and checking results are consistent
            java.lang.reflect.Method autovacuumMethod = AutovacuumDaemon.class.getDeclaredMethod(
                    "computeMinActiveTxnId", Database.class, Table.class);
            autovacuumMethod.setAccessible(true);
            long autovacuumResult = (long) autovacuumMethod.invoke(null, db, olapTable);
            Assertions.assertEquals(50L, autovacuumResult);

            long fullVacuumResult = FullVacuumDaemon.computeMinActiveTxnId(db, olapTable);
            Assertions.assertEquals(50L, fullVacuumResult);
        }
    }

    @Test
    public void testParallelPartitionsPoolResize() throws Exception {
        // lake_autovacuum_parallel_partitions is mutable: the executor is resized in place by
        // adjustExecutorService() (invoked from the ConfigRefreshDaemon listener) rather than rebuilt.
        int oldParallel = Config.lake_autovacuum_parallel_partitions;
        try {
            Config.lake_autovacuum_parallel_partitions = 8;
            AutovacuumDaemon daemon = new AutovacuumDaemon();

            java.lang.reflect.Method getExecutorService =
                    AutovacuumDaemon.class.getDeclaredMethod("getExecutorService");
            getExecutorService.setAccessible(true);
            java.lang.reflect.Method adjustExecutorService =
                    AutovacuumDaemon.class.getDeclaredMethod("adjustExecutorService");
            adjustExecutorService.setAccessible(true);

            // Lazy creation: a fixed pool with core == max == config.
            ThreadPoolExecutor pool = (ThreadPoolExecutor) getExecutorService.invoke(daemon);
            Assertions.assertEquals(8, pool.getCorePoolSize());
            Assertions.assertEquals(8, pool.getMaximumPoolSize());

            // Grow.
            Config.lake_autovacuum_parallel_partitions = 16;
            adjustExecutorService.invoke(daemon);
            Assertions.assertEquals(16, pool.getCorePoolSize());
            Assertions.assertEquals(16, pool.getMaximumPoolSize());

            // Shrink.
            Config.lake_autovacuum_parallel_partitions = 4;
            adjustExecutorService.invoke(daemon);
            Assertions.assertEquals(4, pool.getCorePoolSize());
            Assertions.assertEquals(4, pool.getMaximumPoolSize());

            // Values above the hard cap are clamped to MAX_PARALLEL_PARTITIONS (256) so in-flight work
            // never exceeds the pool queue.
            Config.lake_autovacuum_parallel_partitions = 1000;
            adjustExecutorService.invoke(daemon);
            Assertions.assertEquals(256, pool.getCorePoolSize());
            Assertions.assertEquals(256, pool.getMaximumPoolSize());

            // Non-positive values are ignored so the pool keeps its previous size (256 from the clamp above).
            Config.lake_autovacuum_parallel_partitions = 0;
            adjustExecutorService.invoke(daemon);
            Assertions.assertEquals(256, pool.getCorePoolSize());
            Assertions.assertEquals(256, pool.getMaximumPoolSize());

            Config.lake_autovacuum_parallel_partitions = -1;
            adjustExecutorService.invoke(daemon);
            Assertions.assertEquals(256, pool.getCorePoolSize());
            Assertions.assertEquals(256, pool.getMaximumPoolSize());
        } finally {
            Config.lake_autovacuum_parallel_partitions = oldParallel;
        }
    }

    @Test
    public void testCollectCandidatesSkipsInFlightAndCapturesLastVacuumTime() throws Exception {
        long oldNaptime = Config.lake_autovacuum_partition_naptime_seconds;
        boolean oldDetect = Config.lake_autovacuum_detect_vaccumed_version;
        try {
            // Make shouldVacuum() deterministic: no naptime gate and no vacuumed-version short-circuit.
            Config.lake_autovacuum_partition_naptime_seconds = 0;
            Config.lake_autovacuum_detect_vaccumed_version = false;

            PhysicalPartition p1 = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
            PhysicalPartition p2 = olapTable2.getPhysicalPartitions().stream().findFirst().orElse(null);
            Assertions.assertNotNull(p1);
            Assertions.assertNotNull(p2);

            long now = System.currentTimeMillis();
            // Both partitions are non-empty and fresh, so shouldVacuum() returns true.
            p1.setMetadataSwitchVersion(0);
            p1.setVisibleVersion(10L, now);
            p1.setLastVacuumTime(2000L);   // vacuumed more recently
            p2.setMetadataSwitchVersion(0);
            p2.setVisibleVersion(10L, now);
            p2.setLastVacuumTime(1000L);   // vacuumed longer ago -> should sort first (LRU)

            AutovacuumDaemon daemon = new AutovacuumDaemon();
            java.lang.reflect.Method collectTableCandidates = AutovacuumDaemon.class.getDeclaredMethod(
                    "collectTableCandidates", Database.class, OlapTable.class, List.class);
            collectTableCandidates.setAccessible(true);

            // Fresh candidates: both partitions are collected, and each candidate snapshots the
            // partition's lastVacuumTime used as the LRU ordering key.
            List<Object> candidates = new ArrayList<>();
            collectTableCandidates.invoke(daemon, db, olapTable, candidates);
            collectTableCandidates.invoke(daemon, db, olapTable2, candidates);

            Object c1 = findCandidate(candidates, p1.getId());
            Object c2 = findCandidate(candidates, p2.getId());
            Assertions.assertNotNull(c1, "partition p1 should be collected");
            Assertions.assertNotNull(c2, "partition p2 should be collected");
            Assertions.assertEquals(2000L, candidateLastVacuumTime(c1));
            Assertions.assertEquals(1000L, candidateLastVacuumTime(c2));

            // LRU ordering: the partition vacuumed longest ago (p2) comes first after sorting.
            candidates.sort(Comparator.comparingLong(candidate -> {
                try {
                    return candidateLastVacuumTime(candidate);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
            Assertions.assertEquals(p2.getId(), candidatePartitionId(candidates.get(0)));

            // Partitions already in flight are excluded from the next round's candidates.
            java.lang.reflect.Field vacuumingField =
                    AutovacuumDaemon.class.getDeclaredField("vacuumingPartitions");
            vacuumingField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Set<Long> vacuumingPartitions = (Set<Long>) vacuumingField.get(daemon);
            vacuumingPartitions.add(p1.getId());
            try {
                List<Object> afterInFlight = new ArrayList<>();
                collectTableCandidates.invoke(daemon, db, olapTable, afterInFlight);
                Assertions.assertNull(findCandidate(afterInFlight, p1.getId()),
                        "in-flight partition must be skipped");
            } finally {
                vacuumingPartitions.remove(p1.getId());
            }
        } finally {
            Config.lake_autovacuum_partition_naptime_seconds = oldNaptime;
            Config.lake_autovacuum_detect_vaccumed_version = oldDetect;
        }
    }

    @Test
    public void testOnStoppedReleasesLeaderSessionState() throws Exception {
        // On leader demotion onStopped() must shut down and dereference the executor (so it is
        // recreated on re-election) and release the reservations of queued-but-never-run tasks.
        // A still-running task keeps its reservation until its own finally releases it: dropping
        // it early would let a re-elected session vacuum the same partition concurrently with the
        // straggler, and the straggler's late finally would then drop the new session's live entry.
        int oldParallel = Config.lake_autovacuum_parallel_partitions;
        try {
            // Single-threaded pool: the first task occupies the worker, the second stays queued.
            Config.lake_autovacuum_parallel_partitions = 1;
            AutovacuumDaemon daemon = new AutovacuumDaemon();
            ThreadPoolExecutor pool = Deencapsulation.invoke(daemon, "getExecutorService");
            Set<Long> vacuumingPartitions = Deencapsulation.getField(daemon, "vacuumingPartitions");

            long runningPartition = 123L;
            long queuedPartition = 456L;
            vacuumingPartitions.add(runningPartition);
            vacuumingPartitions.add(queuedPartition);

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            // Emulates vacuumPartition() for a straggler stuck in a non-interruptible section: it
            // survives the shutdownNow() interrupt and only its finally releases its reservation.
            pool.execute(() -> {
                try {
                    started.countDown();
                    boolean released = false;
                    while (!released) {
                        try {
                            release.await();
                            released = true;
                        } catch (InterruptedException ignored) {
                            // Swallow the shutdownNow() interrupt and keep running.
                        }
                    }
                } finally {
                    vacuumingPartitions.remove(runningPartition);
                }
            });
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            pool.execute(newVacuumTask(queuedPartition, () -> vacuumingPartitions.remove(queuedPartition)));

            Deencapsulation.setField(daemon, "nextCollectTimeMs", 999L);
            Deencapsulation.invoke(daemon, "onStopped");

            Assertions.assertTrue(pool.isShutdown());
            Assertions.assertNull(Deencapsulation.getField(daemon, "executorService"));
            Assertions.assertTrue(
                    ((java.util.Collection<?>) Deencapsulation.getField(daemon, "pendingCandidates")).isEmpty());
            Assertions.assertEquals(0L, (long) Deencapsulation.getField(daemon, "nextCollectTimeMs"));
            // The queued task will never run its finally, so onStopped() released its reservation...
            Assertions.assertFalse(vacuumingPartitions.contains(queuedPartition));
            // ...while the running task's reservation survives, so a re-elected session skips it.
            Assertions.assertTrue(vacuumingPartitions.contains(runningPartition));

            // The straggler eventually finishes and releases its own reservation.
            release.countDown();
            Assertions.assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            Assertions.assertTrue(vacuumingPartitions.isEmpty());
        } finally {
            Config.lake_autovacuum_parallel_partitions = oldParallel;
        }
    }

    @Test
    public void testScheduleVacuumRoundGatesAndRejectionRollback() throws Exception {
        long oldNaptime = Config.lake_autovacuum_partition_naptime_seconds;
        boolean oldDetect = Config.lake_autovacuum_detect_vaccumed_version;
        int oldParallel = Config.lake_autovacuum_parallel_partitions;
        try {
            Config.lake_autovacuum_partition_naptime_seconds = 0;
            Config.lake_autovacuum_detect_vaccumed_version = false;

            AutovacuumDaemon daemon = new AutovacuumDaemon();
            java.util.Collection<?> pendingCandidates = Deencapsulation.getField(daemon, "pendingCandidates");
            Set<Long> vacuumingPartitions = Deencapsulation.getField(daemon, "vacuumingPartitions");

            // Non-positive parallelism disables AutoVacuum: the round returns before collecting anything.
            Config.lake_autovacuum_parallel_partitions = 0;
            Deencapsulation.invoke(daemon, "scheduleVacuumRound");
            Assertions.assertTrue(pendingCandidates.isEmpty());

            // All slots already in flight: the round also returns before collecting.
            Config.lake_autovacuum_parallel_partitions = 2;
            vacuumingPartitions.add(1L);
            vacuumingPartitions.add(2L);
            Deencapsulation.invoke(daemon, "scheduleVacuumRound");
            Assertions.assertTrue(pendingCandidates.isEmpty());
            vacuumingPartitions.clear();

            // Free slots and a vacuumable partition, but an injected shut-down pool makes every
            // execute() throw RejectedExecutionException: the submission must be rolled back so no
            // partition id is left "in flight" forever.
            PhysicalPartition p = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
            Assertions.assertNotNull(p);
            p.setMetadataSwitchVersion(0);
            p.setVisibleVersion(10L, System.currentTimeMillis());
            p.setLastVacuumTime(1000L);

            Config.lake_autovacuum_parallel_partitions = 8;
            ThreadPoolExecutor deadPool = new ThreadPoolExecutor(
                    1, 1, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
            deadPool.shutdown();
            Deencapsulation.setField(daemon, "executorService", deadPool);
            Deencapsulation.invoke(daemon, "scheduleVacuumRound");
            Assertions.assertTrue(vacuumingPartitions.isEmpty());
        } finally {
            Config.lake_autovacuum_partition_naptime_seconds = oldNaptime;
            Config.lake_autovacuum_detect_vaccumed_version = oldDetect;
            Config.lake_autovacuum_parallel_partitions = oldParallel;
        }
    }

    @Test
    public void testRefillOnlyWhenEmptyAndBackoffOnEmptyCollection() throws Exception {
        long oldNaptime = Config.lake_autovacuum_partition_naptime_seconds;
        boolean oldDetect = Config.lake_autovacuum_detect_vaccumed_version;
        try {
            Config.lake_autovacuum_partition_naptime_seconds = 0;
            Config.lake_autovacuum_detect_vaccumed_version = false;

            PhysicalPartition p = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
            Assertions.assertNotNull(p);
            p.setMetadataSwitchVersion(0);
            p.setVisibleVersion(10L, System.currentTimeMillis());
            p.setLastVacuumTime(1000L);

            AutovacuumDaemon daemon = new AutovacuumDaemon();
            java.util.Collection<?> pendingCandidates = Deencapsulation.getField(daemon, "pendingCandidates");

            // First call fills the queue from a collection.
            Deencapsulation.invoke(daemon, "refillPendingCandidatesIfEmpty");
            int sizeAfterFirst = pendingCandidates.size();
            Assertions.assertTrue(sizeAfterFirst > 0);

            // A second call is a no-op while the queue is non-empty: no re-collection, size unchanged.
            Deencapsulation.invoke(daemon, "refillPendingCandidatesIfEmpty");
            Assertions.assertEquals(sizeAfterFirst, pendingCandidates.size());

            // Drain the queue and make every table's partitions non-vacuumable (empty partitions):
            // the next collection comes up empty, arms the back-off, and leaves the queue empty.
            pendingCandidates.clear();
            for (OlapTable table : new OlapTable[] {olapTable, olapTable2, olapTable7}) {
                for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                    partition.setMetadataSwitchVersion(0);
                    partition.setVisibleVersion(1L, System.currentTimeMillis());
                }
            }
            Deencapsulation.invoke(daemon, "refillPendingCandidatesIfEmpty");
            long backoffUntil = Deencapsulation.getField(daemon, "nextCollectTimeMs");
            Assertions.assertTrue(backoffUntil > System.currentTimeMillis());
            Assertions.assertTrue(pendingCandidates.isEmpty());

            // A subsequent call within the back-off window is skipped: the back-off point does not move.
            Deencapsulation.invoke(daemon, "refillPendingCandidatesIfEmpty");
            Assertions.assertEquals(backoffUntil, (long) Deencapsulation.getField(daemon, "nextCollectTimeMs"));
        } finally {
            Config.lake_autovacuum_partition_naptime_seconds = oldNaptime;
            Config.lake_autovacuum_detect_vaccumed_version = oldDetect;
        }
    }

    // VacuumTask is private to AutovacuumDaemon; construct it reflectively so the test can put a
    // task carrying a partition id into the pool queue exactly as submitPendingCandidates() does.
    private static Runnable newVacuumTask(long partitionId, Runnable work) throws Exception {
        Class<?> clazz = Class.forName("com.starrocks.lake.vacuum.AutovacuumDaemon$VacuumTask");
        java.lang.reflect.Constructor<?> ctor = clazz.getDeclaredConstructor(long.class, Runnable.class);
        ctor.setAccessible(true);
        return (Runnable) ctor.newInstance(partitionId, work);
    }

    private static Object findCandidate(List<Object> candidates, long partitionId) throws Exception {
        for (Object candidate : candidates) {
            if (candidatePartitionId(candidate) == partitionId) {
                return candidate;
            }
        }
        return null;
    }

    private static long candidatePartitionId(Object candidate) throws Exception {
        java.lang.reflect.Field field = candidate.getClass().getDeclaredField("partition");
        field.setAccessible(true);
        return ((PhysicalPartition) field.get(candidate)).getId();
    }

    private static long candidateLastVacuumTime(Object candidate) throws Exception {
        java.lang.reflect.Field field = candidate.getClass().getDeclaredField("lastVacuumTime");
        field.setAccessible(true);
        return (long) field.get(candidate);
    }
}

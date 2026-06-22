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
import org.mockito.ArgumentCaptor;
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
<<<<<<< HEAD
=======
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
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
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
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
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
>>>>>>> a10c264de6 ([BugFix] Debounce autovacuum minActiveTxnId before txn-log deletion (#74906))
    public void testLastSuccVacuumVersionUpdateFailed() throws Exception {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        partition = olapTable.getPhysicalPartitions().stream().findFirst().orElse(null);
        partition.setVisibleVersion(10L, System.currentTimeMillis());
        partition.setMinRetainVersion(10L);
        partition.setLastSuccVacuumVersion(4L);
<<<<<<< HEAD
=======
        partition.setMetadataSwitchVersion(5L);
        partition.setLastMinActiveTxnId(1L);  // confirmed predecessor so the round runs (Option A debounce)
>>>>>>> a10c264de6 ([BugFix] Debounce autovacuum minActiveTxnId before txn-log deletion (#74906))
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

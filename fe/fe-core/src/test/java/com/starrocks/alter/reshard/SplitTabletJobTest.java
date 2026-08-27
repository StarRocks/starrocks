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

package com.starrocks.alter.reshard;

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.Utils;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class SplitTabletJobTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "order by(key1)\n" +
                "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public <T> Future<T> submit(Callable<T> task) throws Exception {
                return CompletableFuture.completedFuture(task.call());
            }
        };
    }

    @Test
    public void testRunTabletReshardJob() throws Exception {
        installLakeServiceMock(this::addDataDrivenRanges);

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        List<Long> oldTabletIds = new ArrayList<>();
        for (Tablet tablet : materializedIndex.getTablets()) {
            oldTabletIds.add(tablet.getId());
        }
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Long tabletId : oldTabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId));
        }
        long oldVersion = physicalPartition.getVisibleVersion();

        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        // Admission reserves the table.
        tabletReshardJob.init();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() > materializedIndex.getTablets().size());

        // The superseded (old) split-parent index is scheduled for removal in the recycle bin (issue
        // #75993) but LEFT INSTALLED on the partition, so an in-flight query planned against it can
        // finish reading until the retention expires. Its tablets stay registered in the inverted
        // index, and because the index is still on the partition its shards stay protected from the
        // per-shard StarMgrMetaSyncer reaper.
        for (Long tabletId : oldTabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId));
        }
        Assertions.assertNotNull(physicalPartition.getIndex(materializedIndex.getId()));
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin()
                .isMaterializedIndexRecycled(materializedIndex.getId()));

        for (Tablet tablet : newMaterializedIndex.getTablets()) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tablet.getId()));
        }
    }

    @Test
    public void testPkOrderByFactoryLeavesBoundarySelectionToBePkIndex() throws Exception {
        starRocksAssert.withTable("CREATE TABLE pk_order_by_split "
                + "(pk1 int not null, pk2 int not null, sort_col int not null) "
                + "PRIMARY KEY(pk1, pk2) ORDER BY(sort_col) "
                + "PROPERTIES('replication_num' = '1', 'file_bundling' = 'true')");
        OlapTable pkOrderByTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "pk_order_by_split");
        PhysicalPartition physicalPartition = pkOrderByTable.getAllPhysicalPartitions().iterator().next();
        long tabletId = physicalPartition.getLatestBaseIndex().getTablets().get(0).getId();

        SplitTabletClause clause = new SplitTabletClause(null, new TabletList(List.of(tabletId)),
                Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2"));
        clause.setTabletReshardTargetSize(-2);
        SplitTabletJob job = (SplitTabletJob) new SplitTabletJobFactory(db, pkOrderByTable, clause)
                .createTabletReshardJob();

        SplittingTablet splittingTablet = job.getReshardingPhysicalPartitions().values().stream()
                .flatMap(partition -> partition.getReshardingIndexes().values().stream())
                .flatMap(index -> index.getReshardingTablets().stream())
                .map(ReshardingTablet::getSplittingTablet)
                .filter(java.util.Objects::nonNull)
                .findFirst().orElseThrow();
        Assertions.assertTrue(splittingTablet.getNewTabletRanges().isEmpty(),
                "ordinary split must let BE derive PK boundaries from the cloud-native PK index");
        Assertions.assertTrue(splittingTablet.getNewTabletIds().size() > 1);
    }

    @Test
    public void testRunBumpsOptimisticVersion() throws Exception {
        installLakeServiceMock(this::addDataDrivenRanges);

        long beforeSplit = table.lastSchemaUpdateTime.get();

        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);
        tabletReshardJob.init();
        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, tabletReshardJob.getJobState());

        // runRunningJob() -> addNewMaterializedIndexes() installs the split-child indexes and changes
        // the partition tablet layout. It must bump lastSchemaUpdateTime so a query planned concurrently
        // is re-planned by StatementPlanner's retry loop against the new layout, instead of failing with
        // "Invalid tablet id ... The tablet may have been dropped". This runs on the RUNNING step.
        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, tabletReshardJob.getJobState());

        long afterSplit = table.lastSchemaUpdateTime.get();
        Assertions.assertTrue(afterSplit > beforeSplit,
                "split must bump lastSchemaUpdateTime (before=" + beforeSplit + ", after=" + afterSplit + ")");
    }

    @Test
    public void testFallbackToIdenticalTablet() throws Exception {
        installLakeServiceMock(this::addFallbackToIdenticalRanges);

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        long oldVersion = physicalPartition.getVisibleVersion();

        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        // Admission reserves the table.
        tabletReshardJob.init();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() == materializedIndex.getTablets().size());
    }

    @Test
    public void testReplayTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        long oldVersion = physicalPartition.getVisibleVersion();

        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        // In production the original leader's runPendingJob calls createShardsOnStarOS
        // before the followers ever replay this job, so the new shards exist on staros
        // at replay time. Mirror that here so subsequent tests (which reuse the shared
        // static table fixture) find every tablet in the FE catalog backed by a real
        // staros shard.
        ((SplitTabletJob) tabletReshardJob).createShardsOnStarOS();

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        tabletReshardJob.replay();
        // replayPendingJob now performs the table reservation (moved here from replayPreparingJob).
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.setJobState(TabletReshardJob.JobState.PREPARING);
        tabletReshardJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.setJobState(TabletReshardJob.JobState.RUNNING);
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.CLEANING);
        tabletReshardJob.replay();

        // After CLEANING replay, the new tablets have been added to the partition
        // but without proper ranges (they have default Range.all() with null bounds).
        // We need to set proper contiguous ranges for the new tablets to satisfy
        // the strict validation in shareAdjacentTabletRangeBounds().
        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        List<Tablet> newTablets = newMaterializedIndex.getTablets();
        List<Long> newTabletIds = new ArrayList<>();
        for (Tablet tablet : newTablets) {
            newTabletIds.add(tablet.getId());
        }
        Map<Long, TabletRangePB> tabletRanges = createContiguousTabletRanges(newTabletIds);
        for (Tablet tablet : newTablets) {
            TabletRangePB rangePB = tabletRanges.get(tablet.getId());
            if (rangePB != null) {
                tablet.setRange(TabletRange.fromProto(rangePB));
            }
        }
        // Now call shareAdjacentTabletRangeBounds to optimize memory
        newMaterializedIndex.shareAdjacentTabletRangeBounds();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.FINISHED);
        tabletReshardJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        long newVersion = physicalPartition.getVisibleVersion();
        Assertions.assertTrue(newVersion == oldVersion + 1);

        Assertions.assertTrue(newMaterializedIndex != materializedIndex);

        Assertions.assertTrue(newMaterializedIndex.getTablets().size() > materializedIndex.getTablets().size());
    }

    @Test
    public void testAbortTabletReshardJob() throws Exception {
        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        // Admission reserves the table; abort must release it back to NORMAL.
        tabletReshardJob.init();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.abort("test abort");
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTING, tabletReshardJob.getJobState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTED, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testReplayAbortTabletReshardJob() throws Exception {
        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.ABORTING);
        tabletReshardJob.replay();

        tabletReshardJob.setJobState(TabletReshardJob.JobState.ABORTED);
        tabletReshardJob.replay();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testRunRunningUsesBackgroundComputeResource() throws Exception {
        SplitTabletJob splitJob = (SplitTabletJob) createTabletReshardJob();
        splitJob.init();
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        ComputeResource expectedResource = WarehouseComputeResource.of(10086L);
        AtomicReference<ComputeResource> actualResource = new AtomicReference<>();

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResource(long tableId) {
                Assertions.assertEquals(table.getId(), tableId);
                return expectedResource;
            }
        };

        new MockUp<Utils>() {
            @Mock
            public void publishVersion(List<Tablet> tablets, TxnInfoPB txnInfo,
                                       long baseVersion, long newVersion, Map<Long, Double> compactionScores,
                                       Map<Long, TabletRange> tabletRanges, ComputeResource computeResource,
                                       Map<Long, com.starrocks.proto.TabletStatPB> tabletStats,
                                       boolean useAggregatePublish,
                                       List<VectorIndexBuildInfoPB> vectorIndexBuildInfos) {
                actualResource.set(computeResource);
            }
        };

        // Isolate the publish-resource assertion from StarOS shard creation (which would otherwise run
        // against the mocked synthetic warehouse and fail).
        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForSplit(Map<Long, Long> newToOldShardId,
                                             Map<Long, List<Long>> newShardIdToGroupIds,
                                             FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo,
                                             Map<String, String> properties,
                                             ComputeResource computeResource,
                                             boolean spreadNewShards) {
            }
        };

        try {
            splitJob.run();
            Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, splitJob.getJobState());
            Assertions.assertSame(expectedResource, actualResource.get());
        } finally {
            splitJob.replayAbortedJob();
            physicalPartition.setNextVersion(physicalPartition.getVisibleVersion() + 1);
        }
    }

    // The job's warehouse defaults to "unset" (null -> background) and is settable by the pre-split
    // caller (base-class TabletReshardJob#setWarehouseId, also used by the merge job). The
    // empty-source -> spread / non-empty -> PACK behavior is covered by the end-to-end
    // external-boundaries flow and StarOSAgentTest#testCreateShardsForSplitSpreadDropsWithShardPin.
    @Test
    public void testWarehouseIdDefaultsUnsetAndIsSettable() throws Exception {
        SplitTabletJob job = (SplitTabletJob) createTabletReshardJob();
        Assertions.assertNull(job.getWarehouseId(),
                "warehouse defaults to unset (null -> background warehouse)");
        job.setWarehouseId(12345L);
        Assertions.assertEquals(Long.valueOf(12345L), job.getWarehouseId(),
                "caller applies the load's warehouse id");
    }

    // -------------------------------------------------------------------------
    // external boundaries end-to-end driver.
    //
    // Drives SplitTabletJobFactory.forExternalBoundaries(...) through PENDING
    // -> RUNNING -> FINISHED with a mocked BE that echoes the FE-supplied
    // ranges back verbatim (the BE contract: "external boundaries honored
    // verbatim"). Asserts the SplittingTablet carries newTabletRanges all the
    // way through toProto(), and that the resulting new tablets land with the
    // FE-supplied ranges.
    // -------------------------------------------------------------------------
    @Test
    public void testRunExternalBoundariesTabletReshardJob() throws Exception {
        installLakeServiceMock(this::addEchoedRanges);

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        Tablet oldTablet = materializedIndex.getTablets().get(0);
        long oldTabletId = oldTablet.getId();
        long oldVersion = physicalPartition.getVisibleVersion();
        int oldTabletCount = materializedIndex.getTablets().size();

        // Tests in this class share a single static table, and earlier @Test
        // methods can leave the latest index's tablets with bounded ranges.
        // Reset the chosen old tablet to Range.all() so the K=3 external boundaries ranges
        // below — built for a Range.all() parent — are deterministically
        // valid regardless of test order.
        oldTablet.setRange(new TabletRange());

        // K=3 external boundaries ranges that satisfy the BE-side structural validator on a
        // Range.all() parent: first.lower absent, last.upper absent, interior
        // boundaries set with closed-lower / open-upper, adjacent ranges meet
        // exactly. This mirrors the contract enforced in
        // TabletRangeHelper::validate_new_tablet_ranges (BE).
        List<TabletRange> newTabletRanges = List.of(
                tabletRangeUpperOnly(100),
                tabletRange(100, 200),
                tabletRangeLowerOnly(200));

        TabletReshardJob tabletReshardJob = SplitTabletJobFactory.forExternalBoundaries(
                db, table, Map.of(oldTabletId, newTabletRanges));
        Assertions.assertNotNull(tabletReshardJob);

        // The SplittingTablet inside the job must carry the FE-supplied
        // ranges, and toProto() must serialize them into the wire shape that
        // BE's split_tablet dispatches on.
        SplittingTablet splittingTablet = findSplittingTablet(tabletReshardJob, oldTabletId);
        Assertions.assertEquals(newTabletRanges.size(), splittingTablet.getNewTabletIds().size());
        Assertions.assertEquals(newTabletRanges.size(), splittingTablet.getNewTabletRanges().size());

        ReshardingTabletInfoPB pb = splittingTablet.toProto();
        Assertions.assertNotNull(pb.splittingTabletInfo.newTabletRanges,
                "newTabletRanges must be present on the wire so BE dispatches to external boundaries");
        Assertions.assertEquals(newTabletRanges.size(), pb.splittingTabletInfo.newTabletRanges.size());

        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        // Admission reserves the table.
        tabletReshardJob.init();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, tabletReshardJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(oldVersion + 1, physicalPartition.getVisibleVersion());

        // Net effect: the index gained K-1 tablets; the chosen old tablet is
        // gone; the K new tablets carry the FE-supplied ranges (echoed by the
        // mocked BE).
        MaterializedIndex newMaterializedIndex = physicalPartition.getLatestBaseIndex();
        Assertions.assertEquals(oldTabletCount + (newTabletRanges.size() - 1),
                newMaterializedIndex.getTablets().size());
        // The old tablet is retained (parked in the recycle bin with the superseded index), not deleted
        // immediately, so an in-flight split-parent read can finish (issue #75993).
        Assertions.assertNotNull(GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(oldTabletId));

        // TabletRangePB (generated jprotobuf class) has no equals override;
        // compare the underlying Range<Tuple> which does. The values must
        // match the FE-supplied external boundaries ranges position-for-position.
        List<Long> ids = splittingTablet.getNewTabletIds();
        for (int idx = 0; idx < ids.size(); idx++) {
            Tablet newTablet = newMaterializedIndex.getTablet(ids.get(idx));
            Assertions.assertNotNull(newTablet);
            Assertions.assertNotNull(newTablet.getRange());
            Assertions.assertEquals(newTabletRanges.get(idx).getRange(), newTablet.getRange().getRange(),
                    "new tablet range must match the FE-supplied external boundaries range exactly (idx=" + idx + ")");
        }
    }

    // forExternalBoundaries rejects non-external boundaries-shaped input.
    @Test
    public void testForExternalBoundariesRejectsTooFewRanges() {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        long oldTabletId = physicalPartition.getLatestBaseIndex().getTablets().get(0).getId();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SplitTabletJobFactory.forExternalBoundaries(db, table,
                        Map.of(oldTabletId, List.of(tabletRange(0, 100)))));
    }

    @Test
    public void testForExternalBoundariesRejectsUnknownTablet() {
        Assertions.assertThrows(StarRocksException.class,
                () -> SplitTabletJobFactory.forExternalBoundaries(db, table,
                        Map.of(/*oldTabletId=*/Long.MAX_VALUE, List.of(tabletRange(0, 100), tabletRange(100, 200)))));
    }

    // external boundaries must respect the tablet_reshard_max_split_count cap that the
    // data-driven path enforces via TabletReshardUtils.calcSplitCount.
    @Test
    public void testForExternalBoundariesRejectsTooManyRanges() {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        long oldTabletId = physicalPartition.getLatestBaseIndex().getTablets().get(0).getId();

        int oversize = Config.tablet_reshard_max_split_count + 1;
        List<TabletRange> tooMany = new ArrayList<>(oversize);
        for (int i = 0; i < oversize; i++) {
            tooMany.add(tabletRange(i * 100, (i + 1) * 100));
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SplitTabletJobFactory.forExternalBoundaries(db, table, Map.of(oldTabletId, tooMany)));
    }

    // Installs a MockUp on MockLakeService that intercepts both publishVersion and
    // aggregatePublishVersion, populating the response's tabletRanges by applying
    // {@code rangeBuilder} to every ReshardingTabletInfoPB in the request(s). The
    // status is always OK; null reshardingTabletInfos are short-circuited.
    private void installLakeServiceMock(BiConsumer<ReshardingTabletInfoPB, Map<Long, TabletRangePB>> rangeBuilder) {
        new MockUp<MockLakeService>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                PublishVersionResponse response = new PublishVersionResponse();
                response.status = new StatusPB();
                response.status.statusCode = TStatusCode.OK.getValue();
                if (request.reshardingTabletInfos == null) {
                    return CompletableFuture.completedFuture(response);
                }
                response.tabletRanges = new HashMap<>();
                for (ReshardingTabletInfoPB info : request.reshardingTabletInfos) {
                    rangeBuilder.accept(info, response.tabletRanges);
                }
                return CompletableFuture.completedFuture(response);
            }

            @Mock
            public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
                PublishVersionResponse response = new PublishVersionResponse();
                response.status = new StatusPB();
                response.status.statusCode = TStatusCode.OK.getValue();
                response.tabletRanges = new HashMap<>();
                for (PublishVersionRequest publishRequest : request.publishReqs) {
                    if (publishRequest.reshardingTabletInfos == null) {
                        continue;
                    }
                    for (ReshardingTabletInfoPB info : publishRequest.reshardingTabletInfos) {
                        rangeBuilder.accept(info, response.tabletRanges);
                    }
                }
                return CompletableFuture.completedFuture(response);
            }
        };
    }

    // Mock helper: data-driven BE response. Splitting tablets get evenly-distributed
    // sub-ranges of the old tablet; identical tablets retain the old tablet's range.
    private void addDataDrivenRanges(ReshardingTabletInfoPB info, Map<Long, TabletRangePB> out) {
        if (info.splittingTabletInfo != null) {
            Long oldTabletId = info.splittingTabletInfo.oldTabletId;
            List<Long> newTabletIds = info.splittingTabletInfo.newTabletIds;
            out.putAll(createSplitTabletRanges(oldTabletId, newTabletIds));
        }
        if (info.identicalTabletInfo != null) {
            Long oldTabletId = info.identicalTabletInfo.oldTabletId;
            Long newTabletId = info.identicalTabletInfo.newTabletId;
            out.put(newTabletId, createTabletRangePBFromOldTablet(oldTabletId));
        }
    }

    // Mock helper: BE-side fallback to identical tablet. Only the first new tablet
    // is published, with the old tablet's range (no actual split happened).
    private void addFallbackToIdenticalRanges(ReshardingTabletInfoPB info, Map<Long, TabletRangePB> out) {
        if (info.splittingTabletInfo == null) {
            return;
        }
        Long oldTabletId = info.splittingTabletInfo.oldTabletId;
        Long firstTabletId = info.splittingTabletInfo.newTabletIds.get(0);
        out.put(firstTabletId, createTabletRangePBFromOldTablet(oldTabletId));
    }

    // Mock helper: BE echo of the FE-supplied external boundaries ranges, plus identical-tablet
    // siblings retaining their original ranges. This matches the BE's success
    // contract — newTabletRanges honored verbatim into K new tablets.
    private void addEchoedRanges(ReshardingTabletInfoPB info, Map<Long, TabletRangePB> out) {
        if (info.splittingTabletInfo != null) {
            List<Long> newTabletIds = info.splittingTabletInfo.newTabletIds;
            List<TabletRangePB> ranges = info.splittingTabletInfo.newTabletRanges;
            if (ranges != null && ranges.size() == newTabletIds.size()) {
                for (int i = 0; i < newTabletIds.size(); i++) {
                    out.put(newTabletIds.get(i), ranges.get(i));
                }
            } else {
                // Data-driven path fallback (no external boundaries ranges on the wire).
                out.putAll(createSplitTabletRanges(info.splittingTabletInfo.oldTabletId, newTabletIds));
            }
        }
        if (info.identicalTabletInfo != null) {
            Long oldId = info.identicalTabletInfo.oldTabletId;
            Long newId = info.identicalTabletInfo.newTabletId;
            out.put(newId, createTabletRangePBFromOldTablet(oldId));
        }
    }

    private static SplittingTablet findSplittingTablet(TabletReshardJob job, long oldTabletId) {
        SplitTabletJob splitJob = (SplitTabletJob) job;
        for (ReshardingPhysicalPartition rpp : splitJob.getReshardingPhysicalPartitions().values()) {
            for (ReshardingMaterializedIndex rmi : rpp.getReshardingIndexes().values()) {
                for (ReshardingTablet rt : rmi.getReshardingTablets()) {
                    SplittingTablet st = rt.getSplittingTablet();
                    if (st != null && st.getOldTabletId() == oldTabletId) {
                        return st;
                    }
                }
            }
        }
        throw new AssertionError("SplittingTablet for old tablet " + oldTabletId + " not found");
    }

    private static TabletRange tabletRange(int lowerValue, int upperValue) {
        return new TabletRange(Range.of(createTuple(lowerValue), createTuple(upperValue), true, false));
    }

    // First range of a external boundaries list that splits a Range.all() parent: (-inf, upperValue).
    private static TabletRange tabletRangeUpperOnly(int upperValue) {
        return new TabletRange(Range.lt(createTuple(upperValue)));
    }

    // Last range of a external boundaries list that splits a Range.all() parent: [lowerValue, +inf).
    private static TabletRange tabletRangeLowerOnly(int lowerValue) {
        return new TabletRange(Range.ge(createTuple(lowerValue)));
    }

    private static Tuple createTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    /**
     * Creates a TabletRangePB with proper bounds for contiguous ranges.
     * The ranges will be: [lowerValue, upperValue)
     */
    private static TabletRangePB createTabletRangePB(int lowerValue, int upperValue) {
        Range<Tuple> range = Range.of(createTuple(lowerValue), createTuple(upperValue), true, false);
        return new TabletRange(range).toProto();
    }

    /**
     * Creates a TabletRangePB from an existing tablet's range by looking up the
     * tablet in the table.
     * This is used when fallback to identical tablet - the new tablet should have
     * the same range as the old tablet.
     */
    private TabletRangePB createTabletRangePBFromOldTablet(long oldTabletId) {
        // Find the tablet with the given ID from the table
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            for (MaterializedIndex index : partition
                    .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    if (tablet.getId() == oldTabletId) {
                        TabletRange tabletRange = tablet.getRange();
                        return tabletRange == null ? new TabletRange().toProto() : tabletRange.toProto();
                    }
                }
            }
        }
        // Tablet not found, return unbounded range as fallback
        return new TabletRange().toProto();
    }

    /**
     * Creates contiguous TabletRangePB objects for a list of tablet IDs.
     * Returns a map from tabletId to TabletRangePB with proper contiguous ranges.
     */
    private static Map<Long, TabletRangePB> createContiguousTabletRanges(List<Long> tabletIds) {
        Map<Long, TabletRangePB> result = new HashMap<>();
        int baseValue = 0;
        int step = 100;
        for (int i = 0; i < tabletIds.size(); i++) {
            int lowerValue = baseValue + i * step;
            int upperValue = baseValue + (i + 1) * step;
            result.put(tabletIds.get(i), createTabletRangePB(lowerValue, upperValue));
        }
        return result;
    }

    /**
     * Creates split tablet ranges by dividing the old tablet's range into
     * contiguous sub-ranges.
     * This simulates what BE does when splitting a tablet.
     */
    private Map<Long, TabletRangePB> createSplitTabletRanges(long oldTabletId, List<Long> newTabletIds) {
        Map<Long, TabletRangePB> result = new HashMap<>();
        TabletRangePB oldRange = createTabletRangePBFromOldTablet(oldTabletId);

        // If old tablet has unbounded range (Range.all()), create arbitrary contiguous
        // ranges
        if (oldRange.lowerBound == null && oldRange.upperBound == null) {
            int step = 100;
            for (int i = 0; i < newTabletIds.size(); i++) {
                int lowerValue = i * step;
                int upperValue = (i + 1) * step;
                result.put(newTabletIds.get(i), createTabletRangePB(lowerValue, upperValue));
            }
        } else {
            // For bounded ranges, split evenly (simplified for test)
            // This is a simplified version - in reality, BE would split based on data
            // distribution
            int lowerValue = 0;
            int upperValue = 0;
            if (oldRange.lowerBound != null && oldRange.lowerBound.values != null
                    && !oldRange.lowerBound.values.isEmpty()) {
                lowerValue = Integer.parseInt(oldRange.lowerBound.values.get(0).value);
            }
            if (oldRange.upperBound != null && oldRange.upperBound.values != null
                    && !oldRange.upperBound.values.isEmpty()) {
                upperValue = Integer.parseInt(oldRange.upperBound.values.get(0).value);
            }
            int rangeSize = upperValue - lowerValue;
            int step = rangeSize / newTabletIds.size();
            for (int i = 0; i < newTabletIds.size(); i++) {
                int subLower = lowerValue + i * step;
                int subUpper = (i == newTabletIds.size() - 1) ? upperValue : lowerValue + (i + 1) * step;
                result.put(newTabletIds.get(i), createTabletRangePB(subLower, subUpper));
            }
        }
        return result;
    }

    @Test
    public void testRunCleaningCancelsPreviousCompactions() throws Exception {
        SplitTabletJob splitJob = (SplitTabletJob) createTabletReshardJob();
        splitJob.setJobState(TabletReshardJob.JobState.CLEANING);
        splitJob.endTransactionId = 5000L;

        Set<Long> ignoredCompactionTxnIds = Set.of(7L, 8L);
        splitJob.addCleanupExcludedTransactionId(42L);
        AtomicReference<Set<Long>> includePartitionIdsArg = new AtomicReference<>();
        AtomicReference<Set<Long>> excludeTxnIdsArg = new AtomicReference<>();
        new MockUp<CompactionMgr>() {
            @Mock
            public Set<Long> cancelPreviousCompactions(long endTransactionId, long dbId, long tableId,
                    Set<Long> includePartitionIds) {
                includePartitionIdsArg.set(includePartitionIds);
                return ignoredCompactionTxnIds;
            }
        };
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIds,
                    Set<Long> excludeTransactionIds) {
                excludeTxnIdsArg.set(excludeTransactionIds);
                return false;
            }
        };

        // The cleaning phase cancels the previous compactions on the resharded partitions and forwards
        // the returned ignored txn ids to the wait; while the wait is unsatisfied the job stays CLEANING.
        splitJob.runCleaningJob();
        Assertions.assertEquals(splitJob.getReshardingPhysicalPartitions().keySet(), includePartitionIdsArg.get());
        Assertions.assertEquals(Set.of(7L, 8L, 42L), excludeTxnIdsArg.get());
        Assertions.assertEquals(TabletReshardJob.JobState.CLEANING, splitJob.getJobState());

        // Once the pre-split caller stops waiting, its transaction may start writing at any moment,
        // so the next cleaning cycle must wait for it again.
        splitJob.clearCleanupExcludedTransactionIds();
        splitJob.runCleaningJob();
        Assertions.assertEquals(ignoredCompactionTxnIds, excludeTxnIdsArg.get());
        Assertions.assertEquals(TabletReshardJob.JobState.CLEANING, splitJob.getJobState());
    }

    @Test
    public void testCleaningClearsPlacementPreferenceBeforeFinishing() throws Exception {
        SplitTabletJob splitJob = (SplitTabletJob) createTabletReshardJob();
        splitJob.init();   // reserves the table: NORMAL -> TABLET_RESHARD
        try {
            splitJob.setJobState(TabletReshardJob.JobState.CLEANING);
            splitJob.endTransactionId = 5000L;

            new MockUp<CompactionMgr>() {
                @Mock
                public Set<Long> cancelPreviousCompactions(long endTransactionId, long dbId, long tableId,
                        Set<Long> includePartitionIds) {
                    return Set.of();
                }
            };
            new MockUp<GlobalTransactionMgr>() {
                @Mock
                public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId,
                        List<Long> tableIds, Set<Long> excludeTransactionIds) {
                    return true;
                }
            };

            AtomicReference<List<List<Long>>> cleared = new AtomicReference<>();
            AtomicReference<TabletReshardJob.JobState> stateAtCall = new AtomicReference<>();
            AtomicInteger calls = new AtomicInteger();
            new MockUp<StarOSAgent>() {
                @Mock
                public void clearPlacementPreference(List<List<Long>> preferenceMembers) {
                    // Capture only; assert outside the mock so a binding failure cannot hide a
                    // failed assertion.
                    calls.incrementAndGet();
                    cleared.set(new ArrayList<>(preferenceMembers));
                    stateAtCall.set(splitJob.getJobState());
                }
            };

            List<List<Long>> expected = new ArrayList<>();
            for (ReshardingPhysicalPartition partition : splitJob.getReshardingPhysicalPartitions().values()) {
                for (ReshardingMaterializedIndex index : partition.getReshardingIndexes().values()) {
                    for (ReshardingTablet tablet : index.getReshardingTablets()) {
                        for (long oldId : tablet.getOldTabletIds()) {
                            for (long newId : tablet.getNewTabletIds()) {
                                expected.add(List.of(oldId, newId));
                            }
                        }
                    }
                }
            }
            Assertions.assertFalse(expected.isEmpty(), "test fixture must produce preference members");

            splitJob.runCleaningJob();

            Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, splitJob.getJobState());
            Assertions.assertEquals(1, calls.get(), "the finish path must clear the pin exactly once");
            Assertions.assertEquals(expected, cleared.get());
            Assertions.assertEquals(TabletReshardJob.JobState.CLEANING, stateAtCall.get(),
                    "the pin must be cleared before the job is marked FINISHED");
        } finally {
            if (table.getState() == OlapTable.OlapTableState.TABLET_RESHARD) {
                splitJob.replayAbortedJob();   // restores NORMAL on the shared table fixture
            }
        }
    }

    @Test
    public void testCleaningStillFinishesWhenClearingPlacementPreferenceFails() throws Exception {
        SplitTabletJob splitJob = (SplitTabletJob) createTabletReshardJob();
        splitJob.init();
        try {
            splitJob.setJobState(TabletReshardJob.JobState.CLEANING);
            splitJob.endTransactionId = 5000L;

            new MockUp<CompactionMgr>() {
                @Mock
                public Set<Long> cancelPreviousCompactions(long endTransactionId, long dbId, long tableId,
                        Set<Long> includePartitionIds) {
                    return Set.of();
                }
            };
            new MockUp<GlobalTransactionMgr>() {
                @Mock
                public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId,
                        List<Long> tableIds, Set<Long> excludeTransactionIds) {
                    return true;
                }
            };
            AtomicInteger calls = new AtomicInteger();
            new MockUp<StarOSAgent>() {
                @Mock
                public void clearPlacementPreference(List<List<Long>> preferenceMembers) throws DdlException {
                    calls.incrementAndGet();
                    throw new DdlException("simulated StarOS failure");
                }
            };

            // Best-effort: clearing the pin is an optimization, so a StarOS failure must not keep
            // the job from finishing.
            splitJob.runCleaningJob();

            Assertions.assertEquals(1, calls.get(), "the failing mock must actually have been invoked");
            Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, splitJob.getJobState());
        } finally {
            if (table.getState() == OlapTable.OlapTableState.TABLET_RESHARD) {
                splitJob.replayAbortedJob();
            }
        }
    }

    private TabletReshardJob createTabletReshardJob() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();

        long tabletId = materializedIndex.getTablets().get(0).getId();
        TabletList tabletList = new TabletList(List.of(tabletId));

        Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2");
        SplitTabletClause clause = new SplitTabletClause(null, tabletList, properties);
        clause.setTabletReshardTargetSize(-2);

        TabletReshardJobFactory factory = new SplitTabletJobFactory(db, table, clause);
        TabletReshardJob tabletReshardJob = factory.createTabletReshardJob();

        Assertions.assertTrue(tabletReshardJob.getParallelTablets() > 0);
        Assertions.assertNotNull(tabletReshardJob.toString());
        Assertions.assertNotNull(tabletReshardJob.getInfo());

        return tabletReshardJob;
    }

    /**
     * createShardsOnStarOS wraps any StarRocksException from the StarOS RPC as a
     * TabletReshardException so the run() catch-and-abort wrapper can fire cleanly.
     */
    @Test
    public void testCreateShardsOnStarOSWrapsStarRocksException() throws Exception {
        SplitTabletJob job = (SplitTabletJob) createTabletReshardJob();

        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForSplit(Map<Long, Long> newToOldShardId,
                                             Map<Long, List<Long>> newShardIdToGroupIds,
                                             FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo,
                                             Map<String, String> properties,
                                             ComputeResource computeResource,
                                             boolean spreadNewShards) throws DdlException {
                throw new DdlException("simulated StarOS failure");
            }
        };

        TabletReshardException thrown = Assertions.assertThrows(TabletReshardException.class,
                job::createShardsOnStarOS);
        Assertions.assertTrue(thrown.getMessage().contains("Failed to create new shards on StarOS"),
                "expected wrap message, got: " + thrown.getMessage());
        Assertions.assertTrue(thrown.getMessage().contains("simulated StarOS failure"),
                "expected original cause message, got: " + thrown.getMessage());
    }

    /**
     * init() reserves the table at admission. If the table is not NORMAL (e.g. it became busy with
     * another DDL between job creation and admission), init() fail-fasts with a StarRocksException
     * instead of letting the job be queued and then forced to abort at execution time.
     */
    @Test
    public void testInitRejectsWhenTableNotNormal() throws Exception {
        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        Assertions.assertNotNull(tabletReshardJob);

        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            Assertions.assertThrows(StarRocksException.class, tabletReshardJob::init);
            // Reservation rejected, the table state is left untouched.
            Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
    }

    /**
     * The factory releases the table lock before the job reaches the manager, so another reshard job
     * can complete in that gap and supersede the very index this job was built against -- the
     * creation-time check cannot see it. init() re-checks under the write lock that reserves the
     * table, so the job is rejected at admission instead of publishing against tablets that are no
     * longer part of the table and then spinning in RUNNING forever.
     */
    @Test
    public void testInitRejectsSupersededIndex() throws Exception {
        TabletReshardJob tabletReshardJob = createTabletReshardJob();

        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex sourceIndex = physicalPartition.getLatestBaseIndex();
        MaterializedIndex supersedingIndex = new MaterializedIndex(
                GlobalStateMgr.getCurrentState().getNextId(), sourceIndex.getMetaId(),
                MaterializedIndex.IndexState.NORMAL, sourceIndex.getShardGroupId());
        supersedingIndex.addTablet(new LakeTablet(GlobalStateMgr.getCurrentState().getNextId()), null, false);
        physicalPartition.addMaterializedIndex(supersedingIndex, true);
        try {
            StarRocksException e = Assertions.assertThrows(StarRocksException.class, tabletReshardJob::init);
            Assertions.assertTrue(e.getMessage().contains("superseded by index " + supersedingIndex.getId()),
                    e.getMessage());
            // Rejected before the reservation, so the table is left available to whoever can still use it.
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        } finally {
            // Tests in this class share a single static table.
            physicalPartition.deleteMaterializedIndexByIndexId(supersedingIndex.getId());
        }
    }

    // A publish failure is always retried, never terminal, so it must be reported WITHOUT being
    // written into the journaled errorMessage: a job whose retry later succeeds would otherwise
    // reach FINISHED still advertising an error, and the next state transition would persist it.
    // getInfo() therefore renders publishFailureReason only while the job is RUNNING and has no
    // terminal errorMessage.
    @Test
    public void testGetInfoReportsRetriedPublishFailureWithoutJournalingIt() {
        Map<Long, ReshardingPhysicalPartition> partitions = new HashMap<>();
        ReshardingPhysicalPartition p1 = new ReshardingPhysicalPartition(1L, new HashMap<>());
        ReshardingPhysicalPartition p2 = new ReshardingPhysicalPartition(2L, new HashMap<>());
        partitions.put(1L, p1);
        partitions.put(2L, p2);
        SplitTabletJob job = new SplitTabletJob(GlobalStateMgr.getCurrentState().getNextId(),
                db.getId(), table.getId(), partitions);
        job.jobState = TabletReshardJob.JobState.RUNNING;

        // healthy: nothing to report
        Assertions.assertEquals("", job.getInfo().getError_message());

        // one partition is retrying a failed publish: surfaced, but errorMessage (the journaled
        // field) stays null, and it keeps being reported for as long as that partition holds the
        // reason -- an IN_PROGRESS retry must not blank the diagnostic after a single tick
        p1.setPublishFailureReason("link rpc channel failed");
        Assertions.assertEquals("publish version failed (retrying): link rpc channel failed",
                job.getInfo().getError_message());
        Assertions.assertEquals("publish version failed (retrying): link rpc channel failed",
                job.getInfo().getError_message());
        Assertions.assertNull(job.errorMessage);

        // that partition recovered while the sibling is still publishing: reporting stops even
        // though not every partition has finished
        p1.setPublishFailureReason(null);
        Assertions.assertEquals("", job.getInfo().getError_message());

        // a failure on any partition is reported
        p2.setPublishFailureReason("no alive node");
        Assertions.assertEquals("publish version failed (retrying): no alive node",
                job.getInfo().getError_message());

        // only RUNNING retries a publish, so a reason left on a partition stops being reported once
        // the job moves on -- this is what keeps a partition dropped mid-job, which runRunningJob
        // skips so that no publish result can clear its reason, from making a finished job advertise
        // a failure that is no longer being retried
        for (TabletReshardJob.JobState state : TabletReshardJob.JobState.values()) {
            job.jobState = state;
            Assertions.assertEquals(state == TabletReshardJob.JobState.RUNNING
                            ? "publish version failed (retrying): no alive node" : "",
                    job.getInfo().getError_message(), "job state " + state);
        }

        // a terminal error always wins over a transient publish failure, in any state
        job.jobState = TabletReshardJob.JobState.RUNNING;
        job.errorMessage = "Table not found";
        Assertions.assertEquals("Table not found", job.getInfo().getError_message());
    }

    // runRunningJob() skips a partition that has been dropped mid-job (DROP PARTITION / TRUNCATE are
    // permitted while the table is in TABLET_RESHARD) without marking the job unfinished, so nothing
    // would ever clear a publish failure reason that partition left behind. Clear it on the skip, so
    // the job does not keep attributing a failure to a partition whose publish is no longer retried.
    @Test
    public void testRunRunningJobClearsPublishFailureReasonOfDroppedPartition() throws Exception {
        installLakeServiceMock(this::addDataDrivenRanges);

        TabletReshardJob tabletReshardJob = createTabletReshardJob();
        tabletReshardJob.init();
        tabletReshardJob.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, tabletReshardJob.getJobState());

        SplitTabletJob splitJob = (SplitTabletJob) tabletReshardJob;
        ReshardingPhysicalPartition droppedPartition = new ReshardingPhysicalPartition(
                GlobalStateMgr.getCurrentState().getNextId(), new HashMap<>());
        droppedPartition.setPublishFailureReason("link rpc channel failed");
        splitJob.getReshardingPhysicalPartitions().put(droppedPartition.getPhysicalPartitionId(), droppedPartition);
        Assertions.assertEquals("publish version failed (retrying): link rpc channel failed",
                splitJob.getInfo().getError_message());

        // The partition id is not in the table, so the publish loop skips it -- and the skip must not
        // leave the reason behind for the finished job to report.
        tabletReshardJob.run();
        Assertions.assertNull(droppedPartition.getPublishFailureReason());
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, tabletReshardJob.getJobState());
        Assertions.assertEquals("", splitJob.getInfo().getError_message());
    }
}
